// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "write.h"
#include "tsdb.h"
#include <hdr/auto_buf.h>
#include <algorithm>

#define WITH_GZFILEOP
#include <zlib-ng/zlib-ng.h>

void
tsdb::write_series(series_write_lock& write_lock, size_t npoints,
    size_t bitmap_offset, size_t data_len, const void* data)
{
    // Predicates when writing a series:
    //  1. If the series/time_last file exists then the series is fully
    //     constructed and at least the following paths exist:
    //
    //      series/index
    //      series/time_ns
    //      series/time_first
    //      series/time_last
    //      series/<field>/ for all fields
    //
    //  2. Points stored in time_ns and field data files are valid up to the
    //     timestamp stored in the time_last file.  The time_ns and field data
    //     files may contain partial points past time_last if a write operation
    //     was interrupted.  That is:
    //
    //      time_last <= last indexed timestamp
    //
    //  3. If an entry exists in the index file, then the time_ns and data
    //     files for that entry are guaranteed to exist (but may be empty).
    //
    //  4. There may be random garbage at the end of timestamp and field files
    //     for indices that would correspond to points past time_last.  These
    //     are points that would have been partially written before a crash.
    //     This garbage data will eventually be overwritten if new points come
    //     in, but we will never access it otherwise.

    // ********************** Data Input Validation *************************
    // Build a table of pointers to the timestamps, field bitmaps and field
    // data, and then validate the data length.
    std::vector<const uint64_t*> field_bitmap_ptrs;
    std::vector<const char*> field_data_ptrs;
    const auto* time_data = (const uint64_t*)data;
    auto* data_ptr = (const char*)data + npoints*8;
    for (auto& f : write_lock.m.fields)
    {
        const auto* fti     = &ftinfos[f.type];
        size_t bitmap_words = ceil_div<size_t>(npoints + bitmap_offset,64);
        size_t data_words   = ceil_div<size_t>(npoints*fti->nbytes,8);

        field_bitmap_ptrs.push_back((const uint64_t*)data_ptr);
        data_ptr += bitmap_words*8;
        field_data_ptrs.push_back(data_ptr);
        data_ptr += data_words*8;
    }
    size_t expected_len = data_ptr - (const char*)data;
    if (data_len != expected_len)
    {
        printf("Expected %zu bytes of data, got %zu bytes.\n",
               expected_len,data_len);
        throw tsdb::incorrect_write_chunk_len_exception(expected_len,data_len);
    }

    // Find the first and last time stamps and ensure that the timestamps are
    // in strictly-increasing order.
    uint64_t chunk_first_ns = time_data[0];
    uint64_t chunk_last_ns  = time_data[npoints-1];
    for (size_t i=1; i<npoints; ++i)
    {
        if (time_data[i] <= time_data[i-1])
            throw tsdb::out_of_order_timestamps_exception();
    }

    // Open the index file, taking a shared lock on it to prevent any other
    // client from deleting from the file.
    futil::file index_fd(write_lock.series_dir,"index",O_RDWR | O_SHLOCK);

    // ********************** Overwrite Handling *************************

    // If the timestamps we are appending start before our last stored
    // timestamp, we should validate and discard the overlapping part and then
    // only write the new part.
    size_t n_overlap_points = 0;
    if (chunk_first_ns <= write_lock.time_last)
    {
        // TODO: We should get time_first_stored and silently discard any
        // points before that point in time - those are points that have
        // presumably been deleted and we have no way to verify if they are
        // proper overwrites or not.  This could happen in the following
        // scenario:
        //
        //  1. Client has a store of local points.
        //  2. Client transmits points to server.
        //  3. Server writes points but crashes before transmitting ACK back to
        //     client.
        //  4. Client crashes and goes away.
        //  5. Server is restored, (lots of?) time passes, someone deletes old
        //     points, maybe as part of an automated cleanup script, leaving
        //     some of the client's later points behind but purging the earlier
        //     ones.
        //  6. Client is finally restored and tries to retransmit its entire
        //     store of local points.
        //
        // In step 6, the client will be transmitting points from its local
        // store that precede the server's time_first value.  The client has no
        // way of knowing (without doing a select op which we'd rather avoid
        // for the client) what time_first is on the server.  There's no
        // possible way for the client to even store those old points on the
        // server now since that would be rewriting history which is forbidden,
        // so the options are to either halt the client which now has points it
        // can't write anywhere, or to just discard the very-old points.
        //
        // Select all data points from the overlap.
        uint64_t overlap_time_last = MIN(chunk_last_ns,write_lock.time_last);
        std::vector<std::string> field_names;
        field_names.reserve(write_lock.m.fields.size());
        for (const auto& f : write_lock.m.fields)
            field_names.emplace_back(f.name);
        select_op_first op(write_lock,"<overwrite>",field_names,chunk_first_ns,
                           overlap_time_last,-1);
        kassert(op.npoints);
        
        // Compare all the points.
        for (;;)
        {
            // Start by doing a memcmp of all the timestamps.
            if (memcmp(time_data,op.timestamp_data,op.npoints*8))
            {
                printf("Overwrite mismatch in timestamps.\n");
                throw tsdb::timestamp_overwrite_mismatch_exception();
            }
            time_data += op.npoints;

            // Do a memcmp on all of the fields.
            for (size_t i=0; i<write_lock.m.fields.size(); ++i)
            {
                const auto* fti = &ftinfos[write_lock.m.fields[i].type];
                size_t len = op.npoints*fti->nbytes;
                if (memcmp(field_data_ptrs[i],op.field_data[i],len))
                {
                    printf("Overwrite mismatch in field %s.\n",
                           write_lock.m.fields[i].name);
                    throw tsdb::field_overwrite_mismatch_exception();
                }
                field_data_ptrs[i] += len;
            }

            // Manually check all the bitmaps.
            for (size_t i=0; i<write_lock.m.fields.size(); ++i)
            {
                for (size_t j=0; j<op.npoints; ++j)
                {
                    bool new_bitmap = get_bitmap_bit(
                        field_bitmap_ptrs[i],
                        bitmap_offset + n_overlap_points + j);
                    if (new_bitmap != op.get_bitmap_bit(i,j))
                    {
                        printf("Overwrite mismatch in bitmap %s.\n",
                               write_lock.m.fields[i].name);
                        throw tsdb::bitmap_overwrite_mismatch_exception();
                    }
                }
            }

            // Count how many points we drop.
            n_overlap_points += op.npoints;

            if (op.is_last)
                break;

            op.advance();
        }

        npoints -= n_overlap_points;
        if (!npoints)
        {
            printf("100%% overwrite, abandoning write op.\n");
            return;
        }
        else
            printf("Dropping %zu overlapping points.\n",n_overlap_points);

        // Update chunk_first_ns, although we don't use it again.
        chunk_first_ns = time_data[0];
    }

    // ************ Index search and crash recovery truncation  **************

    // Open the most recent timestamp file if it exists, and perform validation
    // checking on it if found.
    futil::directory time_ns_dir(write_lock.series_dir,"time_ns");
    futil::directory fields_dir(write_lock.series_dir,"fields");
    futil::directory bitmaps_dir(write_lock.series_dir,"bitmaps");
    futil::file tail_fd;
    fixed_vector<futil::path> field_file_paths(write_lock.m.fields.size());
    fixed_vector<futil::file> field_fds(write_lock.m.fields.size());
    fixed_vector<futil::path> bitmap_file_paths(write_lock.m.fields.size());
    fixed_vector<futil::file> bitmap_fds(write_lock.m.fields.size());
    off_t index_len = index_fd.lseek(0,SEEK_END);
    size_t nindices = index_len / sizeof(index_entry);
    size_t avail_points = 0;
    index_entry ie;
    off_t pos;
    for (size_t i=0; i<nindices; ++i)
    {
        // Work from the back.
        size_t index = nindices - i - 1;

        // Open the file pointed to by the last entry in the index.
        index_fd.lseek(index*sizeof(index_entry),SEEK_SET);
        index_fd.read_all(&ie,sizeof(ie));

        // Generate all the paths
        field_file_paths.clear();
        bitmap_file_paths.clear();
        for (const auto& field : write_lock.m.fields)
        {
            field_file_paths.emplace_back(field.name,ie.timestamp_file);
            bitmap_file_paths.emplace_back(field.name,ie.timestamp_file);
        }

        // Open the tail file.
        tail_fd.open(time_ns_dir,ie.timestamp_file,O_RDWR);

        // Validate the file size.
        pos = tail_fd.lseek(0,SEEK_END);
        if (pos > CHUNK_FILE_SIZE)
            throw tsdb::tail_file_too_big_exception(pos);
        if (pos % sizeof(uint64_t))
            throw tsdb::tail_file_invalid_size_exception(pos);

        // For a non-empty timestamp file, try to find the time_last entry so
        // we can append.
        if (pos >= 8)
        {
            // Read the last entry, and leave the file position at the end of
            // the file.
            uint64_t tail_fd_first_entry;
            uint64_t tail_fd_last_entry;
            tail_fd.lseek(0,SEEK_SET);
            tail_fd.read_all(&tail_fd_first_entry,sizeof(uint64_t));
            tail_fd.lseek((pos & ~7) - 8,SEEK_SET);
            tail_fd.read_all(&tail_fd_last_entry,sizeof(uint64_t));

            // Fast path - if the last entry in the last timestamp file matches
            // the time_last file, then the series is consistent and we can
            // break out to writing points.
            if (tail_fd_last_entry == write_lock.time_last)
            {
                // Open all of the field and bitmap files with the same name as
                // the timestamp file.  They are guaranteed to exist and should
                // not be compressed.
                for (size_t j=0; j<write_lock.m.fields.size(); ++j)
                {
                    field_fds.emplace_back(fields_dir,field_file_paths[j],
                                           O_RDWR);
                    bitmap_fds.emplace_back(bitmaps_dir,bitmap_file_paths[j],
                                            O_RDWR);
                }
                avail_points = (CHUNK_FILE_SIZE - pos) / sizeof(uint64_t);
                break;
            }

            // Slow path.  The time_last file and the last timestamp stored in
            // the file pointed to by the last index entry differ.  This
            // indicates that we are recovering from a previous crash.

            // Consistency check - the following predicate should always be
            // held:
            //
            //      time_last <= last indexed timestamp
            //
            // This is because incrementing time_last is the very last step
            // when writing data points.  So, if the value we see stored in
            // the time_last file is larger than the value we found in the
            // index, the series is corrupt.
            if (tail_fd_last_entry < write_lock.time_last)
            {
                throw tsdb::invalid_time_last_exception(tail_fd_last_entry,
                                                        write_lock.time_last);
            }

            // There are extra timestamp entries compared to the value stored
            // in the time_last file.  This means a write_points operation was
            // previously interrupted and we need to clean up the series.
            // First, check if there is any live data at all in this timestamp
            // file.
            if (tail_fd_first_entry <= write_lock.time_last)
            {
                // We have:
                //
                //  tail_fd_first_entry <= time_last_stored <=
                //      tail_fd_last_entry
                //
                // so, the first timestamp in this file is a live value.  We
                // need to search the file for the index of the timestamp that
                // has the time_last value.  When found, we should set pos to
                // immediately after that entry, lseek() there, ftruncate() and
                // exit with success.
                auto m = tail_fd.mmap(0,pos,PROT_READ,MAP_SHARED,0);
                const auto* iter_first = &((const uint64_t*)m.addr)[0];
                const auto* iter_last = &((const uint64_t*)m.addr)[pos/8];
                const auto iter = std::lower_bound(iter_first,iter_last,
                                                   write_lock.time_last);

                // Sanity check: we already know that some position in this
                // file should satisfy the lower_bound criteria, so
                // something is massively broken if this fails.
                kassert(iter < iter_last);

                // If we don't have a match, the series is corrupt.
                if (*iter != write_lock.time_last)
                {
                    throw tsdb::invalid_time_last_exception(
                        *iter,write_lock.time_last);
                }

                // We have found the time_last value in the timestamp file!
                // Truncate the file as a convenience for future self and exit
                // with success.
                pos = (const char*)iter - (const char*)iter_first + 8;
                tail_fd.lseek(pos,SEEK_SET);
                tail_fd.truncate(pos);

                // Open all of the field and bitmap files with the same name as
                // the timestamp file.  They are guaranteed to exist and should
                // not be compressed.
                for (size_t j=0; j<write_lock.m.fields.size(); ++j)
                {
                    field_fds.emplace_back(fields_dir,field_file_paths[j],
                                           O_RDWR);
                    bitmap_fds.emplace_back(bitmaps_dir,bitmap_file_paths[j],
                                            O_RDWR);
                }
                avail_points = (CHUNK_FILE_SIZE - pos) / sizeof(uint64_t);
                break;
            }

            // There are timestamps in the file, but none of them are live
            // because all of them are after the stored time_last value.  We
            // just discard this file completely.
        }

        // Slow path - the index entry points to an empty file, or we
        // determined that it is entirely full of timestamp values that haven't
        // gone live yet.  Delete the file and remove the index entry.
        for (const auto& field_file_path : field_file_paths)
            futil::unlink_if_exists(fields_dir,field_file_path);
        for (const auto& bitmap_file_path : bitmap_file_paths)
            futil::unlink_if_exists(bitmaps_dir,bitmap_file_path);
        // TODO: Probably need to sync fields_dir and bitmaps_dir.
        tail_fd.fcntl(F_BARRIERFSYNC);
        tail_fd.close();
        futil::unlink(time_ns_dir,ie.timestamp_file);
        index_fd.flock(LOCK_EX);
        index_fd.truncate(index*sizeof(index_entry));
        index_fd.flock(LOCK_SH);
    }

    // ************************** Write Points *******************************

    // Write points.  The variable pos is the absolute position in the
    // timestamp file; this should be used to calculate the index for other
    // field sizes.
    size_t rem_points = npoints;
    size_t src_bitmap_offset = bitmap_offset + n_overlap_points;
    fixed_vector<futil::path> unlink_field_paths(write_lock.m.fields.size());
    while (rem_points)
    {
        // If we have overflowed the timestamp file, or we have an empty index,
        // we need to grow into a new timestamp file.
        if (!avail_points)
        {
            // If we have open file descriptors, then we are done with them and
            // they are now full.  Compress them.
            if (!field_fds.empty())
            {
                auto_buf file_buf(CHUNK_FILE_SIZE);
                for (size_t i=0; i<write_lock.m.fields.size(); ++i)
                {
                    const auto* fti = &ftinfos[write_lock.m.fields[i].type];
                    size_t chunk_len = CHUNK_NPOINTS*fti->nbytes;

                    // Read the source file.
                    std::string src_name(ie.timestamp_file);
                    field_fds[i].lseek(SEEK_SET,0);
                    field_fds[i].read_all(file_buf,chunk_len);
                    unlink_field_paths.emplace_back(
                        futil::path(write_lock.m.fields[i].name,
                                    src_name));

                    // Create the destination file and write it out.
                    printf("Compressing %s...\n",unlink_field_paths[i].c_str());
                    int gz_fd = futil::openat(
                        fields_dir,
                        futil::path(write_lock.m.fields[i].name,
                                    src_name + ".gz"),
                        O_CREAT | O_TRUNC | O_RDWR,0660);
                    gzFile gz_file = zng_gzdopen(gz_fd,"wb");
                    int32_t gz_len = zng_gzwrite(gz_file,file_buf,chunk_len);
                    kassert(gz_len == chunk_len);
                    zng_gzflush(gz_file,Z_FINISH);
                    futil::fsync(gz_fd);
                    zng_gzclose(gz_file);
                    printf("Done.\n");
                }
            }

            // Figure out what to name the new files.
            std::string time_data_str = std::to_string(time_data[0]);

            // Create and open all new field and bitmap files.  We fully-
            // populate the bitmap files with zeroes even though they don't
            // have any data yet.  This barely wastes any space and makes
            // bitmap accounting via mmap() much simpler.
            field_fds.clear();
            bitmap_fds.clear();
            for (size_t i=0; i<write_lock.m.fields.size(); ++i)
            {
                field_fds.emplace_back(fields_dir,
                                       futil::path(write_lock.m.fields[i].name,
                                                   time_data_str),
                                       O_CREAT | O_TRUNC | O_RDWR,0660);
                field_fds[i].fsync();

                bitmap_fds.emplace_back(bitmaps_dir,
                                        futil::path(write_lock.m.fields[i].name,
                                                    time_data_str),
                                        O_CREAT | O_TRUNC | O_RDWR,0660);
                bitmap_fds[i].truncate(CHUNK_NPOINTS/8);
                bitmap_fds[i].fsync();
            }
            // TODO: fsync() fields_dir.
            // TODO: fsync() bitmaps_dir.

            // Create the new timestamp file.  As when first creating the
            // series, it is possible that someone got this far when growing
            // the series but crashed before updating the index file.  Truncate
            // the file if it exists.
            tail_fd.open(time_ns_dir,time_data_str,O_CREAT | O_TRUNC | O_WRONLY,
                         0660);
            avail_points = CHUNK_NPOINTS;
            pos = 0;
            // TODO: fsync() time_ns_dir.

            // TODO: If we crash here, there is now a dangling, empty timestamp
            // file.  It isn't in the index, so will never be accessed, but it
            // will still exist.  If someone tries to rewrite the timestamp
            // that created this file in the future then we will truncate/reuse
            // it.  However, if the client doesn't try to rewrite this (maybe
            // the client goes away before our database can recover) then it
            // will sit here forever.  We may wish to periodically scan for
            // orphaned files and delete them.

            // If the series is empty (time_first > lime_last), then we also
            // need to make time_first point at the start of this chunk.
            if (write_lock.time_first > write_lock.time_last)
            {
                // TODO: Double check that in this case we are also writing
                // to the first entry of the index file.
                write_lock.time_first = time_data[0];
                write_lock.time_first_fd.lseek(0,SEEK_SET);
                write_lock.time_first_fd.write_all(&write_lock.time_first,8);
                write_lock.time_first_fd.fsync();
            }

            // Barrier before we update the index file.
            tail_fd.fcntl(F_BARRIERFSYNC);

            // Add the timestamp file to the index.
            memset(&ie,0,sizeof(ie));
            ie.time_ns = time_data[0];
            strcpy(ie.timestamp_file,time_data_str.c_str());
            index_fd.write_all(&ie,sizeof(ie));
            index_fd.fsync();
        }

        // Compute how many points we can write.
        size_t write_points = MIN(rem_points,avail_points);

        // Write out all the field files first.
        size_t timestamp_index = pos / 8;
        for (size_t i=0; i<write_lock.m.fields.size(); ++i)
        {
            const auto* fti = &ftinfos[write_lock.m.fields[i].type];
            size_t nbytes = write_points*fti->nbytes;
            field_fds[i].lseek(timestamp_index*fti->nbytes,SEEK_SET);
            field_fds[i].write_all(field_data_ptrs[i],nbytes);
            field_fds[i].fsync();
            field_data_ptrs[i] += nbytes;
        }

        // Update all the bitmap files.
        for (size_t i=0; i<write_lock.m.fields.size(); ++i)
        {
            auto m = bitmap_fds[i].mmap(0,CHUNK_NPOINTS/8,
                                        PROT_READ | PROT_WRITE,MAP_SHARED,0);
            auto* bitmap = (uint64_t*)m.addr;
            size_t bitmap_index = timestamp_index;
            for (size_t j=0; j<write_points; ++j)
            {
                if (get_bitmap_bit(field_bitmap_ptrs[i],src_bitmap_offset + j))
                    set_bitmap_bit(bitmap,bitmap_index,1);
                ++bitmap_index;
            }
            m.msync();
            bitmap_fds[i].fsync();
        }

        // Update the timestamp file and issue a barrier before updating
        // time_last.
        tail_fd.write_all(time_data,write_points*sizeof(uint64_t));
        tail_fd.fcntl(F_BARRIERFSYNC);

        // Finally, update time_last.
        write_lock.time_last = time_data[write_points - 1];
        write_lock.time_last_fd.lseek(0,SEEK_SET);
        write_lock.time_last_fd.write_all(&write_lock.time_last,
                                              sizeof(uint64_t));
        if (!unlink_field_paths.empty())
        {
            write_lock.time_last_fd.fcntl(F_BARRIERFSYNC);
            for (const auto& ufp : unlink_field_paths)
                futil::unlink_if_exists(fields_dir,ufp);

            unlink_field_paths.clear();

            // TODO: fsync() fields dir.
        }
        else
            write_lock.time_last_fd.fsync();

        // Advance to the next set of points.
        time_data         += write_points;
        rem_points        -= write_points;
        pos               += write_points*sizeof(uint64_t);
        avail_points      -= write_points;
        src_bitmap_offset += write_points;
        if (rem_points)
        {
            kassert(avail_points == 0);
            kassert(pos == CHUNK_NPOINTS*sizeof(uint64_t));
        }
    }

    // Fully synchronize everything.
    // TODO: Check if battery is above 25% and just do a barrier instead?
    write_lock.time_last_fd.fcntl(F_FULLFSYNC);
}
