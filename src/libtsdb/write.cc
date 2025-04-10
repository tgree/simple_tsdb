// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "write.h"
#include "constants.h"
#include <hdr/auto_buf.h>
#include <algorithm>

#define WITH_GZFILEOP
#include <zlib-ng/zlib-ng.h>

tsdb::write_chunk_index::write_chunk_index(const measurement& m,
    size_t npoints, size_t bitmap_offset, size_t data_len, void* data):
        npoints(npoints),
        bitmap_offset(bitmap_offset)
{
    // Build a table of pointers to the timestamps, field bitmaps and field
    // data, and then validate the data length.
    timestamps = (uint64_t*)data;
    auto* data_ptr = (char*)data + npoints*8;
    for (auto& f : m.fields)
    {
        const auto* fti     = &ftinfos[f.type];
        size_t bitmap_words = ceil_div<size_t>(npoints + bitmap_offset,64);
        size_t data_words   = ceil_div<size_t>(npoints*fti->nbytes,8);

        auto* bitmap_ptr = (uint64_t*)data_ptr;
        data_ptr += bitmap_words*8;
        fields.push_back({bitmap_ptr,data_ptr});
        data_ptr += data_words*8;
    }
    size_t expected_len = data_ptr - (char*)data;
    if (data_len != expected_len)
    {
        printf("Expected %zu bytes of data, got %zu bytes.\n",
               expected_len,data_len);
        throw tsdb::incorrect_write_chunk_len_exception(expected_len,data_len);
    }
}

void
tsdb::write_series(series_write_lock& write_lock, write_chunk_index& wci)
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

    // ********************** Overwrite Handling *************************
    // Commiting from the WAL to the main data store should not ever try to do
    // overwrites - that is a programmer error.
    kassert(wci.timestamps[0] > write_lock.time_last);

    // ************ Index search and crash recovery truncation  **************
    // Open the index file, taking a shared lock on it to prevent any other
    // client from deleting from the file.
    // TODO: We hold a write lock.  Is this really necessary?
    futil::file index_fd(write_lock.series_dir,"index",O_RDWR);
    index_fd.flock(LOCK_SH);

    // Open the most recent timestamp file if it exists, and perform validation
    // checking on it if found.
    futil::directory time_ns_dir(write_lock.series_dir,"time_ns");
    futil::directory fields_dir(write_lock.series_dir,"fields");
    futil::directory bitmaps_dir(write_lock.series_dir,"bitmaps");
    futil::file tail_fd;
    field_vector<futil::path> field_file_paths;
    field_vector<futil::file> field_fds;
    field_vector<futil::path> bitmap_file_paths;
    field_vector<futil::file> bitmap_fds;
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
        fields_dir.fsync();
        for (const auto& bitmap_file_path : bitmap_file_paths)
            futil::unlink_if_exists(bitmaps_dir,bitmap_file_path);
        bitmaps_dir.fsync_and_barrier();
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
#if ENABLE_COMPRESSION
    field_vector<futil::path> unlink_field_paths;
#endif
    while (wci.npoints)
    {
        // If we have overflowed the timestamp file, or we have an empty index,
        // we need to grow into a new timestamp file.
        if (!avail_points)
        {
#if ENABLE_COMPRESSION
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
                    kassert((size_t)gz_len == chunk_len);
                    zng_gzflush(gz_file,Z_FINISH);
                    futil::fsync(gz_fd);
                    zng_gzclose(gz_file);
                    printf("Done.\n");
                }
            }
#endif

            // Figure out what to name the new files.
            std::string time_data_str = std::to_string(wci.timestamps[0]);

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
            fields_dir.fsync();
            bitmaps_dir.fsync();

            // Create the new timestamp file.  As when first creating the
            // series, it is possible that someone got this far when growing
            // the series but crashed before updating the index file.  Truncate
            // the file if it exists.
            tail_fd.open(time_ns_dir,time_data_str,O_CREAT | O_TRUNC | O_WRONLY,
                         0660);
            time_ns_dir.fsync();
            avail_points = CHUNK_NPOINTS;
            pos = 0;

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
                // to the first entry of the index file.  Although, in the case
                // of a crash during a delete operation before the index has
                // been shifted, we might not be at the first entry.
                write_lock.time_first = wci.timestamps[0];
                write_lock.time_first_fd.lseek(0,SEEK_SET);
                write_lock.time_first_fd.write_all(&write_lock.time_first,8);
                write_lock.time_first_fd.fsync();
            }

            // Barrier before we update the index file.
            tail_fd.fsync_and_barrier();

            // Add the timestamp file to the index.
            memset(&ie,0,sizeof(ie));
            ie.time_ns = wci.timestamps[0];
            strcpy(ie.timestamp_file,time_data_str.c_str());
            index_fd.write_all(&ie,sizeof(ie));
            index_fd.fsync();
        }

        // Compute how many points we can write.
        size_t write_points = MIN(wci.npoints,avail_points);

        // Write out all the field files first.
        size_t timestamp_index = pos / 8;
        for (size_t i=0; i<write_lock.m.fields.size(); ++i)
        {
            const auto* fti = &ftinfos[write_lock.m.fields[i].type];
            size_t nbytes = write_points*fti->nbytes;
            field_fds[i].lseek(timestamp_index*fti->nbytes,SEEK_SET);
            field_fds[i].write_all(wci.fields[i].data_ptr,nbytes);
            field_fds[i].fsync();
            wci.fields[i].data_ptr += nbytes;
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
                if (wci.get_bitmap_bit(i,j))
                    set_bitmap_bit(bitmap,bitmap_index,1);
                ++bitmap_index;
            }
            m.msync();
            bitmap_fds[i].fsync();
        }

        // Update the timestamp file and issue a barrier before updating
        // time_last.
        tail_fd.write_all(wci.timestamps,write_points*sizeof(uint64_t));
        tail_fd.fsync_and_barrier();

        // Finally, update time_last.
        write_lock.time_last = wci.timestamps[write_points - 1];
        write_lock.time_last_fd.lseek(0,SEEK_SET);
        write_lock.time_last_fd.write_all(&write_lock.time_last,
                                              sizeof(uint64_t));
#if ENABLE_COMPRESSION
        if (!unlink_field_paths.empty())
        {
            write_lock.time_last_fd.fsync_and_barrier();
            for (const auto& ufp : unlink_field_paths)
                futil::unlink_if_exists(fields_dir,ufp);
            fields_dir.fsync();

            unlink_field_paths.clear();
        }
#endif

        // Advance to the next set of points.
        wci.bitmap_offset += write_points;
        wci.timestamps    += write_points;
        wci.npoints       -= write_points;
        pos               += write_points*sizeof(uint64_t);
        avail_points      -= write_points;
        if (wci.npoints)
        {
            write_lock.time_last_fd.fsync();
            kassert(avail_points == 0);
            kassert(pos == CHUNK_NPOINTS*sizeof(uint64_t));
        }
    }

    // Fully synchronize everything.
    // TODO: Check if battery is above 25% and just do a barrier instead?
    write_lock.time_last_fd.fsync_and_flush();
}
