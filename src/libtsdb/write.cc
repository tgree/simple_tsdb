// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "write.h"
#include "database.h"
#include <hdr/auto_buf.h>
#include <algorithm>

#include <zutil/zutil.h>

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
        throw tsdb::incorrect_write_chunk_len_exception(expected_len,data_len);
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

    // Find all the directories of interest and open the index file.
    futil::file index_fd(write_lock.series_dir,"index",O_RDWR);
    futil::directory time_ns_dir(write_lock.series_dir,"time_ns");
    futil::directory fields_dir(write_lock.series_dir,"fields");
    futil::directory bitmaps_dir(write_lock.series_dir,"bitmaps");
    field_vector<futil::directory> field_dirs;
    field_vector<futil::directory> bitmap_dirs;
    for (const auto& field : write_lock.m.fields)
    {
        field_dirs.emplace_back(fields_dir,field.name);
        bitmap_dirs.emplace_back(bitmaps_dir,field.name);
    }

    // Compute some useful constants.
    const size_t nindices = index_fd.lseek(0,SEEK_END) / sizeof(index_entry);
    const size_t chunk_size = write_lock.m.db.root.config.chunk_size;
    const size_t chunk_npoints = chunk_size/8;

    // ************ Index search and crash recovery truncation  **************
    // Open the index file and the most recent timestamp file if it exists, and
    // perform validation checking on it if found.
    //
    // This loop will prune all invalid tail indices from the index file, and
    // will then either open a valid tail file, set pos to the end of the file,
    // compute avail_points and then exit, or complete without finding a valid
    // tail file in which case avail_points will be 0 and pos should be ignored.
    futil::file tail_fd;
    field_vector<futil::file> field_fds;
    field_vector<futil::file> bitmap_fds;
    size_t avail_points = 0;
    index_entry ie;
    off_t pos;
    for (size_t i=0; i<nindices; ++i)
    {
        // Work from the back.
        const size_t index = nindices - i - 1;

        // Open the file pointed to by the last entry in the index.
        index_fd.lseek(index*sizeof(index_entry),SEEK_SET);
        index_fd.read_all(&ie,sizeof(ie));

        // If the index entry comes after time_last, nuke the entry and nuke
        // any partial chunk files.  This could be due to a write operation
        // that was interrupted.
        if (ie.time_ns > write_lock.time_last)
        {
            for (size_t j=0; j<write_lock.m.fields.size(); ++j)
            {
                futil::unlink_if_exists(field_dirs[j],ie.timestamp_file);
                futil::unlink_if_exists(bitmap_dirs[j],ie.timestamp_file);
                field_dirs[j].fsync();
                bitmap_dirs[j].fsync();
            }
            futil::unlink(time_ns_dir,ie.timestamp_file);
            time_ns_dir.fsync_and_barrier();
            index_fd.truncate(index*sizeof(index_entry));
            index_fd.lseek(0,SEEK_END);
            continue;
        }

        // Open the tail file.  The tail file may not exist if a delete
        // operation was interrupted before the index file was shifted.  This
        // would typically happen when we deleted all of the points from a
        // series, since we can only delete from the front and we are working
        // here from the back.
        try
        {
            tail_fd.open(time_ns_dir,ie.timestamp_file,O_RDWR);
        }
        catch (const futil::errno_exception& e)
        {
            if (e.errnov != ENOENT)
                throw;

            kassert(write_lock.time_first > write_lock.time_last);

            for (size_t j=0; j<write_lock.m.fields.size(); ++j)
            {
                futil::unlink_if_exists(field_dirs[j],ie.timestamp_file);
                futil::unlink_if_exists(bitmap_dirs[j],ie.timestamp_file);
                field_dirs[j].fsync();
                bitmap_dirs[j].fsync();
            }
            index_fd.truncate(index*sizeof(index_entry));
            index_fd.lseek(0,SEEK_END);
            continue;
        }

        // We found the tail file pointed to by the index.  This means the tail
        // file must be a valid timestamp file and must be non-empty.
        pos = tail_fd.lseek(0,SEEK_END);
        if (pos < 8)
            throw tsdb::tail_file_invalid_size_exception(pos);
        if ((size_t)pos > chunk_size)
            throw tsdb::tail_file_too_big_exception(pos);
        if (pos % sizeof(uint64_t))
            throw tsdb::tail_file_invalid_size_exception(pos);

        // Read the first and last entries, and leave the file position at the
        // end of the file.
        tail_fd.lseek(0,SEEK_SET);
        const uint64_t tail_fd_first_entry = tail_fd.read_u64();
        tail_fd.lseek((pos & ~7) - 8,SEEK_SET);
        const uint64_t tail_fd_last_entry = tail_fd.read_u64();

        // Validate that the first entry matches the index entry.
        if (tail_fd_first_entry != ie.time_ns)
        {
            throw tsdb::tail_file_invalid_time_first_exception(
                    tail_fd_first_entry,ie.time_ns);
        }

        // A delete operation that crashed early could have attempted to delete
        // this chunk but failed.  A delete operation begins by incrementing
        // time_first, then deletes all the chunk files (possibly for multiple
        // index entries) and then does an atomic shift of the index file.
        if (tail_fd_last_entry < write_lock.time_first)
        {
            tail_fd.close();
            for (size_t j=0; j<write_lock.m.fields.size(); ++j)
            {
                futil::unlink_if_exists(field_dirs[j],ie.timestamp_file);
                futil::unlink_if_exists(bitmap_dirs[j],ie.timestamp_file);
                field_dirs[j].fsync();
                bitmap_dirs[j].fsync();
            }
            futil::unlink(time_ns_dir,ie.timestamp_file);
            time_ns_dir.fsync_and_barrier();
            index_fd.truncate(index*sizeof(index_entry));
            index_fd.lseek(0,SEEK_END);
            continue;
        }

        // Slow path check.  If the time_last file differs from the last index
        // entry file's last stored timestamp then we are recovering from a
        // previous crash.
        if (tail_fd_last_entry != write_lock.time_last)
        {
            // Consistency check - the following predicate should always be
            // held:
            //
            //      time_last <= last indexed timestamp
            //
            // This is because incrementing time_last is the very last step
            // when writing data points.  So, if the value we see stored in
            // the time_last file is larger than the value we found in the
            // index, the series is corrupt.
            if (write_lock.time_last > tail_fd_last_entry)
            {
                throw tsdb::invalid_time_last_exception(tail_fd_last_entry,
                                                        write_lock.time_last);
            }

            // There are extra timestamp entries compared to the value stored
            // in the time_last file.  This means a write_points operation was
            // previously interrupted and we need to clean up the series.
            // We have already verified:
            //
            //      ie.time_ns <= write_lock.time_last
            //
            // And verified:
            //
            //      tail_fd_first_entry == ie.time_ns
            //
            // So:
            //      
            //      tail_fd_first_entry <= write_lock.time_last
            //
            // This means that there must be live data in the timestamp file
            // and at least the first timestamp is a live value.  We need to
            // search the file for the index of the timestamp that has the
            // time_last value.  When found, we should set pos to immediately
            // after that entry, lseek() there, ftruncate() and exit with
            // success.
            {
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
                pos = (const char*)iter - (const char*)iter_first + 8;
            }

            // Truncate the file as a convenience for future self.  We close
            // the mmap() above before truncate() for unittest sanity.
            tail_fd.lseek(pos,SEEK_SET);
            tail_fd.truncate(pos);
        }

        // Fast path and slow path converge here.
        // The tail file is the last live entry in the index file, which has
        // been pruned down to this index entry if necessary.  Since the tail
        // file is live, all field and bitmap files are guaranteed to exist.
        // Since the tail file is the last one in the index, the field files
        // are guaranteed to not be compressed (although dangling compressed
        // copies may exist if a crash occurred during compression - these can
        // be safely ignored).
        for (size_t j=0; j<write_lock.m.fields.size(); ++j)
        {
            field_fds.emplace_back(field_dirs[j],ie.timestamp_file,
                                   O_RDWR);
            bitmap_fds.emplace_back(bitmap_dirs[j],ie.timestamp_file,
                                    O_RDWR);
        }
        avail_points = (chunk_size - pos) / sizeof(uint64_t);
        break;
    }

    // ************************** Write Points *******************************

    // Write points.  The variable pos is the absolute position in the
    // tail_fd timestamp file (if we found one); this should be used to
    // calculate the index for other field sizes.
    field_vector<futil::path> unlink_field_paths;
    while (wci.npoints)
    {
        // If we have overflowed the timestamp file, or we have an empty index,
        // we need to grow into a new timestamp file.
        if (!avail_points)
        {
            // If we have open file descriptors, then we are done with them and
            // they are now full.  Compress them.
            if (!field_fds.empty())
            {
                size_t max_gzipped_size = write_lock.m.db.root.max_gzipped_size;

                auto_buf file_buf(chunk_size);
                auto_buf gzip_buf(max_gzipped_size);
                for (size_t i=0; i<write_lock.m.fields.size(); ++i)
                {
                    const auto* fti = &ftinfos[write_lock.m.fields[i].type];
                    size_t chunk_len = chunk_npoints*fti->nbytes;

                    // Read the source file.
                    std::string src_name(ie.timestamp_file);
                    field_fds[i].lseek(SEEK_SET,0);
                    field_fds[i].read_all(file_buf,chunk_len);
                    unlink_field_paths.emplace_back(
                        futil::path(write_lock.m.fields[i].name,
                                    src_name));

                    // Create the destination file and write it out.
                    write_lock.m.db.root.debugf("Compressing %s...\n",
                                                unlink_field_paths[i].c_str());
                    size_t compressed_len =
                        zutil::gzip_compress(gzip_buf.data,max_gzipped_size,
                                             file_buf.data,chunk_len,
                                             Z_BEST_COMPRESSION);
                    futil::file gz_file(
                        fields_dir,
                        futil::path(write_lock.m.fields[i].name,
                                    src_name + ".gz"),
                        O_CREAT | O_TRUNC | O_RDWR,0660);
                    gz_file.write_all(gzip_buf.data,compressed_len);
                    gz_file.fsync();
                    write_lock.m.db.root.debugf("Done.\n");

                    // Note: The fsync() for the field directory containing the
                    // new .gz file happens below; we only ever compress when
                    // spilling over into a new uncompressed data file and the
                    // directory fsync() will happen after creating that file.

                    // TODO: If destination file was larger than the chunk size,
                    // delete it and just keep the uncompressed version around.
                    // TODO: Make whatever file is left behind read-only.
                    //       The issue is that a crash could leave a file
                    //       behind that we will later try to reopen with
                    //       O_CREAT | O_TRUNC | O_RDWR when we recover.  That
                    //       will fail if the file is marked read-only.
                }
            }

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
                field_fds.emplace_back(field_dirs[i],time_data_str,
                                       O_CREAT | O_TRUNC | O_RDWR,0660);
                field_fds[i].fsync();
                field_dirs[i].fsync();

                bitmap_fds.emplace_back(bitmap_dirs[i],time_data_str,
                                        O_CREAT | O_TRUNC | O_RDWR,0660);
                bitmap_fds[i].truncate(chunk_npoints/8);
                bitmap_fds[i].fsync();
                bitmap_dirs[i].fsync();
            }

            // Create the new timestamp file.  As when first creating the
            // series, it is possible that someone got this far when growing
            // the series but crashed before updating the index file.  Truncate
            // the file if it exists.
            tail_fd.open(time_ns_dir,time_data_str,O_CREAT | O_TRUNC | O_WRONLY,
                         0660);
            time_ns_dir.fsync();
            avail_points = chunk_npoints;
            pos = 0;

            // TODO: If we crash here, there are now dangling, empty timestamp
            // and field/bitmap files.  Their timestamp isn't in the index, so
            // will never be accessed, but they will still exist.  If someone
            // tries to rewrite the timestamp that created this file in the
            // future then we will truncate/reuse them.  However, if the client
            // doesn't try to rewrite this (maybe the client goes away before
            // our database can recover) then it will sit here forever.  We may
            // wish to periodically scan for orphaned files and delete them.

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

            // Add the timestamp file to the index.  The index's tail file now
            // points to an empty timestamp file, which is normally forbidden.
            // However, this index entry isn't covered by time_last yet, so we
            // are okay to have it here while we populate it and a crash will
            // automatically be handled by time_last validation.
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
            auto m = bitmap_fds[i].mmap(0,chunk_npoints/8,
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
        if (!unlink_field_paths.empty())
        {
            write_lock.time_last_fd.fsync_and_barrier();
            for (const auto& ufp : unlink_field_paths)
                futil::unlink_if_exists(fields_dir,ufp);
            for (auto& field_dir : field_dirs)
                field_dir.fsync();

            unlink_field_paths.clear();
        }

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
            kassert((size_t)pos == chunk_npoints*sizeof(uint64_t));
        }
    }

    // Fully synchronize everything.
    // TODO: Check if battery is above 25% and just do a barrier instead?
    write_lock.time_last_fd.fsync_and_flush();
}
