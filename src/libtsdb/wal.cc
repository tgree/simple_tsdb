// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "wal.h"
#include "write.h"
#include "tsdb.h"
#include <hdr/auto_buf.h>
#include <hdr/kmath.h>

void
tsdb::write_wal(series_write_lock& write_lock, size_t npoints,
    size_t bitmap_offset, size_t data_len, const void* data)
{
    if (!npoints)
        return;

    // Index whatever it is we are trying to write.
    write_chunk_index wci(write_lock.m,npoints,bitmap_offset,data_len,data);

    // Ensure that timestamps are in strictly-increasing order.
    for (size_t i=1; i<wci.npoints; ++i)
    {
        if (wci.timestamps[i] <= wci.timestamps[i-1])
            throw tsdb::out_of_order_timestamps_exception();
    }

    // Discard any previously-deleted points.
    while (wci.timestamps[0] < write_lock.time_first)
    {
        if (!--wci.npoints)
        {
            printf("100%% previously deleted, abandoning write op.\n");
            return;
        }

        ++wci.bitmap_offset;
        for (size_t i=0; i<write_lock.m.fields.size(); ++i)
        {
            const auto* fti = &ftinfos[write_lock.m.fields[i].type];
            wci.fields[i].data_ptr += fti->nbytes;
        }
    }

    // Check any overwrites that exist in the main data store.  Note: this is
    // not inconsistent with the previous loop avoce.  It is allowed to have
    // time_first > time_last if we have deleted points that only ever existed
    // in the WAL.
    if (wci.timestamps[0] <= write_lock.time_last)
    {
        // Select all data points from the overlap.
        uint64_t overlap_time_last = MIN(wci.timestamps[wci.npoints-1],
                                         write_lock.time_last);
        select_op_first op(write_lock,"<overwrite>",{},wci.timestamps[0],
                           overlap_time_last,-1);
        kassert(op.npoints);

        // Compare all the points.
        while (op.npoints)
        {
            // Start by doing a memcmp of all the timestamps.
            if (memcmp(wci.timestamps,op.timestamps_begin,op.npoints*8))
            {
                printf("Overwrite mismatch in timestamps.\n");
                throw tsdb::timestamp_overwrite_mismatch_exception();
            }
            wci.timestamps += op.npoints;

            // Do a memcmp on all of the fields.
            for (size_t i=0; i<write_lock.m.fields.size(); ++i)
            {
                const auto* fti = &ftinfos[write_lock.m.fields[i].type];
                size_t len = op.npoints*fti->nbytes;
                if (memcmp(wci.fields[i].data_ptr,op.field_data[i],len))
                {
                    printf("Overwrite mismatch in field %s.\n",
                           write_lock.m.fields[i].name);
                    throw tsdb::field_overwrite_mismatch_exception();
                }
                wci.fields[i].data_ptr += len;
            }

            // Manually check all the bitmaps.
            for (size_t i=0; i<write_lock.m.fields.size(); ++i)
            {
                for (size_t j=0; j<op.npoints; ++j)
                {
                    if (wci.get_bitmap_bit(i,j) != op.get_bitmap_bit(i,j))
                    {
                        printf("Overwrite mismatch in bitmap %s.\n",
                               write_lock.m.fields[i].name);
                        throw tsdb::bitmap_overwrite_mismatch_exception();
                    }
                }
            }
            wci.bitmap_offset += op.npoints;

            // Count how many points we drop.
            wci.npoints -= op.npoints;

            op.next();
        }

        if (!wci.npoints)
        {
            printf("100%% main store overwrite, abandoning write op.\n");
            return;
        }
    }

    // Map the WAL file.
    futil::file wal_fd(write_lock.series_dir,"wal",O_RDWR);
    size_t wal_size = wal_fd.lseek(0,SEEK_END);
    size_t entry_size = sizeof(wal_entry) + write_lock.m.fields.size()*8;
    size_t wal_nentries = wal_size/entry_size;

    // Check for any overwrites in the WAL.
    if (wal_nentries)
    {
        wal_fd.lseek((wal_nentries - 1)*entry_size,SEEK_SET);
        uint64_t wal_time_last = wal_fd.read_u64();

        if (wci.timestamps[0] <= wal_time_last)
        {
            wal_query wq(write_lock,wci.timestamps[0],
                         wci.timestamps[wci.npoints-1]);
            if (wq.nentries > wci.npoints)
            {
                printf("WAL overwrite point count mismatch.\n");
                throw tsdb::timestamp_overwrite_mismatch_exception();
            }

            // Check all the timestamps.
            for (size_t i=0; i<wq.nentries; ++i)
            {
                if (wci.timestamps[i] != wq[i].time_ns)
                {
                    printf("WAL overwrite mismatch in timestamps.\n");
                    throw tsdb::timestamp_overwrite_mismatch_exception();
                }
            }

            // Check each bitmap.
            for (size_t i=0; i<write_lock.m.fields.size(); ++i)
            {
                for (size_t j=0; j<wq.nentries; ++j)
                {
                    if (wci.get_bitmap_bit(i,j) != wq[j].get_bitmap_bit(i))
                    {
                        printf("WAL overwrite mismatch in bitmaps.\n");
                        throw tsdb::timestamp_overwrite_mismatch_exception();
                    }
                }
            }

            // Check each field.
            for (size_t i=0; i<write_lock.m.fields.size(); ++i)
            {
                auto& fti = ftinfos[write_lock.m.fields[i].type];
                auto* field_data = (const void*)wci.fields[i].data_ptr;
                for (size_t j=0; j<wq.nentries; ++j)
                {
                    bool match = false;
                    switch (fti.nbytes)
                    {
                        case 1:
                            match = (((uint8_t*)field_data)[j] ==
                                    wq[j].get_field<uint8_t>(i));
                        break;

                        case 4:
                            match = (((uint32_t*)field_data)[j] ==
                                    wq[j].get_field<uint32_t>(i));
                        break;

                        case 8:
                            match = (((uint64_t*)field_data)[j] ==
                                    wq[j].get_field<uint64_t>(i));
                        break;
                    }
                    if (!match)
                    {
                        printf("WAL overwrite mismatch in fields.\n");
                        throw tsdb::timestamp_overwrite_mismatch_exception();
                    }
                }
            }

            wci.timestamps += wq.nentries;
            for (size_t i=0; i<write_lock.m.fields.size(); ++i)
            {
                const auto* fti = &ftinfos[write_lock.m.fields[i].type];
                size_t len = wq.nentries*fti->nbytes;
                wci.fields[i].data_ptr += len;
            }
            wci.bitmap_offset += wq.nentries;
            wci.npoints -= wq.nentries;
        }

        if (!wci.npoints)
        {
            printf("100%% WAL overwrite, abandoning write op.\n");
            return;
        }
    }

    // Convert the column-based write format into WAL row-based format.
    auto_buf rows_buf(entry_size*wci.npoints);
    wal_entry_iterator rows_iter((wal_entry*)rows_buf.data,entry_size);
    for (size_t i=0; i<wci.npoints; ++i)
    {
        auto& we = *rows_iter++;
        we.time_ns = wci.timestamps[i];
        we.bitmap = 0;
        for (size_t j=0; j<wci.fields.size(); ++j)
        {
            we.bitmap |= (wci.get_bitmap_bit(j,i) << j);
            auto* dp = wci.fields[j].data_ptr;
            switch (ftinfos[write_lock.m.fields[j].type].nbytes)
            {
                case 1:
                    we.fields[j].u8 = ((const uint8_t*)dp)[i];
                break;

                case 4:
                    we.fields[j].u32 = ((const uint32_t*)dp)[i];
                break;

                case 8:
                    we.fields[j].u64 = ((const uint64_t*)dp)[i];
                break;
            }
        }
    }

    // Write the new points.
    // On APFS, writes seem to happen in 16K blocks - but for the final 16K
    // block only enough 4K sectors are written to satisfy the operation.  So,
    // a 3K file writes 4K if you add a byte, a 7K file writes 8K if you add
    // a byte, an 11K file writes 12K if you add a byte and a 15K file writes
    // 16K if you add a byte.  But then, a 19K file only writes 4K if you add a
    // byte!
    //
    // Watching Disk Utility, it took a single sensor (logging only to 1 series)
    // roughly 82 seconds to get tsdbserver up to 1000K, which looks like we
    // are averaging around 12.5K/second.  The throttle period was set to 1
    // second, so that says this should be averaging around 12.5K per small
    // write.  We always need to write at least 4K, and in the worst case we
    // cross two 16K blocks so need to write 20K.  Without doing any real math,
    // 12.5K is around the middle of that range and maybe we also need to
    // update metadata from time to time.
    wal_fd.lseek(wal_nentries*entry_size,SEEK_SET);
    wal_fd.write_all(rows_buf.data,entry_size*wci.npoints);
    wal_fd.fsync_and_flush();
}
