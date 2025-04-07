// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "wal.h"
#include "write.h"
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

    // TODO: Overwrite handling.
    kassert(wci.timestamps[0] > write_lock.time_last);

    // Map the WAL file.
    futil::file wal_fd(write_lock.series_dir,"wal",O_RDWR);
    size_t wal_size = wal_fd.lseek(0,SEEK_END);
    size_t entry_size = sizeof(wal_entry) + write_lock.m.fields.size()*8;
    size_t wal_nentries = wal_size/entry_size;

    // TODO: Overwrite handling.
    if (wal_nentries)
    {
        wal_fd.lseek((wal_nentries - 1)*entry_size,SEEK_SET);
        uint64_t wal_time_last = wal_fd.read_u64();
        kassert(wci.timestamps[0] > wal_time_last);
    }

    // Convert the column-based write format into WAL row-based format.
    auto_buf rows_buf(entry_size*npoints);
    wal_entry_iterator rows_iter((wal_entry*)rows_buf.data,entry_size);
    for (size_t i=0; i<npoints; ++i)
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
    wal_fd.write_all(rows_buf.data,entry_size*npoints);
    wal_fd.fsync_and_flush();
}
