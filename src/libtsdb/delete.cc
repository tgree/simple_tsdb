// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "delete.h"
#include "measurement.h"
#include "series.h"
#include <algorithm>

void
tsdb::delete_points(const measurement& m, const futil::path& series, uint64_t t)
{
    // We need to find the first valid timestamp > t.  This becomes the new
    // value for time_first and then we drop all points before it.  If no such
    // timestamp exists, then time_first becomes time_last + 1 and we drop
    // everything except the last chunk, which only drop if it is full.  This
    // all has to be done while holding the time_first lock.
    futil::directory series_dir(m.dir,series);
    futil::directory time_ns_dir(series_dir,"time_ns");

    // We need to get time_first and time_last.  We lock time_first so that
    // nobody else tries to access the series while we are adjusting things.
    futil::file time_first_fd(series_dir,"time_first",O_RDWR | O_EXLOCK);
    futil::file time_last_fd(series_dir,"time_last",O_RDONLY);
    uint64_t time_first = time_first_fd.read_u64();
    uint64_t time_last = time_last_fd.read_u64();
    if (t < time_first)
        return;

    // Map the index file and find the beginning and end.
    futil::file index_fd(series_dir,"index",O_RDWR);
    auto index_m = index_fd.mmap(NULL,index_fd.lseek(0,SEEK_END),
                                 PROT_READ | PROT_WRITE,MAP_SHARED,0);
    auto* index_begin = (index_entry*)index_m.addr;
    auto* index_end = index_begin + index_m.len / sizeof(index_entry);
    auto* index_slot = index_begin;

    // Find the target slot.  std::upper_bound returns the first slot greater
    // than the requested value, meaning that t must appear in the slot
    // preceding it - IF t appears at all.  It could be the case that t is
    // a value in the gap between slots.
    index_slot = std::upper_bound(index_begin,index_end,t);
    kassert(index_slot != index_begin);
    --index_slot;

    // Map the timestamp chunk and search it for t.  The std::upper_bound will
    // find the timestamp after t, unless it is the end of the chunk.
    futil::file timestamp_fd(time_ns_dir,index_slot->timestamp_file,O_RDONLY);
    auto timestamp_m = timestamp_fd.mmap(NULL,timestamp_fd.lseek(0,SEEK_END),
                                         PROT_READ,MAP_SHARED,0);
    auto* timestamps_begin = (const uint64_t*)timestamp_m.addr;
    auto* timestamps_end = timestamps_begin + timestamp_m.len / 8;
    auto* timestamp_slot = std::upper_bound(timestamps_begin,timestamps_end,t);
    if (timestamp_slot < timestamps_end)
    {
        // We can delete all chunks before this one.
        time_first = *timestamp_slot;
    }
    else if (index_slot < index_end - 1)
    {
        // This is not the last chunk, so it must be full.  We can delete all
        // chunks up to and including this one.
        ++index_slot;
        time_first = index_slot->time_ns;
    }
    else
    {
        // This is equivalent to t >= time_last (which maybe we could have
        // checked above?).  We can delete every single chunk in the series.
        ++index_slot;
        time_first = time_last + 1;
    }

    // Update time_first.
    time_first_fd.lseek(0,SEEK_SET);
    time_first_fd.write_all(&time_first,8);
    time_first_fd.fcntl(F_BARRIERFSYNC);

    // Delete all orphaned chunks.
    futil::directory fields_dir(series_dir,"fields");
    futil::directory bitmaps_dir(series_dir,"bitmaps");
    for (auto* slot = index_begin; slot < index_slot; ++slot)
    {
        futil::unlink_if_exists(time_ns_dir,slot->timestamp_file);
        std::string gz_file = std::string(slot->timestamp_file) + ".gz";
        for (const auto& f : m.fields)
        {
            futil::path sub_path(f.name,slot->timestamp_file);
            futil::path gz_sub_path(f.name,gz_file);
            futil::unlink_if_exists(fields_dir,sub_path);
            futil::unlink_if_exists(fields_dir,gz_sub_path);
            futil::unlink_if_exists(bitmaps_dir,sub_path);
        }
    }

    // Shift the index file appropriately.
    // TODO: This is unsafe.  If we crash in the middle of memmove, but the OS
    // stays up, we will have corrupted the OS' page cache copy of the index
    // file, which will eventually get flushed back to disk.  We need to make
    // the shift appear atomic.
    memmove(index_begin,index_slot,
            (index_end - index_slot)*sizeof(index_entry));
    index_m.msync();
    index_fd.truncate((index_end - index_slot)*sizeof(index_entry));
    index_fd.fcntl(F_BARRIERFSYNC);
    printf("Deleted %zu slots from the start of the index file.\n",
           index_slot - index_begin);
}
