// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "delete.h"
#include "series.h"
#include "measurement.h"
#include "database.h"
#include <futil/xact.h>
#include <algorithm>

void
tsdb::delete_points(series_total_lock& stl, uint64_t t)
{
    // We need to get time_first.  We lock time_first so that nobody else tries
    // to access the series while we are adjusting things.
    if (t < stl.time_first)
        return;

    // We need to find the first valid timestamp > t.  This becomes the new
    // value for time_first and then we drop all points before it.  If no such
    // timestamp exists, then time_first becomes time_last + 1 and we drop
    // everything except the last chunk, which only drop if it is full.  This
    // all has to be done while holding the time_first lock.
    futil::directory time_ns_dir(stl.series_dir,"time_ns");

    // Map the index file and find the beginning and end.
    futil::file index_fd(stl.series_dir,"index",O_RDWR);
    auto index_m = index_fd.mmap(NULL,index_fd.lseek(0,SEEK_END),
                                 PROT_READ | PROT_WRITE,MAP_SHARED,0);
    auto* index_begin = (index_entry*)index_m.addr;
    auto* index_end = index_begin + index_m.len / sizeof(index_entry);

    // Find the target slot.  std::upper_bound returns the first slot greater
    // than the requested value, meaning that t must appear in the slot
    // preceding it - IF t appears at all.  It could be the case that t is
    // a value in the gap between slots.  It could also be the case that the
    // index is completely empty (everything is in the WAL) and so no index
    // slot exists to even hold t.
    uint64_t time_first;
    auto* index_slot = std::upper_bound(index_begin,index_end,t);
    if (index_slot != index_begin)
    {
        --index_slot;

        // Map the timestamp chunk and search it for t.  The std::upper_bound
        // will find the timestamp after t, unless it is the end of the chunk.
        futil::file timestamp_fd(time_ns_dir,index_slot->timestamp_file,
                                 O_RDONLY);
        auto timestamp_m = timestamp_fd.mmap(NULL,
                                             timestamp_fd.lseek(0,SEEK_END),
                                             PROT_READ,MAP_SHARED,0);
        auto* timestamps_begin = (const uint64_t*)timestamp_m.addr;
        auto* timestamps_end = timestamps_begin + timestamp_m.len / 8;
        auto* timestamp_slot = std::upper_bound(timestamps_begin,
                                                timestamps_end,t);
        if (timestamp_slot < timestamps_end)
        {
            // We can delete all chunks before this one.  Set time_first to the
            // first live timestamp >= t.
            time_first = *timestamp_slot;
        }
        else if (index_slot < index_end - 1)
        {
            // This is not the last chunk, so it must be full.  We can delete
            // all chunks up to and including this one.  Set time_first to the
            // first timestamp of the next index slot.
            ++index_slot;
            time_first = index_slot->time_ns;
        }
        else
        {
            // This is equivalent to t >= time_last (which maybe we could have
            // checked above?).  We can delete every single chunk in the series.
            // Set time_first to t + 1.  This has the effect of "deleting from
            // the future", while also trivially deleting from the WAL.  We have
            // already checked that t >= time_first, therefore this update will
            // always increase time_first to a higher value.
            ++index_slot;
            time_first = t + 1;
        }
    }
    else
    {
        // No index slots exist, so everything is in the WAL.  Just increment
        // time_first past the deletion time and a future WAL commit will take
        // care of discarding deleted points.
        time_first = t + 1;
    }

    // Update time_first.
    stl.time_first_fd.lseek(0,SEEK_SET);
    stl.time_first_fd.write_all(&time_first,8);
    stl.time_first = time_first;

    // If we are keeping all the index slots or we have an empty index, then we
    // are done now.
    if (index_slot == index_begin)
    {
        stl.time_first_fd.fsync_and_flush();
        return;
    }
    stl.time_first_fd.fsync_and_barrier();

    // A crash here leaves slots in the index file that precede time_first.  As
    // long as code honors the time_first limit, then these extra slots won't
    // hurt anything, and they will get cleaned up on the next delete
    // operation.

    // Delete all orphaned chunks.
    futil::directory fields_dir(stl.series_dir,"fields");
    futil::directory bitmaps_dir(stl.series_dir,"bitmaps");
    for (auto* slot = index_begin; slot < index_slot; ++slot)
    {
        futil::unlink_if_exists(time_ns_dir,slot->timestamp_file);
        time_ns_dir.fsync_and_flush();
        std::string gz_file = std::string(slot->timestamp_file) + ".gz";
        for (const auto& f : stl.m.fields)
        {
            futil::path sub_path(f.name,slot->timestamp_file);
            futil::path gz_sub_path(f.name,gz_file);
            futil::unlink_if_exists(fields_dir,sub_path);
            futil::unlink_if_exists(fields_dir,gz_sub_path);
            futil::unlink_if_exists(bitmaps_dir,sub_path);
            futil::directory(fields_dir,f.name).fsync_and_flush();
            futil::directory(bitmaps_dir,f.name).fsync_and_flush();
        }
    }

    // A crash here leaves the slots in the index file, after we have deleted
    // the backing files for those slots.  Again, this should not cause any
    // issue as long as time_first is honored everywhere.  It means that during
    // cleanup on the next delete operation we need to consider that the target
    // file being deleted might no longer exist.

    // Shift the index file appropriately.
    futil::xact_mktemp tmp_index_fd(stl.m.db.root.tmp_dir,0660);
    tmp_index_fd.write_all(index_slot,
                           (index_end - index_slot)*sizeof(index_entry));
    tmp_index_fd.fsync_and_barrier();
    futil::rename(stl.m.db.root.tmp_dir,tmp_index_fd.name,stl.series_dir,
                  "index");
    tmp_index_fd.commit();
    stl.series_dir.fsync_and_flush();
    stl.m.db.root.debugf("Deleted %zu slots from the start of the index "
                         "file.\n",index_slot - index_begin);
}
