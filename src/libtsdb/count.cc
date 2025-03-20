// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "count.h"
#include "tsdb.h"
#include <algorithm>

size_t
tsdb::count_points(const series_read_lock& read_lock, uint64_t t0, uint64_t t1)
{
    // Validate the time and limit ranges.
    uint64_t time_last = futil::file(read_lock.series_dir,"time_last",
                                     O_RDONLY).read_u64();
    if (read_lock.time_first > time_last)
        return 0;

    t0 = MAX(t0,read_lock.time_first);
    t1 = MIN(t1,time_last);
    if (t0 > t1)
        return 0;
    if (t1 < read_lock.time_first)
        return 0;
    if (t0 > time_last)
        return 0;

    // Map the index.
    futil::file index_fd(read_lock.series_dir,"index",O_RDONLY);
    auto index_map = index_fd.mmap(NULL,index_fd.lseek(0,SEEK_END),PROT_READ,
                                   MAP_SHARED,0);
    auto* index_begin = (const index_entry*)index_map.addr;
    auto* index_end = index_begin + index_map.len / sizeof(index_entry);

    // Find the index slots for the lower and upper bounds.
    auto* index_lower = std::upper_bound(index_begin,index_end,t0);
    auto* index_upper = std::upper_bound(index_begin,index_end,t1);
    kassert(index_lower != index_begin);
    kassert(index_upper != index_begin);
    --index_lower;
    --index_upper;

    // Open and map both files.
    futil::directory time_ns_dir(read_lock.series_dir,"time_ns");
    futil::file lower_fd(time_ns_dir,index_lower->timestamp_file,O_RDONLY);
    futil::file upper_fd(time_ns_dir,index_upper->timestamp_file,O_RDONLY);
    auto lower_map = lower_fd.mmap(NULL,lower_fd.lseek(0,SEEK_END),PROT_READ,
                                   MAP_SHARED,0);
    auto upper_map = upper_fd.mmap(NULL,upper_fd.lseek(0,SEEK_END),PROT_READ,
                                   MAP_SHARED,0);

    // Find the start end end indices.
    auto* lower_begin = (const uint64_t*)lower_map.addr;
    auto* upper_begin = (const uint64_t*)upper_map.addr;
    auto* lower_end = lower_begin + lower_map.len / 8;
    auto* upper_end = upper_begin + upper_map.len / 8;
    auto tslot_lower = std::lower_bound(lower_begin,lower_end,t0) - lower_begin;
    auto tslot_upper = std::upper_bound(upper_begin,upper_end,t1) - upper_begin;

    // Compute the result.
    size_t n_chunks = index_upper - index_lower;
    return n_chunks*CHUNK_NPOINTS + tslot_upper - tslot_lower;
}
