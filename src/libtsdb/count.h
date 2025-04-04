// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_COUNT_H
#define __SRC_LIBTSDB_COUNT_H

#include "series.h"

namespace tsdb
{
    struct count_result
    {
        size_t      npoints;
        uint64_t    time_first;
        uint64_t    time_last;
    };

    // Counts the number of points between t0 and t1, inclusive.
    count_result count_points(const series_read_lock& read_lock, uint64_t t0,
                              uint64_t t1);
}

#endif /* __SRC_LIBTSDB_COUNT_H */
