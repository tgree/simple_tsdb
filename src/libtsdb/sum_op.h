// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_SUM_OP_H
#define __SRC_LIBTSDB_SUM_OP_H

#include "select_op.h"
#include "wal.h"

namespace tsdb
{
    // Iterator to compute the sum and the count of non-null points in each
    // window range.  This allows computing the sum operation, the non-null
    // count operation, and the mean operation (by dividing in the client if
    // desired).
    struct sum_op
    {
        // Query range.
        const uint64_t  t0;
        const uint64_t  t1;
        const uint64_t  window_ns;
        bool            is_first;

        // Select op that we are using to iterate.
        wal_query               wq;
        wal_entry_iterator      wqiter;
        select_op_first         op;
        size_t                  op_index;

        // Latest result.
        uint64_t                range_t0;
        std::vector<double>     sums;
        std::vector<uint64_t>   npoints;

        bool next();

        sum_op(const series_read_lock& read_lock,
               const futil::path& series_id,
               const std::vector<std::string>& field_names,
               uint64_t t0, uint64_t t1, uint64_t window_ns);
    };
}

#endif /* __SRC_LIBTSDB_SUM_OP_H */
