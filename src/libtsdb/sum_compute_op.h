// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_SUM_COMPUTE_OP_H
#define __SRC_LIBTSDB_SUM_COMPUTE_OP_H

#include "select_op.h"
#include "wal.h"
#include <shunter/shunting_yard.h>

namespace tsdb
{
    // Iterator to compute the sum and the count of a user-provided computation
    // on the non-null points in each window range.  This allows computing the
    // sum operation, the non-null count operation, and the mean operation (by
    // dividing in the client if desired).
    //
    // Given a user equation that reduces to a function F, this computes:
    //
    //      sums = SUM(F(points))
    //      mins = MIN(F(points))
    //      maxs = MAX(F(points))
    //
    // I.e. it finds the sum, min and max of the values after evaluating the
    // user function.
    struct sum_compute_op
    {
        // User function.
        shunting_yard::shunter& shunter;

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
        double                  sum;
        double                  min;
        double                  max;
        uint64_t                npoints;

        void zero();
        bool next();

        sum_compute_op(const series_read_lock& read_lock,
                       const futil::path& series_id,
                       const std::vector<std::string>& field_names,
                       shunting_yard::shunter& shunter,
                       uint64_t t0, uint64_t t1, uint64_t window_ns);
    };
}

#endif /* __SRC_LIBTSDB_SUM_COMPUTE_OP_H */
