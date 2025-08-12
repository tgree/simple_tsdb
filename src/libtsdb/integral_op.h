// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_INTEGRAL_OP_H
#define __SRC_LIBTSDB_INTEGRAL_OP_H

#include "select_op.h"
#include "wal.h"

namespace tsdb
{
    // Compute the trapezoidal integral of the fields in the time range.  If a
    // field has a NULL value as part of the integral then the entire integral
    // should be considered NULL.
    //
    // The t0_ns and t1_ns values contain the actual timestamps of the first
    // and last points integrated and not the original t0 and t1 values.  They
    // can be used to compute the average value of the data set over the range
    // of integration.
    struct integral_op
    {
        uint64_t                t0_ns;
        uint64_t                t1_ns;
        field_vector<double>    integral;
        field_vector<bool>      is_null;

        integral_op(const series_read_lock& read_lock,
                    const futil::path& series_id,
                    const std::vector<std::string>& field_names,
                    uint64_t t0_ns, uint64_t t1_ns);
    };
}

#endif /* __SRC_LIBTSDB_INTEGRAL_OP_H */
