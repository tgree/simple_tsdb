// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "sum_op.h"

tsdb::sum_op::sum_op(const series_read_lock& read_lock,
    const futil::path& series_id, const std::vector<std::string>& field_names,
    uint64_t _t0, uint64_t _t1, uint64_t window_ns):
        t0(round_up_to_nearest_multiple(_t0,window_ns)),
        t1(MAX(t0,round_down_to_nearest_multiple(_t1,window_ns))),
        window_ns(window_ns),
        nranges((t1 - t0) / window_ns),
        op(read_lock,series_id,field_names,t0,t1,-1),
        op_index(0),
        range_t0(round_up_to_nearest_multiple(op.t0,window_ns)),
        range_t1(range_t0 + window_ns - 1)
{
}

bool
tsdb::sum_op::next()
{
    if (op.fields.empty() || range_t0 == t1)
        return false;

    sums.clear();
    sums.resize(op.fields.size());
    npoints.clear();
    npoints.resize(op.fields.size());
    for (;;)
    {
        // Advance the op if needed.
        if (op_index == op.npoints)
        {
            if (op.is_last)
                break;
            op.advance();
            op_index = 0;
        }

        // Advance the op if we need to to get to the start of this range.
        if (range_t0 > op.timestamp_data[op_index])
        {
            ++op_index;
            continue;
        }

        // If we have gone past the end of this range, break.
        if (range_t1 < op.timestamp_data[op_index])
            break;

        // Compute sums.
        for (size_t j=0; j<op.fields.size(); ++j)
        {
            if (op.is_field_null(j,op_index))
                continue;

            switch (op.fields[j].type)
            {
                case tsdb::FT_BOOL:
                    sums[j] += ((uint8_t*)op.field_data[j])[op_index];
                break;

                case tsdb::FT_U32:
                    sums[j] += ((uint32_t*)op.field_data[j])[op_index];
                break;

                case tsdb::FT_U64:
                    sums[j] += ((uint64_t*)op.field_data[j])[op_index];
                break;

                case tsdb::FT_F32:
                    sums[j] += ((float*)op.field_data[j])[op_index];
                break;

                case tsdb::FT_F64:
                    sums[j] += ((double*)op.field_data[j])[op_index];
                break;

                case tsdb::FT_I32:
                    sums[j] += ((int32_t*)op.field_data[j])[op_index];
                break;

                case tsdb::FT_I64:
                    sums[j] += ((int64_t*)op.field_data[j])[op_index];
                break;
            }
            ++npoints[j];
        }

        ++op_index;
    }

    range_t0 += window_ns;
    range_t1 += window_ns;

    return true;
}
