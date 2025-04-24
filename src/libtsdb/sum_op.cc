// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "sum_op.h"
#include <limits>

tsdb::sum_op::sum_op(const series_read_lock& read_lock,
    const futil::path& series_id, const std::vector<std::string>& field_names,
    uint64_t _t0, uint64_t _t1, uint64_t window_ns):
        t0(MAX(round_up_to_nearest_multiple(_t0,window_ns),
               round_down_to_nearest_multiple(read_lock.time_first,window_ns))),
        t1(_t1),
        window_ns(window_ns),
        is_first(true),
        wq(read_lock,t0,t1),
        wqiter(wq.begin()),
        op(read_lock,series_id,field_names,t0,t1,-1),
        op_index(0),
        range_t0(t0)
{
    for (size_t i=0; i<op.fields.size(); ++i)
    {
        sums.push_back(0);
        npoints.push_back(0);
        mins.push_back({0});
        maxs.push_back({0});
    }
}

void
tsdb::sum_op::zero()
{
    for (size_t i=0; i<sums.size(); ++i)
    {
        sums[i] = 0;
        npoints[i] = 0;
        
        switch (op.fields[i]->type)
        {
            case tsdb::FT_BOOL:
                mins[i].u8 = 1;
                maxs[i].u8 = 0;
            break;

            case tsdb::FT_U32:
                mins[i].u32 = std::numeric_limits<uint32_t>::max();
                maxs[i].u32 = std::numeric_limits<uint32_t>::min();
            break;

            case tsdb::FT_U64:
                mins[i].u64 = std::numeric_limits<uint64_t>::max();
                maxs[i].u64 = std::numeric_limits<uint64_t>::min();
            break;

            case tsdb::FT_F32:
                mins[i].f32 = std::numeric_limits<float>::infinity();
                maxs[i].f32 = -std::numeric_limits<float>::infinity();
            break;

            case tsdb::FT_F64:
                mins[i].f64 = std::numeric_limits<double>::infinity();
                maxs[i].f64 = -std::numeric_limits<double>::infinity();
            break;

            case tsdb::FT_I32:
                mins[i].i32 = std::numeric_limits<int32_t>::max();
                maxs[i].i32 = std::numeric_limits<int32_t>::min();
            break;

            case tsdb::FT_I64:
                mins[i].i64 = std::numeric_limits<int64_t>::max();
                maxs[i].i64 = std::numeric_limits<int64_t>::min();
            break;
        }
    }
}

bool
tsdb::sum_op::next()
{
    if (!is_first)
        range_t0 += window_ns;
    else
        is_first = false;

    zero();
    size_t range_npoints = 0;

    while (op.npoints)
    {
        // Advance the op if needed.
        if (op_index == op.npoints)
        {
            op.next();
            op_index = 0;
            continue;
        }

        // Advance the op if we need to to get to the start of this range.
        // When using strict mmap-ing of the timestamp files, a profile sample
        // shows we spend a huge amount of time on this line of code.  What
        // happens is that we take thousands of page faults as we advance
        // through the timestamp file and the OS just faults them in small bits
        // at a time (probably in 16K chunks).  Changing select_op over to use
        // a simple read() to load the entire timestamp file leads to a
        // massive speedup.
        uint64_t time_ns = op.timestamps_begin[op_index];
        kassert(range_t0 <= time_ns);

        // If we have gone past the end of this range, return.
        if (range_t0 + window_ns <= time_ns)
            return true;

        // Compute sums.
        for (size_t j=0; j<op.fields.size(); ++j)
        {
            if (op.is_field_null(j,op_index))
                continue;

            switch (op.fields[j]->type)
            {
                case tsdb::FT_BOOL:
                {
                    auto v     = ((uint8_t*)op.field_data[j])[op_index];
                    sums[j]   += v;
                    mins[j].u8 = MIN(mins[j].u8,v);
                    maxs[j].u8 = MAX(maxs[j].u8,v);
                }
                break;

                case tsdb::FT_U32:
                {
                    auto v      = ((uint32_t*)op.field_data[j])[op_index];
                    sums[j]    += v;
                    mins[j].u32 = MIN(mins[j].u32,v);
                    maxs[j].u32 = MAX(maxs[j].u32,v);
                }
                break;

                case tsdb::FT_U64:
                {
                    auto v      = ((uint64_t*)op.field_data[j])[op_index];
                    sums[j]    += v;
                    mins[j].u64 = MIN(mins[j].u64,v);
                    maxs[j].u64 = MAX(maxs[j].u64,v);
                }
                break;

                case tsdb::FT_F32:
                {
                    auto v      = ((float*)op.field_data[j])[op_index];
                    sums[j]    += v;
                    mins[j].f32 = MIN(mins[j].f32,v);
                    maxs[j].f32 = MAX(maxs[j].f32,v);
                }
                break;

                case tsdb::FT_F64:
                {
                    auto v      = ((double*)op.field_data[j])[op_index];
                    sums[j]    += v;
                    mins[j].f64 = MIN(mins[j].f64,v);
                    maxs[j].f64 = MAX(maxs[j].f64,v);
                }
                break;

                case tsdb::FT_I32:
                {
                    auto v      = ((int32_t*)op.field_data[j])[op_index];
                    sums[j]    += v;
                    mins[j].i32 = MIN(mins[j].i32,v);
                    maxs[j].i32 = MAX(maxs[j].i32,v);
                }
                break;

                case tsdb::FT_I64:
                {
                    auto v      = ((int64_t*)op.field_data[j])[op_index];
                    sums[j]    += v;
                    mins[j].i64 = MIN(mins[j].i64,v);
                    maxs[j].i64 = MAX(maxs[j].i64,v);
                }
                break;
            }
            ++npoints[j];
        }

        ++op_index;
        ++range_npoints;
    }

    // We have consumed all points from the select_op, but haven't gone past the
    // end of the range yet.  Consume points from the WAL now.
    while (wqiter != wq.end())
    {
        kassert(range_t0 <= wqiter->time_ns);

        // If we have gone past the end of this range, return.
        if (range_t0 + window_ns <= wqiter->time_ns)
            return true;

        // Compute sums.
        for (size_t j=0; j<sums.size(); ++j)
        {
            size_t field_index = op.fields[j]->index;
            if (wqiter->is_field_null(field_index))
                continue;

            switch (wq.read_lock.m.fields[field_index].type)
            {
                case tsdb::FT_BOOL:
                {
                    auto v     = wqiter->get_field<uint8_t>(field_index);
                    sums[j]   += v;
                    mins[j].u8 = MIN(mins[j].u8,v);
                    maxs[j].u8 = MAX(maxs[j].u8,v);
                }
                break;

                case tsdb::FT_U32:
                {
                    auto v      = wqiter->get_field<uint32_t>(field_index);
                    sums[j]    += v;
                    mins[j].u32 = MIN(mins[j].u32,v);
                    maxs[j].u32 = MAX(maxs[j].u32,v);
                }
                break;

                case tsdb::FT_U64:
                {
                    auto v      = wqiter->get_field<uint64_t>(field_index);
                    sums[j]    += v;
                    mins[j].u64 = MIN(mins[j].u64,v);
                    maxs[j].u64 = MAX(maxs[j].u64,v);
                }
                break;

                case tsdb::FT_F32:
                {
                    auto v      = wqiter->get_field<float>(field_index);
                    sums[j]    += v;
                    mins[j].f32 = MIN(mins[j].f32,v);
                    maxs[j].f32 = MAX(maxs[j].f32,v);
                }
                break;

                case tsdb::FT_F64:
                {
                    auto v      = wqiter->get_field<double>(field_index);
                    sums[j]    += v;
                    mins[j].f64 = MIN(mins[j].f64,v);
                    maxs[j].f64 = MAX(maxs[j].f64,v);
                }
                break;

                case tsdb::FT_I32:
                {
                    auto v      = wqiter->get_field<int32_t>(field_index);
                    sums[j]    += v;
                    mins[j].i32 = MIN(mins[j].i32,v);
                    maxs[j].i32 = MAX(maxs[j].i32,v);
                }
                break;

                case tsdb::FT_I64:
                {
                    auto v      = wqiter->get_field<int64_t>(field_index);
                    sums[j]    += v;
                    mins[j].i64 = MIN(mins[j].i64,v);
                    maxs[j].i64 = MAX(maxs[j].i64,v);
                }
                break;
            }
            ++npoints[j];
        }

        ++wqiter;
        ++range_npoints;
    }

    return range_npoints != 0;
}
