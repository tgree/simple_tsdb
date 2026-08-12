// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "sum_compute_op.h"
#include <limits>

tsdb::sum_compute_op::sum_compute_op(const series_read_lock& read_lock,
    const futil::path& series_id, const std::vector<std::string>& field_names,
    shunting_yard::shunter& shunter, uint64_t _t0, uint64_t _t1,
    uint64_t window_ns):
        shunter(shunter),
        t0(MAX(round_up_to_nearest_multiple(_t0,window_ns),
               round_down_to_nearest_multiple(read_lock.time_first,window_ns))),
        t1(_t1),
        window_ns(window_ns),
        is_first(true),
        wq(read_lock,t0,t1),
        wqiter(wq.begin()),
        op(read_lock,series_id,field_names,t0,t1,-1),
        op_index(0),
        range_t0(t0),
        sum(0),
        min(0),
        max(0),
        npoints(0)
{
}

void
tsdb::sum_compute_op::zero()
{
    sum = 0;
    min = std::numeric_limits<double>::infinity();
    max = -std::numeric_limits<double>::infinity();
    npoints = 0;
}

bool
tsdb::sum_compute_op::next()
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
        bool is_null = false;
        for (size_t j=0; j<op.fields.size(); ++j)
        {
            is_null |= op.is_field_null(j,op_index);
            shunter.variables[j].value = op.cast_field<double>(j,op_index);
        }
        if (!is_null)
        {
            double v = shunter.evaluate();
            sum     += v;
            min      = MIN(min,v);
            max      = MAX(max,v);
            ++npoints;
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
        bool is_null = false;
        for (size_t j=0; j<op.fields.size(); ++j)
        {
            is_null |= wqiter->is_field_null(op.fields[j]->index);
            shunter.variables[j].value =
                wqiter->cast_field<double>(op.fields[j]->index,
                                           op.fields[j]->type);
        }
        if (!is_null)
        {
            double v = shunter.evaluate();
            sum     += v;
            min      = MIN(min,v);
            max      = MAX(max,v);
            ++npoints;
        }

        ++wqiter;
        ++range_npoints;
    }

    return range_npoints != 0;
}
