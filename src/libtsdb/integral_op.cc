// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "integral_op.h"

tsdb::integral_op::integral_op(const series_read_lock& read_lock,
    const futil::path& series_id, const std::vector<std::string>& field_names,
    uint64_t _t0, uint64_t _t1):
        t0_ns(0),
        t1_ns(0)
{
    wal_query               wq(read_lock,_t0,_t1);
    select_op_first         op(read_lock,series_id,field_names,_t0,_t1,-1);
    uint64_t                prev_t_ns = 0;
    field_vector<double>    prev_v;
    for (size_t i=0; i<op.fields.size(); ++i)
    {
        integral.push_back(0);
        is_null.push_back(false);
        prev_v.push_back(0);
    }

    // TODO: Updating the averages in parallel is probably not cache-efficient.
    while (op.npoints)
    {
        for (size_t i=0; i<op.npoints; ++i)
        {
            uint64_t t_ns = t1_ns = op.timestamps_begin[i];

            // Handle the first point.
            if (prev_t_ns == 0)
            {
                t0_ns = prev_t_ns = t_ns;
                for (size_t j=0; j<op.fields.size(); ++j)
                {
                    is_null[j] = op.is_field_null(j,i);
                    prev_v[j] = op.cast_field<double>(j,i);
                }
                continue;
            }

            // Update the integral.
            double dt = ((double)(t_ns - prev_t_ns)) / 1e9;
            for (size_t j=0; j<op.fields.size(); ++j)
            {
                double v = op.cast_field<double>(j,i);
                integral[j] += 0.5 * (prev_v[j] + v) * dt;
                prev_v[j] = v;
                is_null[j] |= op.is_field_null(j,i);
            }

            prev_t_ns = t_ns;
        }

        op.next();
    }

    for (auto wqiter = wq.begin(); wqiter != wq.end(); ++wqiter)
    {
        uint64_t t_ns = t1_ns = wqiter->time_ns;

        // Handle the first point.
        if (prev_t_ns == 0)
        {
            t0_ns = prev_t_ns = t_ns;
            for (size_t j=0; j<op.fields.size(); ++j)
            {
                size_t field_index = op.fields[j]->index;
                is_null[j] = wqiter->is_field_null(field_index);
                prev_v[j] = wqiter->cast_field<double>(
                        field_index,op.fields[field_index]->type);
            }
            continue;
        }

        // Update the integral.
        double dt = ((double)(t_ns - prev_t_ns)) / 1e9;
        for (size_t j=0; j<op.fields.size(); ++j)
        {
            size_t field_index = op.fields[j]->index;
            double v = wqiter->cast_field<double>(
                    field_index,op.fields[field_index]->type);
            integral[j] += 0.5 * (prev_v[j] + v) * dt;
            prev_v[j] = v;
            is_null[j] |= wqiter->is_field_null(field_index);
        }

        prev_t_ns = t_ns;
    }

    if (t0_ns == 0)
    {
        // We got no points at all.
        for (size_t j=0; j<op.fields.size(); ++j)
            is_null[j] = true;
    }
    else if (t0_ns == t1_ns)
    {
        // We got only a single point.  Just set the integral to be the
        // value of that point, even though it is undefined.
        for (size_t j=0; j<op.fields.size(); ++j)
            integral[j] = prev_v[j];
    }
}
