// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "print_compute_results.h"

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#define MAX_PRINT_RESULTS   12
KASSERT(MAX_PRINT_RESULTS % 2 == 0);

static void
print_op_points(const tsdb::select_op& op, shunting_yard::shunter& shunter,
    size_t index, size_t n)
{
    for (size_t i=index; i<index + n; ++i)
    {
        printf("%20" PRIu64 " ",op.timestamps_begin[i]);
        bool is_null = false;
        for (size_t j=0; j<op.fields.size(); ++j)
            is_null |= op.is_field_null(j,i);
        if (is_null)
        {
            printf("%20s\n","null");
            continue;
        }

        for (size_t j=0; j<op.fields.size(); ++j)
            shunter.variables[j].value = op.cast_field<double>(j,i);
        printf("%20f\n",shunter.evaluate());
    }
}

static void
print_wq_entries(const tsdb::wal_query& wq, shunting_yard::shunter& shunter,
    const tsdb::field_vector<const tsdb::schema_entry*>& fields, size_t index,
    size_t n)
{
    for (size_t i=index; i<index + n; ++i)
    {
        printf("%20" PRIu64 " ",wq[i].time_ns);
        bool is_null = false;
        for (auto* f : fields)
            is_null |= wq[i].is_field_null(f->index);
        if (is_null)
        {
            printf("%20s\n","null");
            continue;
        }

        for (size_t j=0; j<fields.size(); ++j)
        {
            shunter.variables[j].value =
                wq[i].cast_field<double>(fields[j]->index,fields[j]->type);
        }
        printf("%20f\n",shunter.evaluate());
    }
}

void
print_compute_results(tsdb::select_op& op, tsdb::wal_query& wq,
    shunting_yard::shunter& shunter, size_t N)
{
    printf("%20s %20s\n","time_ns","result");

    while (op.npoints)
    {
        N -= op.npoints;
        printf("--------CHUNK------- --------CHUNK-------\n");
        if (op.npoints <= MAX_PRINT_RESULTS)
            print_op_points(op,shunter,0,op.npoints);
        else
        {
            print_op_points(op,shunter,0,MAX_PRINT_RESULTS/2);
            printf("... [%zu points omitted] ...\n",
                   op.npoints-MAX_PRINT_RESULTS);
            print_op_points(op,shunter,op.npoints-MAX_PRINT_RESULTS/2,
                            MAX_PRINT_RESULTS/2);
        }

        op.next();
    }
    if (N && wq.nentries)
    {
        printf("---------WAL-------- ---------WAL--------\n");

        size_t wal_nentries = MIN(N,wq.nentries);
        if (wal_nentries <= MAX_PRINT_RESULTS)
            print_wq_entries(wq,shunter,op.fields,0,wal_nentries);
        else
        {
            print_wq_entries(wq,shunter,op.fields,0,MAX_PRINT_RESULTS/2);
            printf("... [%zu points omitted] ...\n",
                   wal_nentries-MAX_PRINT_RESULTS);
            print_wq_entries(wq,shunter,op.fields,
                             wal_nentries-MAX_PRINT_RESULTS/2,
                             MAX_PRINT_RESULTS/2);
        }
    }
}
