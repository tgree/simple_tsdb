// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "print_op_results.h"

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#define MAX_PRINT_RESULTS   12
KASSERT(MAX_PRINT_RESULTS % 2 == 0);

static void
print_op_points(const tsdb::select_op& op, size_t index, size_t n)
{
    for (size_t i=index; i<index + n; ++i)
    {
        printf("%20" PRIu64 " ",op.timestamps_begin[i]);
        for (size_t j=0; j<op.fields.size(); ++j)
        {
            if (op.is_field_null(j,i))
            {
                printf("%20s ","null");
                continue;
            }

            const auto* p = op.field_data[j];
            switch (op.fields[j].type)
            {
                case tsdb::FT_BOOL:
                    printf("%20s ",((const uint8_t*)p)[i] ? "true" : "false");
                break;

                case tsdb::FT_U32:
                    printf("%20u ",((const uint32_t*)p)[i]);
                break;

                case tsdb::FT_U64:
                    printf("%20" PRIu64 " ",((const uint64_t*)p)[i]);
                break;

                case tsdb::FT_F32:
                    printf("%20f ",((const float*)p)[i]);
                break;

                case tsdb::FT_F64:
                    printf("%20f ",((const double*)p)[i]);
                break;

                case tsdb::FT_I32:
                    printf("%20d ",((const int32_t*)p)[i]);
                break;

                case tsdb::FT_I64:
                    printf("%20" PRId64 " ",((const int64_t*)p)[i]);
                break;
            }
        }
        printf("\n");
    }
}

static void
print_wq_entries(const tsdb::wal_query& wq,
    const fixed_vector<size_t>& field_indices, size_t index, size_t n)
{
    for (size_t i=index; i<index + n; ++i)
    {
        printf("%20" PRIu64 " ",wq[i].time_ns);
        for (size_t j : field_indices)
        {
            if (wq[i].is_field_null(j))
            {
                printf("%20s ","null");
                continue;
            }

            switch (wq.read_lock.m.fields[j].type)
            {
                case tsdb::FT_BOOL:
                    printf("%20s ",wq[i].get_field<bool>(j) ? "true" : "false");
                break;

                case tsdb::FT_U32:
                    printf("%20u ",wq[i].get_field<uint32_t>(j));
                break;

                case tsdb::FT_U64:
                    printf("%20" PRIu64 " ",wq[i].get_field<uint64_t>(j));
                break;

                case tsdb::FT_F32:
                    printf("%20f ",wq[i].get_field<float>(j));
                break;

                case tsdb::FT_F64:
                    printf("%20f ",wq[i].get_field<double>(j));
                break;

                case tsdb::FT_I32:
                    printf("%20d ",wq[i].get_field<int32_t>(j));
                break;

                case tsdb::FT_I64:
                    printf("%20" PRId64 " ",wq[i].get_field<int64_t>(j));
                break;
            }
        }
        printf("\n");
    }
}

void
print_op_results(const fields_list& fs, tsdb::select_op& op,
    tsdb::wal_query& wq, size_t N)
{
    printf("%20s ","time_ns");
    for (const auto& f : fs.fields)
        printf("%20s ",f.c_str());
    printf("\n");

    while (op.npoints)
    {
        N -= op.npoints;
        for (size_t i=0; i<fs.fields.size() + 1; ++i)
            printf("--------CHUNK------- ");
        printf("\n");
        if (op.npoints <= MAX_PRINT_RESULTS)
            print_op_points(op,0,op.npoints);
        else
        {
            print_op_points(op,0,MAX_PRINT_RESULTS/2);
            printf("... [%zu points omitted] ...\n",
                   op.npoints-MAX_PRINT_RESULTS);
            print_op_points(op,op.npoints-MAX_PRINT_RESULTS/2,
                            MAX_PRINT_RESULTS/2);
        }

        op.next();
    }
    if (N && wq.nentries)
    {
        for (size_t i=0; i<fs.fields.size() + 1; ++i)
            printf("---------WAL-------- ");
        printf("\n");

        auto field_indices = wq.read_lock.m.gen_indices(fs.fields);
        size_t wal_nentries = MIN(N,wq.nentries);
        if (wal_nentries <= MAX_PRINT_RESULTS)
            print_wq_entries(wq,field_indices,0,wal_nentries);
        else
        {
            print_wq_entries(wq,field_indices,0,MAX_PRINT_RESULTS/2);
            printf("... [%zu points omitted] ...\n",
                   wal_nentries-MAX_PRINT_RESULTS);
            print_wq_entries(wq,field_indices,wal_nentries-MAX_PRINT_RESULTS/2,
                             MAX_PRINT_RESULTS/2);
        }
    }
}
