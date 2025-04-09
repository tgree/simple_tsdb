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

void
print_op_results(tsdb::select_op& op)
{
    if (!op.npoints)
        return;

    printf("%20s ","time_ns");
    for (const auto& f : op.fields)
        printf("%20s ",f.name);
    printf("\n");

    while (op.npoints)
    {
        for (size_t i=0; i<op.fields.size() + 1; ++i)
            printf("-------------------- ");
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
}
