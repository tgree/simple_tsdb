// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../sum_op.h"
#include "../database.h"
#include "../series.h"
#include "fake_db.h"
#include <futil/fakefs/fakefs.h>
#include <hdr/types.h>
#include <tmock/tmock.h>
#include <limits>

static void
generate_db()
{
    init_db(16,128);

    // Populate the database.  We do a 45-point write followed by an 8-point
    // write, yielding 3 chunk entries and some WAL points:
    //
    //  CH0 100 - 250 [16]
    //  CH1 260 - 410 [16]
    //  CH2 420 - 540 [13]
    //  WAL 550 - 620 [8]
    //
    // Then we write 8 points, which will all end
    // up in the WAL.
    populate_db(100,10,{45,8});

    // Validate the index.
    auto* sdn = fs_root
        ->get_dir("databases")
        ->get_dir("db1")
        ->get_dir("measurement1")
        ->get_dir("series1");
    auto* index_fn = sdn->get_file("index");
    tmock::assert_equiv(index_fn->data.size(),3*sizeof(tsdb::index_entry));
    auto* ies = index_fn->as_array<tsdb::index_entry>();
    tmock::assert_equiv(ies[0].time_ns,100UL);
    tmock::assert_equiv(ies[1].time_ns,260UL);
    tmock::assert_equiv(ies[2].time_ns,420UL);
}

class tmock_test
{
    TMOCK_TEST(test_sum_gaps)
    {
        generate_db();

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");

        for (size_t t = 81; t <= 631; t += 10)
        {
            auto op = tsdb::sum_op(srl,"series1",{"field1","field2","field3"},
                                   t,t+8,5);
            TASSERT(!op.next());
        }
    }

    TMOCK_TEST(test_sum_100s)
    {
        generate_db();

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");
        auto op = tsdb::sum_op(srl,"series1",{"field1","field2","field3"},
                               100,700,100);
        auto* dp = &dps[0];
        auto* dp_end = dp + 45 + 8;
        uint64_t t = 100;
        while (op.next())
        {
            tmock::assert_equiv(op.range_t0,t);
            t += 100;

            double sums[3] = {};
            size_t npoints[3] = {};
            uint32_t field1_min = 0xFFFFFFFF;
            uint32_t field1_max = 0;
            double field2_min = std::numeric_limits<double>::infinity();
            double field2_max = -std::numeric_limits<double>::infinity();
            uint32_t field3_min = 0xFFFFFFFF;
            uint32_t field3_max = 0;

            for (size_t i=0; i<10; ++i)
            {
                if (dp == dp_end)
                    break;

                if (dp->is_non_null[0])
                {
                    ++npoints[0];
                    sums[0] += dp->field1;
                    field1_min = MIN(field1_min,dp->field1);
                    field1_max = MAX(field1_max,dp->field1);
                }
                if (dp->is_non_null[1])
                {
                    ++npoints[1];
                    sums[1] += dp->field2;
                    field2_min = MIN(field2_min,dp->field2);
                    field2_max = MAX(field2_max,dp->field2);
                }
                if (dp->is_non_null[2])
                {
                    ++npoints[2];
                    sums[2] += dp->field3;
                    field3_min = MIN(field3_min,dp->field3);
                    field3_max = MAX(field3_max,dp->field3);
                }

                ++dp;
            }

            tmock::assert_equiv(op.sums[0],sums[0]);
            tmock::assert_equiv(op.sums[1],sums[1]);
            tmock::assert_equiv(op.sums[2],sums[2]);
            tmock::assert_equiv(op.mins[0].u32,field1_min);
            tmock::assert_equiv(op.mins[1].f64,field2_min);
            tmock::assert_equiv(op.mins[2].u32,field3_min);
            tmock::assert_equiv(op.maxs[0].u32,field1_max);
            tmock::assert_equiv(op.maxs[1].f64,field2_max);
            tmock::assert_equiv(op.maxs[2].u32,field3_max);
            tmock::assert_equiv(op.npoints[0],npoints[0]);
            tmock::assert_equiv(op.npoints[1],npoints[1]);
            tmock::assert_equiv(op.npoints[2],npoints[2]);
        }
    }
};

TMOCK_MAIN();
