// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../select_op.h"
#include "../database.h"
#include "../series.h"
#include "fake_db.h"
#include <futil/fakefs/fakefs.h>
#include <hdr/types.h>
#include <tmock/tmock.h>

static constexpr const size_t expected_npoints[] =
{
    0,
    16,
    32,
    45,
};

static constexpr const uint64_t last_timestamp[] =
{
    0,
    250,
    410,
    540,
};

static void
generate_db()
{
    init_db(16,128);

    snapshot_auto_begin();

    // Populate the database.  We starts by writing 45 points which will leave
    // 3 chunk entries:
    //
    //  100 - 250 [16]
    //  260 - 410 [16]
    //  420 - 540 [13]
    //
    // And the WAL will be empty.  Then we write 8 points, which will all end
    // up in the WAL.
    populate_db(100,10,{45,8});

    snapshot_auto_end();

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
    TMOCK_TEST(test_select_first_gaps)
    {
        generate_db();

        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");

            for (size_t t = 81; t <= 551; t += 10)
            {
                tsdb::select_op_first op(srl,"series1",
                                         {"field1","field2","field3"},
                                         t,t+8,2);
                tmock::assert_equiv(op.npoints,0UL);
            }
        }
    }

    TMOCK_TEST(test_select_last_gaps)
    {
        generate_db();

        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");

            for (size_t t = 81; t <= 551; t += 10)
            {
                tsdb::select_op_last op(srl,"series1",
                                        {"field1","field2","field3"},
                                        t,t+8,2);
                tmock::assert_equiv(op.npoints,0UL);
            }
        }
    }

    TMOCK_TEST(test_select_first_overlap)
    {
        generate_db();

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");

        for (size_t t = 91; t <= 531; t += 10)
        {
            tsdb::select_op_first op(srl,"series1",
                                     {"field1","field2","field3"},
                                     t,t+9,-1);
            tmock::assert_equiv(op.npoints,1UL);
            op.next();
            tmock::assert_equiv(op.npoints,0UL);
        }

        for (size_t t = 100; t <= 540; t += 10)
        {
            tsdb::select_op_first op(srl,"series1",
                                     {"field1","field2","field3"},
                                     t,t+9,-1);
            tmock::assert_equiv(op.npoints,1UL);
            op.next();
            tmock::assert_equiv(op.npoints,0UL);
        }
    }

    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(test_select_last_overlap)
    {
        generate_db();

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");

        for (size_t t = 91; t <= 531; t += 10)
        {
            tsdb::select_op_last op(srl,"series1",
                                    {"field1","field2","field3"},
                                    t,t+9,-1);
            tmock::assert_equiv(op.npoints,1UL);
            op.next();
            tmock::assert_equiv(op.npoints,0UL);
        }

        for (size_t t = 100; t <= 540; t += 10)
        {
            tsdb::select_op_last op(srl,"series1",
                                    {"field1","field2","field3"},
                                    t,t+9,-1);
            tmock::assert_equiv(op.npoints,1UL);
            op.next();
            tmock::assert_equiv(op.npoints,0UL);
        }
    }

    TMOCK_TEST(test_select_first_from_wal_returns_none)
    {
        generate_db();

        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");
            tsdb::select_op_first op(srl,"series1",
                                     {"field1","field2","field3"},
                                     545,-1,-1);
            tmock::assert_equiv(op.npoints,0UL);
        }
    }

    TMOCK_TEST(test_select_last_from_wal_returns_none)
    {
        generate_db();

        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");
            tsdb::select_op_last op(srl,"series1",
                                    {"field1","field2","field3"},
                                    545,-1,5);
            tmock::assert_equiv(op.npoints,0UL);
        }
    }

    TMOCK_TEST(test_select_first_honors_time_last)
    {
        generate_db();

        bool found_extra = false;
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");

            auto* sdn = fd_table[srl.series_dir.fd].directory;
            auto* index_fn = sdn->get_file("index");
            size_t nindices = index_fn->data.size() / sizeof(tsdb::index_entry);
            size_t live_indices = 0;
            for (size_t i=0; i<nindices; ++i)
            {
                auto* ies = index_fn->as_array<tsdb::index_entry>();
                if (ies[i].time_ns <= srl.time_last)
                    ++live_indices;
                else
                    found_extra = true;
            }
            TASSERT(live_indices < NELEMS(expected_npoints));

            tsdb::select_op_first op(srl,"series1",
                                    {"field1","field2","field3"},
                                    0,-1,-1);
            size_t total_points = 0;
            while (op.npoints)
            {
                total_points += op.npoints;
                op.next();
            }
            tmock::assert_equiv(total_points,expected_npoints[live_indices]);
        }
        TASSERT(found_extra);
    }

    TMOCK_TEST(test_select_last_honors_time_last)
    {
        generate_db();

        bool found_extra = false;
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");

            auto* sdn = fd_table[srl.series_dir.fd].directory;
            auto* index_fn = sdn->get_file("index");
            size_t nindices = index_fn->data.size() / sizeof(tsdb::index_entry);
            size_t live_indices = 0;
            for (size_t i=0; i<nindices; ++i)
            {
                auto* ies = index_fn->as_array<tsdb::index_entry>();
                if (ies[i].time_ns <= srl.time_last)
                    ++live_indices;
                else
                    found_extra = true;
            }
            TASSERT(live_indices < NELEMS(expected_npoints));

            tsdb::select_op_last op(srl,"series1",
                                    {"field1","field2","field3"},
                                    0,-1,5);
            if (op.npoints)
            {
                tmock::assert_equiv(op.npoints,5UL);
                tmock::assert_equiv(op.timestamps_begin[0],
                                    last_timestamp[live_indices] - 40);
                tmock::assert_equiv(op.timestamps_begin[4],
                                    last_timestamp[live_indices]);
                op.next();
                tmock::assert_equiv(op.npoints,0UL);
            }
        }
        TASSERT(found_extra);
    }

    TMOCK_TEST(test_select_first_sliding)
    {
        generate_db();

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");

        for (uint64_t t0 = 90; t0 <= 550; t0 += 5)
        {
            for (uint64_t t1 = 90; t1 <= 550; t1 += 5)
            {
                size_t N = 0;
                for (uint64_t t = t0; t <= t1; ++ t)
                {
                    if (t < 100 || t > 540)
                        continue;
                    N += ((t % 10) == 0);
                }
                for (size_t limit = 1; limit <= 10; ++limit)
                {
                    tsdb::select_op_first op(srl,"series1",
                                             {"field1","field2","field3"},
                                             t0,t1,limit);

                    if (N == 0)
                    {
                        tmock::assert_equiv(op.npoints,0UL);
                        continue;
                    }

                    uint64_t _t0 =
                        round_up_to_nearest_multiple<uint64_t>(t0,10ULL);
                    uint64_t _t1 =
                        round_down_to_nearest_multiple<uint64_t>(t1,10ULL);
                    int64_t i0 = MAX(((int64_t)_t0 - 100) / 10,0);
                    int64_t i1 = MIN(((int64_t)_t1 - 100) / 10,44);
                    tmock::assert_equiv<int64_t>(N,i1 - i0 + 1);

                    size_t total_points = 0;
                    auto* dp = &dps[i0];
                    uint64_t timestamp = 100 + i0*10;
                    while (op.npoints)
                    {
                        for (size_t i=0; i<op.npoints; ++i)
                        {
                            TASSERT(op.timestamps_begin[i] == timestamp);

                            tmock::assert_equiv(
                                op.get_field<uint32_t,0>(i),dp->field1);
                            tmock::assert_equiv(
                                op.get_field<double,1>(i),dp->field2);
                            tmock::assert_equiv(
                                op.get_field<uint32_t,2>(i),dp->field3);
                            tmock::assert_equiv(
                                op.is_field_null(0,i),!dp->is_non_null[0]);
                            tmock::assert_equiv(
                                op.is_field_null(1,i),!dp->is_non_null[1]);
                            tmock::assert_equiv(
                                op.is_field_null(2,i),!dp->is_non_null[2]);

                            ++dp;
                            ++total_points;
                            timestamp += 10;
                        }
                        op.next();
                    }
                    tmock::assert_equiv<uint64_t>(total_points,MIN(N,limit));
                }
            }
        }
    }

    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(test_select_last_sliding)
    {
        generate_db();

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");

        for (uint64_t t0 = 90; t0 <= 550; t0 += 5)
        {
            for (uint64_t t1 = 90; t1 <= 550; t1 += 5)
            {
                size_t N = 0;
                for (uint64_t t = t0; t <= t1; ++ t)
                {
                    if (t < 100 || t > 540)
                        continue;
                    N += ((t % 10) == 0);
                }
                for (size_t limit = 1; limit <= 10; ++limit)
                {
                    tsdb::select_op_last op(srl,"series1",
                                            {"field1","field2","field3"},
                                            t0,t1,limit);

                    if (N == 0)
                    {
                        tmock::assert_equiv(op.npoints,0UL);
                        continue;
                    }

                    uint64_t _t0 =
                        round_up_to_nearest_multiple<uint64_t>(t0,10ULL);
                    uint64_t _t1 =
                        round_down_to_nearest_multiple<uint64_t>(t1,10ULL);
                    int64_t i0 = MAX(((int64_t)_t0 - 100) / 10,0);
                    int64_t i1 = MIN(((int64_t)_t1 - 100) / 10,44);
                    tmock::assert_equiv<int64_t>(N,i1 - i0 + 1);
                    if (N > limit)
                        i0 += (N - limit);

                    size_t total_points = 0;
                    auto* dp = &dps[i0];
                    uint64_t timestamp = 100 + i0*10;
                    while (op.npoints)
                    {
                        for (size_t i=0; i<op.npoints; ++i)
                        {
                            TASSERT(op.timestamps_begin[i] == timestamp);

                            tmock::assert_equiv(
                                op.get_field<uint32_t,0>(i),dp->field1);
                            tmock::assert_equiv(
                                op.get_field<double,1>(i),dp->field2);
                            tmock::assert_equiv(
                                op.get_field<uint32_t,2>(i),dp->field3);
                            tmock::assert_equiv(
                                op.is_field_null(0,i),!dp->is_non_null[0]);
                            tmock::assert_equiv(
                                op.is_field_null(1,i),!dp->is_non_null[1]);
                            tmock::assert_equiv(
                                op.is_field_null(2,i),!dp->is_non_null[2]);

                            ++dp;
                            ++total_points;
                            timestamp += 10;
                        }
                        op.next();
                    }
                    tmock::assert_equiv<uint64_t>(total_points,MIN(N,limit));
                }
            }
        }
    }
};

TMOCK_MAIN();
