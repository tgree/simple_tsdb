// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../count.h"
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

static void
generate_db()
{
    init_db(16,128);

    snapshot_auto_begin();

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
    TMOCK_TEST(test_count_gaps)
    {
        generate_db();

        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");

            for (size_t t = 81; t <= 631; t += 10)
            {
                auto cr = tsdb::count_points(srl,t,t+8);
                tmock::assert_equiv(cr.npoints,0UL);
                tmock::assert_equiv(cr.time_first,t);
                tmock::assert_equiv(cr.time_last,t+8);
            }
        }
    }

    TMOCK_TEST(test_count_first_overlap)
    {
        generate_db();

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");

        for (size_t t = 91; t <= 611; t += 10)
        {
            auto cr = tsdb::count_points(srl,t,t+9);
            tmock::assert_equiv(cr.npoints,1UL);
            tmock::assert_equiv(cr.time_first,t+9);
            tmock::assert_equiv(cr.time_last,t+9);
        }

        for (size_t t = 100; t <= 620; t += 10)
        {
            auto cr = tsdb::count_points(srl,t,t+9);
            tmock::assert_equiv(cr.npoints,1UL);
            tmock::assert_equiv(cr.time_first,t);
            tmock::assert_equiv(cr.time_last,t);
        }
    }

    TMOCK_TEST(test_count_from_wal_returns_all)
    {
        generate_db();

        size_t last_npoints = 0;
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");
            auto cr = tsdb::count_points(srl,545,-1);
            TASSERT(cr.npoints >= last_npoints);
            TASSERT(cr.npoints == 0 || cr.npoints == 8);
            last_npoints = cr.npoints;
        }
        tmock::assert_equiv(last_npoints,8UL);
    }

    TMOCK_TEST(test_count_honors_time_last)
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

            auto cr = tsdb::count_points(srl,0,545);
            tmock::assert_equiv(cr.npoints,expected_npoints[live_indices]);
        }
        TASSERT(found_extra);
    }

    TMOCK_TEST(test_count_sliding)
    {
        generate_db();

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");

        for (uint64_t t0 = 90; t0 <= 630; t0 += 5)
        {
            for (uint64_t t1 = 90; t1 <= 630; t1 += 5)
            {
                size_t N = 0;
                for (uint64_t t = t0; t <= t1; ++ t)
                {
                    if (t < 100 || t > 620)
                        continue;
                    N += ((t % 10) == 0);
                }
                auto cr = tsdb::count_points(srl,t0,t1);
                tmock::assert_equiv(cr.npoints,N);

                if (t0 <= 100)
                {
                    if (cr.npoints)
                        tmock::assert_equiv(cr.time_first,100UL);
                    else
                        tmock::assert_equiv(cr.time_first,t0);
                }
                else if (t0 > 620)
                    tmock::assert_equiv(cr.time_first,t0);
                else
                {
                    if (cr.npoints)
                    {
                        tmock::assert_equiv(cr.time_first,
                            round_up_to_nearest_multiple<uint64_t>(t0,10));
                    }
                    else
                        tmock::assert_equiv(cr.time_first,t0);
                }

                if (t1 <= 100)
                    tmock::assert_equiv(cr.time_last,t1);
                else if (t1 > 620)
                {
                    if (cr.npoints)
                        tmock::assert_equiv(cr.time_last,620UL);
                    else
                        tmock::assert_equiv(cr.time_last,t1);
                }
                else
                {
                    if (cr.npoints)
                    {
                        tmock::assert_equiv(cr.time_last,
                            round_down_to_nearest_multiple<uint64_t>(t1,10));
                    }
                    else
                        tmock::assert_equiv(cr.time_last,t1);
                }
            }
        }
    }
};

TMOCK_MAIN();
