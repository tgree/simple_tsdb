// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../delete.h"
#include "../write.h"
#include "../database.h"
#include "../count.h"
#include "../select_op.h"
#include "fake_db.h"
#include <futil/fakefs/fakefs.h>
#include <hdr/types.h>
#include <tmock/tmock.h>

static void
validate_timestamp_inodes(dir_node* series_dn, uint64_t* timestamps, size_t N)
{
    dir_node* time_ns_dn = series_dn->get_dir("time_ns");
    dir_node* fields_dn = series_dn->get_dir("fields");
    dir_node* field1_dn = fields_dn->get_dir("field1");
    dir_node* field2_dn = fields_dn->get_dir("field2");
    dir_node* field3_dn = fields_dn->get_dir("field3");
    dir_node* bitmaps_dn = series_dn->get_dir("bitmaps");
    dir_node* bitmap1_dn = bitmaps_dn->get_dir("field1");
    dir_node* bitmap2_dn = bitmaps_dn->get_dir("field2");
    dir_node* bitmap3_dn = bitmaps_dn->get_dir("field3");
    for (size_t i=0; i<N; ++i)
    {
        std::string s = std::to_string(timestamps[i]);

        TASSERT(time_ns_dn->files.contains(s));
        TASSERT(bitmap1_dn->files.contains(s));
        TASSERT(bitmap2_dn->files.contains(s));
        TASSERT(bitmap3_dn->files.contains(s));

        if (i < N - 1)
            s += ".gz";

        TASSERT(field1_dn->files.contains(s));
        TASSERT(field2_dn->files.contains(s));
        TASSERT(field3_dn->files.contains(s));
    }
    tmock::assert_equiv(time_ns_dn->files.size(),N);
    tmock::assert_equiv(field1_dn->files.size(),N);
    tmock::assert_equiv(field2_dn->files.size(),N);
    tmock::assert_equiv(field3_dn->files.size(),N);
    tmock::assert_equiv(bitmap1_dn->files.size(),N);
    tmock::assert_equiv(bitmap2_dn->files.size(),N);
    tmock::assert_equiv(bitmap3_dn->files.size(),N);
}

class tmock_test
{
    TMOCK_TEST(test_reuse_stl)
    {
        init_db(512);

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_total_lock stl(m1,"series1");
        auto time_first_fn = fd_table[stl.time_first_fd.fd].file;

        tsdb::delete_points(stl,1000);
        assert_tree_fsynced(fs_root);
        tmock::assert_equiv(time_first_fn->get_data<uint64_t>(),1001UL);

        tsdb::delete_points(stl,500);
        assert_tree_fsynced(fs_root);
        tmock::assert_equiv(time_first_fn->get_data<uint64_t>(),1001UL);

        tsdb::delete_points(stl,1500);
        assert_tree_fsynced(fs_root);
        tmock::assert_equiv(time_first_fn->get_data<uint64_t>(),1501UL);
    }

    TMOCK_TEST(test_delete_chunks_inodes)
    {
        init_db(128);
        std::vector<size_t> nvec;
        for (size_t i=0; i<813; ++i)
            nvec.push_back(1);
        populate_db(100,10,nvec);
        uint64_t timestamps[] =
        {
            100,       // 0
            1380,      // 128
            2660,      // 256
            3940,      // 384
            5220,      // 512
            6500,      // 640
        };

        tsdb::count_result prev_cr;
        {
            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_total_lock stl(m1,"series1");
            auto series_dn = fd_table[stl.series_dir.fd].directory;
            validate_timestamp_inodes(series_dn,timestamps,NELEMS(timestamps));
            prev_cr = tsdb::count_points(stl,0,-1);
            tmock::assert_equiv(prev_cr.npoints,813UL);
            tmock::assert_equiv(prev_cr.time_first,100UL);
            tmock::assert_equiv(prev_cr.time_last,8220UL);

            snapshot_fs();
            snapshot_auto_begin();
            tsdb::delete_points(stl,5230);
            snapshot_auto_end();

            assert_tree_fsynced(fs_root);
            validate_timestamp_inodes(series_dn,timestamps+4,
                                      NELEMS(timestamps)-4);

            auto cr = tsdb::count_points(stl,0,-1);
            tmock::assert_equiv(cr.npoints,813UL - 128UL*4UL - 2UL);
            tmock::assert_equiv(cr.time_first,5240UL);
            tmock::assert_equiv(cr.time_last,8220UL);
        }

        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");

            auto cr = tsdb::count_points(srl,0,-1);
            TASSERT(cr.npoints <= prev_cr.npoints);
            TASSERT(cr.time_first >= prev_cr.time_first);
            tmock::assert_equiv(cr.time_last,8220UL);
            prev_cr = cr;
        }
    }

    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(test_delete_consistency)
    {
        init_db(16);
        std::vector<size_t> nvec;
        for (size_t i=0; i<45; ++i)
            nvec.push_back(1);
        populate_db(100,10,nvec);

        tsdb::count_result prev_cr;
        {
            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_total_lock stl(m1,"series1");
            prev_cr = tsdb::count_points(stl,0,-1);

            snapshot_fs();
            snapshot_auto_begin();
            tsdb::delete_points(stl,455);
            snapshot_auto_end();

            assert_tree_fsynced(fs_root);
        }

        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);
            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");

            {
                tsdb::series_read_lock srl(m1,"series1");
                auto cr = tsdb::count_points(srl,0,-1);
                TASSERT(cr.npoints <= prev_cr.npoints);
                if (cr.npoints)
                {
                    TASSERT(cr.time_first >= prev_cr.time_first);
                    tmock::assert_equiv(cr.time_last,540UL);
                }
                else
                {
                    tmock::assert_equiv(cr.time_first,0UL); // Want t+1 instead
                    tmock::assert_equiv(cr.time_last,-1UL); // Want 840 instead
                }
                prev_cr = cr;
            }

            nvec.resize(20);
            populate_db(850,10,nvec);

            {
                tsdb::series_read_lock srl(m1,"series1");
                auto cr = tsdb::count_points(srl,0,-1);
                tmock::assert_equiv(cr.npoints,prev_cr.npoints + 20);
            }

            {
                tsdb::series_total_lock stl(m1,"series1");
                tsdb::delete_points(stl,1039);
                auto cr = tsdb::count_points(stl,0,-1);
                tmock::assert_equiv(cr.npoints,1UL);
                tmock::assert_equiv(cr.time_first,1040UL);
                tmock::assert_equiv(cr.time_last,1040UL);
            }
        }
    }
};

TMOCK_MAIN();
