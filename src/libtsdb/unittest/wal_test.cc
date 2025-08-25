// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../wal.h"
#include "../write.h"
#include "../database.h"
#include "../select_op.h"
#include "fake_db.h"
#include <futil/fakefs/fakefs.h>
#include <hdr/auto_buf.h>
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(test_wal_empty_queries)
    {
        init_db(512);

        snapshot_fs();
        snapshot_auto_begin();
        populate_db(1000,100,{76, 128, 93, 1, 1, 2, 700, 12});
        snapshot_auto_end();

        auto dn_final __UNUSED__ = fs_root;
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            TASSERT(fd_table[root.root_dir.fd].directory == dn);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");

            // Do a query that hits before the WAL.
            tsdb::wal_query wq(srl,1,100);
            tmock::assert_equiv(wq.nentries,0UL);
            TASSERT(wq.begin() == wq.end());

            // Do a query that hits in the WAL but backwards.
            tsdb::wal_query wq2(srl,1450,1150);
            tmock::assert_equiv(wq2.nentries,0UL);
            TASSERT(wq2.begin() == wq2.end());

            // Do a query that hits after the WAL.
            tsdb::wal_query wq3(srl,100000000,200000000);
            tmock::assert_equiv(wq3.nentries,0UL);
            TASSERT(wq3.begin() == wq3.end());

            // Do a query that hits between points in the WAL.
            tsdb::wal_query wq4(srl,1025,1075);
            tmock::assert_equiv(wq4.nentries,0UL);
            TASSERT(wq4.begin() == wq4.end());
        }
    }

    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(
        test_wal_consistency_stride_100_randpop)
    {
        init_db(512);

        snapshot_fs();
        snapshot_auto_begin();
        auto pop_points = populate_db(1000,100,{76, 128, 93, 1, 1, 2, 700, 12});
        snapshot_auto_end();
        tmock::assert_equiv(pop_points,1013UL);

        auto dn_final __UNUSED__ = fs_root;
        size_t prev_total = 0;
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            // Validate whatever points did make it to disk.
            size_t total = validate_points(1000,100);
            TASSERT(total >= prev_total);
            prev_total = total;

            // Finish populating the series from whatever intermediate state it
            // was interrupted in.
            {
                tsdb::root root(".",false);
                TASSERT(fd_table[root.root_dir.fd].directory == dn);
                tsdb::database db1(root,"db1");
                tsdb::measurement m1(db1,"measurement1");
                auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
                write_points(swl,pop_points-total,1000 + 100*total,100,total);
            }

            // Validate it all again to make sure it is sane.
            tmock::assert_equiv(validate_points(1000,100),pop_points);
        }
    }

    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(test_wal_consistency_stride_1_slowpop)
    {
        std::vector<size_t> nvec;
        for (size_t i=0; i<64; ++i)
            nvec.push_back(1);

        init_db(16);

        snapshot_fs();
        snapshot_auto_begin();
        auto pop_points = populate_db(1,1,nvec);
        snapshot_auto_end();
        tmock::assert_equiv(pop_points,64UL);

        auto dn_final __UNUSED__ = fs_root;
        size_t prev_total = 0;
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            // Validate whatever points did make it to disk.
            size_t total = validate_points(1,1);
            TASSERT(total >= prev_total);
            prev_total = total;

            // Finish populating the series from whatever intermediate state it
            // was interrupted in.  This is slow for the first 512 points
            // because the completion triggers a compression every time.  But
            // on the last 501 points it zooms past.
            {
                tsdb::root root(".",false);
                TASSERT(fd_table[root.root_dir.fd].directory == dn);
                tsdb::database db1(root,"db1");
                tsdb::measurement m1(db1,"measurement1");
                auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
                write_points(swl,pop_points-total,1 + 1*total,1,total);
            }

            // Validate it all again to make sure it is sane.
            tmock::assert_equiv(validate_points(1,1),pop_points);
        }
    }
};

TMOCK_MAIN();
