// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../series.h"
#include "../database.h"
#include <futil/fakefs/fakefs.h>
#include <tmock/tmock.h>

#define DUMP_STATS  0

const std::vector<tsdb::schema_entry> test_fields =
{
    {tsdb::FT_U32,SCHEMA_VERSION,0, 0,"field1"},
    {tsdb::FT_F64,SCHEMA_VERSION,1, 4,"field2"},
    {tsdb::FT_F32,SCHEMA_VERSION,2,12,"field3"},
};

static void
validate_series_inodes(dir_node* dn)
{
    TASSERT(dn->subdirs.contains("time_ns"));
    TASSERT(dn->subdirs.contains("fields"));
    TASSERT(dn->subdirs.contains("bitmaps"));
    TASSERT(dn->files.contains("time_first"));
    TASSERT(dn->files.contains("time_last"));
    TASSERT(dn->files.contains("index"));
    TASSERT(dn->files.contains("wal"));
    for (const auto& se : test_fields)
    {
        TASSERT(dn->subdirs["fields"]->subdirs.contains(se.name));
        TASSERT(dn->subdirs["bitmaps"]->subdirs.contains(se.name));
    }
}

class tmock_test
{
    TMOCK_TEST(test_create_series_bad_name)
    {
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",true);
        root.create_database("db1");
        tsdb::database db1(root,"db1");
        tsdb::create_measurement(db1,"measurement1",test_fields);
        tsdb::measurement m1(db1,"measurement1");

        try
        {
            tsdb::open_or_create_and_lock_series(m1,"series1/dumb/name");
            tmock::abort("Expected invalid series exception.");
        }
        catch (const tsdb::invalid_series_exception&)
        {
        }

        try
        {
            tsdb::open_or_create_and_lock_series(m1,"series1 dumb name");
            tmock::abort("Expected invalid series exception.");
        }
        catch (const tsdb::invalid_series_exception&)
        {
        }

        try
        {
            tsdb::open_or_create_and_lock_series(m1,"series1\\dumb\\name");
            tmock::abort("Expected invalid series exception.");
        }
        catch (const tsdb::invalid_series_exception&)
        {
        }
    }

    TMOCK_TEST(test_create_new_series)
    {
        {
            tsdb::configuration c = {
                .chunk_size = 2*1024*1024,
                .wal_max_entries = 10240,
                .write_throttle_ns = 1000000000,
            };
            tsdb::create_root(".",c);
            tsdb::root root(".",true);
            root.create_database("db1");
            tsdb::database db1(root,"db1");
            tsdb::create_measurement(db1,"measurement1",test_fields);
            tsdb::measurement m1(db1,"measurement1");

            snapshot_auto_begin();

            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");

            snapshot_auto_end();

            auto dn = fd_table[swl.series_dir.fd].directory;
            validate_series_inodes(dn);
            assert_tree_fsynced(fs_root);
            TASSERT(dn->files["time_first"]->shared_locks);
            TASSERT(!dn->files["time_first"]->exclusive_locks);
            TASSERT(!dn->files["time_last"]->shared_locks);
            TASSERT(dn->files["time_last"]->exclusive_locks);
            tmock::assert_equiv(dn->files["time_first"]->get_data<uint64_t>(),
                                (uint64_t)1);
            tmock::assert_equiv(dn->files["time_last"]->get_data<uint64_t>(),
                                (uint64_t)0);
        }

        size_t no_series_count = 0;
        size_t found_series_count = 0;
#if DUMP_STATS
        printf("%zu snapshots from open_or_create_and_lock_series()\n",
               snapshots.size());
#endif
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",true);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            auto* mdn = fd_table[m1.dir.fd].directory;

            if (!mdn->subdirs.contains("series1"))
            {
                try
                {
                    tsdb::series_read_lock(m1,"series1");
                    tmock::abort("Expected no-such-series exception!");
                }
                catch (const tsdb::no_such_series_exception&)
                {
                    ++no_series_count;
                    continue;
                }
            }

            validate_series_inodes(mdn->subdirs["series1"]);
            assert_children_fsynced(mdn->subdirs["series1"]);
            tsdb::series_read_lock(m1,"series1");
            ++found_series_count;
        }
#if DUMP_STATS
        printf("found_series_count %zu no_series_count %zu\n",
               found_series_count,no_series_count);
#endif
        TASSERT(found_series_count);
        TASSERT(no_series_count);
    }

    TMOCK_TEST(test_write_lock)
    {
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",true);
        root.create_database("db1");
        tsdb::database db1(root,"db1");
        tsdb::create_measurement(db1,"measurement1",test_fields);
        tsdb::measurement m1(db1,"measurement1");

        dir_node* dn;
        {
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
            dn = fd_table[swl.series_dir.fd].directory;
        }

        auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
        TASSERT(dn == fd_table[swl.series_dir.fd].directory);
        TASSERT(dn->files["time_first"]->shared_locks);
        TASSERT(!dn->files["time_first"]->exclusive_locks);
        TASSERT(!dn->files["time_last"]->shared_locks);
        TASSERT(dn->files["time_last"]->exclusive_locks);
        tmock::assert_equiv(swl.time_first,
                            dn->files["time_first"]->get_data<uint64_t>());
        tmock::assert_equiv(swl.time_last,
                            dn->files["time_last"]->get_data<uint64_t>());
    }

    TMOCK_TEST(test_read_lock)
    {
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",true);
        root.create_database("db1");
        tsdb::database db1(root,"db1");
        tsdb::create_measurement(db1,"measurement1",test_fields);
        tsdb::measurement m1(db1,"measurement1");

        try
        {
            tsdb::series_read_lock(m1,"series1");
            tmock::abort("Expected no-such-series exception!");
        }
        catch (const tsdb::no_such_series_exception&)
        {
        }

        dir_node* dn;
        {
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
            dn = fd_table[swl.series_dir.fd].directory;
        }

        tsdb::series_read_lock srl(m1,"series1");
        TASSERT(dn == fd_table[srl.series_dir.fd].directory);
        TASSERT(dn->files["time_first"]->shared_locks);
        TASSERT(!dn->files["time_first"]->exclusive_locks);
        TASSERT(!dn->files["time_last"]->shared_locks);
        TASSERT(!dn->files["time_last"]->exclusive_locks);
        tmock::assert_equiv(srl.time_first,
                            dn->files["time_first"]->get_data<uint64_t>());
        tmock::assert_equiv(srl.time_last,
                            dn->files["time_last"]->get_data<uint64_t>());
    }

    TMOCK_TEST(test_total_lock)
    {
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",true);
        root.create_database("db1");
        tsdb::database db1(root,"db1");
        tsdb::create_measurement(db1,"measurement1",test_fields);
        tsdb::measurement m1(db1,"measurement1");

        try
        {
            tsdb::series_total_lock(m1,"series1");
            tmock::abort("Expected no-such-series exception!");
        }
        catch (const tsdb::no_such_series_exception&)
        {
        }

        dir_node* dn;
        {
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
            dn = fd_table[swl.series_dir.fd].directory;
        }

        tsdb::series_total_lock stl(m1,"series1");
        TASSERT(dn == fd_table[stl.series_dir.fd].directory);
        TASSERT(!dn->files["time_first"]->shared_locks);
        TASSERT(dn->files["time_first"]->exclusive_locks);
        TASSERT(!dn->files["time_last"]->shared_locks);
        TASSERT(!dn->files["time_last"]->exclusive_locks);
        tmock::assert_equiv(stl.time_first,
                            dn->files["time_first"]->get_data<uint64_t>());
        tmock::assert_equiv(stl.time_last,
                            dn->files["time_last"]->get_data<uint64_t>());
    }

    TMOCK_TEST(test_get_set_bitmap_bit)
    {
        uint64_t bitmap[] =
        {
            0x0000000000000000,
            0x0000100000000000,
            0x0000000000200000,
        };

        for (size_t i=0; i<64*3; ++i)
            TASSERT(tsdb::get_bitmap_bit(bitmap,i) == (i == 108 || i == 149));

        tsdb::set_bitmap_bit(bitmap,17,1);
        tsdb::set_bitmap_bit(bitmap,59,0);
        tsdb::set_bitmap_bit(bitmap,77,0);
        tsdb::set_bitmap_bit(bitmap,99,1);
        tsdb::set_bitmap_bit(bitmap,107,0);
        tmock::assert_equiv(bitmap[0],0x0000000000020000ULL);
        tmock::assert_equiv(bitmap[1],0x0000100800000000ULL);
        tmock::assert_equiv(bitmap[2],0x0000000000200000ULL);
    }
};

TMOCK_MAIN();
