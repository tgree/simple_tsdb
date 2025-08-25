// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../measurement.h"
#include "../database.h"
#include <futil/fakefs/fakefs.h>
#include <tmock/tmock.h>

const std::vector<tsdb::schema_entry> test_fields =
{
    {tsdb::FT_U32,SCHEMA_VERSION,0, 0,"field1"},
    {tsdb::FT_F64,SCHEMA_VERSION,1, 4,"field2"},
    {tsdb::FT_F32,SCHEMA_VERSION,2,12,"field3"},
};
const std::vector<tsdb::schema_entry> test_fields_different_name =
{
    {tsdb::FT_U32,SCHEMA_VERSION,0, 0,"fieldA"},
    {tsdb::FT_F64,SCHEMA_VERSION,1, 4,"fieldB"},
    {tsdb::FT_F32,SCHEMA_VERSION,2,12,"fieldC"},
};
const std::vector<tsdb::schema_entry> test_fields2 =
{
    {tsdb::FT_F32,SCHEMA_VERSION, 0,0,"2field1"},
    {tsdb::FT_BOOL,SCHEMA_VERSION,1,4,"2field2"},
};
const std::vector<tsdb::schema_entry> duped_fields =
{
    {tsdb::FT_F32,SCHEMA_VERSION, 0,0,"field1"},
    {tsdb::FT_BOOL,SCHEMA_VERSION,1,4,"field1"},
};

class tmock_test
{
    TMOCK_TEST(test_create_measurement)
    {
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",true);
        root.create_database("db1");
        assert_tree_fsynced(fs_root);
        tsdb::database db1(root,"db1");

        try
        {
            tsdb::measurement m(db1,"measurement1");
            tmock::abort("Expected no-such-measurement exception!");
        }
        catch (const tsdb::no_such_measurement_exception&)
        {
        }

        tsdb::create_measurement(db1,"measurement1",test_fields);
        assert_tree_fsynced(fs_root);
        tsdb::create_measurement(db1,"measurement1",test_fields);
        assert_tree_fsynced(fs_root);

        try
        {
            tsdb::create_measurement(db1,"measurement1",test_fields2);
            tmock::abort("Expected measurement-exists exception!");
        }
        catch (const tsdb::measurement_exists_exception&)
        {
        }

        try
        {
            tsdb::create_measurement(db1,"measurement1",
                                     test_fields_different_name);
            tmock::abort("Expected measurement-exists exception!");
        }
        catch (const tsdb::measurement_exists_exception&)
        {
        }

        tsdb::create_measurement(db1,"measurement2",test_fields2);
        assert_tree_fsynced(fs_root);

        try
        {
            tsdb::create_measurement(db1,"measurement3",duped_fields);
            tmock::abort("Expected duplicate-field exception!");
        }
        catch (const tsdb::duplicate_field_exception&)
        {
        }

        tsdb::measurement m1(db1,"measurement1");
        tsdb::measurement m2(db1,"measurement2");

        TASSERT(fd_table[m1.dir.fd].directory ==
                    fs_root->subdirs["databases"]->subdirs["db1"]->
                    subdirs["measurement1"]);
        TASSERT(fd_table[m2.dir.fd].directory ==
                    fs_root->subdirs["databases"]->subdirs["db1"]->
                    subdirs["measurement2"]);

        auto schema_fn1 = fd_table[m1.schema_fd.fd].file;
        auto schema_fn2 = fd_table[m2.schema_fd.fd].file;
        TASSERT(schema_fn1 ==
                    fs_root->subdirs["databases"]->subdirs["db1"]->
                    subdirs["measurement1"]->files["schema"]);
        TASSERT(schema_fn2 ==
                    fs_root->subdirs["databases"]->subdirs["db1"]->
                    subdirs["measurement2"]->files["schema"]);
        TASSERT(m1.schema_mapping.addr == &schema_fn1->data[0]);
        TASSERT(m1.schema_mapping.len == schema_fn1->data.size());
        TASSERT(m2.schema_mapping.addr == &schema_fn2->data[0]);
        TASSERT(m2.schema_mapping.len == schema_fn2->data.size());
        tmock::assert_equiv(m1.fields.size(),test_fields.size());
        tmock::assert_equiv(m2.fields.size(),test_fields2.size());
        for (size_t i=0; i<m1.fields.size(); ++i)
        {
            TASSERT(!memcmp(&test_fields[i],&m1.fields[i],
                            sizeof(tsdb::schema_entry)));
        }
        for (size_t i=0; i<m2.fields.size(); ++i)
        {
            TASSERT(!memcmp(&test_fields2[i],&m2.fields[i],
                            sizeof(tsdb::schema_entry)));
        }

        tmock::assert_equiv(m1.list_series().size(),0UL);
        tmock::assert_equiv(m2.list_series().size(),0UL);

        tmock::assert_equiv(fs_root->subdirs["tmp"]->subdirs.size(),0UL);
        tmock::assert_equiv(fs_root->subdirs["tmp"]->files.size(),0UL);
    }

#if 0
    TMOCK_TEST(test_create_collision)
    {
        // tsdb::create_measurement() races against other people trying to
        // create the measurement at the same time.  The process is that we
        // first create the measurement in the tmp/ directory and then try to
        // do an atomic rename into the target database.  However, someone else
        // could have already done this while we were working, in which case
        // the rename fails and the code loops around and tries to open the
        // presumably now-present measurement.
        //
        // It would be nice to unittest that somehow, although it doesn't
        // really exercise any extra code paths.
    }
#endif

    TMOCK_TEST(test_create_consistency)
    {
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);

        {
            tsdb::root root(".",true);
            root.create_database("db1");
            tsdb::database db1(root,"db1");

            snapshot_fs();
            snapshot_auto_begin();

            tsdb::create_measurement(db1,"measurement1",test_fields);

            snapshot_auto_end();
        }

        bool found_nomeasurement = false;
        bool found_measurement = false;
        auto dn_final __UNUSED__ = fs_root;
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",true);
            TASSERT(fd_table[root.root_dir.fd].directory == dn);
            tsdb::database db1(root,"db1");
            try
            {
                tsdb::measurement m(db1,"measurement1");
                found_measurement = true;

                tmock::assert_equiv(m.fields.size(),test_fields.size());
                for (size_t i=0; i<m.fields.size(); ++i)
                {
                    TASSERT(!memcmp(&test_fields[i],&m.fields[i],
                                    sizeof(tsdb::schema_entry)));
                }
                tmock::assert_equiv(m.list_series().size(),0UL);
                tmock::assert_equiv(dn->subdirs["tmp"]->subdirs.size(),0UL);
            }
            catch (const tsdb::no_such_measurement_exception&)
            {
                found_nomeasurement = true;
                continue;
            }
        }
        TASSERT(found_nomeasurement);
        TASSERT(found_measurement);
    }

    TMOCK_TEST(test_missing_schema_file)
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

        // Just an empty measurement file.  This should never happen, but it
        // also shouldn't crash us if we try to do a create.
        futil::mkdir(db1.dir,"measurement1",0777);

        try
        {
            tsdb::create_measurement(db1,"measurement1",test_fields);
            tmock::abort("Expected corrupt measurement exception!");
        }
        catch (const tsdb::corrupt_measurement_exception&)
        {
        }
    }

    TMOCK_TEST(test_compute_write_chunk_len)
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
        tsdb::create_measurement(db1,"measurement2",test_fields2);

        // A write chunk is constructed as follows:
        //  1. Timestamps
        //  2. Fields
        //      1. Bitmap
        //          1. Bitmap offset bits
        //          2. Bitmap bits
        //          3. Padding to 64 bits
        //      2. Field data, padded to 64 bits
        tsdb::measurement m1(db1,"measurement1");
        tmock::assert_equiv<size_t>(m1.compute_write_chunk_len(1),
                                    1*8 + (8 + 8) + (8 + 8) + (8 + 8));
        tmock::assert_equiv<size_t>(m1.compute_write_chunk_len(1,70),
                                    1*8 + (16 + 8) + (16 + 8) + (16 + 8));
        tmock::assert_equiv<size_t>(m1.compute_write_chunk_len(2),
                                    2*8 + (8 + 8) + (8 + 16) + (8 + 8));
        tmock::assert_equiv<size_t>(m1.compute_write_chunk_len(3),
                                    3*8 + (8 + 16) + (8 + 24) + (8 + 16));
        tmock::assert_equiv<size_t>(m1.compute_write_chunk_len(65),
                                    65*8 + (16 + 33*8) + (16 + 65*8) +
                                    (16 + 33*8));

        tsdb::measurement m2(db1,"measurement2");
        tmock::assert_equiv<size_t>(m2.compute_write_chunk_len(1),
                                    1*8 + (8 + 8) + (8 + 8));
        tmock::assert_equiv<size_t>(m2.compute_write_chunk_len(1,70),
                                    1*8 + (16 + 8) + (16 + 8));
        tmock::assert_equiv<size_t>(m2.compute_write_chunk_len(2),
                                    2*8 + (8 + 8) + (8 + 8));
        tmock::assert_equiv<size_t>(m2.compute_write_chunk_len(3),
                                    3*8 + (8 + 16) + (8 + 8));
        tmock::assert_equiv<size_t>(m2.compute_write_chunk_len(65),
                                    65*8 + (16 + 33*8) + (16 + 9*8));
    }

    TMOCK_TEST(test_max_points_for_data_len)
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
        tsdb::create_measurement(db1,"measurement2",test_fields2);

        // Given data length X, how many points can we stuff inside it?
        tsdb::measurement m1(db1,"measurement1");
        tmock::assert_equiv(m1.max_points_for_data_len(1559),0UL);
        tmock::assert_equiv(m1.max_points_for_data_len(1560),64UL);
        tmock::assert_equiv(m1.max_points_for_data_len(3119),64UL);
        tmock::assert_equiv(m1.max_points_for_data_len(3120),128UL);

        tsdb::measurement m2(db1,"measurement2");
        tmock::assert_equiv(m2.max_points_for_data_len(847),0UL);
        tmock::assert_equiv(m2.max_points_for_data_len(848),64UL);
        tmock::assert_equiv(m2.max_points_for_data_len(1695),64UL);
        tmock::assert_equiv(m2.max_points_for_data_len(1696),128UL);
    }

    TMOCK_TEST(test_gen_entries)
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
        tsdb::create_measurement(db1,"measurement2",test_fields2);
        tsdb::measurement m1(db1,"measurement1");
        tsdb::measurement m2(db1,"measurement2");

        try
        {
            m1.gen_entries({"field1","alpha"});
            tmock::abort("Expected no such field exception!");
        }
        catch (const tsdb::no_such_field_exception&)
        {
        }

        try
        {
            m1.gen_entries({"field1","field2","field1","field3"});
            tmock::abort("Expected duplicate field exception!");
        }
        catch (const tsdb::duplicate_field_exception&)
        {
        }

        auto entries = m1.gen_entries({});
        tmock::assert_equiv(entries.size(),test_fields.size());
        for (size_t i=0; i<entries.size(); ++i)
            tmock::assert_mem_same(*entries[i],test_fields[i]);

        entries = m1.gen_entries({"field3","field1"});
        tmock::assert_equiv(entries.size(),2UL);
        tmock::assert_mem_same(*entries[0],test_fields[2]);
        tmock::assert_mem_same(*entries[1],test_fields[0]);

        entries = m2.gen_entries({"2field2"});
        tmock::assert_equiv(entries.size(),1UL);
        tmock::assert_mem_same(*entries[0],test_fields2[1]);
    }
};

TMOCK_MAIN();
