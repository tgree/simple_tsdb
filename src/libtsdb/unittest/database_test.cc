// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../database.h"
#include <futil/fakefs/fakefs.h>
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_tsdb_database)
    {
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",true);
        
        try
        {
            tsdb::database db(root,"db1");
            TABORT("Expected no such database exception!");
        }
        catch (const tsdb::no_such_database_exception&)
        {
        }

        root.create_database("db1");
        assert_tree_fsynced(fs_root);
        root.create_database("db2");
        assert_tree_fsynced(fs_root);
        root.create_database("db3");
        assert_tree_fsynced(fs_root);
        
        try
        {
            tsdb::database db(root,"db");
            TABORT("Expected no such database exception!");
        }
        catch (const tsdb::no_such_database_exception&)
        {
        }

        tsdb::database db1(root,"db1");
        tsdb::database db2(root,"db1");
        tsdb::database db3(root,"db1");
    }

    TMOCK_TEST(test_tsdb_database_bad_names)
    {
        // Each of these names either reaches outside the databases directory,
        // isn't a single path component, or contains whitespace, a backslash,
        // a control character or a byte outside 7-bit ASCII.  None of them
        // should be usable as a database name.
        static const std::string bad_names[] = {
            "../databases/db1",
            "db1/../db2",
            "",
            ".",
            "..",
            "db1/",
            "/db1",
            "*",    // Reserved: ACLs use it to mean every database.
            std::string("db1\0x",5),
            "db 1",
            "db\\1",
            "db\t1",
            "db\n1",
            "db\x1b[1m",
            "db\x7f",
            "db\xc3\xa9",
        };

        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",true);
        root.create_database("db1");
        root.create_database("db2");

        for (const auto& name : bad_names)
        {
            try
            {
                tsdb::database db(root,name);
                TABORT("Expected invalid database exception!");
            }
            catch (const tsdb::invalid_database_exception&)
            {
            }

            try
            {
                root.database_exists(name);
                TABORT("Expected invalid database exception!");
            }
            catch (const tsdb::invalid_database_exception&)
            {
            }

            try
            {
                root.create_database(name);
                TABORT("Expected invalid database exception!");
            }
            catch (const tsdb::invalid_database_exception&)
            {
            }
        }
    }

    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(test_tsdb_database_exact_case)
    {
        // On a case-insensitive file system, a database is reachable only by
        // the name it was created with: "DB1" must not open "db1", and
        // creating "DB1" must report a case conflict.
        fakefs_case_insensitive = true;
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",true);
        root.create_database("db1");
        tsdb::database db1(root,"db1");
        TASSERT(root.database_exists("db1"));

        for (const char* name : {"DB1","Db1","dB1"})
        {
            try
            {
                tsdb::database db(root,name);
                TABORT("Expected no such database exception!");
            }
            catch (const tsdb::no_such_database_exception&)
            {
            }

            TASSERT(!root.database_exists(name));

            try
            {
                root.create_database(name);
                TABORT("Expected name case conflict exception!");
            }
            catch (const tsdb::name_case_conflict_exception&)
            {
            }
        }

        auto dbs = root.list_databases();
        TASSERT_EQUIV(dbs.size(),1UL);
        TASSERT_EQUIV(dbs[0],"db1");
    }

    TMOCK_TEST(test_tsdb_database_case_sensitive)
    {
        // On a case-sensitive file system, names that differ only in case are
        // separate databases.
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",true);
        root.create_database("db1");
        root.create_database("DB1");
        TASSERT(root.database_exists("db1"));
        TASSERT(root.database_exists("DB1"));
        TASSERT_EQUIV(root.list_databases().size(),2UL);

        tsdb::database db1(root,"db1");
        tsdb::database DB1(root,"DB1");
        TASSERT(fd_table[db1.dir.fd].directory !=
                fd_table[DB1.dir.fd].directory);
    }
};

TMOCK_MAIN();
