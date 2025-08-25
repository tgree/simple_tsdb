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
            tmock::abort("Expected no such database exception!");
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
            tmock::abort("Expected no such database exception!");
        }
        catch (const tsdb::no_such_database_exception&)
        {
        }

        tsdb::database db1(root,"db1");
        tsdb::database db2(root,"db1");
        tsdb::database db3(root,"db1");
    }
};

TMOCK_MAIN();
