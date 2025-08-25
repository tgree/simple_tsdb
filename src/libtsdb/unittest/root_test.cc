// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../root.h"
#include "../exception.h"
#include <futil/fakefs/fakefs.h>
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_create_root)
    {
        try
        {
            tsdb::root root(".",true);
            tmock::abort("Expected exception because root doesn't exist!");
        }
        catch (const tsdb::not_a_tsdb_root&)
        {
        }

        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };

        tsdb::create_root(".",c);

        tmock::assert_equiv(fs_root->subdirs.size(),2UL);
        tmock::assert_equiv(fs_root->files.size(),3UL);
        TASSERT(fs_root->subdirs.contains("databases"));
        TASSERT(fs_root->subdirs.contains("tmp"));
        TASSERT(fs_root->files.contains("passwd.lock"));
        TASSERT(fs_root->files.contains("passwd"));
        TASSERT(fs_root->files.contains("config.txt"));
        TASSERT(fs_root->subdirs["databases"]->subdirs.empty());
        TASSERT(fs_root->subdirs["databases"]->files.empty());
        TASSERT(fs_root->subdirs["tmp"]->subdirs.empty());
        TASSERT(fs_root->subdirs["tmp"]->files.empty());
        TASSERT(fs_root->files["passwd.lock"]->data.empty());
        TASSERT(fs_root->files["passwd"]->data.empty());
        assert_tree_fsynced(fs_root);

        const char* expected_config =
            "chunk_size        2M\n"
            "wal_max_entries   10240\n"
            "write_throttle_ns 1000000000\n";
        tmock::assert_equiv(
            fs_root->files["config.txt"]->data_as_string(),
            expected_config);

        try
        {
            tsdb::create_root(".",c);
            tmock::abort("Expected exception because root exists!");
        }
        catch (const tsdb::init_io_error_exception& e)
        {
            tmock::assert_equiv(e.errnov,EEXIST);
        }

        tsdb::root root(".",true);
        tmock::assert_equiv(root.config.chunk_size,2UL*1024UL*1024UL);
        tmock::assert_equiv(root.config.wal_max_entries,10240UL);
        tmock::assert_equiv(root.config.write_throttle_ns,1000000000UL);
        TASSERT(!root.database_exists("test_db"));
        TASSERT(fd_table[root.root_dir.fd].directory == fs_root);
        TASSERT(fd_table[root.tmp_dir.fd].directory ==
                fs_root->subdirs["tmp"]);
        TASSERT(fd_table[root.databases_dir.fd].directory ==
                fs_root->subdirs["databases"]);
    }

    TMOCK_TEST(test_users)
    {
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);

        tsdb::root root(".",true);
        root.add_user("user1","password1");
        assert_tree_fsynced(fs_root);
        root.add_user("user2","password2");
        assert_tree_fsynced(fs_root);
        tmock::assert_equiv(fs_root->files["passwd"]->data_as_string(),
                "user1 A41620DDCC3ED2BA56453ED0D217C1BFAD091A8C80692515"
                "EEB941BAF361C8ED3095ACEAE98EC19A2279D54905B4368257DB9B"
                "1F7FB62081CAA72C6DF2EC2819\n"
                "user2 FA24F1BF62F859DB34B9417EFC5FDB4EAFDC5ACE6ACFA7AF"
                "DB068F80A9AFD9C001AF3EA42D9F5C4B16B32DA0C3D4799947B81C"
                "7FAADFC948372564FDA8E585F4\n");

        try
        {
            root.add_user("user1","blah");
            tmock::abort("Expected duplicate user exception!");
        }
        catch (const tsdb::user_exists_exception&)
        {
        }

        try
        {
            root.add_user("user2","blah");
            tmock::abort("Expected duplicate user exception!");
        }
        catch (const tsdb::user_exists_exception&)
        {
        }

        TASSERT(root.verify_user("user1","password1"));
        TASSERT(root.verify_user("user2","password2"));
        TASSERT(!root.verify_user("user1","password2"));
        TASSERT(!root.verify_user("user2","password1"));
        TASSERT(!root.verify_user("user1","passwor"));
        TASSERT(!root.verify_user("user1","password12345"));

        try
        {
            root.verify_user("user12","password1");
            tmock::abort("Expected no-such-user exception!");
        }
        catch (const tsdb::no_such_user_exception&)
        {
        }

        try
        {
            root.verify_user("user","password1");
            tmock::abort("Expected no-such-user exception!");
        }
        catch (const tsdb::no_such_user_exception&)
        {
        }
    }

    TMOCK_TEST(test_create_databases)
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
        root.create_database("db2");
        assert_tree_fsynced(fs_root);
        root.create_database("db3");
        assert_tree_fsynced(fs_root);
        TASSERT(fs_root->subdirs["databases"]->subdirs.contains("db1"));
        TASSERT(fs_root->subdirs["databases"]->subdirs.contains("db2"));
        TASSERT(fs_root->subdirs["databases"]->subdirs.contains("db3"));

        TASSERT(root.database_exists("db1"));
        TASSERT(root.database_exists("db2"));
        TASSERT(root.database_exists("db3"));
        TASSERT(!root.database_exists("db4"));
        TASSERT(!root.database_exists("db"));

        auto db_list = root.list_databases();
        tmock::assert_equiv(db_list.size(),3UL);
        tmock::assert_equiv(db_list[0],"db1");
        tmock::assert_equiv(db_list[1],"db2");
        tmock::assert_equiv(db_list[2],"db3");
    }
};

TMOCK_MAIN();
