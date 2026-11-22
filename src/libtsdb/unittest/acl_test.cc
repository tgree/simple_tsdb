// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../acl.h"
#include "../root.h"
#include <futil/fakefs/fakefs.h>
#include <tmock/tmock.h>

static const std::string test_acl_text =
    "# A comment.\n"
    "\n"
    "hub1   hub1_data  write\n"
    "alice  *          read\n"
    "alice  db1        delete,write\n"
    "  \n"
    "admin  *          all\n";

static void
expect_invalid(const std::string& text, size_t line_num)
{
    try
    {
        tsdb::acl::parse(text);
        TABORT("Expected invalid ACL file exception!");
    }
    catch (const tsdb::invalid_acl_file_exception& e)
    {
        TASSERT_EQUIV(e.line_num,line_num);
    }
}

static void
write_acl_file(const std::string& text)
{
    futil::directory root_dir(".");
    futil::file acl_fd(root_dir,"acl",O_CREAT | O_EXCL | O_RDWR,0660);
    acl_fd.write_all(text.c_str(),text.size());
}

static tsdb::root*
make_root()
{
    tsdb::create_root(".",tsdb::default_configuration);
    return new tsdb::root(".",true);
}

static std::string
acl_file_text()
{
    return fs_root->get_file("acl")->data_as_string();
}

class tmock_test
{
    TMOCK_TEST(test_parse_permissions)
    {
        TASSERT_EQUIV(tsdb::parse_permissions("read"),
                      (uint32_t)tsdb::PERM_READ);
        TASSERT_EQUIV(tsdb::parse_permissions("write"),
                      (uint32_t)tsdb::PERM_WRITE);
        TASSERT_EQUIV(tsdb::parse_permissions("delete"),
                      (uint32_t)tsdb::PERM_DELETE);
        TASSERT_EQUIV(tsdb::parse_permissions("all"),(uint32_t)tsdb::PERM_ALL);
        TASSERT_EQUIV(tsdb::parse_permissions("write,READ"),
                      (uint32_t)(tsdb::PERM_READ | tsdb::PERM_WRITE));
        TASSERT_EQUIV(tsdb::parse_permissions("read,write,delete"),
                      (uint32_t)tsdb::PERM_ALL);
        TASSERT_EQUIV(tsdb::parse_permissions("read,all"),
                      (uint32_t)tsdb::PERM_ALL);

        for (const char* bad : {"","read,","bogus","read,bogus",",write"})
            TASSERT_EQUIV(tsdb::parse_permissions(bad),(uint32_t)0);
    }

    TMOCK_TEST(test_permissions_to_string)
    {
        TASSERT_EQUIV(tsdb::permissions_to_string(tsdb::PERM_READ),"read");
        TASSERT_EQUIV(tsdb::permissions_to_string(tsdb::PERM_WRITE |
                                                  tsdb::PERM_DELETE),
                      "write,delete");
        TASSERT_EQUIV(tsdb::permissions_to_string(tsdb::PERM_ALL),"all");
    }

    TMOCK_TEST(test_parse_and_lookup)
    {
        auto a = tsdb::acl::parse(test_acl_text);
        TASSERT_EQUIV(a.entries.size(),4UL);

        // Entries are sorted by user, then database.
        TASSERT_EQUIV(a.entries[0].user,"admin");
        TASSERT_EQUIV(a.entries[1].user,"alice");
        TASSERT_EQUIV(a.entries[1].database,"*");
        TASSERT_EQUIV(a.entries[2].user,"alice");
        TASSERT_EQUIV(a.entries[2].database,"db1");
        TASSERT_EQUIV(a.entries[3].user,"hub1");

        TASSERT_EQUIV(a.permissions("hub1","hub1_data"),
                      (uint32_t)tsdb::PERM_WRITE);
        TASSERT_EQUIV(a.permissions("hub1","db1"),(uint32_t)0);
        TASSERT_EQUIV(a.permissions("hub1","*"),(uint32_t)0);

        // A user's "*" permissions add to their per-database ones.
        TASSERT_EQUIV(a.permissions("alice","db1"),(uint32_t)tsdb::PERM_ALL);
        TASSERT_EQUIV(a.permissions("alice","db2"),
                      (uint32_t)tsdb::PERM_READ);
        TASSERT_EQUIV(a.permissions("admin","anything"),
                      (uint32_t)tsdb::PERM_ALL);

        // Names match exactly.
        TASSERT_EQUIV(a.permissions("hub1","HUB1_DATA"),(uint32_t)0);
        TASSERT_EQUIV(a.permissions("HUB1","hub1_data"),(uint32_t)0);
        TASSERT_EQUIV(a.permissions("nobody","hub1_data"),(uint32_t)0);
    }

    TMOCK_TEST(test_parse_empty)
    {
        TASSERT_EQUIV(tsdb::acl::parse("").entries.size(),0UL);
        TASSERT_EQUIV(tsdb::acl::parse(tsdb::acl_file_header).entries.size(),
                      0UL);
    }

    TMOCK_TEST(test_parse_errors)
    {
        expect_invalid("hub1 hub1_data\n",1);
        expect_invalid("# Comment\nhub1 hub1_data write extra\n",2);
        expect_invalid("hub1 hub1_data write\nhub1 db2 bogus\n",2);
        expect_invalid("hub1 hub1_data write\n\nhub1 a/b read\n",3);
        expect_invalid("hub1 .. read\n",1);
        expect_invalid("hub1 hub1_data write\nhub1 hub1_data read\n",2);
        expect_invalid("hub1 * write\nhub1 * read\n",2);
    }

    TMOCK_TEST(test_to_string)
    {
        auto a = tsdb::acl::parse(test_acl_text);
        auto text = a.to_string();
        TASSERT_EQUIV(text,
            std::string(tsdb::acl_file_header) + "\n"
            "admin *         all\n"
            "alice *         read\n"
            "alice db1       write,delete\n"
            "hub1  hub1_data write\n");

        auto b = tsdb::acl::parse(text);
        TASSERT_EQUIV(b.entries.size(),a.entries.size());
        for (size_t i=0; i<a.entries.size(); ++i)
        {
            TASSERT_EQUIV(b.entries[i].user,a.entries[i].user);
            TASSERT_EQUIV(b.entries[i].database,a.entries[i].database);
            TASSERT_EQUIV(b.entries[i].perms,a.entries[i].perms);
        }

        TASSERT_EQUIV(tsdb::acl().to_string(),
                      std::string(tsdb::acl_file_header) + "\n");
    }

    TMOCK_TEST(test_load_acl)
    {
        tsdb::create_root(".",tsdb::default_configuration);
        tsdb::root root(".",true);
        TASSERT(!root.load_acl().has_value());

        write_acl_file(test_acl_text);
        auto a = root.load_acl();
        TASSERT(a.has_value());
        TASSERT_EQUIV(a->entries.size(),4UL);
        TASSERT_EQUIV(a->permissions("hub1","hub1_data"),
                      (uint32_t)tsdb::PERM_WRITE);
    }

    TMOCK_TEST(test_load_acl_invalid)
    {
        tsdb::create_root(".",tsdb::default_configuration);
        tsdb::root root(".",true);
        write_acl_file("hub1 hub1_data write\nhub1 db2\n");
        try
        {
            root.load_acl();
            TABORT("Expected invalid ACL file exception!");
        }
        catch (const tsdb::invalid_acl_file_exception& e)
        {
            TASSERT_EQUIV(e.line_num,2UL);
            TASSERT_EQUIV(std::string(e.what()),
                          "Invalid ACL file at line 2.");
        }
    }

    TMOCK_TEST(test_acl_grant_revoke)
    {
        tsdb::acl a;
        a.grant("hub2","hub2_data",tsdb::PERM_WRITE);
        a.grant("hub1","hub1_data",tsdb::PERM_READ);
        a.grant("hub1","hub1_data",tsdb::PERM_WRITE);
        a.grant("hub1","*",tsdb::PERM_READ);
        TASSERT_EQUIV(a.entries.size(),3UL);
        TASSERT_EQUIV(a.entries[0].user,"hub1");
        TASSERT_EQUIV(a.entries[0].database,"*");
        TASSERT_EQUIV(a.entries[1].database,"hub1_data");
        TASSERT_EQUIV(a.entries[1].perms,
                      (uint32_t)(tsdb::PERM_READ | tsdb::PERM_WRITE));
        TASSERT_EQUIV(a.entries[2].user,"hub2");

        a.revoke("hub1","hub1_data",tsdb::PERM_READ);
        TASSERT_EQUIV(a.entries[1].perms,(uint32_t)tsdb::PERM_WRITE);

        // Revoking on "*" only changes the "*" entry.
        a.revoke("hub1","*",tsdb::PERM_ALL);
        TASSERT_EQUIV(a.entries.size(),2UL);
        TASSERT_EQUIV(a.permissions("hub1","hub1_data"),
                      (uint32_t)tsdb::PERM_WRITE);

        // Revoking the last permission removes the entry.
        a.revoke("hub1","hub1_data",tsdb::PERM_WRITE);
        TASSERT_EQUIV(a.entries.size(),1UL);

        // Revoking something nobody holds does nothing.
        a.revoke("nobody","db1",tsdb::PERM_ALL);
        TASSERT_EQUIV(a.entries.size(),1UL);
    }

    TMOCK_TEST(test_enable_acls)
    {
        tsdb::root* root = make_root();
        TASSERT(root->enable_acls());
        TASSERT(!root->enable_acls());
        TASSERT_EQUIV(acl_file_text(),
                      std::string(tsdb::acl_file_header) + "\n");
        auto a = root->load_acl();
        TASSERT(a.has_value());
        TASSERT(a->entries.empty());
        assert_tree_fsynced(fs_root);
        TASSERT_EQUIV(fs_root->subdirs["tmp"]->files.size(),0UL);
        delete root;
    }

    TMOCK_TEST(test_grant_revoke_need_acls)
    {
        tsdb::root* root = make_root();
        root->add_user("hub1","hub1pw");
        try
        {
            root->grant("hub1","hub1_data",tsdb::PERM_WRITE);
            TABORT("Expected ACLs disabled exception!");
        }
        catch (const tsdb::acls_disabled_exception&)
        {
        }
        try
        {
            root->revoke("hub1","hub1_data",tsdb::PERM_WRITE);
            TABORT("Expected ACLs disabled exception!");
        }
        catch (const tsdb::acls_disabled_exception&)
        {
        }
        TASSERT(!root->load_acl().has_value());
        delete root;
    }

    TMOCK_TEST(test_grant_and_revoke)
    {
        tsdb::root* root = make_root();
        root->add_user("hub1","hub1pw");
        root->add_user("alice","alicepw");
        TASSERT(root->enable_acls());

        // The database doesn't have to exist yet.
        root->grant("hub1","hub1_data",tsdb::PERM_WRITE);
        root->grant("alice","*",tsdb::PERM_READ);
        root->grant("alice","*",tsdb::PERM_DELETE);
        TASSERT_EQUIV(acl_file_text(),
                      std::string(tsdb::acl_file_header) + "\n"
                      "alice *         read,delete\n"
                      "hub1  hub1_data write\n");
        assert_tree_fsynced(fs_root);
        TASSERT_EQUIV(fs_root->subdirs["tmp"]->files.size(),0UL);

        auto a = root->load_acl();
        TASSERT_EQUIV(a->permissions("hub1","hub1_data"),
                      (uint32_t)tsdb::PERM_WRITE);
        TASSERT_EQUIV(a->permissions("alice","hub1_data"),
                      (uint32_t)(tsdb::PERM_READ | tsdb::PERM_DELETE));

        root->revoke("alice","*",tsdb::PERM_READ | tsdb::PERM_DELETE);
        a = root->load_acl();
        TASSERT_EQUIV(a->entries.size(),1UL);
        TASSERT_EQUIV(a->permissions("alice","hub1_data"),(uint32_t)0);

        // Unknown users and invalid database names are refused, leaving the
        // file alone.
        try
        {
            root->grant("nobody","hub1_data",tsdb::PERM_READ);
            TABORT("Expected no such user exception!");
        }
        catch (const tsdb::no_such_user_exception&)
        {
        }
        for (const char* bad : {"a/b","..","","hub 1"})
        {
            try
            {
                root->grant("hub1",bad,tsdb::PERM_READ);
                TABORT("Expected invalid database exception!");
            }
            catch (const tsdb::invalid_database_exception&)
            {
            }
        }
        TASSERT_EQUIV(acl_file_text(),
                      std::string(tsdb::acl_file_header) + "\n"
                      "hub1 hub1_data write\n");
        delete root;
    }

    TMOCK_TEST(test_acl_changes_hold_passwd_lock)
    {
        // The acl file may only be replaced while passwd.lock is held
        // exclusively, so that two tsdbcli2 sessions can't both read the old
        // file and have the second rename throw away the first's change.
        tsdb::root* root = make_root();
        root->add_user("hub1","hub1pw");
        file_node* lock_fn = fs_root->get_file("passwd.lock");
        size_t renames = 0;
        futil::hook_func = [&](uint32_t id)
        {
            if (id == futil::HI_RENAMEAT)
            {
                TASSERT_EQUIV(lock_fn->exclusive_locks,1UL);
                ++renames;
            }
        };
        TASSERT(root->enable_acls());
        root->grant("hub1","hub1_data",tsdb::PERM_WRITE);
        root->revoke("hub1","hub1_data",tsdb::PERM_WRITE);
        futil::hook_func = NULL;
        TASSERT_EQUIV(renames,3UL);
        TASSERT_EQUIV(lock_fn->exclusive_locks,0UL);
        delete root;
    }
};

TMOCK_MAIN();
