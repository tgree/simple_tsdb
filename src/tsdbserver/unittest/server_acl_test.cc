// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../server.h"
#include <futil/fakefs/fakefs.h>
#include <tmock/tmock.h>

static const command_token read_commands[] =
{
    CT_LIST_MEASUREMENTS,
    CT_LIST_SERIES,
    CT_ACTIVE_SERIES,
    CT_COUNT_POINTS,
    CT_SELECT_POINTS_LIMIT,
    CT_SELECT_POINTS_LAST,
    CT_SUM_POINTS,
    CT_INTEGRATE_POINTS,
    CT_COMPUTE_POINTS_LIMIT,
    CT_COMPUTE_POINTS_LAST,
    CT_SUM_COMPUTE_POINTS,
};

static const command_token write_commands[] =
{
    CT_CREATE_MEASUREMENT,
    CT_WRITE_POINTS,
};

static const command_token unrestricted_commands[] =
{
    CT_GET_VERSION,
    CT_NOP,
    CT_LIST_DATABASES,
};

static std::vector<parsed_data_token>
db_tokens(const std::string& database)
{
    // The parsed tokens of a command whose first token is DT_DATABASE, as
    // every command that names a database has.
    parsed_data_token pdt;
    pdt.type = DT_DATABASE;
    pdt.len = database.size();
    char* data = (char*)malloc(database.size());
    memcpy(data,database.data(),database.size());
    pdt.data = data;

    std::vector<parsed_data_token> tokens;
    tokens.push_back(std::move(pdt));
    return tokens;
}

static bool
is_allowed(const std::string& user, command_token ct,
    const std::string& database)
{
    auto tokens = database.empty() ? std::vector<parsed_data_token>() :
                                     db_tokens(database);
    try
    {
        authorize_command(user,ct,tokens);
        return true;
    }
    catch (const tsdb::permission_denied_exception&)
    {
        return false;
    }
}

static void
make_root()
{
    tsdb::create_root(".",tsdb::default_configuration);
    root = new tsdb::root(".",true);
    for (const char* user : {"reader","writer","deleter","admin","db1admin",
                             "starreader","nobody"})
    {
        root->add_user(user,"pw");
    }
    root->create_database("db1");
    root->create_database("db2");
}

static void
make_grants()
{
    root->grant("reader","db1",tsdb::PERM_READ);
    root->grant("writer","db1",tsdb::PERM_WRITE);
    root->grant("deleter","db1",tsdb::PERM_DELETE);
    root->grant("admin","*",tsdb::PERM_ALL);
    root->grant("db1admin","db1",tsdb::PERM_ALL);
    root->grant("starreader","*",tsdb::PERM_READ);
}

class tmock_test
{
    TMOCK_TEST(test_no_user_may_do_anything)
    {
        // The plain TCP port has no user and isn't restricted, even though
        // a new root has access control on with nothing granted.
        make_root();
        for (auto ct : read_commands)
            TASSERT(is_allowed("",ct,"db1"));
        for (auto ct : write_commands)
            TASSERT(is_allowed("",ct,"db1"));
        TASSERT(is_allowed("",CT_GET_SCHEMA,"db1"));
        TASSERT(is_allowed("",CT_DELETE_POINTS,"db1"));
        TASSERT(is_allowed("",CT_CREATE_DATABASE,"db3"));
    }

    TMOCK_TEST(test_acls_off)
    {
        // A root without an acl file lets every user do anything, as before
        // access control existed.
        make_root();
        futil::unlink_if_exists(root->root_dir,"acl");
        for (auto ct : read_commands)
            TASSERT(is_allowed("nobody",ct,"db1"));
        for (auto ct : write_commands)
            TASSERT(is_allowed("nobody",ct,"db1"));
        TASSERT(is_allowed("nobody",CT_DELETE_POINTS,"db1"));
        TASSERT(is_allowed("nobody",CT_CREATE_DATABASE,"db3"));
    }

    TMOCK_TEST(test_unrestricted_commands)
    {
        make_root();
        for (auto ct : unrestricted_commands)
            TASSERT(is_allowed("nobody",ct,""));
    }

    TMOCK_TEST(test_nothing_granted)
    {
        make_root();
        make_grants();
        for (auto ct : read_commands)
            TASSERT(!is_allowed("nobody",ct,"db1"));
        for (auto ct : write_commands)
            TASSERT(!is_allowed("nobody",ct,"db1"));
        TASSERT(!is_allowed("nobody",CT_GET_SCHEMA,"db1"));
        TASSERT(!is_allowed("nobody",CT_DELETE_POINTS,"db1"));
        TASSERT(!is_allowed("nobody",CT_CREATE_DATABASE,"db3"));
    }

    TMOCK_TEST(test_read)
    {
        make_root();
        make_grants();
        for (auto ct : read_commands)
        {
            TASSERT(is_allowed("reader",ct,"db1"));
            TASSERT(!is_allowed("reader",ct,"db2"));
        }
        TASSERT(is_allowed("reader",CT_GET_SCHEMA,"db1"));
        for (auto ct : write_commands)
            TASSERT(!is_allowed("reader",ct,"db1"));
        TASSERT(!is_allowed("reader",CT_DELETE_POINTS,"db1"));
        TASSERT(!is_allowed("reader",CT_CREATE_DATABASE,"db3"));
    }

    TMOCK_TEST(test_write)
    {
        // Write also allows GET SCHEMA, which tsdbreflector needs.
        make_root();
        make_grants();
        for (auto ct : write_commands)
        {
            TASSERT(is_allowed("writer",ct,"db1"));
            TASSERT(!is_allowed("writer",ct,"db2"));
        }
        TASSERT(is_allowed("writer",CT_GET_SCHEMA,"db1"));
        for (auto ct : read_commands)
            TASSERT(!is_allowed("writer",ct,"db1"));
        TASSERT(!is_allowed("writer",CT_DELETE_POINTS,"db1"));
        TASSERT(!is_allowed("writer",CT_CREATE_DATABASE,"db3"));
    }

    TMOCK_TEST(test_delete)
    {
        make_root();
        make_grants();
        TASSERT(is_allowed("deleter",CT_DELETE_POINTS,"db1"));
        TASSERT(!is_allowed("deleter",CT_DELETE_POINTS,"db2"));
        for (auto ct : read_commands)
            TASSERT(!is_allowed("deleter",ct,"db1"));
        for (auto ct : write_commands)
            TASSERT(!is_allowed("deleter",ct,"db1"));
        TASSERT(!is_allowed("deleter",CT_GET_SCHEMA,"db1"));
    }

    TMOCK_TEST(test_star_grants)
    {
        // Grants on "*" cover every database, including ones that don't
        // exist yet.  Only all permissions on "*" allow CREATE DATABASE.
        make_root();
        make_grants();
        for (const char* db : {"db1","db2","db3"})
        {
            for (auto ct : read_commands)
            {
                TASSERT(is_allowed("starreader",ct,db));
                TASSERT(is_allowed("admin",ct,db));
            }
            for (auto ct : write_commands)
            {
                TASSERT(!is_allowed("starreader",ct,db));
                TASSERT(is_allowed("admin",ct,db));
            }
            TASSERT(is_allowed("admin",CT_DELETE_POINTS,db));
        }
        TASSERT(is_allowed("admin",CT_CREATE_DATABASE,"db3"));
        TASSERT(!is_allowed("starreader",CT_CREATE_DATABASE,"db3"));
    }

    TMOCK_TEST(test_all_on_one_database)
    {
        // All permissions on one database allow everything there, but not
        // CREATE DATABASE.
        make_root();
        make_grants();
        for (auto ct : read_commands)
            TASSERT(is_allowed("db1admin",ct,"db1"));
        for (auto ct : write_commands)
            TASSERT(is_allowed("db1admin",ct,"db1"));
        TASSERT(is_allowed("db1admin",CT_DELETE_POINTS,"db1"));
        TASSERT(!is_allowed("db1admin",CT_SELECT_POINTS_LIMIT,"db2"));
        TASSERT(!is_allowed("db1admin",CT_CREATE_DATABASE,"db3"));
    }

    TMOCK_TEST(test_exact_names)
    {
        // Database names match exactly, so another spelling or an
        // unvalidated path doesn't match a grant.
        make_root();
        make_grants();
        for (const char* db : {"DB1","Db1","db1/","./db1","db1/../db1"})
            TASSERT(!is_allowed("reader",CT_SELECT_POINTS_LIMIT,db));
    }

    TMOCK_TEST(test_changes_take_effect_at_once)
    {
        // tsdbserver reads the acl file for every command, so grants and
        // revokes apply to connections that are already open.
        make_root();
        TASSERT(!is_allowed("writer",CT_WRITE_POINTS,"db1"));
        root->grant("writer","db1",tsdb::PERM_WRITE);
        TASSERT(is_allowed("writer",CT_WRITE_POINTS,"db1"));
        root->revoke("writer","db1",tsdb::PERM_WRITE);
        TASSERT(!is_allowed("writer",CT_WRITE_POINTS,"db1"));
    }

    TMOCK_TEST(test_invalid_acl_file)
    {
        // A corrupt acl file refuses every authenticated command that names a
        // database, saying why.  The plain TCP port and commands that don't
        // name a database still work.
        make_root();
        futil::unlink_if_exists(root->root_dir,"acl");
        futil::file acl_fd(root->root_dir,"acl",O_CREAT | O_EXCL | O_RDWR,
                           0660);
        acl_fd.write_all("admin * everything\n",19);

        try
        {
            authorize_command("admin",CT_SELECT_POINTS_LIMIT,db_tokens("db1"));
            TABORT("Expected invalid ACL file exception!");
        }
        catch (const tsdb::invalid_acl_file_exception& e)
        {
            TASSERT_EQUIV(e.line_num,1UL);
        }
        TASSERT(is_allowed("admin",CT_NOP,""));
        TASSERT(is_allowed("",CT_SELECT_POINTS_LIMIT,"db1"));
    }

    TMOCK_TEST(test_unknown_command)
    {
        // A command authorize_command() doesn't know is refused, even to a
        // user with every permission, so that a new command can't be
        // allowed by accident.
        make_root();
        make_grants();
        TASSERT(!is_allowed("admin",(command_token)0x12345678,"db1"));
    }

    TMOCK_TEST(test_missing_database_token)
    {
        // A command that should name a database but somehow doesn't is
        // refused rather than checked against an empty name.
        make_root();
        make_grants();
        TASSERT(!is_allowed("admin",CT_SELECT_POINTS_LIMIT,""));
    }

    TMOCK_TEST(test_list_visible_databases)
    {
        // LIST DATABASES shows a user only the databases they hold some
        // permission on.
        make_root();
        make_grants();
        root->create_database("db3");
        const std::vector<std::string> all = {"db1","db2","db3"};
        const std::vector<std::string> db1 = {"db1"};
        const std::vector<std::string> none;

        TASSERT(list_visible_databases("") == all);
        TASSERT(list_visible_databases("reader") == db1);
        TASSERT(list_visible_databases("writer") == db1);
        TASSERT(list_visible_databases("deleter") == db1);
        TASSERT(list_visible_databases("db1admin") == db1);
        TASSERT(list_visible_databases("starreader") == all);
        TASSERT(list_visible_databases("admin") == all);
        TASSERT(list_visible_databases("nobody") == none);

        // Without an acl file, everyone sees everything.
        futil::unlink_if_exists(root->root_dir,"acl");
        TASSERT(list_visible_databases("nobody") == all);
    }
};

TMOCK_MAIN();
