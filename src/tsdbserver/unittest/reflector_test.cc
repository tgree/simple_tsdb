// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../reflector.h"
#include <libtsdb/unittest/fake_db.h>
#include <futil/fakefs/fakefs.h>
#include <futil/mock/mock_net.h>
#include <tmock/tmock.h>

// A mock client that never touches the network.
client::client(const std::string& remote_hostname, uint16_t remote_port,
    const std::string& remote_user, const std::string& remote_password):
        remote_hostname(remote_hostname),
        remote_port(remote_port),
        remote_user(remote_user),
        remote_password(remote_password)
{
}

// Intentionally not mocked: connect() is invoked frequently internally,
// including at the start of every series flush.
void
client::connect()
{
}

void
client::push_string(data_token dt, const std::string& s)
{
    const char* str = s.c_str();
    mock("client::push_string",dt,str);
}

std::vector<tsdb::schema_entry>
client::get_schema(const std::string& database,
    const std::string& measurement)
{
    const char* db = database.c_str();
    const char* m = measurement.c_str();
    mock("client::get_schema",db,m);

    // A mock can't return a schema, which is a vector.  Returning an empty
    // one just lets this compile: for now the mock only asserts that
    // get_schema() is never called.
    return {};
}

void
client::create_measurement(const std::string& database,
    const std::string& measurement,
    const std::vector<tsdb::schema_entry>& fields)
{
    const char* db = database.c_str();
    const char* m = measurement.c_str();
    mock("client::create_measurement",db,m);
}

void
client::write_points(const std::string& database,
    const std::string& measurement, const std::string& series,
    uint32_t npoints, uint32_t bitmap_offset, uint32_t data_len,
    const void* data)
{
    const char* db = database.c_str();
    const char* m = measurement.c_str();
    const char* s = series.c_str();
    mock("client::write_points",db,m,s,npoints);
}

// Sets up db1/measurement1/series1 with 8 points in its WAL and maps db1 to
// the remote database rdb1.
static void
init_reflector()
{
    init_db(16);
    populate_db(100,10,{8});
    root = new tsdb::root(".",false);
    reflector_cfg.db_map["db1"] = "rdb1";
}

// Makes the next futil call fail with EMFILE, as it does when the reflector
// has run out of file descriptors.  The probe clears futil::hook_func while
// the hook runs and doesn't restore it if the hook throws, so this fails
// exactly one call.
static void
fail_next_futil_call_with_emfile()
{
    futil::hook_func = [](uint32_t id)
    {
        throw futil::errno_exception(EMFILE);
    };
}

// Expects a complete flush of the 8 points set up by init_reflector().
static void
expect_flush()
{
    texpect("client::write_points",want_str(db,"rdb1"),
            want_str(m,"measurement1"),want_str(s,"series1"),
            want(npoints,8));
}

class tmock_test
{
    TMOCK_TEST(test_flush_pass)
    {
        init_reflector();
        client c("remote",4000,"user","password");
        std::set<std::string> validated_dbs;
        auto_buf chunk_storage(10*1024*1024);

        texpect("client::create_measurement",want_str(db,"rdb1"),
                want_str(m,"measurement1"));
        expect_flush();
        flush_pass(c,validated_dbs,chunk_storage);
        tcheckpoint();
        TASSERT(validated_dbs.count("db1"));

        // Nothing is left to flush.
        flush_pass(c,validated_dbs,chunk_storage);
    }

    TMOCK_TEST(test_flush_pass_survives_emfile)
    {
        // The first futil call in a pass over an already-validated database
        // lists it, before flush_series() runs.  The reflector used to
        // abort here when it ran out of file descriptors.
        init_reflector();
        client c("remote",4000,"user","password");
        std::set<std::string> validated_dbs = {"db1"};
        auto_buf chunk_storage(10*1024*1024);

        fail_next_futil_call_with_emfile();
        flush_pass(c,validated_dbs,chunk_storage);
        TASSERT(!futil::hook_func);

        // The next pass flushes normally.
        expect_flush();
        flush_pass(c,validated_dbs,chunk_storage);
    }

    TMOCK_TEST(test_create_remote_measurements_survives_emfile)
    {
        // The first futil call in a pass over an unvalidated database is
        // root->database_exists().
        init_reflector();
        client c("remote",4000,"user","password");
        std::set<std::string> validated_dbs;
        auto_buf chunk_storage(10*1024*1024);

        fail_next_futil_call_with_emfile();
        flush_pass(c,validated_dbs,chunk_storage);
        TASSERT(!futil::hook_func);
        TASSERT(!validated_dbs.count("db1"));

        // The next pass validates and flushes normally.
        texpect("client::create_measurement",want_str(db,"rdb1"),
                want_str(m,"measurement1"));
        expect_flush();
        flush_pass(c,validated_dbs,chunk_storage);
    }

    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(
        test_request_handler_survives_setsockopt_failure)
    {
        // On macOS, every socket option fails with EINVAL once the client
        // has reset the connection.  request_handler() sets its options
        // outside any try block, so one reset connection used to abort the
        // reflector.  fakefs can't close a real socket, so this uses a mock
        // stream.
        texpect("mock_net::tcp::stream::enable_keepalive",returns(EINVAL));
        request_handler(std::make_unique<mock_net::tcp::stream>());
        tcheckpoint();
    }
};

TMOCK_MAIN();
