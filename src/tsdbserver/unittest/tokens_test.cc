// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../tokens.h"
#include <tmock/tmock.h>
#include <string.h>

void
debugf(const char* fmt, ...)
{
}

// An in-memory stream: the server reads rx, which holds what a client sent,
// and appends its replies to tx.  Running out of rx looks like the client
// closing the connection.
struct fake_stream
{
    std::vector<uint8_t>    rx;
    size_t                  rx_pos = 0;
    std::vector<uint8_t>    tx;

    void recv_all(void* p, size_t n)
    {
        if (rx_pos + n > rx.size())
            throw futil::errno_exception(ECONNRESET);
        memcpy(p,&rx[rx_pos],n);
        rx_pos += n;
    }

    template<typename T>
    T pop()
    {
        T v;
        recv_all(&v,sizeof(v));
        return v;
    }

    void send_all(const void* p, size_t n)
    {
        tx.insert(tx.end(),(const uint8_t*)p,(const uint8_t*)p + n);
    }

    template<typename T>
    void push_client(T v)
    {
        rx.insert(rx.end(),(const uint8_t*)&v,(const uint8_t*)&v + sizeof(v));
    }

    void push_client_string(data_token dt, const std::string& s)
    {
        push_client<uint32_t>(dt);
        push_client<uint16_t>(s.size());
        rx.insert(rx.end(),s.begin(),s.end());
    }
};

// A connection that refuses any command on the "secret" database and
// records what it was asked to authorize and what the handler ran on.
struct fake_connection
{
    fake_stream                 s;
    std::vector<std::string>    authorized;
    std::vector<std::string>    handled;

    void log_idle() {}
    std::vector<parsed_data_token>& log_tokens(
        command_token ct, std::vector<parsed_data_token>& tokens)
    {
        return tokens;
    }

    void authorize(command_token ct,
                   const std::vector<parsed_data_token>& tokens)
    {
        TASSERT_EQUIV(ct,CT_LIST_MEASUREMENTS);
        authorized.push_back(tokens[0].to_string());
        if (tokens[0].to_string() == "secret")
            throw tsdb::permission_denied_exception();
    }
};

static void
handle_list_measurements(fake_connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    conn.handled.push_back(tokens[0].to_string());
}

static const command_syntax<fake_connection&> commands[] =
{
    {
        func_delegate(handle_list_measurements),
        CT_LIST_MEASUREMENTS,
        {DT_DATABASE, DT_END},
    },
};

static void
send_list_measurements(fake_connection& conn, const std::string& database)
{
    conn.s.push_client<uint32_t>(CT_LIST_MEASUREMENTS);
    conn.s.push_client_string(DT_DATABASE,database);
    conn.s.push_client<uint32_t>(DT_END);
}

class tmock_test
{
    TMOCK_TEST(test_authorize_before_handler)
    {
        // A refused command gets a status instead of running, and the
        // connection carries on with the next command.
        fake_connection conn;
        send_list_measurements(conn,"secret");
        send_list_measurements(conn,"public");
        process_stream(conn,commands);

        TASSERT_EQUIV(conn.authorized.size(),2UL);
        TASSERT_EQUIV(conn.authorized[0],"secret");
        TASSERT_EQUIV(conn.authorized[1],"public");
        TASSERT_EQUIV(conn.handled.size(),1UL);
        TASSERT_EQUIV(conn.handled[0],"public");

        const uint32_t expected[] = {
            DT_STATUS_CODE, (uint32_t)tsdb::PERMISSION_DENIED,
            DT_STATUS_CODE, 0,
        };
        TASSERT_EQUIV(conn.s.tx.size(),sizeof(expected));
        TASSERT(!memcmp(&conn.s.tx[0],expected,sizeof(expected)));
    }
};

TMOCK_MAIN();
