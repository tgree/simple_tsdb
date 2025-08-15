// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __TSDBSERVER_CLIENT_H
#define __TSDBSERVER_CLIENT_H

#include "tokens.h"
#include <futil/tcp.h>
#include <futil/ssl.h>
#include <libtsdb/exception.h>
#include <libtsdb/measurement.h>

struct protocol_exception : public std::exception
{
    const char* msg;

    virtual const char* what() const noexcept
    {
        return msg;
    }

    protocol_exception(const char* msg):msg(msg) {}
};

struct client
{
    const std::string               remote_hostname;
    const uint16_t                  remote_port;
    const std::string               remote_user;
    const std::string               remote_password;
    tcp::ssl::client_context        ssl_context;
    std::unique_ptr<tcp::stream>    s;

    // Writes a string token.
    void push_string(data_token dt, const std::string& s);

    // Connect to the remote database if we aren't currently connected.  The
    // client code doesn't have to call this, it will be called internally
    // automatically if the connection hasn't been made yet or was dropped for
    // some reason.
    void connect();

    // Retrieves a schema for the specified measurement.
    std::vector<tsdb::schema_entry> get_schema(const std::string& database,
                                               const std::string& measurement);

    // Creates a measurement in the specified database; this is a no-op if the
    // measurement already exists and matches the requested schema.
    void create_measurement(const std::string& database,
                            const std::string& measurement,
                            const std::vector<tsdb::schema_entry>& fields);

    // Writes data points to the specified series.
    void write_points(const std::string& database,
                      const std::string& measurement,
                      const std::string& series,
                      uint32_t npoints,
                      uint32_t bitmap_offset,
                      uint32_t data_len,
                      const void* data);

    client(const std::string& remote_hostname, uint16_t remote_port,
           const std::string& remote_user,
           const std::string& remote_password);
};

#endif /* __TSDBSERVER_CLIENT_H */
