// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __TSDBSERVER_CLIENT_H
#define __TSDBSERVER_CLIENT_H

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

    void connect();
    std::vector<tsdb::schema_entry> get_schema(const std::string& database,
                                               const std::string& measurement);

    client(const std::string& remote_hostname, uint16_t remote_port,
           const std::string& remote_user,
           const std::string& remote_password);
};

#endif /* __TSDBSERVER_CLIENT_H */
