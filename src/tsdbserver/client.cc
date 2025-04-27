// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "client.h"
#include "tokens.h"

client::client(const std::string& remote_hostname, uint16_t remote_port,
    const std::string& remote_user, const std::string& remote_password):
        remote_hostname(remote_hostname),
        remote_port(remote_port),
        remote_user(remote_user),
        remote_password(remote_password)
{
}

std::vector<tsdb::schema_entry>
client::get_schema(const std::string& database,
    const std::string& measurement) try
{
    connect();
    s->push(CT_GET_SCHEMA);
    s->push(DT_DATABASE);
    s->push<uint16_t>(database.size());
    s->push(database);
    s->push(DT_MEASUREMENT);
    s->push<uint16_t>(measurement.size());
    s->push(measurement);
    s->push(DT_END);

    std::vector<tsdb::schema_entry> schema;
    size_t index = 0;
    size_t offset = 0;
    data_token dt;
    for (;;)
    {
        dt = s->pop<data_token>();
        if (dt != DT_FIELD_TYPE)
            break;

        tsdb::schema_entry se{};
        se.version = 1;
        se.index   = index;
        se.offset  = offset;
        se.type    = (tsdb::field_type)s->pop<uint32_t>();
        if (se.type > LAST_FIELD_TYPE)
            throw protocol_exception("Invalid field type");

        dt = s->pop<data_token>();
        if (dt != DT_FIELD_NAME)
            throw protocol_exception("Expected DT_FIELD_NAME");

        uint16_t len = s->pop<uint16_t>();
        if (len >= sizeof(se.name))
            throw protocol_exception("Field name too long");
        s->recv_all(se.name,len);

        schema.emplace_back(se);

        ++index;
        offset += tsdb::ftinfos[se.type].nbytes;
    }

    if (dt != DT_STATUS_CODE)
        throw protocol_exception("Expected DT_STATUS_CODE");
    auto sc = (tsdb::status_code)s->pop<int32_t>();
    if (sc != 0)
        throw status_exception(sc);

    return schema;
}
catch (const status_exception&)
{
    throw;
}
catch (...)
{
    s.reset();
    throw;
}

void
client::connect() try
{
    if (s)
        return;

    auto addrs = net::get_addrs(remote_hostname.c_str(),remote_port);
    if (addrs.empty())
        return;

    s = ssl_context.wrap(std::make_unique<tcp::client_socket>(addrs[0]),
                         remote_hostname.c_str());
    s->push(CT_AUTHENTICATE);
    s->push(DT_USERNAME);
    s->push<uint16_t>(remote_user.size());
    s->push(remote_user);
    s->push(DT_PASSWORD);
    s->push<uint16_t>(remote_password.size());
    s->push(remote_password);
    s->push(DT_END);

    if (s->pop<data_token>() != DT_STATUS_CODE)
        throw protocol_exception("Expected DT_STATUS_CODE");
    if (s->pop<uint32_t>() != 0)
        throw protocol_exception("Expected status 0");
}
catch (...)
{
    s.reset();
    throw;
}
