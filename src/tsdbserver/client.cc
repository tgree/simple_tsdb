// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "client.h"
#include "tokens.h"

static void
check_status(tsdb::status_code sc)
{
    if (sc == 0)
        return;

    switch (sc)
    {
        case tsdb::INIT_IO_ERROR:
            throw tsdb::init_io_error_exception(0);
        case tsdb::CREATE_DATABASE_IO_ERROR:
            throw tsdb::create_database_io_error_exception(0);
        case tsdb::CREATE_MEASUREMENT_IO_ERROR:
            throw tsdb::create_measurement_io_error_exception(0);
        case tsdb::INCORRECT_WRITE_CHUNK_LEN:
            throw tsdb::incorrect_write_chunk_len_exception(0,0);
        case tsdb::TAIL_FILE_TOO_BIG:
            throw tsdb::tail_file_too_big_exception(0);
        case tsdb::TAIL_FILE_INVALID_SIZE:
            throw tsdb::tail_file_invalid_size_exception(0);
        case tsdb::INVALID_TIME_LAST:
            throw tsdb::invalid_time_last_exception(0,0);
        case tsdb::NO_SUCH_DATABASE:
            throw tsdb::no_such_database_exception();
        case tsdb::NO_SUCH_MEASUREMENT:
            throw tsdb::no_such_measurement_exception();
        case tsdb::INVALID_MEASUREMENT:
            throw tsdb::invalid_measurement_exception();
        case tsdb::MEASUREMENT_EXISTS:
            throw tsdb::measurement_exists_exception();
        case tsdb::INVALID_SERIES:
            throw tsdb::invalid_series_exception();
        case tsdb::NO_SUCH_SERIES:
            throw tsdb::no_such_series_exception();
        case tsdb::CORRUPT_SCHEMA_FILE:
            throw tsdb::corrupt_schema_file_exception();
        case tsdb::NO_SUCH_FIELD:
            throw tsdb::no_such_field_exception();
        case tsdb::END_OF_SELECT:
            throw tsdb::end_of_select_exception();
        case tsdb::OUT_OF_ORDER_TIMESTAMPS:
            throw tsdb::out_of_order_timestamps_exception();
        case tsdb::TIMESTAMP_OVERWRITE_MISMATCH:
            throw tsdb::timestamp_overwrite_mismatch_exception();
        case tsdb::FIELD_OVERWRITE_MISMATCH:
            throw tsdb::field_overwrite_mismatch_exception();
        case tsdb::BITMAP_OVERWRITE_MISMATCH:
            throw tsdb::bitmap_overwrite_mismatch_exception();
        case tsdb::USER_EXISTS:
            throw tsdb::user_exists_exception();
        case tsdb::NO_SUCH_USER:
            throw tsdb::no_such_user_exception();
        case tsdb::NOT_A_TSDB_ROOT:
            throw tsdb::not_a_tsdb_root();
        case tsdb::DUPLICATE_FIELD:
            throw tsdb::duplicate_field_exception();
        case tsdb::TOO_MANY_FIELDS:
            throw tsdb::too_many_fields_exception();
        case tsdb::INVALID_CONFIG_FILE:
            throw tsdb::invalid_config_file_exception();
        case tsdb::INVALID_CHUNK_SIZE:
            throw tsdb::invalid_chunk_size_exception();
    }

    throw protocol_exception("Unknown status code");
}

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
    check_status((tsdb::status_code)s->pop<int32_t>());

    return schema;
}
catch (const tsdb::exception&)
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
