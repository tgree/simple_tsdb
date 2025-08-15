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

void
client::push_string(data_token dt, const std::string& st)
{
    s->push(dt);
    s->push<uint16_t>(st.size());
    s->push(st);
}

std::vector<tsdb::schema_entry>
client::get_schema(const std::string& database,
    const std::string& measurement) try
{
    connect();
    s->push(CT_GET_SCHEMA);
    push_string(DT_DATABASE,database);
    push_string(DT_MEASUREMENT,measurement);
    s->push(DT_END);

    std::vector<tsdb::schema_entry> schema;
    size_t index = 0;
    size_t offset = 0;
    data_token dt = s->pop<data_token>();
    while (dt == DT_FIELD_TYPE)
    {
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

        dt = s->pop<data_token>();
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
client::create_measurement(const std::string& database,
    const std::string& measurement,
    const std::vector<tsdb::schema_entry>& fields) try
{
    std::string typed_fields;
    for (size_t i=0; i<fields.size(); ++i)
    {
        const auto& f = fields[i];
        typed_fields += f.name;
        typed_fields += "/";
        typed_fields += tsdb::ftinfos[f.type].name;
        if (i+1 < fields.size())
            typed_fields += ",";
    }

    connect();
    s->push(CT_CREATE_MEASUREMENT);
    push_string(DT_DATABASE,database);
    push_string(DT_MEASUREMENT,measurement);
    push_string(DT_TYPED_FIELDS,typed_fields);
    s->push(DT_END);

    data_token dt = s->pop<data_token>();
    if (dt != DT_STATUS_CODE)
        throw protocol_exception("Expected DT_STATUS_CODE");
    check_status((tsdb::status_code)s->pop<int32_t>());
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
client::write_points(const std::string& database,
    const std::string& measurement, const std::string& series, uint32_t npoints,
    uint32_t bitmap_offset, uint32_t data_len, const void* data) try
{
    // We only allow writing a single chunk for now.  In practice, this is not
    // a big issue since the tsdbserver client is currently only used by the
    // reflector, which reflects only one chunk at a time.
    kassert(data_len <= 10*1024*1024);

    connect();

    s->push(CT_WRITE_POINTS);
    push_string(DT_DATABASE,database);
    push_string(DT_MEASUREMENT,measurement);
    push_string(DT_SERIES,series);

    data_token dt;
    for (;;)
    {
        dt = s->pop<data_token>();
        if (dt == DT_STATUS_CODE)
        {
            check_status((tsdb::status_code)s->pop<int32_t>());
            throw protocol_exception("Unexpected DT_STATUS_CODE 0");
        }
        if (dt != DT_READY_FOR_CHUNK)
            throw protocol_exception("Expected DT_READY_FOR_CHUNK");
        uint32_t avail_len = s->pop<uint32_t>();

        // Only do one chunk for now.
        kassert(data_len <= avail_len);
        if (!npoints)
            break;

        uint32_t hdr[] = {DT_CHUNK,npoints,bitmap_offset,data_len};
        s->send_all(hdr,sizeof(hdr));
        s->send_all(data,data_len);

        npoints -= npoints;
    }
    s->push(DT_END);

    dt = s->pop<data_token>();
    if (dt != DT_STATUS_CODE)
        throw protocol_exception("Expected DT_STATUS_CODE");
    check_status((tsdb::status_code)s->pop<int32_t>());
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

    auto cs = std::make_unique<tcp::client_socket>(addrs[0]);
    cs->enable_keepalive();
    s = ssl_context.wrap(std::move(cs),remote_hostname.c_str());
    s->push(CT_AUTHENTICATE);
    push_string(DT_USERNAME,remote_user);
    push_string(DT_PASSWORD,remote_password);
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
