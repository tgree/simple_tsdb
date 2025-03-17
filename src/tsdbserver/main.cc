// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include <version.h>
#include <hdr/kmath.h>
#include <strutil/strutil.h>
#include <futil/tcp.h>
#include <libtsdb/tsdb.h>

#include <algorithm>
#include <thread>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

enum command_token : uint32_t
{
    CT_CREATE_DATABASE      = 0x60545A42,
    CT_CREATE_MEASUREMENT   = 0xBB632CE1,
    CT_WRITE_POINTS         = 0xEAF5E003,
    CT_SELECT_POINTS_LIMIT  = 0x7446C560,
    CT_SELECT_POINTS_LAST   = 0x76CF2220,
    CT_DELETE_POINTS        = 0xD9082F2C,
    CT_GET_SCHEMA           = 0x87E5A959,
    CT_NOP                  = 0x22CF1296,
};

enum data_token : uint32_t
{
    DT_DATABASE         = 0x39385A4F,   // <database>
    DT_MEASUREMENT      = 0xDC1F48F3,   // <measurement>
    DT_SERIES           = 0x4E873749,   // <series>
    DT_TYPED_FIELDS     = 0x02AC7330,   // <f1>/<type1>,<f2>/<type2>,...
    DT_FIELD_LIST       = 0xBB62ACC3,   // <f1>,<f2>,...
    DT_CHUNK            = 0xE4E8518F,   // <chunk header>, then data
    DT_TIME_FIRST       = 0x55BA37B4,   // <t0> (uint64_t)
    DT_TIME_LAST        = 0xC4EE45BA,   // <t1> (uint64_t)
    DT_NLIMIT           = 0xEEF2BB02,   // LIMIT <N> (uint64_t)
    DT_NLAST            = 0xD74F10A3,   // LAST <N> (uint64_t)
    DT_END              = 0x4E29ADCC,   // end of command
    DT_STATUS_CODE      = 0x8C8C07D9,   // <errno> (uint32_t)
    DT_FIELD_TYPE       = 0x7DB40C2A,   // <type> (uint32_t)
    DT_FIELD_NAME       = 0x5C0D45C1,   // <name>
    DT_READY_FOR_CHUNK  = 0x6000531C,   // <max_data_len> (uint32_t)
};

struct parsed_data_token
{
    data_token type;
    const char* data;
    union
    {
        size_t      len;
        uint64_t    u64;
    };

    std::string to_string() const {return std::string(data,len);}
};

struct chunk_header
{
    uint32_t    npoints;
    uint32_t    bitmap_offset;
    uint32_t    data_len;
    uint8_t     data[];
};

struct auto_buf
{
    void* const data;

    operator void*() const
    {
        return data;
    }

    auto_buf(size_t len):
        data(malloc(len))
    {
        if (!data)
            throw futil::errno_exception(ENOMEM);
    }
    ~auto_buf()
    {
        free(data);
    }
};

struct command_syntax
{
    void (* const handler)(tcp::socket4& s,
                           const std::vector<parsed_data_token>& tokens);
    const command_token cmd_token;
    const std::vector<data_token> data_tokens;
};

static void handle_create_database(
    tcp::socket4& s, const std::vector<parsed_data_token>& tokens);
static void handle_create_measurement(
    tcp::socket4& s, const std::vector<parsed_data_token>& tokens);
static void handle_get_schema(
    tcp::socket4& s, const std::vector<parsed_data_token>& tokens);
static void handle_write_points(
    tcp::socket4& s, const std::vector<parsed_data_token>& tokens);
static void handle_delete_points(
    tcp::socket4& s, const std::vector<parsed_data_token>& tokens);
static void handle_select_points_limit(
    tcp::socket4& s, const std::vector<parsed_data_token>& tokens);
static void handle_select_points_last(
    tcp::socket4& s, const std::vector<parsed_data_token>& tokens);
static void handle_nop(
    tcp::socket4& s, const std::vector<parsed_data_token>& tokens);

static const command_syntax commands[] =
{
    {
        handle_create_database,
        CT_CREATE_DATABASE,
        {DT_DATABASE, DT_END},
    },
    {
        handle_create_measurement,
        CT_CREATE_MEASUREMENT,
        {DT_DATABASE, DT_MEASUREMENT, DT_TYPED_FIELDS, DT_END},
    },
    {
        handle_get_schema,
        CT_GET_SCHEMA,
        {DT_DATABASE, DT_MEASUREMENT, DT_END},
    },
    {
        handle_write_points,
        CT_WRITE_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES},
    },
    {
        handle_delete_points,
        CT_DELETE_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_TIME_LAST, DT_END},
    },
    {
        handle_select_points_limit,
        CT_SELECT_POINTS_LIMIT,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_FIELD_LIST, DT_TIME_FIRST,
         DT_TIME_LAST, DT_NLIMIT, DT_END},
    },
    {
        handle_select_points_last,
        CT_SELECT_POINTS_LAST,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_FIELD_LIST, DT_TIME_FIRST,
         DT_TIME_LAST, DT_NLAST, DT_END},
    },
    {
        handle_nop,
        CT_NOP,
        {DT_END},
    },
};

static const uint8_t pad_bytes[8] = {};

static void
handle_create_database(tcp::socket4& s,
    const std::vector<parsed_data_token>& tokens)
{
    // TODO: Use string_view.
    std::string database(tokens[0].data,tokens[0].len);
    printf("CREATE DATABASE %s\n",database.c_str());
    tsdb::create_database(database.c_str());
}

static void
handle_create_measurement(tcp::socket4& s,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    futil::path path(database,measurement);
    std::string typed_fields(tokens[2].data,tokens[2].len);

    std::vector<tsdb::schema_entry> fields;
    auto field_specifiers = str::split(typed_fields,",");
    for (const auto& fs : field_specifiers)
    {
        auto field_specifier = str::split(fs,"/");
        if (field_specifier.size() != 2 || field_specifier[0].empty() ||
            field_specifier[1].empty() || field_specifiers[0].size() >= 124)
        {
            throw futil::errno_exception(EINVAL);
        }

        tsdb::schema_entry se{};
        strcpy(se.name,field_specifier[0].c_str());
        if (field_specifier[1] == "bool")
            se.type = tsdb::FT_BOOL;
        else if (field_specifier[1] == "u32")
            se.type = tsdb::FT_U32;
        else if (field_specifier[1] == "u64")
            se.type = tsdb::FT_U64;
        else if (field_specifier[1] == "f32")
            se.type = tsdb::FT_F32;
        else if (field_specifier[1] == "f64")
            se.type = tsdb::FT_F64;
        else if (field_specifier[1] == "i32")
            se.type = tsdb::FT_I32;
        else if (field_specifier[1] == "i64")
            se.type = tsdb::FT_I64;
        else
            throw futil::errno_exception(EINVAL);

        fields.push_back(se);
    }

    printf("CREATE MEASUREMENT %s\n",path.c_str());
    tsdb::database db(database);
    tsdb::create_measurement(db,measurement,fields);
}

static void
handle_get_schema(tcp::socket4& s,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    futil::path measurement_path(database,measurement);

    printf("GET SCHEMA FOR %s\n",measurement_path.c_str());
    tsdb::database db(tokens[0].to_string());
    tsdb::measurement m(db,tokens[1].to_string());
    for (const auto& f : m.fields)
    {
        uint32_t ft[3] = {DT_FIELD_TYPE, f.type, DT_FIELD_NAME};
        s.send_all(&ft,sizeof(ft));
        uint16_t len = strlen(f.name);
        s.send_all(&len,sizeof(len));
        s.send_all(f.name,len);
    }
}

static void
handle_write_points(tcp::socket4& s,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    futil::path path(database,measurement,series);

    printf("WRITE TO %s\n",path.c_str());
    tsdb::database db(tokens[0].to_string());
    tsdb::measurement m(db,tokens[1].to_string());
    auto write_lock = tsdb::open_or_create_and_lock_series(m,series);

    for (;;)
    {
        uint32_t tokens[2] = {DT_READY_FOR_CHUNK,10*1024*1024};
        s.send_all(tokens,sizeof(tokens));

        uint32_t dt = s.pop<uint32_t>();
        if (dt == DT_END)
        {
            printf("WRITE END\n");
            return;
        }
        if (dt != DT_CHUNK)
            throw futil::errno_exception(EINVAL);

        chunk_header ch = s.pop<chunk_header>();
        if (ch.data_len > 10*1024*1024)
            throw futil::errno_exception(ENOMEM);

        printf("RECV %u BYTES\n",ch.data_len);
        auto_buf data(ch.data_len);
        s.recv_all(data,ch.data_len);

        printf("WRITE %u POINTS TO %s\n",ch.npoints,path.c_str());
        tsdb::write_series(write_lock,ch.npoints,ch.bitmap_offset,ch.data_len,
                           data);
    }
}

static void
handle_delete_points(tcp::socket4& s,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    futil::path path(database,measurement,series);
    uint64_t t = tokens[3].u64;

    printf("DELETE FROM %s WHERE time_ns <= %llu\n",path.c_str(),t);
    tsdb::database db(database);
    tsdb::measurement m(db,measurement);
    tsdb::delete_points(m,series,t);
}

static void
_handle_select_points(tcp::socket4& s, tsdb::select_op& op)
{
    if (!op.npoints)
    {
        uint32_t dt = DT_END;
        s.send_all(&dt,sizeof(dt));
        return;
    }

    for (;;)
    {
        size_t len = op.compute_chunk_len();
        uint32_t tokens[4] = {DT_CHUNK,(uint32_t)op.npoints,
                              (uint32_t)(op.bitmap_offset % 64),(uint32_t)len};
        s.send_all(tokens,sizeof(tokens));

        // Start by sending timestamp data.
        s.send_all(op.timestamp_data,8*op.npoints);

        // Send each field in turn.
        size_t bitmap_index = op.bitmap_offset / 64;
        size_t bitmap_n = ceil_div<size_t>(op.bitmap_offset + op.npoints,64) -
                          bitmap_index;
        for (size_t i=0; i<op.fields.size(); ++i)
        {
            // First we send the bitmap.
            auto* bitmap = (const uint64_t*)op.bitmap_mappings[i].addr;
            s.send_all(&bitmap[bitmap_index],bitmap_n*8);

            // Next we send the field data.
            const auto* fti = &tsdb::ftinfos[op.fields[i].type];
            size_t data_len = op.npoints*fti->nbytes;
            s.send_all(op.field_data[i],data_len);

            // Finally, pad to 8 bytes if necessary.
            if (data_len % 8)
                s.send_all(pad_bytes,8 - (data_len % 8));
        }

        if (op.is_last)
        {
            uint32_t dt = DT_END;
            s.send_all(&dt,sizeof(dt));
            return;
        }

        op.advance();
    }
}

static void
handle_select_points_limit(tcp::socket4& s,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    futil::path path(database,measurement,series);
    std::string field_list(tokens[3].data,tokens[3].len);
    uint64_t t0 = tokens[4].u64;
    uint64_t t1 = tokens[5].u64;
    uint64_t N = tokens[6].u64;

    printf("SELECT %s FROM %s WHERE %llu <= time_ns <= %llu LIMIT %llu\n",
           field_list.c_str(),path.c_str(),t0,t1,N);
    tsdb::database db(database);
    tsdb::measurement m(db,measurement);
    tsdb::series_read_lock read_lock(m,series);
    tsdb::select_op_first op(read_lock,path,str::split(field_list,","),t0,t1,N);
    _handle_select_points(s,op);
}

static void
handle_select_points_last(tcp::socket4& s,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    futil::path path(database,measurement,series);
    std::string field_list(tokens[3].data,tokens[3].len);
    uint64_t t0 = tokens[4].u64;
    uint64_t t1 = tokens[5].u64;
    uint64_t N = tokens[6].u64;

    printf("SELECT %s FROM %s WHERE %llu <= time_ns <= %llu LAST %llu\n",
           field_list.c_str(),path.c_str(),t0,t1,N);
    tsdb::database db(database);
    tsdb::measurement m(db,measurement);
    tsdb::series_read_lock read_lock(m,series);
    tsdb::select_op_last op(read_lock,path,str::split(field_list,","),t0,t1,N);
    _handle_select_points(s,op);
}

static void
handle_nop(tcp::socket4& s, const std::vector<parsed_data_token>& tokens)
{
    // Do nothing.
}

static void
parse_cmd(tcp::socket4& s, const command_syntax& cs)
{
    printf("Got command 0x%08X.\n",cs.cmd_token);

    std::vector<parsed_data_token> tokens;
    try
    {
        for (auto dt : cs.data_tokens)
        {
            uint32_t v = s.pop<uint32_t>();
            if (v != dt)
            {
                printf("Expected 0x%08X got 0x%08X\n",dt,v);
                throw futil::errno_exception(EINVAL);
            }
            printf("Got token 0x%08X.\n",dt);

            parsed_data_token pdt;
            pdt.type = dt;
            pdt.data = NULL;
            switch (dt)
            {
                case DT_DATABASE:
                case DT_MEASUREMENT:
                case DT_SERIES:
                case DT_TYPED_FIELDS:
                case DT_FIELD_LIST:
                    pdt.len = s.pop<uint16_t>();
                    if (pdt.len >= 1024)
                    {
                        printf("String length %zu too long.\n",pdt.len);
                        throw futil::errno_exception(EINVAL);
                    }
                    pdt.data = (char*)malloc(pdt.len);
                    tokens.push_back(pdt);

                    s.recv_all((char*)pdt.data,pdt.len);
                break;

                case DT_CHUNK:
                    throw futil::errno_exception(ENOTSUP);
                    tokens.push_back(pdt);
                break;

                case DT_TIME_FIRST:
                case DT_TIME_LAST:
                case DT_NLIMIT:
                case DT_NLAST:
                    pdt.u64 = s.pop<uint64_t>();
                    tokens.push_back(pdt);
                break;

                case DT_END:
                    tokens.push_back(pdt);
                break;

                default:
                    throw futil::errno_exception(EINVAL);
                break;
            }
        }

        uint32_t status[2] = {DT_STATUS_CODE, 0};
        try
        {
            cs.handler(s,tokens);
        }
        catch (const tsdb::exception& e)
        {
            printf("TSDB exception: %s\n",e.what());
            status[1] = e.sc;
        }
        
        printf("Sending status...\n");
        s.send_all(&status,sizeof(status));
    }
    catch (...)
    {
        for (const auto& t : tokens)
            free((void*)t.data);
        throw;
    }

    for (const auto& t : tokens)
        free((void*)t.data);
}

static void
parse_cmd(tcp::socket4& s)
{
    // 0. Set receive timeout to something very short.
    // 1. Pop command token.
    // 2. If not in commands[], close socket.
    // 3. Pop data tokens as defined in commands[].
    // 4. Call handler.
    try
    {
        for (;;)
        {
            uint32_t ct = s.pop<uint32_t>();

            bool found = false;
            for (auto& cmd : commands)
            {
                if (cmd.cmd_token == ct)
                {
                    parse_cmd(s,cmd);
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                printf("No such command 0x%08X.\n",ct);
                return;
            }
        }
    }
    catch (const std::exception& e)
    {
        printf("Error: %s\n",e.what());
    }
    catch (...)
    {
        printf("Random exception!\n");
    }
}

static void
request_handler(tcp::socket4&& s)
{
    printf("Accepted local %s remote %s.\n",
           s.local_addr.to_string().c_str(),
           s.remote_addr.to_string().c_str());

    parse_cmd(s);

    printf("Closing local %s remote %s.\n",
           s.local_addr.to_string().c_str(),
           s.remote_addr.to_string().c_str());
}

int
main(int argc, const char* argv[])
{
    if (argc == 2)
        futil::chdir(argv[1]);

    printf("%s\n",GIT_VERSION);

    signal(SIGPIPE,SIG_IGN);

    tcp::addr4 sa(4000,INADDR_LOOPBACK);
    tcp::server_socket4 ss(sa);
    ss.listen(4);
    printf("Listening on %s.\n",ss.bind_addr.to_string().c_str());
    for (;;)
    {
        std::thread t(request_handler,ss.accept());
        t.detach();
    }
}
