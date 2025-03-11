// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include <version.h>
#include <hdr/kmath.h>
#include <strutil/strutil.h>
#include <libtsdb/tsdb.h>

#include <algorithm>
#include <stdio.h>
#include <stdlib.h>

enum command_token : uint32_t
{
    CT_CREATE_DATABASE      = 0x60545A42,
    CT_CREATE_MEASUREMENT   = 0xBB632CE1,
    CT_WRITE_POINTS         = 0xEAF5E003,
    CT_SELECT_POINTS_LIMIT  = 0x7446C560,
    CT_SELECT_POINTS_LAST   = 0x76CF2220,
    CT_DELETE_POINTS        = 0xD9082F2C,
};

enum data_token : uint32_t
{
    DT_DATABASE         = 0x39385A4F,   // <database>
    DT_MEASUREMENT      = 0xDC1F48F3,   // <measurement>
    DT_SERIES           = 0x4E873749,   // <series>
    DT_TYPED_FIELDS     = 0x02AC7330,   // <f1>/<type1>,<f2>/<type2>,...
    DT_FIELD_LIST       = 0xBB62ACC3,   // <f1>,<f2>,...
    DT_CHUNK_POINTS     = 0xE4E8518F,   // <len> (uint32_t), then point data
    DT_TIME_FIRST       = 0x55BA37B4,   // <t0> (uint64_t)
    DT_TIME_LAST        = 0xC4EE45BA,   // <t1> (uint64_t)
    DT_NLIMIT           = 0xEEF2BB02,   // LIMIT <N> (uint64_t)
    DT_NLAST            = 0xD74F10A3,   // LAST <N> (uint64_t)
    DT_END              = 0x4E29ADCC,   // end of command
};

struct parsed_data_token
{
    data_token type;

    // Contents differ depending on token type.
    union
    {
        struct
        {
            size_t      len;
            const char* data;
        } buf;
        uint64_t u64;
    };
};

struct chunk_header
{
    uint32_t    npoints;
    uint8_t     data[];
};

struct command_syntax
{
    void (* const handler)(const std::vector<parsed_data_token>& tokens);
    const command_token cmd_token;
    const std::vector<data_token> data_tokens;
};

static void handle_create_database(
    const std::vector<parsed_data_token>& tokens);
static void handle_create_measurement(
    const std::vector<parsed_data_token>& tokens);
static void handle_write_points(
    const std::vector<parsed_data_token>& tokens);
static void handle_delete_points(
    const std::vector<parsed_data_token>& tokens);
static void handle_select_points_limit(
    const std::vector<parsed_data_token>& tokens);
static void handle_select_points_last(
    const std::vector<parsed_data_token>& tokens);

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
        handle_write_points,
        CT_WRITE_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_CHUNK_POINTS},
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
};

static void
handle_create_database(const std::vector<parsed_data_token>& tokens)
{
    // TODO: Use string_view.
    std::string database(tokens[0].buf.data,tokens[0].buf.len);
    printf("CREATE DATA %s\n",database.c_str());
    tsdb::create_database(database.c_str());
}

static void
handle_create_measurement(const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].buf.data,tokens[0].buf.len);
    std::string measurement(tokens[1].buf.data,tokens[1].buf.len);
    futil::path path(database,measurement);
    std::string typed_fields(tokens[2].buf.data,tokens[2].buf.len);

    std::vector<tsdb::field> fields;
    auto field_specifiers = str::split(typed_fields,",");
    for (const auto& fs : field_specifiers)
    {
        auto field_specifier = str::split(fs,"/");
        if (field_specifier.size() != 2 || field_specifier[0].empty() ||
            field_specifier[1].empty())
        {
            throw futil::errno_exception(EINVAL);
        }

        tsdb::field f;
        f.name = field_specifier[0];
        if (field_specifier[1] == "bool")
            f.type = tsdb::FT_BOOL;
        else if (field_specifier[1] == "u32")
            f.type = tsdb::FT_U32;
        else if (field_specifier[1] == "u64")
            f.type = tsdb::FT_U64;
        else if (field_specifier[1] == "f32")
            f.type = tsdb::FT_F32;
        else if (field_specifier[1] == "f64")
            f.type = tsdb::FT_F64;
        else
            throw futil::errno_exception(EINVAL);

        fields.push_back(f);
    }

    printf("CREATE MEASUREMENT %s\n",path.c_str());
    tsdb::create_measurement(path,fields);
}

static void
handle_write_points(const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].buf.data,tokens[0].buf.len);
    std::string measurement(tokens[1].buf.data,tokens[1].buf.len);
    std::string series(tokens[2].buf.data,tokens[2].buf.len);
    futil::path path(database,measurement,series);
    auto* chunk = (const chunk_header*)tokens[3].buf.data;
    uint32_t chunk_len = tokens[3].buf.len - sizeof(chunk_header);

    printf("WRITE %u POINTS TO %s\n",chunk->npoints,path.c_str());
    tsdb::write_series(path,chunk->npoints,chunk_len,chunk->data);
}

static void
handle_delete_points(const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].buf.data,tokens[0].buf.len);
    std::string measurement(tokens[1].buf.data,tokens[1].buf.len);
    std::string series(tokens[2].buf.data,tokens[2].buf.len);
    futil::path path(database,measurement,series);
    uint64_t t = tokens[3].u64;

    printf("DELETE FROM %s WHERE time_ns <= %llu\n",path.c_str(),t);
    tsdb::delete_points(path,t);
}

static void
_handle_select_points(tsdb::select_op& op)
{
    if (!op.npoints)
    {
        // Write DT_END.
        return;
    }

    // 1. For each chunk:
    //      1. Write DT_CHUNK_POINTS.
    //      2. Write timestamps.
    //      3. For each field:
    //          1. Write bitmap bit start offset.
    //          2. Write bitmap data (only from uint64_t word start offset).
    //          3. Write field data.
    // 2. Write DT_CHUNK_END
    for (;;)
    {
        if (op.is_last)
            break;

        op.advance();
    }
}

static void
handle_select_points_limit(const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].buf.data,tokens[0].buf.len);
    std::string measurement(tokens[1].buf.data,tokens[1].buf.len);
    std::string series(tokens[2].buf.data,tokens[2].buf.len);
    futil::path path(database,measurement,series);
    std::string field_list(tokens[3].buf.data,tokens[3].buf.len);
    uint64_t t0 = tokens[4].u64;
    uint64_t t1 = tokens[5].u64;
    uint64_t N = tokens[6].u64;

    printf("SELECT %s FROM %s WHERE %llu <= time_ns <= %llu LIMIT %llu\n",
           field_list.c_str(),path.c_str(),t0,t1,N);
    tsdb::select_op_first op(series,str::split(field_list,","),t0,t1,N);
    _handle_select_points(op);
}

static void
handle_select_points_last(const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].buf.data,tokens[0].buf.len);
    std::string measurement(tokens[1].buf.data,tokens[1].buf.len);
    std::string series(tokens[2].buf.data,tokens[2].buf.len);
    futil::path path(database,measurement,series);
    std::string field_list(tokens[3].buf.data,tokens[3].buf.len);
    uint64_t t0 = tokens[4].u64;
    uint64_t t1 = tokens[5].u64;
    uint64_t N = tokens[6].u64;

    printf("SELECT %s FROM %s WHERE %llu <= time_ns <= %llu LAST %llu\n",
           field_list.c_str(),path.c_str(),t0,t1,N);
    tsdb::select_op_last op(series,str::split(field_list,","),t0,t1,N);
    _handle_select_points(op);
}

int
main(int argc, const char* argv[])
{
    printf("%s\n",GIT_VERSION);
    for (;;)
    {
        // 1. Pop command token.
        // 2. If not in commands[], close socket.
        // 3. Pop data tokens as defined in commands[].
        // 4. Call handler.
        //
        // try
        // {
        //      doIt();
        // }
        // catch (const futil::errno_exception& e)
        // {
        //     printf("Error: %s\n",e.c_str());
        // }
    }
}
