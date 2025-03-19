// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include <version.h>
#include <hdr/kmath.h>
#include <strutil/strutil.h>
#include <libtsdb/tsdb.h>

#include <algorithm>
#include <stdio.h>
#include <stdlib.h>

#define MAX_PRINT_RESULTS   12
KASSERT(MAX_PRINT_RESULTS % 2 == 0);

// Common commands:
//  create measurement pt-1/xtalx_data with fields pressure_psi/f64,temp_c/f32,pressure_hz/f64,temp_hz/f64
//  write series pt-1/xtalx_data/XTI-10-1000000

static uint32_t u32 = 500000;
static uint64_t u64 = 0x0000000100000000ULL;
static float f32    = 12345.67;
static double f64   = 12345678.9;

enum command_token
{
    // Keywords.
    CT_STR_INIT,
    CT_STR_CREATE,
    CT_STR_DATABASE,
    CT_STR_MEASUREMENT,
    CT_STR_WRITE,
    CT_STR_SERIES,
    CT_STR_WITH,
    CT_STR_FIELDS,
    CT_STR_SELECT,
    CT_STR_FROM,
    CT_STR_WHERE,
    CT_STR_TIME_NS,
    CT_STR_LIMIT,
    CT_STR_LAST,
    CT_STR_DELETE,
    CT_STR_LIST,
    CT_STR_DATABASES,
    CT_STR_MEASUREMENTS,
    CT_STR_SCHEMA,
    CT_STR_COUNT,

    // Other types.
    CT_DATABASE_SPECIFIER,      // <database>
    CT_MEASUREMENT_SPECIFIER,   // <database>/<measurement>
    CT_SERIES_SPECIFIER,        // <database>/<measurement>/<series>
    CT_TYPED_FIELDS,            // <f1>/<type1>,<f2>/<type2>,...
    CT_UINT64,                  // <uint64_t>
    CT_FIELD_SPECIFIER,         // <f1>,<f2>,...
    CT_COMPARISON,              // <, <=, ==, >=, >
    CT_COMPARISON_LEFT,         // <, <=
    CT_COMPARISON_RIGHT,        // >=, >
};

constexpr const char* const keyword_strings[] =
{
    [CT_STR_INIT]           = "init",
    [CT_STR_CREATE]         = "create",
    [CT_STR_DATABASE]       = "database",
    [CT_STR_MEASUREMENT]    = "measurement",
    [CT_STR_WRITE]          = "write",
    [CT_STR_SERIES]         = "series",
    [CT_STR_WITH]           = "with",
    [CT_STR_FIELDS]         = "fields",
    [CT_STR_SELECT]         = "select",
    [CT_STR_FROM]           = "from",
    [CT_STR_WHERE]          = "where",
    [CT_STR_TIME_NS]        = "time_ns",
    [CT_STR_LIMIT]          = "limit",
    [CT_STR_LAST]           = "last",
    [CT_STR_DELETE]         = "delete",
    [CT_STR_LIST]           = "list",
    [CT_STR_DATABASES]      = "databases",
    [CT_STR_MEASUREMENTS]   = "measurements",
    [CT_STR_SCHEMA]         = "schema",
    [CT_STR_COUNT]          = "count",
};

struct command_syntax
{
    void (* const handler)(const std::vector<std::string>& cmd);
    const std::vector<command_token> tokens;

    bool parse(const std::vector<std::string>& cmd) const
    {
        if (cmd.size() != tokens.size())
            return false;
        for (size_t i=0; i<tokens.size(); ++i)
        {
            switch (tokens[i])
            {
                case CT_STR_INIT:
                case CT_STR_CREATE:
                case CT_STR_DATABASE:
                case CT_STR_MEASUREMENT:
                case CT_STR_WRITE:
                case CT_STR_SERIES:
                case CT_STR_WITH:
                case CT_STR_FIELDS:
                case CT_STR_SELECT:
                case CT_STR_FROM:
                case CT_STR_WHERE:
                case CT_STR_TIME_NS:
                case CT_STR_LIMIT:
                case CT_STR_LAST:
                case CT_STR_DELETE:
                case CT_STR_LIST:
                case CT_STR_DATABASES:
                case CT_STR_MEASUREMENTS:
                case CT_STR_SCHEMA:
                case CT_STR_COUNT:
                    if (strcasecmp(cmd[i].c_str(),keyword_strings[tokens[i]]))
                        return false;
                break;

                case CT_DATABASE_SPECIFIER:
                    if (cmd[i].find('/') != std::string::npos)
                        return false;
                break;

                case CT_MEASUREMENT_SPECIFIER:
                    if (std::count(cmd[i].begin(),cmd[i].end(),'/') != 1)
                        return false;
                break;

                case CT_SERIES_SPECIFIER:
                    if (std::count(cmd[i].begin(),cmd[i].end(),'/') != 2)
                        return false;
                break;

                case CT_TYPED_FIELDS:
                case CT_FIELD_SPECIFIER:
                break;

                case CT_UINT64:
                    try
                    {
                        std::stoul(cmd[i]);
                    }
                    catch (const std::exception&)
                    {
                        return false;
                    }
                break;

                case CT_COMPARISON:
                    if (cmd[i] != "<" && cmd[i] != "<=" && cmd[i] != "==" &&
                        cmd[i] != ">=" && cmd[i] != ">")
                    {
                        return false;
                    }
                break;

                case CT_COMPARISON_LEFT:
                    if (cmd[i] != "<" && cmd[i] != "<=")
                        return false;
                break;

                case CT_COMPARISON_RIGHT:
                    if (cmd[i] != ">=" && cmd[i] != ">")
                        return false;
                break;
            }
        }
        return true;
    }
};

static void handle_select_series_one_op(
    const std::vector<std::string>& cmd);
static void handle_select_series_one_op_limit(
    const std::vector<std::string>& cmd);
static void handle_select_series_two_op(
    const std::vector<std::string>& cmd);
static void handle_select_series_two_op_limit(
    const std::vector<std::string>& cmd);
static void handle_select_series_one_op_last(
    const std::vector<std::string>& cmd);
static void handle_select_series_two_op_last(
    const std::vector<std::string>& cmd);
static void handle_count_from_series(const std::vector<std::string>& cmd);
static void handle_delete_from_series(const std::vector<std::string>& cmd);
static void handle_write_series(const std::vector<std::string>& cmd);
static void handle_list_series(const std::vector<std::string>& cmd);
static void handle_list_schema(const std::vector<std::string>& cmd);
static void handle_create_measurement(const std::vector<std::string>& cmd);
static void handle_list_measurements(const std::vector<std::string>& cmd);
static void handle_create_database(const std::vector<std::string>& cmd);
static void handle_list_databases(const std::vector<std::string>& cmd);
static void handle_init(const std::vector<std::string>& cmd);

static const command_syntax commands[] =
{
    {
        handle_init,
        {CT_STR_INIT},
    },
    {
        handle_create_database,
        {CT_STR_CREATE, CT_STR_DATABASE, CT_DATABASE_SPECIFIER},
    },
    {
        handle_list_databases,
        {CT_STR_LIST, CT_STR_DATABASES},
    },
    {
        handle_create_measurement,
        {CT_STR_CREATE, CT_STR_MEASUREMENT, CT_MEASUREMENT_SPECIFIER,
         CT_STR_WITH, CT_STR_FIELDS, CT_TYPED_FIELDS},
    },
    {
        handle_list_measurements,
        {CT_STR_LIST, CT_STR_MEASUREMENTS, CT_STR_FROM, CT_DATABASE_SPECIFIER},
    },
    {
        handle_write_series,
        {CT_STR_WRITE, CT_STR_SERIES, CT_SERIES_SPECIFIER, CT_UINT64},
    },
    {
        handle_list_series,
        {CT_STR_LIST, CT_STR_SERIES, CT_STR_FROM, CT_MEASUREMENT_SPECIFIER},
    },
    {
        handle_list_schema,
        {CT_STR_LIST, CT_STR_SCHEMA, CT_STR_FROM, CT_MEASUREMENT_SPECIFIER},
    },
    {
        handle_select_series_one_op,
        {CT_STR_SELECT, CT_FIELD_SPECIFIER, CT_STR_FROM, CT_SERIES_SPECIFIER,
         CT_STR_WHERE, CT_STR_TIME_NS, CT_COMPARISON, CT_UINT64},
    },
    {
        handle_select_series_one_op_limit,
        {CT_STR_SELECT, CT_FIELD_SPECIFIER, CT_STR_FROM, CT_SERIES_SPECIFIER,
         CT_STR_WHERE, CT_STR_TIME_NS, CT_COMPARISON, CT_UINT64, CT_STR_LIMIT,
         CT_UINT64},
    },
    {
        handle_select_series_two_op,
        {CT_STR_SELECT, CT_FIELD_SPECIFIER, CT_STR_FROM, CT_SERIES_SPECIFIER,
         CT_STR_WHERE, CT_UINT64, CT_COMPARISON_LEFT, CT_STR_TIME_NS,
         CT_COMPARISON_LEFT, CT_UINT64},
    },
    {
        handle_select_series_two_op_limit,
        {CT_STR_SELECT, CT_FIELD_SPECIFIER, CT_STR_FROM, CT_SERIES_SPECIFIER,
         CT_STR_WHERE, CT_UINT64, CT_COMPARISON_LEFT, CT_STR_TIME_NS,
         CT_COMPARISON_LEFT, CT_UINT64, CT_STR_LIMIT, CT_UINT64},
    },
    {
        handle_select_series_one_op_last,
        {CT_STR_SELECT, CT_FIELD_SPECIFIER, CT_STR_FROM, CT_SERIES_SPECIFIER,
         CT_STR_WHERE, CT_STR_TIME_NS, CT_COMPARISON, CT_UINT64, CT_STR_LAST,
         CT_UINT64},
    },
    {
        handle_select_series_two_op_last,
        {CT_STR_SELECT, CT_FIELD_SPECIFIER, CT_STR_FROM, CT_SERIES_SPECIFIER,
         CT_STR_WHERE, CT_UINT64, CT_COMPARISON_LEFT, CT_STR_TIME_NS,
         CT_COMPARISON_LEFT, CT_UINT64, CT_STR_LAST, CT_UINT64},
    },
    {
        handle_count_from_series,
        {CT_STR_COUNT, CT_STR_FROM, CT_SERIES_SPECIFIER, CT_STR_WHERE, 
         CT_UINT64, CT_COMPARISON_LEFT, CT_STR_TIME_NS, CT_COMPARISON_LEFT,
         CT_UINT64},
    },
    {
        handle_delete_from_series,
        {CT_STR_DELETE, CT_STR_FROM, CT_SERIES_SPECIFIER, CT_STR_WHERE,
         CT_STR_TIME_NS, CT_COMPARISON_LEFT, CT_UINT64},
    },
};

static void
print_op_points(const tsdb::select_op& op, size_t index, size_t n)
{
    for (size_t i=index; i<index + n; ++i)
    {
        printf("%20llu ",op.timestamp_data[i]);
        for (size_t j=0; j<op.fields.size(); ++j)
        {
            if (op.is_field_null(j,i))
            {
                printf("%20s ","null");
                continue;
            }

            const auto* p = op.field_data[j];
            switch (op.fields[j].type)
            {
                case tsdb::FT_BOOL:
                    printf("%20s ",((const uint8_t*)p)[i] ? "true" : "false");
                break;

                case tsdb::FT_U32:
                    printf("%20u ",((const uint32_t*)p)[i]);
                break;

                case tsdb::FT_U64:
                    printf("%20llu ",((const uint64_t*)p)[i]);
                break;

                case tsdb::FT_F32:
                    printf("%20f ",((const float*)p)[i]);
                break;

                case tsdb::FT_F64:
                    printf("%20f ",((const double*)p)[i]);
                break;

                case tsdb::FT_I32:
                    printf("%20d ",((const int32_t*)p)[i]);
                break;

                case tsdb::FT_I64:
                    printf("%20lld ",((const int64_t*)p)[i]);
                break;
            }
        }
        printf("\n");
    }
}

static void
_handle_select_series(tsdb::select_op& op)
{
    if (!op.npoints)
        return;

    printf("%20s ","time_ns");
    for (const auto& f : op.fields)
        printf("%20s ",f.name);
    printf("\n");

    for (;;)
    {
        for (size_t i=0; i<op.fields.size() + 1; ++i)
            printf("-------------------- ");
        printf("\n");
        if (op.npoints <= MAX_PRINT_RESULTS)
            print_op_points(op,0,op.npoints);
        else
        {
            print_op_points(op,0,MAX_PRINT_RESULTS/2);
            printf("... [%zu points omitted] ...\n",
                   op.npoints-MAX_PRINT_RESULTS);
            print_op_points(op,op.npoints-MAX_PRINT_RESULTS/2,
                            MAX_PRINT_RESULTS/2);
        }
        if (op.is_last)
            break;

        op.advance();
    }
}

static void
_handle_select_series_limit(const std::string& series,
    const std::string& field_specifier, uint64_t t0, uint64_t t1,
    uint64_t N)
{
    std::vector<std::string> fields;
    if (field_specifier != "*")
        fields = str::split(field_specifier,",");
    auto components = futil::path(series).decompose();
    tsdb::database db(components[0]);
    tsdb::measurement m(db,components[1]);
    tsdb::series_read_lock read_lock(m,components[2]);
    tsdb::select_op_first op(read_lock,series,fields,t0,t1,N);
    _handle_select_series(op);
}

static void
_handle_select_series_one_op(const std::vector<std::string>& cmd, uint64_t N)
{
    // Handles a command such as:
    //
    //  select a,b,c from <series> where time_ns <= T
    uint64_t t0 = 0;
    uint64_t t1 = 0xFFFFFFFFFFFFFFFF;
    uint64_t T = std::stoul(cmd[7]);
    if (cmd[6] == ">")
        t0 = T + 1;
    else if (cmd[6] == ">=")
        t0 = T;
    else if (cmd[6] == "==")
        t0 = t1 = T;
    else if (cmd[6] == "<=")
        t1 = T;
    else
        t1 = T - 1;

    _handle_select_series_limit(cmd[3],cmd[1],t0,t1,N);
}

static void
handle_select_series_one_op(const std::vector<std::string>& cmd)
{
    _handle_select_series_one_op(cmd,0xFFFFFFFFFFFFFFFF);
}

static void
handle_select_series_one_op_limit(const std::vector<std::string>& cmd)
{
    _handle_select_series_one_op(cmd,std::stoul(cmd[9]));
}

static void
_handle_select_series_two_op(const std::vector<std::string>& cmd, uint64_t N)
{
    // Handles a command such as:
    //
    //  select a,b,c from <series> where T0 <= time_ns <= T1
    uint64_t t0 = 0;
    uint64_t t1 = 0xFFFFFFFFFFFFFFFF;
    uint64_t T0 = std::stoul(cmd[5]);
    uint64_t T1 = std::stoul(cmd[9]);
    if (cmd[6] == "<")
        t0 = T0 + 1;
    else
        t0 = T0;
    if (cmd[8] == "<")
        t1 = T1 - 1;
    else
        t1 = T1;

    _handle_select_series_limit(cmd[3],cmd[1],t0,t1,N);
}

static void
handle_select_series_two_op(const std::vector<std::string>& cmd)
{
    _handle_select_series_two_op(cmd,0xFFFFFFFFFFFFFFFF);
}

static void
handle_select_series_two_op_limit(const std::vector<std::string>& cmd)
{
    _handle_select_series_two_op(cmd,std::stoul(cmd[11]));
}

static void
_handle_select_series_last(const std::string& series,
    const std::string& field_specifier, uint64_t t0, uint64_t t1,
    uint64_t N)
{
    std::vector<std::string> fields;
    if (field_specifier != "*")
        fields = str::split(field_specifier,",");
    auto components = futil::path(series).decompose();
    tsdb::database db(components[0]);
    tsdb::measurement m(db,components[1]);
    tsdb::series_read_lock read_lock(m,components[2]);
    tsdb::select_op_last op(read_lock,series,fields,t0,t1,N);
    _handle_select_series(op);
}

static void
handle_select_series_one_op_last(const std::vector<std::string>& cmd)
{
    // Handles a command such as:
    //
    //  select a,b,c from <series> where time_ns <= T last N
    uint64_t t0 = 0;
    uint64_t t1 = 0xFFFFFFFFFFFFFFFF;
    uint64_t T = std::stoul(cmd[7]);
    size_t N = std::stoul(cmd[9]);
    if (cmd[6] == ">")
        t0 = T + 1;
    else if (cmd[6] == ">=")
        t0 = T;
    else if (cmd[6] == "==")
        t0 = t1 = T;
    else if (cmd[6] == "<=")
        t1 = T;
    else
        t1 = T - 1;

    _handle_select_series_last(cmd[3],cmd[1],t0,t1,N);
}

static void
handle_select_series_two_op_last(const std::vector<std::string>& cmd)
{
    // Handles a command such as:
    //
    //  select a,b,c from <series> where T0 <= time_ns <= T1 last N
    uint64_t t0 = 0;
    uint64_t t1 = 0xFFFFFFFFFFFFFFFF;
    uint64_t T0 = std::stoul(cmd[5]);
    uint64_t T1 = std::stoul(cmd[9]);
    size_t N = std::stoul(cmd[11]);
    if (cmd[6] == "<")
        t0 = T0 + 1;
    else
        t0 = T0;
    if (cmd[8] == "<")
        t1 = T1 - 1;
    else
        t1 = T1;

    _handle_select_series_last(cmd[3],cmd[1],t0,t1,N);
}

static void
handle_count_from_series(const std::vector<std::string>& cmd)
{
    // Handles a command such as:
    //
    //  count from <series> where T0 <= time_ns <= T1
    uint64_t t0 = std::stoul(cmd[4]);
    uint64_t t1 = std::stoul(cmd[8]);
    if (cmd[5] == "<")
        ++t0;
    if (cmd[7] == "<")
    {
        if (t1 == 0)
        {
            printf("Invalid end time.\n");
            return;
        }
        --t1;
    }
    auto components = futil::path(cmd[2]).decompose();
    tsdb::database db(components[0]);
    tsdb::measurement m(db,components[1]);
    tsdb::series_read_lock read_lock(m,components[2]);
    printf("%zu\n",tsdb::count_points(read_lock,t0,t1));
}

static void
handle_delete_from_series(const std::vector<std::string>& cmd)
{
    // Handles a command such as:
    //
    //  delete from <series> where time_ns <= T
    uint64_t t = std::stoul(cmd[6]);
    if (cmd[5] == "<")
    {
        if (t == 0)
            return;
        --t;
    }
    auto components = futil::path(cmd[2]).decompose();
    tsdb::database db(components[0]);
    tsdb::measurement m(db,components[1]);
    tsdb::delete_points(m,components[2],t);
}

static void
handle_list_schema(const std::vector<std::string>& v)
{
    // Handles a command such as
    //
    //  list schema from pt-1/xtalx_data
    auto components = futil::path(v[3]).decompose();
    tsdb::database db(components[0]);
    tsdb::measurement m(db,components[1]);
    for (const auto& se : m.fields)
        printf("%4s %s\n",tsdb::ftinfos[se.type].name,se.name);
}

static void
handle_list_series(const std::vector<std::string>& v)
{
    // Handles a command such as
    //
    //  list series from pt-1/xtalx_data
    auto components = futil::path(v[3]).decompose();
    tsdb::database db(components[0]);
    tsdb::measurement m(db,components[1]);
    auto ss = tsdb::list_series(m);
    for (const auto& s : ss)
        printf("%s\n",s.c_str());
}

static void
handle_write_series(const std::vector<std::string>& v)
{
    // Handles a command such as:
    //
    //  write series pt-1/xtalx_data/XTI-10-1000000 N
    uint32_t n = std::stoul(v[3]);
    auto components = futil::path(v[2]).decompose();
    tsdb::database db(components[0]);
    tsdb::measurement m(db,components[1]);

    // Generate points.
    tsdb::series_write_lock write_lock =
        tsdb::open_or_create_and_lock_series(m,components[2]);

    uint64_t t0 = write_lock.time_last + 1;
    if (t0 == 1)
        t0 = 1741235979144457000;

    std::vector<uint64_t> data_points;
    for (size_t i=0; i<n; ++i)
        data_points.push_back(t0++);
    for (const auto& field : m.fields)
    {
        // Generate a bitmap with some random null values.  We nullify one
        // value out of every 64.
        for (size_t i=0; i<ceil_div(n,64U); ++i)
        {
            uint64_t v = 0xFFFFFFFFFFFFFFFFULL;
            v ^= (1ULL << (random() % 64));
            data_points.push_back(v);
        }

        // Put in values.
        switch (field.type)
        {
            case tsdb::FT_BOOL:
                for (size_t i=0; i<ceil_div(n,8U); ++i)
                    data_points.push_back(0x0101010101010101ULL);
            break;

            case tsdb::FT_U32:
            case tsdb::FT_I32:
                for (size_t i=0; i<ceil_div(n,2U); ++i)
                {
                    data_points.push_back((((uint64_t)u32 + 0ULL) <<  0) |
                                          (((uint64_t)u32 + 1ULL) << 32));
                    u32 += 2;
                }
            break;

            case tsdb::FT_U64:
            case tsdb::FT_I64:
                for (size_t i=0; i<n; ++i)
                    data_points.push_back(u64++);
            break;

            case tsdb::FT_F32:
                for (size_t i=0; i<ceil_div(n,2U); ++i)
                {
                    union
                    {
                        float       f[2];
                        uint64_t    v;
                    } u = {{f32, f32 + 0.1f}};
                    f32 += 0.2;
                    data_points.push_back(u.v);
                }
            break;

            case tsdb::FT_F64:
                for (size_t i=0; i<n; ++i)
                {
                    union
                    {
                        double      f;
                        uint64_t    v;
                    } u = {f64};
                    f64 += 0.01;
                    data_points.push_back(u.v);
                }
            break;
        }
    }

    tsdb::write_series(write_lock,n,0,data_points.size()*sizeof(uint64_t),
                       &data_points[0]);
}

static void
handle_list_measurements(const std::vector<std::string>& v)
{
    // Handles a command such as
    //
    //  list measurements from pt-1
    tsdb::database db(v[3]);
    auto ms = tsdb::list_measurements(db);
    for (const auto& s : ms)
        printf("%s\n",s.c_str());
}

static void
handle_create_measurement(const std::vector<std::string>& v)
{
    // Handles a command such as:
    //
    //  create measurement pt-1/xtalx_data with fields \
    //      pressure_psi/f64,temp_c/f32,pressure_hz/f64,temp_hz/f64
    std::vector<tsdb::schema_entry> fields;
    auto field_specifiers = str::split(v[5],",");
    for (const auto& fs : field_specifiers)
    {
        auto field_specifier = str::split(fs,"/");
        if (field_specifier.size() != 2 || field_specifier[0].empty() ||
            field_specifier[1].empty())
        {
            printf("Invalid field specifier: %s\n",fs.c_str());
            return;
        }
        if (field_specifier[0].size() >= 124)
        {
            printf("Field name too long: %s\n",fs.c_str());
            return;
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
        {
            printf("Unrecognized field type '%s'.\n",
                   field_specifier[1].c_str());
            return;
        }

        fields.push_back(se);
    }

    auto components = futil::path(v[2]).decompose();
    tsdb::database db(components[0]);
    tsdb::create_measurement(db,components[1],fields);
}

static void
handle_list_databases(const std::vector<std::string>& cmd)
{
    auto dbs = tsdb::list_databases();
    for (const auto& s : dbs)
        printf("%s\n",s.c_str());
}

static void
handle_create_database(const std::vector<std::string>& cmd)
{
    printf("Creating database \"%s\"...\n",cmd[2].c_str());
    tsdb::create_database(cmd[2].c_str());
}

static void
handle_init(const std::vector<std::string>&)
{
    printf("Initializing TSDB directories...\n");
    tsdb::init();
}

int
main(int argc, const char* argv[])
{
    std::string cmd;

    printf("%s\n",GIT_VERSION);
    for (;;)
    {
        printf("tsdbcli> ");

        bool invalid = false;
        cmd.clear();
        for (;;)
        {
            int c = getchar();
            if (c == EOF)
                return 0;
            if (c == '\n')
                break;
            if (!isprint(c))
                invalid = true;

            cmd += c;
        }

        if (invalid)
        {
            printf("Invalid characters\n");
            continue;
        }

        auto v = str::split(cmd);
        bool handled = false;
        for (const auto& c : commands)
        {
            if (c.parse(v))
            {
                handled = true;
                try
                {
                    c.handler(v);
                }
                catch (const std::exception& e)
                {
                    printf("Error: %s\n",e.what());
                }
                break;
            }
        }

        if (!handled)
            printf("Syntax error.\n");
    }
}
