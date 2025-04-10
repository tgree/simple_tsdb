// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include <version.h>
#include <hdr/kmath.h>
#include <hdr/auto_buf.h>
#include <hdr/fixed_vector.h>
#include <strutil/strutil.h>
#include <futil/ipv4.h>
#include <futil/ssl.h>
#include <libtsdb/tsdb.h>

#include <algorithm>
#include <thread>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <inttypes.h>

enum command_token : uint32_t
{
    CT_CREATE_DATABASE      = 0x60545A42,
    CT_CREATE_MEASUREMENT   = 0xBB632CE1,
    CT_WRITE_POINTS         = 0xEAF5E003,
    CT_SELECT_POINTS_LIMIT  = 0x7446C560,
    CT_SELECT_POINTS_LAST   = 0x76CF2220,
    CT_DELETE_POINTS        = 0xD9082F2C,
    CT_GET_SCHEMA           = 0x87E5A959,
    CT_LIST_DATABASES       = 0x29200D6D,
    CT_LIST_MEASUREMENTS    = 0x0FEB1399,
    CT_LIST_SERIES          = 0x7B8238D6,
    CT_COUNT_POINTS         = 0x0E329B19,
    CT_SUM_POINTS           = 0x90305A39,
    CT_NOP                  = 0x22CF1296,
    CT_AUTHENTICATE         = 0x0995EBDA,
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
    DT_NPOINTS          = 0x5F469D08,   // <npoints> (uint64_t)
    DT_WINDOW_NS        = 0x76F0C374,   // <window_ns> (uint64_t)
    DT_SUMS_CHUNK       = 0x53FC76FC,   // <chunk_npoints> (uint16_t)
    DT_USERNAME         = 0x6E39D1DE,   // <username>
    DT_PASSWORD         = 0x602E5B01,   // <password>
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

    parsed_data_token():
        data(NULL)
    {
    }

    parsed_data_token(const parsed_data_token&) = delete;

    parsed_data_token(parsed_data_token&& other):
        type(other.type),
        data(other.data),
        u64(other.u64)
    {
        other.data = NULL;
    }

    ~parsed_data_token()
    {
        free((void*)data);
        data = NULL;
    }
};

struct chunk_header
{
    uint32_t    npoints;
    uint32_t    bitmap_offset;
    uint32_t    data_len;
    uint8_t     data[];
};

struct connection
{
    tcp::stream&    s;
};

struct command_syntax
{
    void (* const handler)(connection& c,
                           const std::vector<parsed_data_token>& tokens);
    const command_token cmd_token;
    const std::vector<data_token> data_tokens;
};

static void handle_create_database(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_list_databases(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_create_measurement(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_list_measurements(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_list_series(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_get_schema(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_count_points(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_write_points(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_delete_points(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_select_points_limit(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_select_points_last(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_sum_points(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_nop(
    connection& conn, const std::vector<parsed_data_token>& tokens);

static const command_syntax commands[] =
{
    {
        handle_create_database,
        CT_CREATE_DATABASE,
        {DT_DATABASE, DT_END},
    },
    {
        handle_list_databases,
        CT_LIST_DATABASES,
        {DT_END},
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
        handle_list_measurements,
        CT_LIST_MEASUREMENTS,
        {DT_DATABASE, DT_END},
    },
    {
        handle_list_series,
        CT_LIST_SERIES,
        {DT_DATABASE, DT_MEASUREMENT, DT_END},
    },
    {
        handle_count_points,
        CT_COUNT_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_TIME_FIRST, DT_TIME_LAST,
         DT_END},
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
        handle_sum_points,
        CT_SUM_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_FIELD_LIST, DT_TIME_FIRST,
         DT_TIME_LAST, DT_WINDOW_NS, DT_END},
    },
    {
        handle_nop,
        CT_NOP,
        {DT_END},
    },
};

static const command_syntax auth_command =
{
    NULL,
    CT_AUTHENTICATE,
    {DT_USERNAME, DT_PASSWORD, DT_END},
};

static const uint8_t pad_bytes[8] = {};

static tsdb::root* root;

static uint64_t
time_ns()
{
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC_RAW,&tp);
    return tp.tv_sec*1000000000ULL + tp.tv_nsec;
}

static void
sleep_for_ns(uint64_t nsec)
{
    struct timespec rqtp;
    struct timespec rmtp;
    rqtp.tv_sec  = (nsec / 1000000000);
    rqtp.tv_nsec = (nsec % 1000000000);
    while (nanosleep(&rqtp,&rmtp) != 0)
        rqtp = rmtp;
}

static void
handle_create_database(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    // TODO: Use string_view.
    std::string database(tokens[0].data,tokens[0].len);
    printf("CREATE DATABASE %s\n",database.c_str());
    root->create_database(database.c_str());
}

static void
handle_list_databases(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    auto dbs = root->list_databases();
    printf("LIST DATABASES\n");
    for (const auto& d_name : dbs)
    {
        uint32_t dt  = DT_DATABASE;
        uint16_t len = d_name.size();
        conn.s.send_all(&dt,sizeof(dt));
        conn.s.send_all(&len,sizeof(len));
        conn.s.send_all(d_name.c_str(),len);
    }
}

static void
handle_create_measurement(connection& conn,
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
    tsdb::database db(*root,database);
    tsdb::create_measurement(db,measurement,fields);
}

static void
handle_get_schema(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    futil::path measurement_path(database,measurement);

    printf("GET SCHEMA FOR %s\n",measurement_path.c_str());
    tsdb::database db(*root,tokens[0].to_string());
    tsdb::measurement m(db,tokens[1].to_string());
    for (const auto& f : m.fields)
    {
        uint32_t ft[3] = {DT_FIELD_TYPE, f.type, DT_FIELD_NAME};
        conn.s.send_all(&ft,sizeof(ft));
        uint16_t len = strlen(f.name);
        conn.s.send_all(&len,sizeof(len));
        conn.s.send_all(f.name,len);
    }
}

static void
handle_list_measurements(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string db_name(tokens[0].data,tokens[0].len);
    printf("LIST MEASUREMENTS FROM %s\n",db_name.c_str());
    auto db = tsdb::database(*root,db_name);
    auto ms = db.list_measurements();
    for (const auto& m_name : ms)
    {
        uint32_t dt  = DT_MEASUREMENT;
        uint16_t len = m_name.size();
        conn.s.send_all(&dt,sizeof(dt));
        conn.s.send_all(&len,sizeof(len));
        conn.s.send_all(m_name.c_str(),len);
    }
}

static void
handle_list_series(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string db_name(tokens[0].data,tokens[0].len);
    std::string m_name(tokens[1].data,tokens[1].len);
    printf("LIST SERIES FROM %s/%s\n",db_name.c_str(),m_name.c_str());
    auto db = tsdb::database(*root,db_name);
    auto m = tsdb::measurement(db,m_name);
    auto ss = m.list_series();
    for (const auto& s_name : ss)
    {
        uint32_t dt  = DT_SERIES;
        uint16_t len = s_name.size();
        conn.s.send_all(&dt,sizeof(dt));
        conn.s.send_all(&len,sizeof(len));
        conn.s.send_all(s_name.c_str(),len);
    }
}

static void
handle_count_points(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    uint64_t t0 = tokens[3].u64;
    uint64_t t1 = tokens[4].u64;

    futil::path path(database,measurement,series);
    printf("COUNT FROM %s\n",path.c_str());

    tsdb::database db(*root,database);
    tsdb::measurement m(db,measurement);
    tsdb::series_read_lock read_lock(m,series);
    auto cr = tsdb::count_points(read_lock,t0,t1);

    uint32_t dt = DT_TIME_FIRST;
    conn.s.send_all(&dt,sizeof(dt));
    conn.s.send_all(&cr.time_first,sizeof(cr.time_first));

    dt = DT_TIME_LAST;
    conn.s.send_all(&dt,sizeof(dt));
    conn.s.send_all(&cr.time_last,sizeof(cr.time_last));

    dt = DT_NPOINTS;
    conn.s.send_all(&dt,sizeof(dt));
    conn.s.send_all(&cr.npoints,sizeof(cr.npoints));
}

static void
handle_write_points(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    futil::path path(database,measurement,series);

    printf("WRITE TO %s\n",path.c_str());
    tsdb::database db(*root,tokens[0].to_string());
    tsdb::measurement m(db,tokens[1].to_string());
    auto write_lock = tsdb::open_or_create_and_lock_series(m,series);

    for (;;)
    {
        uint32_t tokens[2] = {DT_READY_FOR_CHUNK,10*1024*1024};
        conn.s.send_all(tokens,sizeof(tokens));

        uint32_t dt = conn.s.pop<uint32_t>();
        if (dt == DT_END)
        {
            printf("WRITE END\n");
            return;
        }
        if (dt != DT_CHUNK)
            throw futil::errno_exception(EINVAL);

        chunk_header ch = conn.s.pop<chunk_header>();
        if (ch.data_len > 10*1024*1024)
            throw futil::errno_exception(ENOMEM);

        printf("RECV %u BYTES\n",ch.data_len);
        auto_buf data(ch.data_len);
        conn.s.recv_all(data,ch.data_len);

        printf("WRITE %u POINTS TO %s\n",ch.npoints,path.c_str());
        tsdb::write_wal(write_lock,ch.npoints,ch.bitmap_offset,ch.data_len,
                        data);
    }
}

static void
handle_delete_points(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    futil::path path(database,measurement,series);
    uint64_t t = tokens[3].u64;

    printf("DELETE FROM %s WHERE time_ns <= %" PRIu64 "\n",path.c_str(),t);
    tsdb::database db(*root,database);
    tsdb::measurement m(db,measurement);
    tsdb::delete_points(m,series,t);
}

static void
_handle_select_points(connection& conn, tsdb::select_op& op, tsdb::wal_query& wq,
    size_t N)
{
    while (op.npoints)
    {
        N -= op.npoints;

        size_t len = op.compute_chunk_len();
        uint32_t tokens[4] = {DT_CHUNK,(uint32_t)op.npoints,
                              (uint32_t)(op.bitmap_offset % 64),(uint32_t)len};
        conn.s.send_all(tokens,sizeof(tokens));

        // Start by sending timestamp data.
        conn.s.send_all(op.timestamps_begin,8*op.npoints);

        // Send each field in turn.
        size_t bitmap_index = op.bitmap_offset / 64;
        size_t bitmap_n = ceil_div<size_t>(op.bitmap_offset + op.npoints,64) -
                          bitmap_index;
        for (size_t i=0; i<op.fields.size(); ++i)
        {
            // First we send the bitmap.
            auto* bitmap = (const uint64_t*)op.bitmap_mappings[i].addr;
            conn.s.send_all(&bitmap[bitmap_index],bitmap_n*8);

            // Next we send the field data.
            const auto* fti = &tsdb::ftinfos[op.fields[i]->type];
            size_t data_len = op.npoints*fti->nbytes;
            conn.s.send_all(op.field_data[i],data_len);

            // Finally, pad to 8 bytes if necessary.
            if (data_len % 8)
                conn.s.send_all(pad_bytes,8 - (data_len % 8));
        }

        op.next();
    }
    if (N && wq.nentries)
    {
        // We are going to assume that the WAL is small compared to the maximum
        // chunk size, so we won't be allocating a horrible amount of memory.
        // Typically the maximum WAL size will be 1024 entries since we don't
        // really need a very large WAL in order to shrink the main storage
        // write overhead to a very small number.
        N = MIN(N,wq.nentries);

        size_t len = op.compute_new_chunk_len(N);
        uint32_t tokens[4] = {DT_CHUNK,(uint32_t)N,0,(uint32_t)len};
        conn.s.send_all(tokens,sizeof(tokens));

        // Generate and send the timestamp data.  We reuse the same buffer for
        // everything.
        auto_buf u64_buf(sizeof(uint64_t)*N);
        auto* timestamps = (uint64_t*)u64_buf.data;
        for (size_t i=0; i<N; ++i)
            timestamps[i] = wq[i].time_ns;
        conn.s.send_all(timestamps,N*sizeof(uint64_t));

        // Generate and send each field in turn.
        void* data = u64_buf.data;
        auto* bitmap = (uint64_t*)u64_buf.data;
        size_t bitmap_len = ceil_div<size_t>(N,64)*sizeof(uint64_t);
        for (size_t i=0; i<op.fields.size(); ++i)
        {
            auto& fti = tsdb::ftinfos[op.fields[i]->type];
            size_t field_index = op.fields[i]->index;

            // Generate and send the bitmap.
            for (size_t j=0; j<N; ++j)
            {
                tsdb::set_bitmap_bit(bitmap,j,
                                     wq[j].get_bitmap_bit(field_index));
            }
            conn.s.send_all(bitmap,bitmap_len);

            // Generate and send the field data.
            switch (fti.nbytes)
            {
                case 1:
                    for (size_t j=0; j<N; ++j)
                        ((uint8_t*)data)[j] = wq[j].fields[field_index].u8;
                break;

                case 4:
                    for (size_t j=0; j<N; ++j)
                        ((uint32_t*)data)[j] = wq[j].fields[field_index].u32;
                break;

                case 8:
                    for (size_t j=0; j<N; ++j)
                        ((uint64_t*)data)[j] = wq[j].fields[field_index].u64;
                break;
            }
            size_t data_len = fti.nbytes*N;
            conn.s.send_all(data,data_len);

            // Finally, pad to 8 bytes if necessary.
            if (data_len % 8)
                conn.s.send_all(pad_bytes,8 - (data_len % 8));
        }
    }

    uint32_t dt = DT_END;
    conn.s.send_all(&dt,sizeof(dt));
}

static void
handle_select_points_limit(connection& conn,
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

    printf("SELECT %s FROM %s WHERE %" PRIu64 " <= time_ns <= %" PRIu64
           " LIMIT %" PRIu64 "\n",
           field_list.c_str(),path.c_str(),t0,t1,N);
    tsdb::database db(*root,database);
    tsdb::measurement m(db,measurement);
    tsdb::series_read_lock read_lock(m,series);
    tsdb::wal_query wq(read_lock,t0,t1);
    tsdb::select_op_first op(read_lock,path,str::split(field_list,","),t0,t1,N);
    _handle_select_points(conn,op,wq,N);
}

static void
handle_select_points_last(connection& conn,
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

    printf("SELECT %s FROM %s WHERE %" PRIu64 " <= time_ns <= %" PRIu64
           " LAST %" PRIu64 "\n",
           field_list.c_str(),path.c_str(),t0,t1,N);
    tsdb::database db(*root,database);
    tsdb::measurement m(db,measurement);
    tsdb::series_read_lock read_lock(m,series);
    tsdb::wal_query wq(read_lock,t0,t1);
    if (wq.nentries > N)
    {
        wq._begin += (wq.nentries - N);
        wq.nentries = N;
    }
    tsdb::select_op_last op(read_lock,path,str::split(field_list,","),t0,t1,
                            N - wq.nentries);
    _handle_select_points(conn,op,wq,N);
}

static void
handle_sum_points(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    futil::path path(database,measurement,series);
    std::string field_list(tokens[3].data,tokens[3].len);
    uint64_t t0 = tokens[4].u64;
    uint64_t t1 = tokens[5].u64;
    uint64_t window_ns = tokens[6].u64;

    printf("SUM %s FROM %s WHERE %" PRIu64 " <= time_ns <= %" PRIu64
           " WINDOW_NS %" PRIu64 "\n",
           field_list.c_str(),path.c_str(),t0,t1,window_ns);
    tsdb::database db(*root,database);
    tsdb::measurement m(db,measurement);
    tsdb::series_read_lock read_lock(m,series);
    tsdb::sum_op op(read_lock,path,str::split(field_list,","),t0,t1,window_ns);

    const size_t nfields = op.op.fields.size();
    fixed_vector<uint64_t> timestamps(1024);
    tsdb::field_vector<std::vector<double>> field_sums;
    tsdb::field_vector<std::vector<uint64_t>> field_npoints;
    for (size_t i=0; i<nfields; ++i)
    {
        field_sums.emplace_back(std::vector<double>());
        field_sums[i].reserve(1024);
        field_npoints.emplace_back(std::vector<uint64_t>());
        field_npoints[i].reserve(1024);
    }

    bool done = false;
    while (!done)
    {
        while (timestamps.size() < 1024)
        {
            if (!op.next())
            {
                done = true;
                break;
            }

            timestamps.emplace_back(op.range_t0);
            for (size_t j=0; j<nfields; ++j)
            {
                field_sums[j].push_back(op.sums[j]);
                field_npoints[j].push_back(op.npoints[j]);
            }
        }

        uint16_t chunk_npoints = timestamps.size();
        if (chunk_npoints)
        {
            data_token dt = DT_SUMS_CHUNK;
            conn.s.send_all(&dt,sizeof(dt));
            conn.s.send_all(&chunk_npoints,sizeof(chunk_npoints));
            conn.s.send_all(&timestamps[0],chunk_npoints*sizeof(uint64_t));
            timestamps.clear();
            for (size_t j=0; j<nfields; ++j)
            {
                conn.s.send_all(&field_sums[j][0],chunk_npoints*sizeof(double));
                field_sums[j].clear();
            }
            for (size_t j=0; j<nfields; ++j)
            {
                conn.s.send_all(&field_npoints[j][0],
                                chunk_npoints*sizeof(uint64_t));
                field_npoints[j].clear();
            }
        }
    }

    data_token dt = DT_END;
    conn.s.send_all(&dt,sizeof(dt));
}

static void
handle_nop(connection& conn, const std::vector<parsed_data_token>& tokens)
{
    // Do nothing.
}

static void
parse_cmd(tcp::stream& s, const command_syntax& cs,
    std::vector<parsed_data_token>& tokens)
{
    printf("Got command 0x%08X.\n",cs.cmd_token);

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
        pdt.u64 = 0;
        switch (dt)
        {
            case DT_DATABASE:
            case DT_MEASUREMENT:
            case DT_SERIES:
            case DT_TYPED_FIELDS:
            case DT_FIELD_LIST:
            case DT_USERNAME:
            case DT_PASSWORD:
                pdt.len = s.pop<uint16_t>();
                if (pdt.len >= 1024)
                {
                    printf("String length %zu too long.\n",pdt.len);
                    throw futil::errno_exception(EINVAL);
                }
                pdt.data = (char*)malloc(pdt.len);
                s.recv_all((char*)pdt.data,pdt.len);
                tokens.push_back(std::move(pdt));
            break;

            case DT_CHUNK:
                throw futil::errno_exception(ENOTSUP);
                tokens.push_back(std::move(pdt));
            break;

            case DT_TIME_FIRST:
            case DT_TIME_LAST:
            case DT_NLIMIT:
            case DT_NLAST:
            case DT_WINDOW_NS:
                pdt.u64 = s.pop<uint64_t>();
                tokens.push_back(std::move(pdt));
            break;

            case DT_END:
                tokens.push_back(std::move(pdt));
            break;

            default:
                throw futil::errno_exception(EINVAL);
            break;
        }
    }
}

static void
parse_and_exec(connection& conn, const command_syntax& cs)
{
    std::vector<parsed_data_token> tokens;
    parse_cmd(conn.s,cs,tokens);

    uint32_t status[2] = {DT_STATUS_CODE, 0};
    try
    {
        cs.handler(conn,tokens);
    }
    catch (const tsdb::exception& e)
    {
        printf("TSDB exception: %s\n",e.what());
        status[1] = e.sc;
    }
    
    printf("Sending status...\n");
    conn.s.send_all(&status,sizeof(status));
}

static void
process_stream(connection& conn)
{
    try
    {
        for (;;)
        {
            uint32_t ct = conn.s.pop<uint32_t>();

            bool found = false;
            for (auto& cmd : commands)
            {
                if (cmd.cmd_token == ct)
                {
                    parse_and_exec(conn,cmd);
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
request_handler(std::unique_ptr<tcp::stream> s)
{
    printf("Handling local %s remote %s.\n",
           s->local_addr_string().c_str(),s->remote_addr_string().c_str());

    connection conn{*s};
    process_stream(conn);

    printf("Teardown local %s remote %s.\n",
           s->local_addr_string().c_str(),s->remote_addr_string().c_str());
}

static void
auth_request_handler(std::unique_ptr<tcp::stream> s)
{
    uint64_t t0 = time_ns();

    printf("Authenticating local %s remote %s.\n",
           s->local_addr_string().c_str(),s->remote_addr_string().c_str());

    std::string username;
    std::string password;
    try
    {
        uint32_t dt = s->pop<uint32_t>();
        if (dt != CT_AUTHENTICATE)
            throw futil::errno_exception(EINVAL);

        std::vector<parsed_data_token> tokens;
        parse_cmd(*s,auth_command,tokens);

        username = tokens[0].to_string();
        password = tokens[1].to_string();
        printf("Authenticating user %s from %s...\n",
               username.c_str(),s->remote_addr_string().c_str());
        if (!root->verify_user(username,password))
            throw futil::errno_exception(EPERM);
        printf("Authentication from %s for user %s succeeded.\n",
               s->remote_addr_string().c_str(),
               username.c_str());

        uint32_t status[2] = {DT_STATUS_CODE, 0};
        s->send_all(&status,sizeof(status));
    }
    catch (const std::exception& e)
    {
        if (!username.empty())
        {
            printf("Authentication from %s for user %s failed.\n",
                   s->remote_addr_string().c_str(),
                   username.c_str());
        }
        else
        {
            printf("Authentication from %s failed.\n",
                   s->remote_addr_string().c_str());
        }
        printf("Authentication exception: %s\n",e.what());
        uint64_t t1 = time_ns();
        uint64_t nsecs = round_up_to_nearest_multiple(t1 - t0,
                                                      (uint64_t)2000000000);
        sleep_for_ns(nsecs + 1);
        return;
    }

    request_handler(std::move(s));
}

static void
socket4_workloop()
{
    tcp::ipv4::addr sa(4000,INADDR_LOOPBACK);
    tcp::ipv4::server_socket ss(sa);
    ss.listen(4);
    printf("TCP listening on %s.\n",ss.bind_addr.to_string().c_str());
    for (;;)
    {
        try
        {
            std::thread t(request_handler,ss.accept());
            t.detach();
        }
        catch (const std::exception& e)
        {
            printf("Exception accepting TCP connection: %s\n",e.what());
        }
    }
}

static void
ssl_workloop(const char* cert_file, const char* key_file)
{
    tcp::ssl::context sslctx(cert_file,key_file);
    tcp::ipv4::addr sa(4000,INADDR_ANY);
    tcp::ipv4::server_socket ss(sa);
    ss.listen(4);
    printf("SSL listening on %s.\n",ss.bind_addr.to_string().c_str());
    for (;;)
    {
        try
        {
            std::thread t(auth_request_handler,sslctx.wrap(ss.accept()));
            t.detach();
        }
        catch (const std::exception& e)
        {
            printf("Exception accepting SSL connection: %s\n",e.what());
        }
    }
}

int
main(int argc, const char* argv[])
{
    const char* root_path;
    if (argc == 2 || argc == 4)
        root_path = argv[1];
    else
        root_path = ".";

    printf("%s\n",GIT_VERSION);

    signal(SIGPIPE,SIG_IGN);

    root = new tsdb::root(root_path);

    if (argc == 1 || argc == 2)
        socket4_workloop();
    else if (argc == 3)
        ssl_workloop(argv[1],argv[2]);
    else if (argc == 4)
        ssl_workloop(argv[2],argv[3]);
}
