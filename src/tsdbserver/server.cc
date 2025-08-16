// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "tokens.h"
#include <version.h>
#include <hdr/kmath.h>
#include <hdr/auto_buf.h>
#include <hdr/fixed_vector.h>
#include <strutil/strutil.h>
#include <futil/tcp.h>
#include <futil/ssl.h>
#include <libtsdb/tsdb.h>

#include <algorithm>
#include <thread>
#if IS_MACOS
#include <pthread.h>
#elif IS_LINUX
#include <unistd.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <inttypes.h>

static uint64_t
time_ns()
{
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC_RAW,&tp);
    return tp.tv_sec*1000000000ULL + tp.tv_nsec;
}

static uint64_t
get_thread_id()
{
#if IS_MACOS
    uint64_t thread_id;
    int err = pthread_threadid_np(NULL,&thread_id);
    if (err)
        throw futil::errno_exception(err);
    return thread_id;
#elif IS_LINUX
    return gettid();
#else
#error Dont know how to get a thread ID.
#endif
}

struct connection
{
    // Connection state.
    tcp::stream&                    s;
    const uint64_t                  tid;
    uint64_t                        last_write_ns;
    std::string                     username;

    connection(tcp::stream& s, const std::string& username):
        s(s),
        tid(get_thread_id()),
        last_write_ns(0),
        username(username)
    {
    }
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
static void handle_active_series(
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
static void handle_integrate_points(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_nop(
    connection& conn, const std::vector<parsed_data_token>& tokens);

static const command_syntax<connection&> commands[] =
{
    {
        func_delegate(handle_create_database),
        CT_CREATE_DATABASE,
        {DT_DATABASE, DT_END},
    },
    {
        func_delegate(handle_list_databases),
        CT_LIST_DATABASES,
        {DT_END},
    },
    {
        func_delegate(handle_create_measurement),
        CT_CREATE_MEASUREMENT,
        {DT_DATABASE, DT_MEASUREMENT, DT_TYPED_FIELDS, DT_END},
    },
    {
        func_delegate(handle_get_schema),
        CT_GET_SCHEMA,
        {DT_DATABASE, DT_MEASUREMENT, DT_END},
    },
    {
        func_delegate(handle_list_measurements),
        CT_LIST_MEASUREMENTS,
        {DT_DATABASE, DT_END},
    },
    {
        func_delegate(handle_list_series),
        CT_LIST_SERIES,
        {DT_DATABASE, DT_MEASUREMENT, DT_END},
    },
    {
        func_delegate(handle_active_series),
        CT_ACTIVE_SERIES,
        {DT_DATABASE, DT_MEASUREMENT, DT_TIME_FIRST, DT_TIME_LAST, DT_END},
    },
    {
        func_delegate(handle_count_points),
        CT_COUNT_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_TIME_FIRST, DT_TIME_LAST,
         DT_END},
    },
    {
        func_delegate(handle_write_points),
        CT_WRITE_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES},
    },
    {
        func_delegate(handle_delete_points),
        CT_DELETE_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_TIME_LAST, DT_END},
    },
    {
        func_delegate(handle_select_points_limit),
        CT_SELECT_POINTS_LIMIT,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_FIELD_LIST, DT_TIME_FIRST,
         DT_TIME_LAST, DT_NLIMIT, DT_END},
    },
    {
        func_delegate(handle_select_points_last),
        CT_SELECT_POINTS_LAST,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_FIELD_LIST, DT_TIME_FIRST,
         DT_TIME_LAST, DT_NLAST, DT_END},
    },
    {
        func_delegate(handle_sum_points),
        CT_SUM_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_FIELD_LIST, DT_TIME_FIRST,
         DT_TIME_LAST, DT_WINDOW_NS, DT_END},
    },
    {
        func_delegate(handle_integrate_points),
        CT_INTEGRATE_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES, DT_FIELD_LIST, DT_TIME_FIRST,
         DT_TIME_LAST, DT_END},
    },
    {
        func_delegate(handle_nop),
        CT_NOP,
        {DT_END},
    },
};

static const command_syntax<connection&> auth_command =
{
    nop_delegate(),
    CT_AUTHENTICATE,
    {DT_USERNAME, DT_PASSWORD, DT_END},
};

static const uint8_t pad_bytes[8] = {};

static tsdb::root* root;

void
debugf(const char* fmt, ...)
{
    va_list ap;
    va_start(ap,fmt);
    root->vdebugf(fmt,ap);
    va_end(ap);
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
    root->debugf("CREATE DATABASE %s\n",database.c_str());
    root->create_database(database.c_str());
}

static void
handle_list_databases(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    auto dbs = root->list_databases();
    root->debugf("LIST DATABASES\n");
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
    size_t index = 0;
    size_t offset = 0;
    for (const auto& fs : field_specifiers)
    {
        auto field_specifier = str::split(fs,"/");
        if (field_specifier.size() != 2 || field_specifier[0].empty() ||
            field_specifier[1].empty() || field_specifiers[0].size() >= 124)
        {
            throw futil::errno_exception(EINVAL);
        }

        tsdb::schema_entry se{};
        se.version = SCHEMA_VERSION;
        se.index = index;
        se.offset = offset;
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

        ++index;
        offset += tsdb::ftinfos[se.type].nbytes;
    }

    root->debugf("CREATE MEASUREMENT %s\n",path.c_str());
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

    root->debugf("GET SCHEMA FOR %s\n",measurement_path.c_str());
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
    root->debugf("LIST MEASUREMENTS FROM %s\n",db_name.c_str());
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
    root->debugf("LIST SERIES FROM %s/%s\n",db_name.c_str(),m_name.c_str());
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
handle_active_series(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string db_name(tokens[0].data,tokens[0].len);
    std::string m_name(tokens[1].data,tokens[1].len);
    root->debugf("ACTIVE SERIES FROM %s/%s\n",db_name.c_str(),m_name.c_str());
    auto db = tsdb::database(*root,db_name);
    auto m = tsdb::measurement(db,m_name);
    uint64_t t0 = tokens[2].u64;
    uint64_t t1 = tokens[3].u64;
    auto as = m.list_active_series(t0,t1);
    for (const auto& s_name : as)
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
    root->debugf("COUNT FROM %s\n",path.c_str());

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

    root->debugf("WRITE TO %s\n",path.c_str());
    tsdb::database db(*root,tokens[0].to_string());
    tsdb::measurement m(db,tokens[1].to_string());

    uint64_t now = time_ns();
    const size_t write_throttle_ns = db.root.config.write_throttle_ns;
    if (now - conn.last_write_ns < write_throttle_ns)
    {
        sleep_for_ns(conn.last_write_ns + write_throttle_ns - now);
        now = time_ns();
    }
    conn.last_write_ns = now;

    auto write_lock = tsdb::open_or_create_and_lock_series(m,series);
    for (;;)
    {
        uint32_t tokens[2] = {DT_READY_FOR_CHUNK,10*1024*1024};
        conn.s.send_all(tokens,sizeof(tokens));

        uint32_t dt = conn.s.pop<uint32_t>();
        if (dt == DT_END)
        {
            root->debugf("WRITE END\n");
            return;
        }
        if (dt != DT_CHUNK)
            throw futil::errno_exception(EINVAL);

        chunk_header ch = conn.s.pop<chunk_header>();
        if (ch.data_len > 10*1024*1024)
            throw futil::errno_exception(ENOMEM);

        root->debugf("RECV %u BYTES\n",ch.data_len);
        auto_buf data(ch.data_len);
        conn.s.recv_all(data,ch.data_len);

        root->debugf("WRITE %u POINTS TO %s\n",ch.npoints,path.c_str());
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

    root->debugf("DELETE FROM %s WHERE time_ns <= %" PRIu64 "\n",path.c_str(),
                 t);
    tsdb::database db(*root,database);
    tsdb::measurement m(db,measurement);
    tsdb::series_total_lock total_lock(m,series);
    tsdb::delete_points(total_lock,t);
}

static void
_handle_select_points(connection& conn, tsdb::select_op& op,
    tsdb::wal_query& wq, size_t N)
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
            auto* bitmap = (const uint64_t*)op.bitmap_bufs[i].data;
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

    root->debugf("SELECT %s FROM %s WHERE %" PRIu64 " <= time_ns <= %" PRIu64
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

    root->debugf("SELECT %s FROM %s WHERE %" PRIu64 " <= time_ns <= %" PRIu64
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

    root->debugf("SUM %s FROM %s WHERE %" PRIu64 " <= time_ns <= %" PRIu64
                 " WINDOW_NS %" PRIu64 "\n",
                 field_list.c_str(),path.c_str(),t0,t1,window_ns);
    tsdb::database db(*root,database);
    tsdb::measurement m(db,measurement);
    tsdb::series_read_lock read_lock(m,series);
    tsdb::sum_op op(read_lock,path,str::split(field_list,","),t0,t1,window_ns);

    const size_t nfields = op.op.fields.size();
    fixed_vector<uint64_t> timestamps(1024);
    tsdb::field_vector<std::vector<double>> field_sums;
    tsdb::field_vector<std::vector<tsdb::wal_field>> field_mins;
    tsdb::field_vector<std::vector<tsdb::wal_field>> field_maxs;
    tsdb::field_vector<std::vector<uint64_t>> field_npoints;
    for (size_t i=0; i<nfields; ++i)
    {
        field_sums.emplace_back(std::vector<double>());
        field_sums[i].reserve(1024);
        field_mins.emplace_back(std::vector<tsdb::wal_field>());
        field_mins[i].reserve(1024);
        field_maxs.emplace_back(std::vector<tsdb::wal_field>());
        field_maxs[i].reserve(1024);
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
                field_mins[j].push_back(op.mins[j]);
                field_maxs[j].push_back(op.maxs[j]);
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
                conn.s.send_all(&field_mins[j][0],
                                chunk_npoints*sizeof(tsdb::wal_field));
                field_mins[j].clear();
            }
            for (size_t j=0; j<nfields; ++j)
            {
                conn.s.send_all(&field_maxs[j][0],
                                chunk_npoints*sizeof(tsdb::wal_field));
                field_maxs[j].clear();
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
handle_integrate_points(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    futil::path path(database,measurement,series);
    std::string field_list(tokens[3].data,tokens[3].len);
    uint64_t t0 = tokens[4].u64;
    uint64_t t1 = tokens[5].u64;

    root->debugf("INTEGRATE %s FROM %s WHERE %" PRIu64
                 " <= time_ns <= %" PRIu64 "\n",
                 field_list.c_str(),path.c_str(),t0,t1);
    tsdb::database db(*root,database);
    tsdb::measurement m(db,measurement);
    tsdb::series_read_lock read_lock(m,series);
    tsdb::integral_op op(read_lock,path,str::split(field_list,","),t0,t1);

    uint64_t bitmap = 0;
    for (size_t i=0; i<op.is_null.size(); ++i)
        bitmap |= ((uint64_t)op.is_null[i] << i);

    conn.s.push(DT_TIME_FIRST);
    conn.s.push(op.t0_ns);

    conn.s.push(DT_TIME_LAST);
    conn.s.push(op.t1_ns);

    conn.s.push(DT_INTEGRAL_BITMAP);
    conn.s.push(bitmap);
    
    conn.s.push(DT_INTEGRALS);
    conn.s.send_all(op.integral.elems,op.integral.size()*sizeof(double));
}

static void
handle_nop(connection& conn, const std::vector<parsed_data_token>& tokens)
{
    // Do nothing.
}

static void
stream_request_handler(connection& conn)
{
    printf("Handling local %s remote %s.\n",
           conn.s.local_addr_string().c_str(),
           conn.s.remote_addr_string().c_str());

    process_stream(conn.s,commands,conn);

    printf("Teardown local %s remote %s.\n",
           conn.s.local_addr_string().c_str(),
           conn.s.remote_addr_string().c_str());
}

static void
request_handler(std::unique_ptr<tcp::socket> sock)
{
    sock->enable_keepalive();
    sock->nodelay();

    std::unique_ptr<tcp::stream> stream(std::move(sock));
    connection conn(*stream,"");
    stream_request_handler(conn);
}

static void
auth_request_handler(tcp::ssl::server_context* sslctx,
    std::unique_ptr<tcp::socket> sock)
{
    uint64_t t0 = time_ns();

    std::string local_addr_string = sock->local_addr_string();
    std::string remote_addr_string = sock->remote_addr_string();
    printf("Authenticating local %s remote %s.\n",
           local_addr_string.c_str(),remote_addr_string.c_str());

    // Set a 3-second timeout on the socket and then attemp to wrap() it into
    // an SSL stream.  The timeout should handle the case where the remote
    // just doesn't send anything.
    sock->enable_keepalive();
    sock->nodelay();
    sock->set_send_timeout_us(3*1000000);
    sock->set_recv_timeout_us(3*1000000);

    std::unique_ptr<tcp::ssl::stream> s;

    try
    {
        try
        {
            s = sslctx->wrap(std::move(sock));
        }
        catch (const tcp::ssl::ssl_error_exception& e)
        {
            if (e.ssl_error == SSL_ERROR_WANT_READ)
                printf("Zombie detected: %s\n",remote_addr_string.c_str());
            else
            {
                printf("SSL wrap failure: %s %s\n",remote_addr_string.c_str(),
                       e.what());
            }

            throw;
        }
        catch (const std::exception& e)
        {
            printf("SSL wrap failure: %s %s\n",remote_addr_string.c_str(),
                   e.what());
            throw;
        }
        catch (...)
        {
            printf("SSL wrap failure: %s (?)\n",remote_addr_string.c_str());
            throw;
        }
    }
    catch (...)
    {
        uint64_t t1 = time_ns();
        uint64_t nsecs = round_up_to_nearest_multiple(t1 - t0,
                                                      (uint64_t)2000000000);
        sleep_for_ns(nsecs + 1);
        return;
    }

    connection conn(*s,"");

    try
    {
        uint32_t ct = conn.s.pop<uint32_t>();
        if (ct != CT_AUTHENTICATE)
            throw futil::errno_exception(EINVAL);

        std::vector<parsed_data_token> tokens;
        parse_cmd(conn.s,auth_command,tokens);

        conn.username = tokens[0].to_string();
        printf("Authenticating user %s from %s...\n",
               conn.username.c_str(),remote_addr_string.c_str());
        if (!root->verify_user(conn.username,tokens[1].to_string()))
            throw futil::errno_exception(EPERM);
        printf("Authentication from %s for user %s succeeded.\n",
               remote_addr_string.c_str(),conn.username.c_str());

        uint32_t status[2] = {DT_STATUS_CODE, 0};
        conn.s.send_all(&status,sizeof(status));
    }
    catch (const std::exception& e)
    {
        if (!conn.username.empty())
        {
            printf("Authentication from %s for user %s failed.\n",
                   remote_addr_string.c_str(),conn.username.c_str());
        }
        else
        {
            printf("Authentication from %s failed.\n",
                   remote_addr_string.c_str());
        }
        printf("Authentication exception: %s\n",e.what());
        uint64_t t1 = time_ns();
        uint64_t nsecs = round_up_to_nearest_multiple(t1 - t0,
                                                      (uint64_t)2000000000);
        sleep_for_ns(nsecs + 1);
        return;
    }

    s->s->set_send_timeout_us(0);
    s->s->set_recv_timeout_us(0);
    stream_request_handler(conn);
}

static void
socket4_workloop(uint16_t port)
{
    tcp::server_socket ss(net::ipv4::loopback_addr(port));
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
ssl_workloop(const char* cert_file, const char* key_file, uint16_t port)
{
    tcp::ssl::server_context sslctx(cert_file,key_file);
    tcp::server_socket ss(net::ipv4::any_addr(port));
    ss.listen(4);
    printf("SSL listening on %s.\n",ss.bind_addr.to_string().c_str());
    for (;;)
    {
        try
        {
            // Handle the request, letting the target thread wrap it.
            std::thread t(auth_request_handler,&sslctx,ss.accept());
            t.detach();
        }
        catch (const std::exception& e)
        {
            printf("Exception accepting SSL connection: %s\n",e.what());
        }
    }
}

static void
usage(const char* err)
{
    printf("Usage: tsdbserver [options]\n"
           "  [--ssl-files cert_file key_file]\n"
           "    cert_file - path to fullchain.pem file\n"
           "    key_file  - path to privkey.pem file\n"
           "  [--root root_dir]\n"
           "    root_dir  - path to root directory of database\n"
           "                (defaults to current working directory\n"
           "  [--port port]\n"
           "    port      - TCP listening port number (defaults to 4000)\n"
           "  [--no-debug]\n"
           "              - disable debug output\n"
           );
    if (err)
        printf("\n%s\n",err);
}

int
main(int argc, const char* argv[])
{
    const char* root_path = ".";
    const char* cert_file = NULL;
    const char* key_file = NULL;
    uint16_t port = 4000;
    bool no_debug = false;
    bool unbuffered = false;

    for (size_t i=1; i<(size_t)argc;)
    {
        auto* arg  = argv[i];
        size_t rem = (size_t)argc - i - 1;
        if (!strcmp(arg,"--ssl-files"))
        {
            if (rem < 2)
            {
                usage("Expected --ssl-files fullchain.pem privkey.pem");
                return -1;
            }
            cert_file = argv[i + 1];
            key_file  = argv[i + 2];
            i += 3;
        }
        else if (!strcmp(arg,"--root"))
        {
            if (!rem)
            {
                usage("Expected --root root_dir");
                return -1;
            }
            root_path = argv[i + 1];
            i += 2;
        }
        else if (!strcmp(arg,"--help"))
        {
            usage(NULL);
            return 0;
        }
        else if (!strcmp(arg,"--port"))
        {
            if (!rem)
            {
                usage("Expected --port port");
                return -1;
            }
            char* endptr;
            port = strtoul(argv[i + 1],&endptr,10);
            if (*endptr)
            {
                std::string err("Not a number: ");
                err += argv[i + 1];
                usage(err.c_str());
                return -1;
            }
            i += 2;
        }
        else if (!strcmp(arg,"--no-debug"))
        {
            no_debug = true;
            ++i;
        }
        else if (!strcmp(arg,"--unbuffered"))
        {
            unbuffered = true;
            ++i;
        }
        else
        {
            std::string err("Unrecognized argument: ");
            err += arg;
            usage(err.c_str());
            return -1;
        }
    }

    if (unbuffered)
        setlinebuf(stdout);


    printf("simple_tsdb " SIMPLE_TSDB_VERSION_STR " " GIT_VERSION "\n");

    signal(SIGPIPE,SIG_IGN);

    root = new tsdb::root(root_path,!no_debug);
    printf("    Chunk size: %zu bytes\n",root->config.chunk_size);
    printf("      WAL size: %zu points\n",root->config.wal_max_entries);
    printf("Write throttle: %zu ns\n",root->config.write_throttle_ns);

    if (cert_file && key_file)
        ssl_workloop(cert_file,key_file,port);
    else
        socket4_workloop(port);
}
