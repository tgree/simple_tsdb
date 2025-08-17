// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "tokens.h"
#include "client.h"
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
#include <map>
#include <set>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <inttypes.h>

struct reflector_config
{
    std::string                         remote_host;
    uint16_t                            remote_port;
    std::string                         remote_user;
    std::string                         remote_password;
    std::map<std::string,std::string>   db_map;

    reflector_config():remote_port(0) {}
};

struct db_mapping
{
    std::string     local_db;
    std::string     remote_db;
};

struct connection
{
    tcp::stream&    s;
    client&         reflector_client;
    uint64_t        last_write_ns;

    void log_idle() {}
    std::vector<parsed_data_token>& log_tokens(
        uint32_t ct, std::vector<parsed_data_token>& tokens)
    {
        return tokens;
    }
};

static void handle_get_schema(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_write_points(
    connection& conn, const std::vector<parsed_data_token>& tokens);

static const command_syntax<connection&> commands[] =
{
    {
        func_delegate(handle_get_schema),
        CT_GET_SCHEMA,
        {DT_DATABASE, DT_MEASUREMENT, DT_END},
    },
    {
        func_delegate(handle_write_points),
        CT_WRITE_POINTS,
        {DT_DATABASE, DT_MEASUREMENT, DT_SERIES},
    },
};

static tsdb::root* root;
static reflector_config reflector_cfg;

void
debugf(const char* fmt, ...)
{
    va_list ap;
    va_start(ap,fmt);
    root->vdebugf(fmt,ap);
    va_end(ap);
}

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
reflect_chunk_to_local_database(const chunk_header& ch, const auto_buf& data,
    tsdb::series_write_lock& write_lock, const futil::path& path)
{
    // TODO: Maybe we should maintain a list of series which we know have local
    //       data so that the flush thread doesn't have to scan everything all
    //       the time.
    root->debugf("LOCAL WRITE %u POINTS TO %s\n",ch.npoints,path.c_str());
    tsdb::write_wal(write_lock,ch.npoints,ch.bitmap_offset,ch.data_len,data);
    root->debugf("LOCAL WRITE COMPLETE\n");
}

static void
reflect_chunk_to_remote_database(const chunk_header& ch, const auto_buf& data,
    client& reflector_client, const std::string& remote_database,
    const std::string& measurement, const std::string& series)
{
    futil::path path(remote_database,measurement,series);

    root->debugf("REMOTE WRITE %u POINTS TO %s\n",ch.npoints,path.c_str());
    reflector_client.write_points(remote_database,measurement,series,ch.npoints,
                                  ch.bitmap_offset,ch.data_len,data);
    root->debugf("REMOTE WRITE COMPLETE\n");
}

static void
handle_write_points(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].to_string());
    auto iter = reflector_cfg.db_map.find(database);
    if (iter == reflector_cfg.db_map.end())
        throw tsdb::no_such_database_exception();
    std::string remote_database = iter->second;
    std::string measurement(tokens[1].to_string());
    std::string series(tokens[2].to_string());
    futil::path path(database,measurement,series);

    root->debugf("REFLECT TO %s\n",path.c_str());
    tsdb::database db(*root,database);
    tsdb::measurement m(db,measurement);

    uint64_t now = time_ns();
    const size_t write_throttle_ns = db.root.config.write_throttle_ns;
    if (now - conn.last_write_ns < write_throttle_ns)
    {
        sleep_for_ns(conn.last_write_ns + write_throttle_ns - now);
        now = time_ns();
    }

    // Check if there are any local points.
    auto write_lock = tsdb::open_or_create_and_lock_series(m,series);
    auto cr = tsdb::count_points(write_lock,0,-1);
    size_t nlocal_points = cr.npoints;

    // Read chunks and reflect them to the right destination.
    for (;;)
    {
        uint32_t tokens[2] = {DT_READY_FOR_CHUNK,10*1024*1024};
        conn.s.send_all(tokens,sizeof(tokens));

        uint32_t dt = conn.s.pop<uint32_t>();
        if (dt == DT_END)
        {
            root->debugf("REFLECT END\n");
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

        // If there are no local points, try writing to the reflector.
        if (!nlocal_points)
        {
            try
            {
                reflect_chunk_to_remote_database(ch,data,conn.reflector_client,
                                                 remote_database,measurement,
                                                 series);
                continue;
            }
            catch (const std::exception& e)
            {
                root->debugf("Remote write failed: %s\n",e.what());
            }
        }

        // We don't (or no longer) have a reflector stream, so write to the
        // local database.  The background thread will eventually come around
        // and write these all to the remote database when it is able to talk
        // to it again.
        reflect_chunk_to_local_database(ch,data,write_lock,path);
        nlocal_points += ch.npoints;
        conn.last_write_ns = now;
    }
}

static void
request_handler(std::unique_ptr<tcp::stream> s)
{
    printf("Handling local %s remote %s.\n",
           s->local_addr_string().c_str(),s->remote_addr_string().c_str());

    client c(reflector_cfg.remote_host,
             reflector_cfg.remote_port,
             reflector_cfg.remote_user,
             reflector_cfg.remote_password);
    connection conn{*s,c,0};
    s->enable_keepalive();
    s->nodelay();
    process_stream(conn,commands);

    printf("Teardown local %s remote %s.\n",
           s->local_addr_string().c_str(),s->remote_addr_string().c_str());
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
create_remote_measurements(client& c, std::set<std::string>& validated_dbs)
{
    for (auto const& [local_db_path, remote_db_path] : reflector_cfg.db_map)
    {
        if (validated_dbs.contains(local_db_path))
            continue;
        if (!root->database_exists(local_db_path))
            continue;

        try
        {
            tsdb::database local_db(*root,local_db_path);
            auto local_measurements = local_db.list_measurements();
            for (const auto& m_path : local_measurements)
            {
                printf("Checking %s/%s -> %s/%s.\n",
                       local_db_path.c_str(),m_path.c_str(),
                       remote_db_path.c_str(),m_path.c_str());
                tsdb::measurement local_m(local_db,m_path);
                std::vector<tsdb::schema_entry> fields(
                    local_m.fields.begin(),local_m.fields.end());
                c.create_measurement(remote_db_path,m_path,fields);
            }
        }
        catch (const std::exception& e)
        {
            printf("Error: %s\n",e.what());
            continue;
        }
        catch (...)
        {
            printf("Random exception!\n");
            continue;
        }

        validated_dbs.emplace(local_db_path);
    }
}

static void
flush_series(client& c, tsdb::measurement& local_m,
    const std::string& local_db_path, const std::string& m_path,
    const std::string& s_path, const std::string& remote_db_path,
    auto_buf& chunk_storage) try
{
    auto stl = tsdb::series_total_lock(local_m,s_path);
    size_t max_chunk_points = local_m.max_points_for_data_len(10*1024*1024);
    auto series_id = local_db_path + "/" + m_path + "/" + s_path;
    tsdb::select_op_first op(stl,series_id,std::vector<std::string>(),
                             0,(uint64_t)-1,(uint64_t)-1);
    while (op.npoints)
    {
        size_t op_rem_points = op.npoints;
        size_t index = 0;
        while (op_rem_points)
        {
            const size_t N = MIN(op_rem_points,max_chunk_points);
            const size_t data_len = local_m.compute_write_chunk_len(N);
            tsdb::write_chunk_index wci(local_m,N,0,data_len,chunk_storage);

            // Copy timestamps into place.
            memcpy(wci.timestamps,op.timestamps_begin + index,
                   sizeof(uint64_t)*N);

            // Do each field in turn.
            for (size_t i=0; i<wci.fields.size(); ++i)
            {
                // First copy the field data.
                const auto& fti = tsdb::ftinfos[local_m.fields[i].type];
                memcpy(wci.fields[i].data_ptr,
                      (const char*)op.field_data[i] + index*fti.nbytes,
                      N*fti.nbytes);

                // Do the bitmap.  This could be much faster, I'm sure.
                for (size_t j=0; j<N; ++j)
                    wci.set_bitmap_bit(i,j,op.get_bitmap_bit(i,j + index));
            }

            // Transmit the chunk.
            printf("Transmitting %zu chunk points from %" PRIu64
                   " to %" PRIu64 ".\n",N,wci.timestamps[0],
                   wci.timestamps[N-1]);
            c.write_points(remote_db_path,m_path,s_path,N,0,data_len,
                           chunk_storage);

            // Delete the local points.
            printf("Deleting local points up to %" PRIu64".\n",
                   wci.timestamps[N-1]);
            tsdb::delete_points(stl,wci.timestamps[N-1]);

            // Finish the rest of this main store chunk.
            op_rem_points -= N;
            index += N;
        }

        // Go to the next main store chunk.
        op.next();
    }

    tsdb::wal_query wq(stl,0,(uint64_t)-1);
    if (wq.nentries)
    {
        size_t wq_rem_points = wq.nentries;
        size_t index = 0;
        while (wq_rem_points)
        {
            const size_t N = MIN(wq_rem_points,max_chunk_points);
            const size_t data_len = local_m.compute_write_chunk_len(N);
            tsdb::write_chunk_index wci(local_m,N,0,data_len,chunk_storage);
            for (size_t i=0; i<N; ++i)
                wci.timestamps[i] = wq[i].time_ns;

            for (size_t i=0; i<wci.fields.size(); ++i)
            {
                for (size_t j=0; j<N; ++j)
                    wci.set_bitmap_bit(i,j,wq[index+j].get_bitmap_bit(i));

                const auto& fti = tsdb::ftinfos[local_m.fields[i].type];
                void* data = wci.fields[i].data_ptr;
                switch (fti.nbytes)
                {
                    case 1:
                        for (size_t j=0; j<N; ++j)
                            ((uint8_t*)data)[j] = wq[index+j].fields[i].u8;
                    break;

                    case 4:
                        for (size_t j=0; j<N; ++j)
                            ((uint32_t*)data)[j] = wq[index+j].fields[i].u32;
                    break;

                    case 8:
                        for (size_t j=0; j<N; ++j)
                            ((uint64_t*)data)[j] = wq[index+j].fields[i].u64;
                    break;
                }
            }

            // Transmit the chunk.
            printf("Transmitting %zu WAL points from %" PRIu64
                   " to %" PRIu64 ".\n",N,wci.timestamps[0],
                   wci.timestamps[N-1]);
            c.write_points(remote_db_path,m_path,s_path,N,0,data_len,
                           chunk_storage);

            // Delete the local points.
            printf("Deleting local points up to %" PRIu64".\n",
                   wci.timestamps[N-1]);
            tsdb::delete_points(stl,wci.timestamps[N-1]);

            // Finish the rest of this main store chunk.
            wq_rem_points -= N;
            index += N;
        }
    }
}
catch (const std::exception& e)
{
    printf("Flush error: %s\n",e.what());
}
catch (...)
{
    printf("Random flush exception!\n");
}

static void
flush_workloop()
{
    client c(reflector_cfg.remote_host,
             reflector_cfg.remote_port,
             reflector_cfg.remote_user,
             reflector_cfg.remote_password);
    std::set<std::string> validated_dbs;
    auto_buf chunk_storage(10*1024*1024);

    for (;;)
    {
        debugf("Starting flush scan...\n");

        // Create the remote measurements if they don't exist; validate for a
        // matching schema if they do exist.  Skip any databases that we have
        // already completely validated.
        create_remote_measurements(c,validated_dbs);

        // Iterate over all mapped databases, flushing as we go.
        for (auto const& [local_db_path, remote_db_path] : reflector_cfg.db_map)
        {
            if (!validated_dbs.contains(local_db_path))
                continue;

            tsdb::database local_db(*root,local_db_path);
            auto local_measurements = local_db.list_measurements();
            for (const auto& m_path : local_measurements)
            {
                tsdb::measurement local_m(local_db,m_path);
                auto local_series = local_m.list_active_series();
                for (const auto& s_path : local_series)
                {
                    auto series_id = local_db_path + "/" +
                                     m_path + "/" +
                                     s_path;
                    printf("Flushing %s/%s/%s -> %s/%s/%s.\n",
                           local_db_path.c_str(),m_path.c_str(),s_path.c_str(),
                           remote_db_path.c_str(),m_path.c_str(),
                           s_path.c_str());
                    flush_series(c,local_m,local_db_path,m_path,s_path,
                                 remote_db_path,chunk_storage);
                }
            }
        }

        // Give it a rest.
        debugf("Resting...\n");
        sleep_for_ns(10UL*1000000000UL);
    }
}

static void
parse_reflector_config(const char* path)
{
    futil::file reflector_fd(path,O_RDONLY);
    futil::file::line line;
    size_t line_num = 0;
    do
    {
        ++line_num;

        line = reflector_fd.read_line();
        if (line.text.empty())
            continue;
        if (line.text[0] == '#')
            continue;

        std::vector<std::string> parts = str::split(line.text);
        if (parts[0] == "remote_host")
        {
            if (parts.size() != 2)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: expected 'remote_host <host_or_ip>'",line_num));
            }
            reflector_cfg.remote_host = parts[1];
        }
        else if (parts[0] == "remote_port")
        {
            if (parts.size() != 2)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: expected 'remote_port <port_number>'",line_num));
            }
            reflector_cfg.remote_port = std::stoul(parts[1]);
        }
        else if (parts[0] == "remote_user")
        {
            if (parts.size() != 2)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: expected 'remote_user <username>'",line_num));
            }
            reflector_cfg.remote_user = parts[1];
        }
        else if (parts[0] == "remote_password")
        {
            if (parts.size() != 2)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: expected 'remote_password <password>'",
                    line_num));
            }
            reflector_cfg.remote_password = parts[1];
        }
        else if (parts[0] == "map")
        {
            if (parts.size() != 3)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: expected 'map <local_db> <remote_db>'",
                    line_num));
            }
            auto [iter, ok] =
                reflector_cfg.db_map.try_emplace(parts[1],parts[2]);
            if (!ok)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: local database '%s' already mapped'",
                    line_num,parts[1].c_str()));
            }
        }
        else
        {
            throw std::invalid_argument(str::printf(
                "Line %zu: unrecognized key '%s'",line_num,parts[0].c_str()));
        }
    } while (line);

    if (reflector_cfg.remote_host.empty())
        throw std::invalid_argument("Missing 'remote_host'");
    if (reflector_cfg.remote_port == 0)
        throw std::invalid_argument("Missing 'remote_port'");
    if (reflector_cfg.remote_user.empty())
        throw std::invalid_argument("Missing 'remote_user'");
    if (reflector_cfg.remote_password.empty())
        throw std::invalid_argument("Missing 'remote_password'");
    if (reflector_cfg.db_map.empty())
        throw std::invalid_argument("No databases mapped");
}

static void
usage(const char* err)
{
    printf("Usage: tsdbserver [options]\n"
           "  [--root root_dir]\n"
           "    root_dir  - path to root directory of database\n"
           "                (defaults to current working directory\n"
           "  [--port port]\n"
           "    port      - TCP listening port number (defaults to 4000)\n"
           "  [--no-debug]\n"
           "              - disable debug output\n"
           "  --reflector-cfg\n"
           "              - path to reflector configuration file\n"
           "  [--unbuffered]\n"
           "              - unbuffered STDOUT output\n"
           );
    if (err)
        printf("\n%s\n",err);
}

int
main(int argc, const char* argv[])
{
    const char* root_path = ".";
    const char* reflector_cfg_path = NULL;
    uint16_t port = 4000;
    bool no_debug = false;
    bool unbuffered = false;

    for (size_t i=1; i<(size_t)argc;)
    {
        auto* arg  = argv[i];
        size_t rem = (size_t)argc - i - 1;
        if (!strcmp(arg,"--root"))
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
        else if (!strcmp(arg,"--reflector-cfg"))
        {
            if (!rem)
            {
                usage("Expected --reflector-cfg reflector.cfg");
                return -1;
            }
            reflector_cfg_path = argv[i + 1];
            i += 2;
        }
        else
        {
            std::string err("Unrecognized argument: ");
            err += arg;
            usage(err.c_str());
            return -1;
        }
    }

    if (!reflector_cfg_path)
    {
        usage("Missing --reflector-cfg argument");
        return -1;
    }

    if (unbuffered)
        setlinebuf(stdout);

    printf("simple_tsdb " SIMPLE_TSDB_VERSION_STR " " GIT_VERSION "\n");

    signal(SIGPIPE,SIG_IGN);

    root = new tsdb::root(root_path,!no_debug);
    printf("    Chunk size: %zu bytes\n",root->config.chunk_size);
    printf("      WAL size: %zu points\n",root->config.wal_max_entries);
    printf("Write throttle: %zu ns\n",root->config.write_throttle_ns);

    try
    {
        parse_reflector_config(reflector_cfg_path);
    }
    catch (const std::exception& e)
    {
        printf("Failed to parse --reflector-cfg file: %s\n",e.what());
        return -1;
    }

    std::thread t(flush_workloop);
    t.detach();

    socket4_workloop(port);
}
