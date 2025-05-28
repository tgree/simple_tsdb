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
#include <map>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <inttypes.h>

struct reflector_config
{
    std::string                         remote_host;
    uint16_t                            remote_port;
    net::addr                           remote_addr;
    std::string                         remote_user;
    std::string                         remote_password;
    uint16_t                            local_port;
    std::map<std::string,std::string>   db_map;

    reflector_config():remote_port(0),local_port(0) {}
};

struct connection
{
    tcp::stream&                    s;
    std::unique_ptr<tcp::stream>    reflector_stream;
    uint64_t                        last_write_ns;
};

static void handle_get_schema(
    connection& conn, const std::vector<parsed_data_token>& tokens);
static void handle_write_points(
    connection& conn, const std::vector<parsed_data_token>& tokens);

static const command_syntax<connection&> commands[] =
{
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
};

static const uint8_t pad_bytes[8] = {};

static tsdb::root* root;
static reflector_config reflector_cfg;

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
    root->debugf("LOCAL WRITE %u POINTS TO %s\n",ch.npoints,path.c_str());
    tsdb::write_wal(write_lock,ch.npoints,ch.bitmap_offset,ch.data_len,data);
}

static void
reflect_chunk_to_remote_database(const chunk_header& ch, const auto_buf& data,
    tcp::stream* reflector_stream, const std::string& database,
    const std::string& measurement, const std::string& series)
{
    const std::string* dest = &database;
    auto iter = reflector_cfg.db_map.find(database);
    if (iter != reflector_cfg.db_map.end())
        dest = &iter->second;
    futil::path path(*dest,measurement,series);

    root->debugf("REMOTE WRITE %u POINTS TO %s\n",ch.npoints,path.c_str());
    throw std::runtime_error("Remote reflect not implemented yet!");
}

static void
handle_write_points(connection& conn,
    const std::vector<parsed_data_token>& tokens)
{
    std::string database(tokens[0].data,tokens[0].len);
    std::string measurement(tokens[1].data,tokens[1].len);
    std::string series(tokens[2].data,tokens[2].len);
    futil::path path(database,measurement,series);

    root->debugf("REFLECT TO %s\n",path.c_str());
    tsdb::database db(*root,tokens[0].to_string());
    tsdb::measurement m(db,tokens[1].to_string());

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

    // If there are no local points, and we don't have a reflector connection,
    // try to open one now.
    if (!cr.npoints && !conn.reflector_stream)
    {
        try
        {
            root->debugf("Attempting to connect to remote database...\n");
            tcp::ssl::client_context sslctx;
            conn.reflector_stream = sslctx.wrap(
                std::make_unique<tcp::client_socket>(reflector_cfg.remote_addr),
                reflector_cfg.remote_host.c_str());
        }
        catch (const std::exception& e)
        {
            root->debugf("Connect failed: %s\n",e.what());
        }
    }

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

        // If we have a reflector stream, then there are no locally-stored
        // points and we can write to the remote database.
        if (conn.reflector_stream)
        {
            try
            {
                reflect_chunk_to_remote_database(ch,data,
                                                 conn.reflector_stream.get(),
                                                 database,measurement,series);
                continue;
            }
            catch (const std::exception& e)
            {
                // Clear the reflector_stream since we have somehow lost the
                // connection.
                conn.reflector_stream.reset();
                root->debugf("Remote write failed: %s\n",e.what());
            }
        }

        // We don't (or no longer) have a reflector stream, so write to the
        // local database.  The background thread will eventually come around
        // and write these all to the remote database when it is able to talk
        // to it again.
        reflect_chunk_to_local_database(ch,data,write_lock,path);
        conn.last_write_ns = now;
    }
}

static void
request_handler(std::unique_ptr<tcp::stream> s)
{
    printf("Handling local %s remote %s.\n",
           s->local_addr_string().c_str(),s->remote_addr_string().c_str());

    connection conn{*s,0};
    process_stream(conn);

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
        else if (parts[0] == "local_port")
        {
            if (parts.size() != 2)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: expected 'local_port <port_number>'",line_num));
            }
            reflector_cfg.local_port = std::stoul(parts[1]);
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
    if (reflector_cfg.local_port == 0)
        throw std::invalid_argument("Missing 'local_port'");
    if (reflector_cfg.db_map.empty())
        throw std::invalid_argument("No databases mapped");

    auto addrs = net::get_addrs(reflector_cfg.remote_host.c_str(),
                                reflector_cfg.remote_port);
    if (addrs.empty())
        throw std::invalid_argument("Cannot resolve remote host.");
    for (const auto& addr : addrs)
        printf("Found reflector target: %s\n",addr.to_string().c_str());
    reflector_cfg.remote_addr = addrs[0];
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

    printf("%s\n",GIT_VERSION);

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

    std::thread t(reflector_workloop);
    t.detach();

    socket4_workloop(port);
}
