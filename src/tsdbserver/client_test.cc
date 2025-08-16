// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "client.h"
#include <strutil/strutil.h>
#include <hdr/auto_buf.h>
#include <hdr/with_lock.h>
#include <condition_variable>
#include <thread>
#include <time.h>
#include <math.h>

struct sine_point
{
    uint64_t        time_ns;
    union
    {
        double          v;
        uint64_t        v_u64;
    };
};

struct remote_config
{
    std::string     remote_host;
    uint16_t        remote_port;
    std::string     remote_user;
    std::string     remote_password;
};

std::mutex sine_points_lock;
std::condition_variable sine_points_cond;
std::vector<sine_point> sine_points;

static remote_config
parse_remote_cfg(const char* path)
{
    remote_config cfg;
    futil::file fd(path,O_RDONLY);
    futil::file::line line;
    size_t line_num = 0;
    do
    {
        ++line_num;

        line = fd.read_line();
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
            cfg.remote_host = parts[1];
        }
        else if (parts[0] == "remote_port")
        {
            if (parts.size() != 2)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: expected 'remote_port <port_number>'",line_num));
            }
            cfg.remote_port = std::stoul(parts[1]);
        }
        else if (parts[0] == "remote_user")
        {
            if (parts.size() != 2)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: expected 'remote_user <username>'",line_num));
            }
            cfg.remote_user = parts[1];
        }
        else if (parts[0] == "remote_password")
        {
            if (parts.size() != 2)
            {
                throw std::invalid_argument(str::printf(
                    "Line %zu: expected 'remote_password <password>'",
                    line_num));
            }
            cfg.remote_password = parts[1];
        }
    } while (line);

    if (cfg.remote_host.empty())
        throw std::invalid_argument("Missing 'remote_host'");
    if (cfg.remote_port == 0)
        throw std::invalid_argument("Missing 'remote_port'");
    if (cfg.remote_user.empty())
        throw std::invalid_argument("Missing 'remote_user'");
    if (cfg.remote_password.empty())
        throw std::invalid_argument("Missing 'remote_password'");

    return cfg;
}

static void
print_schema(client& c, const char* database, const char* measurement)
{
    printf("Schema for %s/%s:\n",database,measurement);
    auto fields = c.get_schema(database,measurement);
    for (const auto& f : fields)
        printf("%4s %s\n",tsdb::ftinfos[f.type].name,f.name);
}

static void
push_thread(const remote_config& cfg)
{
    auto c = client(cfg.remote_host,cfg.remote_port,cfg.remote_user,
                    cfg.remote_password);
    print_schema(c,"test_db","sine_points");

    std::vector<sine_point> local_points;
    for (;;)
    {
        {
            std::unique_lock<std::mutex> ul(sine_points_lock);
            while (sine_points.empty())
                sine_points_cond.wait(ul);
            local_points.swap(sine_points);
        }

        size_t npoints = local_points.size();
        size_t bitmap_entries = ceil_div<size_t>(npoints,64);
        size_t data_len = npoints * 16 + bitmap_entries * 8;
        auto_buf data(data_len);
        uint64_t* p = (uint64_t*)data.data;
        for (const auto& sp : local_points)
            *p++ = sp.time_ns;
        for (size_t i=0; i<bitmap_entries; ++i)
            *p++ = 0xFFFFFFFFFFFFFFFFULL;
        for (const auto& sp : local_points)
            *p++ = sp.v_u64;
        local_points.clear();

        try
        {
            c.write_points("test_db","sine_points","test_series",npoints,0,
                           data_len,data);
        }
        catch (const std::exception& e)
        {
            printf("Write exception: %s\n",e.what());
            throw;
        }
    }
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

int
main(int argc, const char* argv[])
{
    if (argc != 2)
    {
        printf("Usage: client_test <remote_cfg_path>\n");
        return -1;
    }

    auto cfg = parse_remote_cfg(argv[1]);
    std::thread t(push_thread,cfg);

    uint64_t last_time_ns = 0;
    for (;;)
    {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME,&ts);
        uint64_t time_ns = ts.tv_sec*1000000000ULL + ts.tv_nsec;
        time_ns = MAX(last_time_ns+1,time_ns);

        uint64_t time_ms = (time_ns / 1000000) % 10000;
        double v = sin(((double)time_ms / 10000.) * 2. * M_PI);

        with_lock_guard (sine_points_lock)
        {
            sine_points.push_back({time_ns,{v}});
            sine_points_cond.notify_one();
        }

        sleep_for_ns(100000000);

        last_time_ns = time_ns;
    }
}
