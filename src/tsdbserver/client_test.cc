// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "client.h"
#include <strutil/strutil.h>

struct remote_config
{
    std::string     remote_host;
    uint16_t        remote_port;
    std::string     remote_user;
    std::string     remote_password;
};

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
        else
        {
            throw std::invalid_argument(str::printf(
                "Line %zu: unrecognized key '%s'",line_num,parts[0].c_str()));
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

int
main(int argc, const char* argv[])
{
    if (argc != 2)
    {
        printf("Usage: client_test <remote_cfg_path>\n");
        return -1;
    }

    auto cfg = parse_remote_cfg(argv[1]);
    auto c = client(cfg.remote_host,cfg.remote_port,cfg.remote_user,
                    cfg.remote_password);
    auto fields = c.get_schema("xdh-n-1000017-data","xtalx_data");
    for (const auto& f : fields)
        printf("%4s %s\n",tsdb::ftinfos[f.type].name,f.name);
}
