// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "mock_net.h"
#include "../tcp.h"
#include <tmock/tmock.h>

static_assert(sizeof(uintptr_t) >= sizeof(uint64_t),
              "mock_net needs 64-bit tmock return values");

int
tcp::_socket(int af)
{
    return (int)mock("tcp::_socket",af);
}

void
tcp::_setsockopt(int fd, int level, int name, const void* val,
    socklen_t len)
{
    // The mock returns an errno value, or 0 for success.
    int err = (int)mock("tcp::_setsockopt",fd,level,name);
    if (err)
        throw futil::errno_exception(err);
}

int
tcp::_accept(int fd, net::addr& remote_addr)
{
    // The mock returns the accepted fd, or a negative errno value to fail.
    // remote_addr is left as it is.
    int afd = (int)mock("tcp::_accept",fd);
    if (afd < 0)
        throw futil::errno_exception(-afd);
    return afd;
}

net::addr
net::getsockname(int fd)
{
    // See mock_net.h for the encoding.
    uint64_t encoded_addr = mock("net::getsockname",fd);
    uint16_t port = (uint16_t)(encoded_addr >> 32);
    uint32_t ip = (uint32_t)encoded_addr;
    if (encoded_addr & 0x0FFF000000000000ULL)
        TABORT("net::getsockname() mock: invalid encoded address");
    switch (encoded_addr >> 60)
    {
        case 0x0:
            return net::ipv4::addr(ip,port);

        case 0x1:
            if (ip)
                TABORT("net::getsockname() mock: invalid loopback address");
            return net::ipv4::loopback_addr(port);
    }
    TABORT("net::getsockname() mock: unsupported address type");
}

std::string
mock_net::tcp::stream::local_addr_string()
{
    return "127.0.0.1:4000";
}

std::string
mock_net::tcp::stream::remote_addr_string()
{
    return "127.0.0.1:50000";
}

size_t
mock_net::tcp::stream::send(const void* buffer, size_t len)
{
    return mock("mock_net::tcp::stream::send",len);
}

size_t
mock_net::tcp::stream::recv(void* buffer, size_t len)
{
    return mock("mock_net::tcp::stream::recv",len);
}

void
mock_net::tcp::stream::enable_keepalive(int keepidle_secs,
    int keepintvl_secs, int keepcnt, int connection_timeout_secs)
{
    int err = (int)mock("mock_net::tcp::stream::enable_keepalive");
    if (err)
        throw futil::errno_exception(err);
}

void
mock_net::tcp::stream::cork()
{
    mock("mock_net::tcp::stream::cork");
}

void
mock_net::tcp::stream::uncork()
{
    mock("mock_net::tcp::stream::uncork");
}

void
mock_net::tcp::stream::nodelay()
{
    int err = (int)mock("mock_net::tcp::stream::nodelay");
    if (err)
        throw futil::errno_exception(err);
}
