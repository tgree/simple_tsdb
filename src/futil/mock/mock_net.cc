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
