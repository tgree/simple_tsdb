// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_MOCK_MOCK_NET_H
#define __SRC_FUTIL_MOCK_MOCK_NET_H

#include <stdint.h>

// The net::getsockname() mock has to return a net::addr, which doesn't fit
// in a tmock return value.  Instead, the test returns an address encoded as
// follows, and the mock decodes it:
//
//     bits 60-63: 0x0 for an IPv4 address, with the port in bits 32-47 and
//                 the address in bits 0-31.
//                 0x1 for an IPv4 loopback address, with the port in bits
//                 32-47 and bits 0-31 zero.
//
// All other bits are zero.  IPv6 addresses aren't supported yet.
namespace mock_net
{
    constexpr uint64_t ipv4_addr(uint32_t ip, uint16_t port)
    {
        return ((uint64_t)port << 32) | ip;
    }

    constexpr uint64_t ipv4_loopback_addr(uint16_t port)
    {
        return (0x1ULL << 60) | ((uint64_t)port << 32);
    }
}

#endif /* __SRC_FUTIL_MOCK_MOCK_NET_H */
