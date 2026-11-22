// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_MOCK_MOCK_NET_H
#define __SRC_FUTIL_MOCK_MOCK_NET_H

#include "../tcp.h"

#include <string>
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

    // Mirrors the global tcp namespace.
    namespace tcp
    {
        // A mock stream for code that takes a ::tcp::stream.  The address
        // strings are fixed.  The socket option mocks return an errno value
        // to throw as a futil::errno_exception, or 0 for success.  The
        // other methods are plain mocks.
        struct stream : public ::tcp::stream
        {
            virtual std::string local_addr_string() override;
            virtual std::string remote_addr_string() override;
            virtual size_t send(const void* buffer, size_t len) override;
            virtual size_t recv(void* buffer, size_t len) override;
            virtual void enable_keepalive(int keepidle_secs,
                                          int keepintvl_secs, int keepcnt,
                                          int connection_timeout_secs) override;
            virtual void cork() override;
            virtual void uncork() override;
            virtual void nodelay() override;
        };
    }
}

#endif /* __SRC_FUTIL_MOCK_MOCK_NET_H */
