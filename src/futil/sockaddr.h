// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_SOCKADDR_H
#define __SRC_FUTIL_SOCKADDR_H

#include "futil.h"
#include <stdexcept>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

namespace net
{
    struct addr
    {
        union
        {
            struct ::sockaddr sa;
            struct ::sockaddr_in sin;
            struct ::sockaddr_in6 sin6;
            struct ::sockaddr_storage ss;
        };

        std::string _to_string_inet() const
        {
            char hbuf[NI_MAXHOST];
            char sbuf[NI_MAXSERV];
            int err = getnameinfo(&sa,sizeof(ss),hbuf,sizeof(hbuf),sbuf,
                                  sizeof(sbuf),NI_NUMERICHOST | NI_NUMERICSERV);
            if (!err)
                return std::string(hbuf) + ":" + sbuf;

            throw std::runtime_error(gai_strerror(err));
        }

        std::string to_string() const
        {
            switch (ss.ss_family)
            {
                case AF_INET:
                    if (sin.sin_addr.s_addr == htonl(INADDR_ANY))
                    {
                        return std::string("*:") +
                            std::to_string(ntohs(sin.sin_port));
                    }
                case AF_INET6:
                    return _to_string_inet();
                break;

                default:
                    throw std::runtime_error("Unhandled SA_FAMILY");
            }
        }

        constexpr addr(const struct ::sockaddr& sa):sa(sa) {}
        constexpr addr(const struct ::sockaddr_in& sin):sin(sin) {}
        constexpr addr(const struct ::sockaddr_in6& sin6):sin6(sin6) {}
        constexpr addr(const struct ::sockaddr_storage& ss):ss(ss) {}
        constexpr addr(const addr& other):ss(other.ss) {}
        constexpr addr():ss{AF_UNSPEC} {}
    };

    inline addr getsockname(int fd)
    {
        struct ::sockaddr_storage ss{};
        socklen_t addr_len = sizeof(ss);
        if (::getsockname(fd,(struct sockaddr*)&ss,&addr_len) == -1)
            throw futil::errno_exception(errno);
        return addr(ss);
    }
}

namespace net::ipv4
{
    inline struct ::sockaddr_in make_sin(uint32_t ip, uint16_t port)
    {
        struct ::sockaddr_in sin{};
        sin.sin_family = AF_INET;
        sin.sin_port = htons(port);
        sin.sin_addr.s_addr = htonl(ip);
        return sin;
    }

    inline net::addr addr(uint32_t ip, uint16_t port)
    {
        return net::addr(make_sin(ip,port));
    }

    inline net::addr loopback_addr(uint16_t port)
    {
        return addr(INADDR_LOOPBACK,port);
    }

    inline net::addr any_addr(uint16_t port)
    {
        return addr(INADDR_ANY,port);
    }
}

namespace net::ipv6
{
    inline net::addr any_addr(uint16_t port)
    {
        struct ::sockaddr_in6 sin6{};
        sin6.sin6_family = AF_INET6;
        sin6.sin6_port = htons(port);
        sin6.sin6_flowinfo = 0;
        sin6.sin6_addr = in6addr_any;
        return net::addr(sin6);
    }
}

#endif /* __SRC_FUTIL_SOCKADDR_H */
