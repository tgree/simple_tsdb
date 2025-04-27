// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_SOCKADDR_H
#define __SRC_FUTIL_SOCKADDR_H

#include "futil.h"
#include <hdr/kassert.h>
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

        addr(const struct ::sockaddr& _sa, size_t sa_len):
            ss{}
        {
            kassert(sa_len <= sizeof(ss));
            memcpy(&sa,&_sa,sa_len);
        }
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

    inline std::vector<net::addr> get_addrs(const char* host, uint16_t port = 0)
    {
        char port_name[6];
        snprintf(port_name,sizeof(port_name),"%u",port);

        struct addrinfo hints{};
        struct addrinfo* res0;

        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_DEFAULT | AI_NUMERICSERV;
        int err = ::getaddrinfo(host,port_name,&hints,&res0);
        if (err)
            throw std::runtime_error(gai_strerror(err));

        try
        {
            std::vector<net::addr> addrs;
            for (auto* res = res0; res; res = res->ai_next)
                addrs.emplace_back(*res->ai_addr,res->ai_addrlen);
            freeaddrinfo(res0);
            return addrs;
        }
        catch (...)
        {
            freeaddrinfo(res0);
            throw;
        }
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
