// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../sockaddr.h"
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_any_addr4)
    {
        auto a = net::ipv4::any_addr(12345);
        TASSERT_EQUIV(a.sa.sa_family,(sa_family_t)AF_INET);
        TASSERT_EQUIV(a.sin.sin_port,htons(12345));
        TASSERT_EQUIV(a.sin.sin_addr.s_addr,0U);
        TASSERT_EQUIV(a.to_string(),"*:12345");
    }

    TMOCK_TEST(test_any_addr6)
    {
        auto a = net::ipv6::any_addr(12345);
        TASSERT_EQUIV(a.sa.sa_family,(sa_family_t)AF_INET6);
        TASSERT_EQUIV(a.sin6.sin6_port,htons(12345));
        TASSERT_EQUIV(a.sin6.sin6_flowinfo,0U);
        TASSERT_EQUIV(a.to_string(),":::12345");
    }

    TMOCK_TEST(test_loopback_addr4)
    {
        auto a = net::ipv4::loopback_addr(12345);
        TASSERT_EQUIV(a.sa.sa_family,(sa_family_t)AF_INET);
        TASSERT_EQUIV(a.sin.sin_port,htons(12345));
        TASSERT_EQUIV(a.sin.sin_addr.s_addr,0x0100007FU);
        TASSERT_EQUIV(a.to_string(),"127.0.0.1:12345");
    }

    TMOCK_TEST(test_sin)
    {
        auto a = net::ipv4::addr(0x01020304,54321);
        TASSERT_EQUIV(a.sa.sa_family,(sa_family_t)AF_INET);
        TASSERT_EQUIV(a.sin.sin_port,htons(54321));
        TASSERT_EQUIV(a.sin.sin_addr.s_addr,0x04030201U);
        TASSERT_EQUIV(a.to_string(),"1.2.3.4:54321");
    }
};

TMOCK_MAIN();
