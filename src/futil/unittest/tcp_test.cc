// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../tcp.h"
#include <assert.h>
#include <stdlib.h>

static const char buf[] = "Hello, world!";

int
main(int argc, const char* argv[])
{
    tcp::server_socket ss(net::ipv4::any_addr(0));
    auto ssa = net::getsockname(ss.fd);
    ss.listen(5);

    tcp::client_socket cs(ssa);
    auto s = ss.accept();

    cs.set_user_timeout_secs(30);
    s->set_user_timeout_secs(30);

    cs.send_all(buf,sizeof(buf));

    char rcv[sizeof(buf)] = {};
    s->recv_all(rcv,sizeof(rcv));

    assert(!memcmp(rcv,buf,sizeof(buf)));

    // Connect again, this time with a connect timeout.
    tcp::client_socket cs2(ssa,5000);
    auto s2 = ss.accept();
    cs2.send_all(buf,sizeof(buf));
    memset(rcv,0,sizeof(rcv));
    s2->recv_all(rcv,sizeof(rcv));
    assert(!memcmp(rcv,buf,sizeof(buf)));

    // A connection to a port with no listener should be refused rather than
    // time out.
    net::addr closed_addr;
    {
        tcp::server_socket css(net::ipv4::any_addr(0));
        closed_addr = net::getsockname(css.fd);
    }
    try
    {
        tcp::client_socket cs3(closed_addr,5000);
        abort();
    }
    catch (const futil::errno_exception& e)
    {
        assert(e.errnov == ECONNREFUSED);
    }
};
