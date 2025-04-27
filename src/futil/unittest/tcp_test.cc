// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../tcp.h"
#include <tmock/tmock.h>

static const char buf[] = "Hello, world!";

class tmock_test
{
    TMOCK_TEST(test_client_server)
    {
        tcp::server_socket ss(net::ipv4::any_addr(0));
        auto ssa = net::getsockname(ss.fd);
        ss.listen(5);

        tcp::client_socket cs(ssa);
        auto s = ss.accept();

        cs.send_all(buf,sizeof(buf));

        char rcv[sizeof(buf)] = {};
        s->recv_all(rcv,sizeof(rcv));

        tmock::assert_equiv(rcv,buf);
    }
};

TMOCK_MAIN();
