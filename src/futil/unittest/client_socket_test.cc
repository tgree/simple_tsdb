// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../tcp.h"
#include "../mock/mock_net.h"
#include <tmock/tmock.h>

// A file descriptor that isn't open in the test process.  ~socket() calls
// ::shutdown() on it directly, which just fails with EBADF.
#define FAKE_FD 1000

bool
futil::connect(int fd, const struct sockaddr* addr, socklen_t addr_len)
{
    // The mock returns an errno value, or 0 for success.
    int err = (int)mock("futil::connect",fd,addr,addr_len);
    if (err)
        throw futil::errno_exception(err);
    return true;
}

class tmock_test
{
    TMOCK_TEST(test_connect_success)
    {
        auto remote_addr = net::ipv4::loopback_addr(4000);
        {
            texpect("tcp::_socket",want(af,AF_INET),returns(FAKE_FD));
            texpect("futil::connect",want(fd,FAKE_FD));
            texpect("net::getsockname",want(fd,FAKE_FD),
                    returns(mock_net::ipv4_loopback_addr(5000)));
            tcp::client_socket cs(remote_addr);
            TASSERT_EQUIV(cs.fd,FAKE_FD);
            TASSERT_EQUIV(cs.local_addr_string(),"127.0.0.1:5000");
            tcheckpoint();

            // The socket is closed when it goes out of scope.
            texpect("futil::close",want(fd,FAKE_FD));
        }
    }

    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(test_connect_failure_closes_socket)
    {
        auto remote_addr = net::ipv4::loopback_addr(4000);
        texpect("tcp::_socket",want(af,AF_INET),returns(FAKE_FD));
        texpect("futil::connect",want(fd,FAKE_FD),returns(ECONNREFUSED));
        texpect("futil::close",want(fd,FAKE_FD));
        try
        {
            tcp::client_socket cs(remote_addr);
            TABORT("expected an ECONNREFUSED exception");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,ECONNREFUSED);
        }
    }
};

TMOCK_MAIN();
