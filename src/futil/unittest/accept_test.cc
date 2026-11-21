// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../tcp.h"
#include <tmock/tmock.h>
#include <time.h>

// The fd that the mocked _accept() hands out.  futil::close() is mocked, so
// it never needs to be a real fd.
static constexpr int accepted_fd = 1000;

// A tcp::server_socket on a real socket, bound to an ephemeral loopback port
// but not listening.  The socket mocks are linked, so this supplies the fd
// and expects the server socket options.  futil::close() is mocked too, so
// the destructor closes the real fd.
struct test_server_socket
{
    int                                 fd;
    std::unique_ptr<tcp::server_socket> ss;

    test_server_socket():
        fd(::socket(AF_INET,SOCK_STREAM,0))
    {
        TASSERT(fd >= 0);
        texpect("tcp::_socket",want(af,AF_INET),returns(fd));
        texpect("tcp::_setsockopt",want(fd,fd),want(name,SO_REUSEADDR),
                returns(0));
        texpect("tcp::_setsockopt",want(fd,fd),want(name,SO_REUSEPORT),
                returns(0));
        ss = std::make_unique<tcp::server_socket>(
                net::ipv4::loopback_addr(0));
        tcheckpoint();
    }

    ~test_server_socket()
    {
        texpect("futil::close",want(fd,fd));
        ss.reset();
        tcheckpoint();
        ::close(fd);
    }
};

static uint64_t
now_ns()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC,&ts);
    return ts.tv_sec*1000000000ULL + ts.tv_nsec;
}

// Runs accept_connection() with accept() failing with err.
static void
fail_accept(test_server_socket& tss, int err = EMFILE)
{
    texpect("tcp::_accept",want(fd,tss.fd),returns(-err));
    tcp::accept_connection(*tss.ss,"TCP",
        [&](std::unique_ptr<tcp::socket> s)
        {
            TABORT("handled a failed accept");
        }
    );
    tcheckpoint();
}

// Runs accept_connection() with accept() succeeding.
static void
succeed_accept(test_server_socket& tss)
{
    texpect("tcp::_accept",want(fd,tss.fd),returns(accepted_fd));
    texpect("futil::close",want(fd,accepted_fd));
    tcp::accept_connection(*tss.ss,"TCP",
        [&](std::unique_ptr<tcp::socket> s)
        {
        }
    );
    tcheckpoint();
}

class tmock_test
{
    TMOCK_TEST(test_accept_connection)
    {
        test_server_socket tss;
        int handled_fd = -1;
        texpect("tcp::_accept",want(fd,tss.fd),returns(accepted_fd));
        texpect("futil::close",want(fd,accepted_fd));
        tcp::accept_connection(*tss.ss,"TCP",
            [&](std::unique_ptr<tcp::socket> s)
            {
                handled_fd = s->fd;
            }
        );
        tcheckpoint();
        TASSERT(handled_fd == accepted_fd);
    }

    TMOCK_TEST(test_accept_connection_error)
    {
        // An accept() error is logged, not thrown, and nothing is handled.
        test_server_socket tss;
        texpect("tcp::_accept",want(fd,tss.fd),returns(-ECONNABORTED));
        tcp::accept_connection(*tss.ss,"TCP",
            [&](std::unique_ptr<tcp::socket> s)
            {
                TABORT("handled a failed accept");
            }
        );
        tcheckpoint();
    }

    TMOCK_TEST(test_accept_connection_handler_error)
    {
        // An error from the handler, such as failing to start a thread, is
        // logged too.
        test_server_socket tss;
        texpect("tcp::_accept",want(fd,tss.fd),returns(accepted_fd));
        texpect("futil::close",want(fd,accepted_fd));
        tcp::accept_connection(*tss.ss,"TCP",
            [&](std::unique_ptr<tcp::socket> s)
            {
                throw futil::errno_exception(EAGAIN);
            }
        );
        tcheckpoint();
    }

    TMOCK_TEST(test_accept_connection_backs_off_after_emfile)
    {
        // Linux leaves a connection queued when accept() fails with EMFILE,
        // so an accept loop that retries at once spins, logging hundreds of
        // thousands of errors a second, until an fd frees up.  The first
        // failure should wait at least 10 ms before the loop retries.
        test_server_socket tss;
        texpect("tcp::_accept",want(fd,tss.fd),returns(-EMFILE));
        uint64_t t0 = now_ns();
        tcp::accept_connection(*tss.ss,"TCP",
            [&](std::unique_ptr<tcp::socket> s)
            {
                TABORT("handled a failed accept");
            }
        );
        uint64_t t1 = now_ns();
        tcheckpoint();
        TASSERT(t1 - t0 >= 10000000);
    }

    TMOCK_TEST(test_accept_backoff_doubles_and_resets)
    {
        test_server_socket tss;
        fail_accept(tss);
        TASSERT(tcp::accept_backoff_ns == 10000000);
        fail_accept(tss);
        TASSERT(tcp::accept_backoff_ns == 20000000);
        fail_accept(tss);
        TASSERT(tcp::accept_backoff_ns == 40000000);
        succeed_accept(tss);
        TASSERT(tcp::accept_backoff_ns == 0);
        fail_accept(tss);
        TASSERT(tcp::accept_backoff_ns == 10000000);
    }

    TMOCK_TEST(test_accept_backoff_cap)
    {
        // Start near the cap rather than failing seven times.  The failure
        // still waits the full second.
        test_server_socket tss;
        tcp::accept_backoff_ns = 600000000;
        fail_accept(tss);
        TASSERT(tcp::accept_backoff_ns == 1000000000);
    }

    TMOCK_TEST(test_accept_backoff_resource_errors)
    {
        test_server_socket tss;
        for (int err : {EMFILE, ENFILE, ENOBUFS, ENOMEM})
        {
            tcp::accept_backoff_ns = 0;
            fail_accept(tss,err);
            TASSERT(tcp::accept_backoff_ns == 10000000);
        }
    }

    TMOCK_TEST(test_accept_backoff_ignores_connection_errors)
    {
        // Errors that belong to one connection are retried at once, so that
        // a remote peer can't slow down the accept loop.  They neither grow
        // nor reset the backoff.
        test_server_socket tss;
        for (int err : {ECONNABORTED, EPROTO, EHOSTUNREACH})
        {
            fail_accept(tss,err);
            TASSERT(tcp::accept_backoff_ns == 0);
        }
        fail_accept(tss);
        fail_accept(tss,ECONNABORTED);
        TASSERT(tcp::accept_backoff_ns == 10000000);
    }

    TMOCK_TEST(test_accept_backoff_handler_error)
    {
        // A handler that fails, such as to start a thread, backs off whatever
        // the error.
        test_server_socket tss;
        texpect("tcp::_accept",want(fd,tss.fd),returns(accepted_fd));
        texpect("futil::close",want(fd,accepted_fd));
        tcp::accept_connection(*tss.ss,"TCP",
            [&](std::unique_ptr<tcp::socket> s)
            {
                throw futil::errno_exception(ECONNABORTED);
            }
        );
        tcheckpoint();
        TASSERT(tcp::accept_backoff_ns == 10000000);
    }
};

TMOCK_MAIN();
