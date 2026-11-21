// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../server.h"
#include <tmock/tmock.h>
#include <sys/socket.h>

// Returns the server end of a new socketpair as a tcp::socket, and the
// client end in *client_fd.  futil::close() is mocked, so destroying the
// tcp::socket doesn't close its fd; the test closes it with ::close().
static std::unique_ptr<tcp::socket>
make_info_socket(int* client_fd)
{
    int fds[2];
    TASSERT(::socketpair(AF_UNIX,SOCK_STREAM,0,fds) == 0);
    *client_fd = fds[1];
    auto addr = net::ipv4::loopback_addr(4001);
    return std::make_unique<tcp::socket>(fds[0],addr,addr);
}

// Reads from fd until EOF.
static std::string
read_all(int fd)
{
    std::string s;
    char buf[1024];
    for (;;)
    {
        ssize_t len = ::recv(fd,buf,sizeof(buf),0);
        TASSERT(len >= 0);
        if (!len)
            return s;
        s.append(buf,len);
    }
}

class tmock_test
{
    TMOCK_TEST(test_info_handler)
    {
        int client_fd;
        auto sock = make_info_socket(&client_fd);
        int server_fd = sock->fd;

        texpect("futil::close",want(fd,server_fd));
        info_handler(std::move(sock));
        tcheckpoint();
        ::close(server_fd);

        std::string output = read_all(client_fd);
        TASSERT(output.starts_with("TID "));
        TASSERT(output.ends_with("\n0 connections.\n"));
        ::close(client_fd);
    }
};

TMOCK_MAIN();
