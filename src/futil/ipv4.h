// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_IPV4_H
#define __SRC_FUTIL_IPV4_H

#include "tcp.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <memory>

namespace tcp::ipv4
{
    struct addr
    {
        struct sockaddr_in sa;

        std::string to_string() const
        {
            uint32_t a = ntohl(sa.sin_addr.s_addr);
            uint16_t p = ntohs(sa.sin_port);
            return std::to_string((a >> 24) & 0xFF) + "." +
                   std::to_string((a >> 16) & 0xFF) + "." +
                   std::to_string((a >>  8) & 0xFF) + "." +
                   std::to_string((a >>  0) & 0xFF) + ":" +
                   std::to_string(p);
        }

        constexpr addr():sa{} {}

        constexpr addr(uint16_t port, uint32_t addr):
            sa{}
        {
            sa.sin_family = AF_INET;
            sa.sin_port = htons(port);
            sa.sin_addr.s_addr = htonl(addr);
        }

        constexpr addr(const struct sockaddr_in& sa):
            sa(sa)
        {
        }
    };

    inline int _socket()
    {
        int fd = ::socket(AF_INET,SOCK_STREAM,0);
        if (fd == -1)
            throw futil::errno_exception(errno);
        return fd;
    }

    struct socket : public futil::file_descriptor,
                    public stream
    {
        const tcp::ipv4::addr local_addr;
        const tcp::ipv4::addr remote_addr;

        virtual std::string local_addr_string() override
        {
            return local_addr.to_string();
        }

        virtual std::string remote_addr_string() override
        {
            return remote_addr.to_string();
        }

        virtual size_t send(const void* buffer, size_t len) override
        {
            for (;;)
            {
                ssize_t slen = ::send(fd,buffer,len,0);
                if (slen != -1)
                    return slen;
                if (errno != EINTR)
                    throw futil::errno_exception(errno);
            }
        }

        size_t recv(void* buffer, size_t len) override
        {
            for (;;)
            {
                ssize_t rlen = ::recv(fd,buffer,len,0);
                if (rlen != -1)
                    return rlen;
                if (errno != EINTR)
                    throw futil::errno_exception(errno);
            }
        }

        constexpr socket(int fd, const tcp::ipv4::addr& local_addr,
                         const tcp::ipv4::addr& remote_addr):
            futil::file_descriptor(fd),
            local_addr(local_addr),
            remote_addr(remote_addr)
        {
        }

        socket(socket&& other):
            futil::file_descriptor(std::move(other)),
            local_addr(other.local_addr),
            remote_addr(other.remote_addr)
        {
        }

        ~socket()
        {
            if (fd == -1)
                return;

            printf("Shutdown fd %d local %s remote %s.\n",
                   fd,local_addr.to_string().c_str(),
                   remote_addr.to_string().c_str());
            ::shutdown(fd,SHUT_RDWR);
        }
    };

    struct server_socket : public futil::file_descriptor
    {
        const tcp::ipv4::addr bind_addr;

        void bind(const tcp::ipv4::addr& a)
        {
            if (::bind(fd,(struct sockaddr*)&a.sa,sizeof(a.sa)) == -1)
                throw futil::errno_exception(errno);
        }

        void listen(int backlog)
        {
            if (::listen(fd,backlog) == -1)
                throw futil::errno_exception(errno);
        }

        std::unique_ptr<tcp::ipv4::socket> accept()
        {
            tcp::ipv4::addr remote_addr;
            socklen_t sl = sizeof(remote_addr.sa);
            int afd = ::accept(fd,(struct sockaddr*)&remote_addr.sa,&sl);
            if (afd == -1)
                throw futil::errno_exception(errno);

            return std::make_unique<socket>(afd,bind_addr,remote_addr);
        }

        server_socket(const tcp::ipv4::addr& bind_addr):
            futil::file_descriptor(_socket()),
            bind_addr(bind_addr)
        {
            int reuse = 1;
            if (setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&reuse,sizeof(reuse)))
                throw futil::errno_exception(errno);

            reuse = 1;
            if (setsockopt(fd,SOL_SOCKET,SO_REUSEPORT,&reuse,sizeof(reuse)))
                throw futil::errno_exception(errno);

            bind(bind_addr);
        }
    };
}

#endif /* __SRC_FUTIL_IPV4_H */
