// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_TCP_H
#define __SRC_FUTIL_TCP_H

#include "futil.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>

namespace tcp
{
    struct addr4
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

        constexpr addr4():sa{} {}

        constexpr addr4(uint16_t port, uint32_t addr):
            sa{}
        {
            sa.sin_family = AF_INET;
            sa.sin_port = htons(port);
            sa.sin_addr.s_addr = htonl(addr);
        }

        constexpr addr4(const struct sockaddr_in& sa):
            sa(sa)
        {
        }
    };

    int _socket4()
    {
        int fd = ::socket(AF_INET,SOCK_STREAM,0);
        if (fd == -1)
            throw futil::errno_exception(errno);
        return fd;
    }

    struct socket4 : public futil::file_descriptor
    {
        const tcp::addr4 local_addr;
        const tcp::addr4 remote_addr;

        ssize_t send(const void* buffer, size_t len, int flags = 0)
        {
            for (;;)
            {
                ssize_t slen = ::send(fd,buffer,len,flags);
                if (slen != -1)
                    return slen;
                if (errno != EINTR)
                    throw futil::errno_exception(errno);
            }
        }

        void send_all(const void* buffer, size_t len)
        {
            const char* p = (const char*)buffer;
            while (len)
            {
                ssize_t slen = send(p,len);
                kassert(slen != 0);
                p   += slen;
                len -= slen;
            }
        }

        ssize_t recv(void* buffer, size_t len, int flags = 0)
        {
            for (;;)
            {
                ssize_t rlen = ::recv(fd,buffer,len,flags);
                if (rlen != -1)
                    return rlen;
                if (errno != EINTR)
                    throw futil::errno_exception(errno);
            }
        }

        void recv_all(void* buffer, size_t len)
        {
            char* p = (char*)buffer;
            while (len)
            {
                ssize_t rlen = recv(p,len);
                if (!rlen)
                    throw futil::errno_exception(ECONNRESET);
                p   += rlen;
                len -= rlen;
            }
        }

        template<typename T>
        T pop()
        {
            T v;
            recv_all(&v,sizeof(v));
            return v;
        }

        constexpr socket4(int fd, const tcp::addr4& local_addr,
                          const tcp::addr4& remote_addr):
            futil::file_descriptor(fd),
            local_addr(local_addr),
            remote_addr(remote_addr)
        {
        }

        socket4(socket4&& other):
            futil::file_descriptor(std::move(other)),
            local_addr(other.local_addr),
            remote_addr(other.remote_addr)
        {
        }

        ~socket4()
        {
            if (fd == -1)
                return;

            printf("Shutdown fd %d local %s remote %s.\n",
                   fd,local_addr.to_string().c_str(),
                   remote_addr.to_string().c_str());
            ::shutdown(fd,SHUT_RDWR);
        }
    };

    struct server_socket4 : public futil::file_descriptor
    {
        const tcp::addr4 bind_addr;

        void bind(const tcp::addr4& a4)
        {
            if (::bind(fd,(struct sockaddr*)&a4.sa,sizeof(a4.sa)) == -1)
                throw futil::errno_exception(errno);
        }

        void listen(int backlog)
        {
            if (::listen(fd,backlog) == -1)
                throw futil::errno_exception(errno);
        }

        socket4 accept()
        {
            tcp::addr4 remote_addr;
            socklen_t sl = sizeof(remote_addr.sa);
            int afd = ::accept(fd,(struct sockaddr*)&remote_addr.sa,&sl);
            if (afd == -1)
                throw futil::errno_exception(errno);

            return socket4(afd,bind_addr,remote_addr);
        }

        server_socket4(const tcp::addr4& bind_addr):
            futil::file_descriptor(_socket4()),
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

#endif /* __SRC_FUTIL_TCP_H */
