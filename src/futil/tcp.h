// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_TCP_H
#define __SRC_FUTIL_TCP_H

#include "futil.h"
#include "sockaddr.h"
#include <hdr/kassert.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <memory>

namespace tcp
{
    struct stream
    {
        // Returns the local or remote address.
        virtual std::string local_addr_string() = 0;
        virtual std::string remote_addr_string() = 0;

        // Sends up to len bytes.  May send less if some sort of signal or
        // other error occurs, or if the connection was closed before the
        // requested number of bytes was sent.  Returns the actual number of
        // bytes sent.  Returns 0 if the connection is now closed.
        virtual size_t send(const void* buffer, size_t len) = 0;

        // Receives up to len bytes.  May receive less if some sort of signal
        // or other error occurs, or if the connecton was closed before the
        // requested number of bytes was received.  Returns the actual number
        // of bytes received.
        virtual size_t recv(void* buffer, size_t len) = 0;

        // Keepalive support.
        virtual void enable_keepalive(int keepidle_secs = 5,
                                      int keepintvl_secs = 1, int keepcnt = 10,
                                      int connection_timeout_secs = 10) = 0;

        // Corking support.
        virtual void cork() = 0;
        virtual void uncork() = 0;
        virtual void nodelay() = 0;

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

        template<typename T>
        void push(const T& v)
        {
            send_all(&v,sizeof(v));
        }

        void push(const std::string& s)
        {
            send_all(&s[0],s.size());
        }

        virtual ~stream() {}
    };

    inline int _socket(int af)
    {
        int fd = ::socket(af,SOCK_STREAM,0);
        if (fd == -1)
            throw futil::errno_exception(errno);
        return fd;
    }

    struct socket : public futil::file_descriptor,
                    public stream
    {
        const net::addr local_addr;
        const net::addr remote_addr;

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

        virtual void enable_keepalive(int keepidle_secs = 5,
                                      int keepintvl_secs = 1, int keepcnt = 10,
                                      int connection_timeout_secs = 10)
                                        override
        {
            // Enables keepalive probing on the socket.
            //  keepidle      - number of seconds of idleness on the socket
            //                  until keepalive probes will be started
            //  keepinvtl     - interval in seconds between keepalive probes
            //  keepcnt       - number of un-acknowledged keepalive probes
            //                  until the connection is declared dead
            //  connection_timeout - timeout for initial connection
            //                       establishment
            int on = 1;
#if IS_MACOS
            if (setsockopt(fd,IPPROTO_TCP,TCP_CONNECTIONTIMEOUT,
                           &connection_timeout_secs,sizeof(int)))
                throw futil::errno_exception(errno);
            if (setsockopt(fd,SOL_SOCKET,SO_KEEPALIVE,&on,sizeof(int)))
                throw futil::errno_exception(errno);
            if (setsockopt(fd,IPPROTO_TCP,TCP_KEEPALIVE,&keepidle_secs,
                           sizeof(int)))
            {
                throw futil::errno_exception(errno);
            }
            if (setsockopt(fd,IPPROTO_TCP,TCP_KEEPINTVL,&keepintvl_secs,
                           sizeof(int)))
            {
                throw futil::errno_exception(errno);
            }
            if (setsockopt(fd,IPPROTO_TCP,TCP_KEEPCNT,&keepcnt,sizeof(int)))
                throw futil::errno_exception(errno);
#elif IS_LINUX
            if (setsockopt(fd,SOL_SOCKET,SO_KEEPALIVE,&on,sizeof(int)))
                throw futil::errno_exception(errno);
            if (setsockopt(fd,IPPROTO_TCP,TCP_KEEPIDLE,&keepidle_secs,
                           sizeof(int)))
            {
                throw futil::errno_exception(errno);
            }
            if (setsockopt(fd,IPPROTO_TCP,TCP_KEEPINTVL,&keepintvl_secs,
                           sizeof(int)))
            {
                throw futil::errno_exception(errno);
            }
            if (setsockopt(fd,IPPROTO_TCP,TCP_KEEPCNT,&keepcnt,sizeof(int)))
                throw futil::errno_exception(errno);
#else
#warning Unknown system, keepalive not supported.
#endif
        }

        virtual void cork() override
        {
            int on = 1;
#if IS_MACOS
        if (setsockopt(fd,IPPROTO_TCP,TCP_NOPUSH,&on,sizeof(int)))
            throw futil::errno_exception(errno);
#elif IS_LINUX
        if (setsockopt(fd,IPPROTO_TCP,TCP_CORK,&on,sizeof(int)))
            throw futil::errno_exception(errno);
#else
#warning Unknown system, corking not supported.
#endif
        }

        virtual void uncork() override
        {
            int on = 0;
#if IS_MACOS
            if (setsockopt(fd,IPPROTO_TCP,TCP_NOPUSH,&on,sizeof(int)))
                throw futil::errno_exception(errno);
#elif IS_LINUX
            if (setsockopt(fd,IPPROTO_TCP,TCP_CORK,&on,sizeof(int)))
                throw futil::errno_exception(errno);
#endif
        }

        virtual void nodelay() override
        {
            int on = 1;
            if (setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&on,sizeof(int)))
                throw futil::errno_exception(errno);
        }

        constexpr socket(int fd, const net::addr& local_addr,
                         const net::addr& remote_addr):
            futil::file_descriptor(fd),
            local_addr(local_addr),
            remote_addr(remote_addr)
        {
        }

        socket(int fd, const net::addr& remote_addr):
            futil::file_descriptor(fd),
            local_addr(net::getsockname(fd)),
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

            ::shutdown(fd,SHUT_RDWR);
        }
    };

    struct client_socket : public socket
    {
        static int _connect(const net::addr& remote_addr)
        {
            int fd = _socket(remote_addr.sa.sa_family);
            if (::connect(fd,&remote_addr.sa,sizeof(remote_addr.sa)) == -1)
                throw futil::errno_exception(errno);
            return fd;
        }

        client_socket(const net::addr& remote_addr):
            socket(_connect(remote_addr),remote_addr)
        {
        }
    };

    struct server_socket : public futil::file_descriptor
    {
        const net::addr bind_addr;

        void bind(const net::addr& a)
        {
            if (::bind(fd,(struct sockaddr*)&a.sa,sizeof(a.sa)) == -1)
                throw futil::errno_exception(errno);
        }

        void listen(int backlog)
        {
            if (::listen(fd,backlog) == -1)
                throw futil::errno_exception(errno);
        }

        std::unique_ptr<tcp::socket> accept()
        {
            net::addr remote_addr;
            socklen_t sl = sizeof(remote_addr.sa);
            int afd = ::accept(fd,(struct sockaddr*)&remote_addr.sa,&sl);
            if (afd == -1)
                throw futil::errno_exception(errno);

            return std::make_unique<socket>(afd,bind_addr,remote_addr);
        }

        server_socket(const net::addr& bind_addr):
            futil::file_descriptor(_socket(bind_addr.sa.sa_family)),
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
