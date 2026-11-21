// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_TCP_H
#define __SRC_FUTIL_TCP_H

#include "futil.h"
#include "sockaddr.h"
#include <hdr/kassert.h>
#include <hdr/unittest.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <functional>
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

#ifdef UNITTEST
    int _socket(int af);
    void _setsockopt(int fd, int level, int name, const void* val,
                     socklen_t len);
    int _accept(int fd, net::addr& remote_addr);
#else
    inline int _socket(int af)
    {
        int fd = ::socket(af,SOCK_STREAM,0);
        if (fd == -1)
            throw futil::errno_exception(errno);
        return fd;
    }

    inline void _setsockopt(int fd, int level, int name, const void* val,
                            socklen_t len)
    {
        if (::setsockopt(fd,level,name,val,len))
            throw futil::errno_exception(errno);
    }

    inline int _accept(int fd, net::addr& remote_addr)
    {
        socklen_t sl = sizeof(remote_addr.sa);
        int afd = ::accept(fd,(struct sockaddr*)&remote_addr.sa,&sl);
        if (afd == -1)
            throw futil::errno_exception(errno);
        return afd;
    }
#endif

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
            _setsockopt(fd,IPPROTO_TCP,TCP_CONNECTIONTIMEOUT,
                        &connection_timeout_secs,sizeof(int));
            _setsockopt(fd,SOL_SOCKET,SO_KEEPALIVE,&on,sizeof(int));
            _setsockopt(fd,IPPROTO_TCP,TCP_KEEPALIVE,&keepidle_secs,
                        sizeof(int));
            _setsockopt(fd,IPPROTO_TCP,TCP_KEEPINTVL,&keepintvl_secs,
                        sizeof(int));
            _setsockopt(fd,IPPROTO_TCP,TCP_KEEPCNT,&keepcnt,sizeof(int));
#elif IS_LINUX
            _setsockopt(fd,SOL_SOCKET,SO_KEEPALIVE,&on,sizeof(int));
            _setsockopt(fd,IPPROTO_TCP,TCP_KEEPIDLE,&keepidle_secs,
                        sizeof(int));
            _setsockopt(fd,IPPROTO_TCP,TCP_KEEPINTVL,&keepintvl_secs,
                        sizeof(int));
            _setsockopt(fd,IPPROTO_TCP,TCP_KEEPCNT,&keepcnt,sizeof(int));
#else
#warning Unknown system, keepalive not supported.
#endif
        }

        virtual void cork() override
        {
            int on = 1;
#if IS_MACOS
        _setsockopt(fd,IPPROTO_TCP,TCP_NOPUSH,&on,sizeof(int));
#elif IS_LINUX
        _setsockopt(fd,IPPROTO_TCP,TCP_CORK,&on,sizeof(int));
#else
#warning Unknown system, corking not supported.
#endif
        }

        virtual void uncork() override
        {
            int on = 0;
#if IS_MACOS
            _setsockopt(fd,IPPROTO_TCP,TCP_NOPUSH,&on,sizeof(int));
#elif IS_LINUX
            _setsockopt(fd,IPPROTO_TCP,TCP_CORK,&on,sizeof(int));
#endif
        }

        virtual void nodelay() override
        {
            int on = 1;
            _setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&on,sizeof(int));
        }

        void set_send_timeout_us(uint64_t us)
        {
            struct timeval timeout;
            timeout.tv_sec = (us / 1000000);
            timeout.tv_usec = (us % 1000000);
            _setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout));
        }

        void set_recv_timeout_us(uint64_t us)
        {
            struct timeval timeout;
            timeout.tv_sec = (us / 1000000);
            timeout.tv_usec = (us % 1000000);
            _setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
        }

        void set_user_timeout_secs(unsigned int secs)
        {
            // Drops the connection if transmitted data (including zero-window
            // probes) remains unacknowledged for this long.  Keepalive only
            // probes an idle connection, so without this a connection with
            // unacknowledged data in flight is only dropped after the
            // kernel's retransmission limit (~15 minutes or more on Linux).
            // A value of 0 restores the system default.
#if IS_MACOS
            int val = secs;
            _setsockopt(fd,IPPROTO_TCP,TCP_RXT_CONNDROPTIME,&val,sizeof(val));
#elif IS_LINUX
            unsigned int ms = secs*1000;
            _setsockopt(fd,IPPROTO_TCP,TCP_USER_TIMEOUT,&ms,sizeof(ms));
#else
#warning Unknown system, user timeout not supported.
#endif
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
        static int _connect(const net::addr& remote_addr, int timeout_ms)
        {
            // Connects with a non-blocking connect() and a poll() for
            // completion, so that the wait can be bounded.  A negative
            // timeout_ms waits until the system's connect timeout expires.
            // The file_descriptor closes the socket if anything fails.
            futil::file_descriptor fd(_socket(remote_addr.sa.sa_family));
            int flags = fd.fcntl(F_GETFL);
            fd.fcntl(F_SETFL,flags | O_NONBLOCK);

            if (!futil::connect(fd.fd,&remote_addr.sa,
                                sizeof(remote_addr.sa)))
            {
                struct pollfd pfd = {fd.fd,POLLOUT,0};
                int rv;
                do
                {
                    // A signal restarts the full timeout; we don't expect
                    // enough signals for that to matter.
                    rv = ::poll(&pfd,1,timeout_ms);
                } while (rv == -1 && errno == EINTR);
                if (rv == -1)
                    throw futil::errno_exception(errno);
                if (rv == 0)
                    throw futil::errno_exception(ETIMEDOUT);

                int err = 0;
                socklen_t len = sizeof(err);
                if (getsockopt(fd.fd,SOL_SOCKET,SO_ERROR,&err,&len) == -1)
                    throw futil::errno_exception(errno);
                if (err)
                    throw futil::errno_exception(err);
            }

            fd.fcntl(F_SETFL,flags);
            return fd.extract_fd();
        }

        // Connects to the remote address.  If timeout_ms is negative, the
        // system's connect timeout applies (~75 seconds on macOS, ~127
        // seconds on Linux); otherwise the attempt fails with ETIMEDOUT if
        // the connection isn't established within timeout_ms milliseconds.
        client_socket(const net::addr& remote_addr, int timeout_ms = -1):
            socket(_connect(remote_addr,timeout_ms),remote_addr)
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
            int afd = _accept(fd,remote_addr);
            return std::make_unique<socket>(afd,bind_addr,remote_addr);
        }

        server_socket(const net::addr& bind_addr):
            futil::file_descriptor(_socket(bind_addr.sa.sa_family)),
            bind_addr(bind_addr)
        {
            int reuse = 1;
            _setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&reuse,sizeof(reuse));

            reuse = 1;
            _setsockopt(fd,SOL_SOCKET,SO_REUSEPORT,&reuse,sizeof(reuse));

            bind(bind_addr);
        }
    };

    // Accepts one connection on ss and passes it to handle(), which
    // normally starts a thread to serve it.  Errors from either are logged,
    // naming the kind of connection, such as "TCP", and not rethrown, so
    // that an accept loop keeps running.
    inline void
    accept_connection(server_socket& ss, const char* kind,
        const std::function<void(std::unique_ptr<socket>)>& handle)
    {
        try
        {
            handle(ss.accept());
        }
        catch (const std::exception& e)
        {
            prod_printf("Exception accepting %s connection: %s\n",kind,
                        e.what());
        }
    }
}

#endif /* __SRC_FUTIL_TCP_H */
