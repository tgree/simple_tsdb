// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_TCP_H
#define __SRC_FUTIL_TCP_H

#include "futil.h"
#include <hdr/kassert.h>

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

        virtual ~stream() {}
    };
}

#endif /* __SRC_FUTIL_TCP_H */
