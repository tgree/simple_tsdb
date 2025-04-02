// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_SSL_H
#define __SRC_FUTIL_SSL_H

#include "ipv4.h"
#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

namespace tcp::ssl
{
    struct exception : public std::exception
    {
        exception() {}
    };

    struct null_ssl_exception : public exception
    {
        virtual const char* what() const noexcept override
        {
            return "SSL_new returned NULL";
        }
    };

    struct ssl_error_exception : public exception
    {
        int ssl_error;

        virtual const char* what() const noexcept override
        {
            switch (ssl_error)
            {
                case SSL_ERROR_NONE:
                    return "No error";

                case SSL_ERROR_ZERO_RETURN:
                    return "Zero return";

                case SSL_ERROR_WANT_READ:
                    return "Want read";

                case SSL_ERROR_WANT_WRITE:
                    return "Want write";

                case SSL_ERROR_WANT_CONNECT:
                    return "Want connect";

                case SSL_ERROR_WANT_ACCEPT:
                    return "Want accept";

                case SSL_ERROR_WANT_X509_LOOKUP:
                    return "Want X509 lookup";

                case SSL_ERROR_WANT_ASYNC:
                    return "Want async";

                case SSL_ERROR_WANT_ASYNC_JOB:
                    return "Want async job";

                case SSL_ERROR_WANT_CLIENT_HELLO_CB:
                    return "Want client hello callback";

                case SSL_ERROR_SYSCALL:
                    // SSL_shutdown() must not be called.
                    return "Fatal I/O syscall error";

                case SSL_ERROR_SSL:
                    // SSL_shutdown() must not be called.
                    return "Fatal SSL library error";

                default:
                    kabort();
            }
        }

        ssl_error_exception(int ssl_error):
            ssl_error(ssl_error)
        {
        }
    };

    struct stream : public tcp::stream
    {
        std::unique_ptr<tcp::ipv4::socket>  s;
        SSL*                                cSSL;
        bool                                should_shutdown;

        virtual std::string local_addr_string() override
        {
            return s->local_addr_string();
        }

        virtual std::string remote_addr_string() override
        {
            return s->remote_addr_string();
        }

        virtual size_t send(const void* buffer, size_t len) override
        {
            int ret = SSL_write(cSSL,buffer,len);
            if (ret > 0)
            {
                // SSL_write is always supposed to write everything unless
                // SSL_MODE_ENABLE_PARTIAL_WRITE is set, which we don't.
                kassert((size_t)ret == len);
                return ret;
            }

            int err = SSL_get_error(cSSL,ret);
            if (err == SSL_ERROR_ZERO_RETURN)
                return 0;

            if (err == SSL_ERROR_SYSCALL || err == SSL_ERROR_SSL)
                should_shutdown = false;
            throw ssl_error_exception(err);
        }

        virtual size_t recv(void* buffer, size_t len) override
        {
            int ret = SSL_read(cSSL,buffer,len);
            if (ret > 0)
                return ret;

            int err = SSL_get_error(cSSL,ret);
            if (err == SSL_ERROR_ZERO_RETURN)
                return 0;

            if (err == SSL_ERROR_SYSCALL || err == SSL_ERROR_SSL)
                should_shutdown = false;
            throw ssl_error_exception(err);
        }

        stream(std::unique_ptr<tcp::ipv4::socket> s, SSL* cSSL):
            s(std::move(s)),
            cSSL(cSSL),
            should_shutdown(true)
        {
        }

        ~stream()
        {
            if (should_shutdown)
            {
                printf("SSL shutdown fd %d local %s remote %s.\n",
                       s->fd,s->local_addr.to_string().c_str(),
                       s->remote_addr.to_string().c_str());
                SSL_shutdown(cSSL);
            }
            else
                printf("SSL shutdown omitted due to previous error.\n");
            SSL_free(cSSL);
        }
    };

    struct context
    {
        SSL_CTX* sslctx;

        std::unique_ptr<stream> wrap(std::unique_ptr<tcp::ipv4::socket> s)
        {
            SSL* cSSL = SSL_new(sslctx);
            if (!cSSL)
                throw null_ssl_exception();

            SSL_set_fd(cSSL,s->fd);
            int ret = SSL_accept(cSSL);
            if (ret <= 0)
            {
                int err = SSL_get_error(cSSL,ret);
                throw ssl_error_exception(err);
            }
            return std::make_unique<ssl::stream>(std::move(s),cSSL);
        }

        context(const futil::path& cert_file, const futil::path& key_file):
            sslctx(SSL_CTX_new(TLS_server_method()))
        {
            kassert(cert_file.ends_with(".pem"));
            kassert(key_file.ends_with(".pem"));
            if (SSL_CTX_use_certificate_chain_file(sslctx,cert_file) <= 0)
            {
                ERR_print_errors_fp(stderr);
                kabort();
            }
            if (SSL_CTX_use_PrivateKey_file(sslctx,key_file,SSL_FILETYPE_PEM) <= 0)
            {
                ERR_print_errors_fp(stderr);
                kabort();
            }
        }
    };
}

#endif /* __SRC_FUTIL_SSL_H */
