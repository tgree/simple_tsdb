// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_SSL_H
#define __SRC_FUTIL_SSL_H

#include "tcp.h"
#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/evp.h>
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
        std::unique_ptr<tcp::socket>    s;
        SSL*                            cSSL;
        bool                            should_shutdown;

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

        stream(std::unique_ptr<tcp::socket> s, SSL* cSSL):
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

        std::unique_ptr<stream> wrap(std::unique_ptr<tcp::socket> s)
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
            if (SSL_CTX_use_PrivateKey_file(sslctx,key_file,SSL_FILETYPE_PEM)
                <= 0)
            {
                ERR_print_errors_fp(stderr);
                kabort();
            }
        }
    };

    struct sha512_digest
    {
        const EVP_MD*   md;
        EVP_MD_CTX*     mdctx;
        unsigned char   value[EVP_MAX_MD_SIZE];
        unsigned int    len;

        void update(const void* d, size_t cnt)
        {
            EVP_DigestUpdate(mdctx,d,cnt);
        }

        void finalize(size_t nrounds = 1)
        {
            EVP_DigestFinal_ex(mdctx,value,&len);
            for (size_t i=0; i<nrounds; ++i)
            {
                EVP_DigestInit_ex(mdctx,md,NULL);
                EVP_DigestUpdate(mdctx,value,len);
                EVP_DigestFinal_ex(mdctx,value,&len);
            }
        }

        sha512_digest():
            md(EVP_get_digestbyname("SHA512")),
            mdctx(EVP_MD_CTX_new()),
            len(0)
        {
            kassert(md != NULL);
            EVP_DigestInit_ex(mdctx,md,NULL);
        }

        ~sha512_digest()
        {
            EVP_MD_CTX_free(mdctx);
        }
    };

    struct sha512_result
    {
        uint8_t data[512/8];

        std::string to_string() const
        {
            char str[2*512/8 + 1];
            for (size_t i=0; i<512/8; ++i)
                snprintf(str + i*2,3,"%02X",data[i]);
            return std::string(str);
        }

        sha512_result()
        {
            memset(data,0,sizeof(data));
        }
    };

    static inline sha512_result
    pbkdf2_sha512(const std::string& password, const std::string& salt,
                  size_t iters)
    {
        sha512_result res;
        kassert(PKCS5_PBKDF2_HMAC(password.c_str(),password.size(),
                                  (const unsigned char*)salt.c_str(),
                                  salt.size(),iters,EVP_sha512(),64,res.data)
                == 1);
        return res;
    }
}

#endif /* __SRC_FUTIL_SSL_H */
