// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../ssl.h"
#include <tmock/tmock.h>

// Fake OpenSSL objects for the mocks to return.  Nothing dereferences them.
static const SSL_METHOD* const client_method = (const SSL_METHOD*)0x1000;
static const SSL_METHOD* const server_method = (const SSL_METHOD*)0x2000;
static SSL_CTX* const sslctx = (SSL_CTX*)0x3000;

class tmock_test
{
    TMOCK_TEST(test_client_context)
    {
        texpect("TLS_client_method",returns(client_method));
        texpect("SSL_CTX_new",want(meth,client_method),returns(sslctx));
        texpect("SSL_CTX_set_verify",want(ctx,sslctx),
                want(mode,SSL_VERIFY_PEER),want(callback,NULL));
        texpect("SSL_CTX_set_default_verify_paths",want(ctx,sslctx),
                returns(1));
        tcp::ssl::client_context ctx;
        tcheckpoint();
        TASSERT(ctx.sslctx == sslctx);
    }

    TMOCK_TEST(test_server_context)
    {
        texpect("TLS_server_method",returns(server_method));
        texpect("SSL_CTX_new",want(meth,server_method),returns(sslctx));
        texpect("SSL_CTX_use_certificate_chain_file",want(ctx,sslctx),
                want_str(file,"cert.pem"),returns(1));
        texpect("SSL_CTX_use_PrivateKey_file",want(ctx,sslctx),
                want_str(file,"key.pem"),want(type,SSL_FILETYPE_PEM),
                returns(1));
        tcp::ssl::server_context ctx("cert.pem","key.pem");
        tcheckpoint();
        TASSERT(ctx.sslctx == sslctx);
    }
};

TMOCK_MAIN();
