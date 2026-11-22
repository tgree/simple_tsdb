// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
//
// Mocks of the OpenSSL functions that futil/ssl.h's contexts call.  A test
// links this instead of libssl and libcrypto.  The OpenSSL headers declare
// these functions extern "C", so the definitions get C linkage, and the
// compiler checks them against OpenSSL's own declarations.
#include <tmock/tmock.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

const SSL_METHOD*
TLS_client_method(void)
{
    return (const SSL_METHOD*)mock("TLS_client_method");
}

const SSL_METHOD*
TLS_server_method(void)
{
    return (const SSL_METHOD*)mock("TLS_server_method");
}

SSL_CTX*
SSL_CTX_new(const SSL_METHOD* meth)
{
    return (SSL_CTX*)mock("SSL_CTX_new",meth);
}

void
SSL_CTX_set_verify(SSL_CTX* ctx, int mode, SSL_verify_cb callback)
{
    mock("SSL_CTX_set_verify",ctx,mode,callback);
}

int
SSL_CTX_set_default_verify_paths(SSL_CTX* ctx)
{
    return (int)mock("SSL_CTX_set_default_verify_paths",ctx);
}

int
SSL_CTX_use_certificate_chain_file(SSL_CTX* ctx, const char* file)
{
    return (int)mock("SSL_CTX_use_certificate_chain_file",ctx,file);
}

int
SSL_CTX_use_PrivateKey_file(SSL_CTX* ctx, const char* file, int type)
{
    return (int)mock("SSL_CTX_use_PrivateKey_file",ctx,file,type);
}

void
ERR_print_errors_fp(FILE* fp)
{
    mock("ERR_print_errors_fp",fp);
}
