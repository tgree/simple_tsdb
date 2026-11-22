// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __TSDBSERVER_SERVER_H
#define __TSDBSERVER_SERVER_H

#ifdef UNITTEST
#include "tokens.h"
#include <futil/tcp.h>
#include <futil/ssl.h>
#include <libtsdb/root.h>
#include <memory>

// Defined prod_static in server.cc.
extern tsdb::root* root;
void authorize_command(const std::string& username, command_token ct,
                       const std::vector<parsed_data_token>& tokens);
void request_handler(std::unique_ptr<tcp::socket> sock);
void auth_request_handler(tcp::ssl::server_context* sslctx,
                          std::unique_ptr<tcp::socket> sock);
void info_handler(std::unique_ptr<tcp::socket> sock);
#endif

#endif /* __TSDBSERVER_SERVER_H */
