// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __TSDBSERVER_SERVER_H
#define __TSDBSERVER_SERVER_H

#ifdef UNITTEST
#include <futil/tcp.h>
#include <libtsdb/root.h>
#include <memory>

// Defined prod_static in server.cc.
extern tsdb::root* root;
void info_handler(std::unique_ptr<tcp::socket> sock);
#endif

#endif /* __TSDBSERVER_SERVER_H */
