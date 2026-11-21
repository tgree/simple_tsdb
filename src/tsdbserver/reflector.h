// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __TSDBSERVER_REFLECTOR_H
#define __TSDBSERVER_REFLECTOR_H

#include <map>
#include <string>
#include <stdint.h>

struct reflector_config
{
    uint16_t                            port = 4000;
    std::string                         remote_host;
    uint16_t                            remote_port = 4000;
    std::string                         remote_user;
    std::string                         remote_password;
    std::map<std::string,std::string>   db_map;
};

#ifdef UNITTEST
#include "client.h"
#include <hdr/auto_buf.h>
#include <futil/tcp.h>
#include <libtsdb/root.h>

#include <memory>
#include <set>

// Defined prod_static in reflector.cc.
extern tsdb::root* root;
extern reflector_config reflector_cfg;
void request_handler(std::unique_ptr<tcp::stream> s);
void flush_pass(client& c, std::set<std::string>& validated_dbs,
                auto_buf& chunk_storage);
#endif

#endif /* __TSDBSERVER_REFLECTOR_H */
