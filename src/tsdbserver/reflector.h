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

#endif /* __TSDBSERVER_REFLECTOR_H */
