// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "tokens.h"
#include <map>

const char*
get_command_token_str(command_token ct)
{
    if (ct == 0)
        return "<idle>";

    switch (ct)
    {
        case CT_CREATE_DATABASE:     return "CREATE DATABASE";
        case CT_CREATE_MEASUREMENT:  return "CREATE MEASUREMENT";
        case CT_WRITE_POINTS:        return "WRITE_POINTS";
        case CT_SELECT_POINTS_LIMIT: return "SELECT POINTS LIMIT";
        case CT_SELECT_POINTS_LAST:  return "SELECT POINTS LAST";
        case CT_DELETE_POINTS:       return "DELETE POINTS";
        case CT_GET_SCHEMA:          return "GET SCHEMA";
        case CT_LIST_DATABASES:      return "LIST DATABASES";
        case CT_LIST_MEASUREMENTS:   return "LIST MEASUREMENTS";
        case CT_LIST_SERIES:         return "LIST SERIES";
        case CT_ACTIVE_SERIES:       return "ACTIVE SERIES";
        case CT_COUNT_POINTS:        return "COUNT POINTS";
        case CT_SUM_POINTS:          return "SUM POINTS";
        case CT_INTEGRATE_POINTS:    return "INTEGRATE POINTS";
        case CT_NOP:                 return "NOP";
        case CT_AUTHENTICATE:        return "AUTHENTICATE";
    }
    return "?";
}
