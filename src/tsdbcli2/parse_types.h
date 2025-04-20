// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_TSDBCLI_PARSE_TYPES_H
#define __SRC_TSDBCLI_PARSE_TYPES_H

#include <libtsdb/tsdb.h>

struct fields_list
{
    // <fields>
    std::vector<std::string> fields;
};

struct database_specifier
{
    // <database>
    std::string database;
};

struct measurement_specifier
{
    // <database/measurement>
    std::string database;
    std::string measurement;
};

struct series_specifier
{
    // <database/measurement/series>
    std::string database;
    std::string measurement;
    std::string series;
};

struct fields_specifier
{
    // WITH FIELDS <fields_spec>
    std::vector<tsdb::schema_entry> fields;
};

struct select_time_range
{
    // time_ns {<, <=, ==, >=, >} T
    // T0 {<, <=} time_ns {<, <=} T1
    // []
    uint64_t    t0;
    uint64_t    t1;
};

struct active_time_range
{
    // time_ns {<, <=, ==, >=, >} T
    // T0 {<, <=} time_ns {<, <=} T1
    uint64_t    t0;
    uint64_t    t1;
};

struct delete_time_range
{
    // time_ns {<, <=} T
    //
    // Delete up to t, inclusive.
    uint64_t    t;
};

struct select_limit
{
    // LIMIT N
    uint64_t    n;
};

struct select_last
{
    // LAST N
    uint64_t    n;
};

struct window_ns
{
    // window_ns N
    uint64_t    n;
};

#endif /* __SRC_TSDBCLI_PARSE_TYPES_H */
