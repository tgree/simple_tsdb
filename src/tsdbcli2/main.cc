// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "parse.h"
#include "parse_types.h"
#include "print_op_results.h"
#include <version.h>
#include <strutil/strutil.h>
#include <libtsdb/tsdb.h>
#include <editline/include/editline.h>

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#define DECL_KEYWORD(KW) \
    struct KW ## _keyword {}; \
    template<> KW ## _keyword \
    parse_type<KW ## _keyword>( \
        std::vector<std::string>::iterator& begin, \
        const std::vector<std::string>::iterator end) \
    { \
        if (begin == end || strcasecmp(begin->c_str(),#KW)) \
            throw parse_exception("Expected " #KW "."); \
        ++begin; \
        return KW ## _keyword{}; \
    }

DECL_KEYWORD(FROM);

static void
handle_init()
{
    // Handles:
    // INIT
    printf("Initializing TSDB directories...\n");
    tsdb::init();
}

static void
handle_add_user(const std::string& username, const std::string& password)
{
    // Handles:
    // ADD USER <username> <password>
    tsdb::add_user(username,password);
    printf("Added user %s.\n",username.c_str());
}

static void
handle_auth_user(const std::string& username, const std::string& password)
{
    // Handles:
    // AUTH <username> <password>
    if (tsdb::verify_user(username,password))
        printf("Authentication successful.\n");
    else
        printf("Authentication failed.\n");
}

static void
handle_create_database(const database_specifier& ds)
{
    // Handles:
    // CREATE DATABASE <database>
    printf("Creating database \"%s\"...\n",ds.database.c_str());
    tsdb::create_database(ds.database.c_str());
}

static void
handle_create_measurement(const measurement_specifier& ms,
    const fields_specifier& fs)
{
    // Handles:
    // CREATE MEASUREMENT <database/measurement> WITH FIELDS <field_spec>
    tsdb::database db(ms.database);
    tsdb::create_measurement(db,ms.measurement,fs.fields);
}

static void
handle_list_databases()
{
    // Handles:
    // LIST DATABASES
    auto dbs = tsdb::list_databases();
    for (const auto& s : dbs)
        printf("%s\n",s.c_str());
}

static void
handle_list_measurements(const FROM_keyword, const database_specifier& ds)
{
    // Handles:
    // LIST MEASUREMENTS FROM <database/measurement>
    tsdb::database db(ds.database);
    auto ms = db.list_measurements();
    for (const auto& s : ms)
        printf("%s\n",s.c_str());
}

static void
handle_list_series(const FROM_keyword, const measurement_specifier& ms)
{
    // Handles:
    // LIST SERIES FROM <database/measurement>
    tsdb::database db(ms.database);
    tsdb::measurement m(db,ms.measurement);
    auto ss = m.list_series();
    for (const auto& s : ss)
        printf("%s\n",s.c_str());
}

static void
handle_list_schema(const FROM_keyword, const measurement_specifier& ms)
{
    // Handles:
    // LIST SCHEMA FROM <database/measurement>
    tsdb::database db(ms.database);
    tsdb::measurement m(db,ms.measurement);
    for (const auto& se : m.fields)
        printf("%4s %s\n",tsdb::ftinfos[se.type].name,se.name);
}

static void
handle_count(
    const FROM_keyword fk,
    const series_specifier& ss,
    const select_time_range& tr)
{
    tsdb::database db(ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_read_lock read_lock(m,ss.series);
    auto cr = tsdb::count_points(read_lock,tr.t0,tr.t1);
    printf("%20s %20s %20s\n","time_first","time_last","num_points");
    printf("-------------------- "
           "-------------------- "
           "--------------------\n");
    printf("%20" PRIu64 " %20" PRIu64 " %20zu\n",
           cr.time_first,cr.time_last,cr.npoints);
}

static void
handle_select_1(
    const fields_list& fs,
    const FROM_keyword,
    const series_specifier& ss,
    const select_time_range& tr,
    const select_limit& n)
{
    // Handles:
    // SELECT <fields> FROM <database/measurement/series>
    //      [WHERE ...time_ns...] LIMIT N
    tsdb::database db(ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_read_lock read_lock(m,ss.series);
    tsdb::select_op_first op(read_lock,ss.series,fs.fields,tr.t0,tr.t1,n.n);
    print_op_results(op);
}

static void
handle_select_2(
    const fields_list& fs,
    const FROM_keyword,
    const series_specifier& ss,
    const select_time_range& tr,
    const select_last& n)
{
    // Handles:
    // SELECT <fields> FROM <database/measurement/series>
    //      [WHERE ...time_ns...] LAST N
    tsdb::database db(ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_read_lock read_lock(m,ss.series);
    tsdb::select_op_last op(read_lock,ss.series,fs.fields,tr.t0,tr.t1,n.n);
    print_op_results(op);
}

static void
handle_select_3(
    const fields_list& fs,
    const FROM_keyword fk,
    const series_specifier& ss,
    const select_time_range& tr)
{
    // Handles:
    // SELECT <fields> FROM <database/measurement/series> [WHERE ...time_ns...]
    handle_select_1(fs,fk,ss,tr,select_limit{(uint64_t)-1});
}

static void
handle_mean(
    const fields_list& fs,
    const FROM_keyword,
    const series_specifier& ss,
    const select_time_range& tr,
    const window_ns& wn)
{
    // Handles:
    // MEAN <fields> FROM <database/measurement/series>
    //      [WHERE ...time_ns...] window_ns N
    tsdb::database db(ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_read_lock read_lock(m,ss.series);
    tsdb::sum_op op(read_lock,ss.series,fs.fields,tr.t0,tr.t1,wn.n);

    printf("%20s ","time_ns");
    for (const auto& f : op.op.fields)
        printf("%20s ",f.name);
    printf("\n");
    for (size_t i=0; i<op.op.fields.size() + 1; ++i)
        printf("-------------------- ");
    printf("\n");

    while (op.next())
    {
        printf("%20" PRIu64 " ",op.range_t0);
        for (size_t j=0; j<op.op.fields.size(); ++j)
        {
            if (op.npoints[j] > 0)
                printf("%20f ",op.sums[j]/op.npoints[j]);
            else
                printf("%20s ","-");
        }
        printf("\n");
    }
}

static void
handle_delete(
    const FROM_keyword,
    const series_specifier& ss,
    const delete_time_range& tr)
{
    tsdb::database db(ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::delete_points(m,ss.series,tr.t);
}

struct command_handler
{
    const std::string keyword;
    const std::vector<parse_handler_func> candidates;
};

static const command_handler command_handlers[] =
{
    {"INIT",{XLATE(handle_init)}},
    {"ADD USER",{XLATE(handle_add_user)}},
    {"AUTH",{XLATE(handle_auth_user)}},
    {"CREATE DATABASE",{XLATE(handle_create_database)}},
    {"CREATE MEASUREMENT",{XLATE(handle_create_measurement)}},
    {"LIST DATABASES",{XLATE(handle_list_databases)}},
    {"LIST MEASUREMENTS",{XLATE(handle_list_measurements)}},
    {"LIST SERIES",{XLATE(handle_list_series)}},
    {"LIST SCHEMA",{XLATE(handle_list_schema)}},
    {"COUNT",{XLATE(handle_count)}},
    {"SELECT",{XLATE(handle_select_1),XLATE(handle_select_2),
               XLATE(handle_select_3)}},
    {"MEAN",{XLATE(handle_mean)}},
    {"DELETE",{XLATE(handle_delete)}},
};

static void
handle_command(
    const command_handler& ch,
    std::vector<std::string>::iterator _begin,
    std::vector<std::string>::iterator end)
{
    std::vector<std::string>::iterator begin;
    std::vector<parse_exception> exceptions;

    for (auto handler : ch.candidates)
    {
        try
        {
            begin = _begin;
            return handler(begin,end);
        }
        catch (const parse_exception& e)
        {
            exceptions.push_back(e);
        }
    }

    printf("Failed to parse %s:\n",ch.keyword.c_str());
    for (auto& e : exceptions)
        printf("    %s\n",e.what());
}

int
main(int argc, const char* argv[])
{
    char* home_dir = getenv("HOME");
    futil::path history_path(home_dir,".tsdbcli_history");
    read_history(history_path);

    printf("%s\n",GIT_VERSION);
    char* p;
    while ((p = readline("tsdbcli2> ")) != NULL)
    {
        std::string cmd(p);
        free(p);

        cmd = str::strip(cmd);
        if (cmd.empty())
            continue;

        bool found = false;
        for (const auto& ch : command_handlers)
        {
            if (cmd.size() < ch.keyword.size())
                continue;
            if (strncasecmp(cmd.c_str(),ch.keyword.c_str(),ch.keyword.size()))
                continue;

            found = true;
            cmd.erase(0,ch.keyword.size());
            auto v = str::split(cmd);
            try
            {
                handle_command(ch,v.begin(),v.end());
            }
            catch (const std::exception& e)
            {
                printf("Error: %s\n",e.what());
            }
            break;
        }

        if (!found)
            printf("Unrecognized command.\n");
    }

    write_history(history_path);
}
