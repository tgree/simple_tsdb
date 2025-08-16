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

static tsdb::root* root;

static void
handle_add_user(const std::string& username, const std::string& password)
{
    // Handles:
    // ADD USER <username> <password>
    root->add_user(username,password);
    printf("Added user %s.\n",username.c_str());
}

static void
handle_auth_user(const std::string& username, const std::string& password)
{
    // Handles:
    // AUTH <username> <password>
    if (root->verify_user(username,password))
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
    root->create_database(ds.database.c_str());
}

static void
handle_create_measurement(const measurement_specifier& ms,
    const fields_specifier& fs)
{
    // Handles:
    // CREATE MEASUREMENT <database/measurement> WITH FIELDS <field_spec>
    tsdb::database db(*root,ms.database);
    tsdb::create_measurement(db,ms.measurement,fs.fields);
}

static void
handle_list_databases()
{
    // Handles:
    // LIST DATABASES
    auto dbs = root->list_databases();
    for (const auto& s : dbs)
        printf("%s\n",s.c_str());
}

static void
handle_list_measurements(const FROM_keyword, const database_specifier& ds)
{
    // Handles:
    // LIST MEASUREMENTS FROM <database/measurement>
    tsdb::database db(*root,ds.database);
    auto ms = db.list_measurements();
    for (const auto& s : ms)
        printf("%s\n",s.c_str());
}

static void
handle_list_series(const FROM_keyword, const measurement_specifier& ms)
{
    // Handles:
    // LIST SERIES FROM <database/measurement>
    tsdb::database db(*root,ms.database);
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
    tsdb::database db(*root,ms.database);
    tsdb::measurement m(db,ms.measurement);
    for (const auto& se : m.fields)
        printf("%4s %s\n",tsdb::ftinfos[se.type].name,se.name);
}

static void
handle_list_active_series(const FROM_keyword, const measurement_specifier& ms,
    const active_time_range& tr)
{
    // Handles:
    // LIST ACTIVE SERIES FROM <database/measurement> WHERE ...time_ns...
    tsdb::database db(*root,ms.database);
    tsdb::measurement m(db,ms.measurement);
    auto as = m.list_active_series(tr.t0,tr.t1);
    for (const auto& s : as)
        printf("%s\n",s.c_str());
}

static void
handle_count(
    const FROM_keyword fk,
    const series_specifier& ss,
    const select_time_range& tr)
{
    tsdb::database db(*root,ss.database);
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
    tsdb::database db(*root,ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_read_lock read_lock(m,ss.series);
    tsdb::wal_query wq(read_lock,tr.t0,tr.t1);
    tsdb::select_op_first op(read_lock,ss.series,fs.fields,tr.t0,tr.t1,n.n);
    print_op_results(op,wq,n.n);
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
    tsdb::database db(*root,ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_read_lock read_lock(m,ss.series);
    tsdb::wal_query wq(read_lock,tr.t0,tr.t1);
    if (wq.nentries > n.n)
    {
        wq._begin += (wq.nentries - n.n);
        wq.nentries = n.n;
    }
    tsdb::select_op_last op(read_lock,ss.series,fs.fields,tr.t0,tr.t1,
                            n.n - wq.nentries);
    print_op_results(op,wq,n.n);
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
    tsdb::database db(*root,ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_read_lock read_lock(m,ss.series);
    tsdb::sum_op op(read_lock,ss.series,fs.fields,tr.t0,tr.t1,wn.n);

    printf("%20s ","time_ns");
    for (const auto* f : op.op.fields)
        printf("%20s ",f->name);
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
handle_integrate(
    const fields_list& fs,
    const FROM_keyword,
    const series_specifier& ss,
    const select_time_range& tr)
{
    // Handles:
    // INTEGRATE <fields> FROM <database/measurement/series>
    //           [WHERE ...time_ns...]
    tsdb::database db(*root,ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_read_lock read_lock(m,ss.series);
    tsdb::integral_op op(read_lock,ss.series,fs.fields,tr.t0,tr.t1);

    printf("%20s %20s\n","t0_ns","t1_ns");
    printf("-------------------- --------------------\n");
    printf("%20" PRIu64 " %20" PRIu64 "\n",op.t0_ns,op.t1_ns);

    printf("          ");
    for (const auto& f : fs.fields)
        printf("%20s ",f.c_str());
    printf("\n");
    printf("          ");
    for (size_t i=0; i<fs.fields.size(); ++i)
        printf("-------------------- ");
    printf("\n");
    printf("Integral: ");
    for (size_t i=0; i<fs.fields.size(); ++i)
    {
        if (op.is_null[i])
            printf("%20s ","null");
        else
            printf("%20.9f ",op.integral[i]);
    }
    printf("\n");
    printf(" Average: ");
    for (size_t i=0; i<fs.fields.size(); ++i)
    {
        if (op.is_null[i])
            printf("%20s ","null");
        else
        {
            double dt = (double)(op.t1_ns - op.t0_ns) / 1e9;
            printf("%20.9f ",op.integral[i] / dt);
        }
    }
    printf("\n");
}

static void
handle_delete(
    const FROM_keyword,
    const series_specifier& ss,
    const delete_time_range& tr)
{
    tsdb::database db(*root,ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_total_lock total_lock(m,ss.series);
    tsdb::delete_points(total_lock,tr.t);
}

static void
handle_update_schema()
{
    for (const auto& db_name : root->list_databases())
    {
        tsdb::database db(*root,db_name);
        for (const auto& m_name : db.list_measurements())
        {
            futil::directory m_dir(db.dir,m_name);
            futil::fchmod(m_dir,"schema",0660);
            try
            {
                futil::file schema_fd(m_dir,"schema",O_RDWR);
                auto mm = schema_fd.mmap(0,schema_fd.lseek(0,SEEK_END),
                                         PROT_READ | PROT_WRITE,MAP_SHARED,0);
                kassert(mm.len > 0);
                kassert(mm.len % sizeof(tsdb::schema_entry) == 0);
                const size_t nentries = mm.len / sizeof(tsdb::schema_entry);

                auto* entries = (tsdb::schema_entry*)mm.addr;
                if (entries[0].version != SCHEMA_VERSION)
                {
                    printf("Updating %s/%s...\n",
                           db_name.c_str(),m_name.c_str());
                    size_t offset = 0;
                    for (size_t i=0; i<nentries; ++i)
                    {
                        entries[i].version = SCHEMA_VERSION;
                        entries[i].index = i;
                        entries[i].offset = offset;
                        offset += tsdb::ftinfos[entries[i].type].nbytes;
                    }
                    mm.msync();
                }

                printf("Validating %s/%s...\n",db_name.c_str(),m_name.c_str());
                size_t offset = 0;
                for (size_t i=0; i<nentries; ++i)
                {
                    if (entries[i].version != SCHEMA_VERSION)
                    {
                        printf("Entry [%zu] has version %u.\n",
                               i,entries[i].version);
                    }
                    if (entries[i].index != i)
                    {
                        printf("Entry [%zu] has index %u.\n",
                               i,entries[i].index);
                    }
                    if (entries[i].offset != offset)
                    {
                        printf("Entry [%zu] as offset %u (expected %zu).\n",
                               i,entries[i].offset,offset);
                    }
                    offset += tsdb::ftinfos[entries[i].type].nbytes;
                }
            }
            catch (...)
            {
                futil::fchmod(m_dir,"schema",0440);
                throw;
            }
        }
    }
}

static void
handle_update_wal()
{
    for (const auto& db_name : root->list_databases())
    {
        tsdb::database db(*root,db_name);
        for (const auto& m_name : db.list_measurements())
        {
            tsdb::measurement m(db,m_name);
            for (const auto& s_name : m.list_series())
            {
                futil::directory series_dir(m.dir,s_name);
                futil::file wal_fd(series_dir,"wal",O_CREAT | O_RDWR,0660);
                wal_fd.fsync();
            }
        }
    }
}

struct command_handler
{
    const std::string keyword;
    const std::vector<parse_handler_func> candidates;
};

static const command_handler command_handlers[] =
{
    {"ADD USER",{XLATE(handle_add_user)}},
    {"AUTH",{XLATE(handle_auth_user)}},
    {"CREATE DATABASE",{XLATE(handle_create_database)}},
    {"CREATE MEASUREMENT",{XLATE(handle_create_measurement)}},
    {"LIST DATABASES",{XLATE(handle_list_databases)}},
    {"LIST MEASUREMENTS",{XLATE(handle_list_measurements)}},
    {"LIST SERIES",{XLATE(handle_list_series)}},
    {"LIST SCHEMA",{XLATE(handle_list_schema)}},
    {"LIST ACTIVE SERIES",{XLATE(handle_list_active_series)}},
    {"COUNT",{XLATE(handle_count)}},
    {"SELECT",{XLATE(handle_select_1),XLATE(handle_select_2),
               XLATE(handle_select_3)}},
    {"MEAN",{XLATE(handle_mean)}},
    {"INTEGRATE",{XLATE(handle_integrate)}},
    {"DELETE",{XLATE(handle_delete)}},
    {"UPDATE SCHEMA",{XLATE(handle_update_schema)}},
    {"UPDATE WAL",{XLATE(handle_update_wal)}},
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
    printf("simple_tsdb " SIMPLE_TSDB_VERSION_STR " " GIT_VERSION "\n");

    for (size_t i=1; i<(size_t)argc; ++i)
    {
        if (!strcmp(argv[i],"--help"))
        {
            printf("tsdbcli2 [--init-root <chunksize>]\n");
            return -1;
        }
    }

    bool init_root = false;
    size_t chunk_size = 0;

    std::vector<const char*> unused_args;
    for (size_t i=1; i<(size_t)argc; ++i)
    {
        size_t rem = (size_t)argc - i - 1;
        if (!strcmp(argv[i],"--init-root"))
        {
            if (!rem)
            {
                printf("Expected chunk size argument to --init-root.\n");
                return -1;
            }
            init_root = true;
            chunk_size = str::decode_number_units_pow2(argv[i+1]);
            ++i;
        }
        else
            unused_args.push_back(argv[i]);
    }

    if (unused_args.size() > 1)
    {
        printf("Unrecognized arguments.\n");
        return -1;
    }

    std::string root_path(unused_args.empty() ? "." : unused_args[0]);
    try
    {
        root = new tsdb::root(root_path,true);
        if (init_root)
        {
            printf("TSDB root already exists but --init-root supplied.\n");
            return -1;
        }
    }
    catch (const tsdb::not_a_tsdb_root& e)
    {
        if (!init_root)
        {
            printf("Failed to open TSDB root: %s\n",e.what());
            return -1;
        }
    }
    catch (const std::exception& e)
    {
        printf("Failed to open TSDB root: %s\n",e.what());
        return -1;
    }
    if (!root)
    {
        try
        {
            auto config = tsdb::default_configuration;
            config.chunk_size = chunk_size;
            tsdb::create_root(root_path,config);
        }
        catch (const std::exception& e)
        {
            printf("Failed to create TSDB root: %s\n",e.what());
            return -1;
        }

        try
        {
            root = new tsdb::root(root_path,true);
        }
        catch (const std::exception& e)
        {
            printf("Failed to open new TSDB root: %s\n",e.what());
            return -1;
        }
    }

    printf("Chunk size: %zu bytes\n",root->config.chunk_size);
    printf("  WAL size: %zu points\n",root->config.wal_max_entries);

    char* home_dir = getenv("HOME");
    futil::path history_path(home_dir,".tsdbcli_history");
    read_history(history_path);

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
