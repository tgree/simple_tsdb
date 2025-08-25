// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "root.h"
#include "exception.h"
#include "constants.h"
#include <hdr/kmath.h>
#include <futil/xact.h>
#include <futil/ssl.h>
#include <strutil/strutil.h>
#include <zutil/zutil.h>
#include <algorithm>

const tsdb::configuration tsdb::default_configuration =
{
    DEFAULT_CHUNK_SIZE_M*1024*1024,
    DEFAULT_WAL_MAX_ENTRIES,
    DEFAULT_WRITE_THROTTLE_NS,
};

static tsdb::configuration
load_configuration(const tsdb::root* r) try
{
    futil::file config_fd(r->root_dir,"config.txt",O_RDONLY);
    tsdb::configuration config = tsdb::default_configuration;
    
    futil::file::line line;
    do
    {
        line = config_fd.read_line();
        if (line.text.empty())
            continue;
        if (line.text[0] == '#')
            continue;

        std::vector<std::string> parts = str::split(line.text);
        if (parts.size() != 2)
            throw tsdb::invalid_config_file_exception();
        if (parts[0] == "chunk_size")
        {
            config.chunk_size = str::decode_number_units_pow2(parts[1]);
            if (!is_pow2(config.chunk_size) ||
                config.chunk_size < MIN_CHUNK_SIZE)
            {
                throw tsdb::invalid_chunk_size_exception();
            }
        }
        else if (parts[0] == "wal_max_entries")
            config.wal_max_entries = str::decode_number_units_pow2(parts[1]);
        else if (parts[0] == "write_throttle_ns")
            config.write_throttle_ns = str::decode_number_units_pow2(parts[1]);
        else
            throw tsdb::invalid_config_file_exception();
    } while (line);

    return config;
}
catch (const std::invalid_argument&)
{
    throw tsdb::invalid_config_file_exception();
}

tsdb::root::root(const futil::path& root_path, bool debug_enabled) try :
    root_dir(root_path),
    tmp_dir(root_dir,"tmp"),
    databases_dir(root_dir,"databases"),
    debug_enabled(debug_enabled),
    config(load_configuration(this)),
    max_gzipped_size(zutil::max_gzipped_size(config.chunk_size))
{
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        throw not_a_tsdb_root();
    throw;
}

tsdb::root::root(bool debug_enabled) try :
    root_dir(AT_FDCWD,"."),
    tmp_dir(root_dir,"tmp"),
    databases_dir(root_dir,"databases"),
    debug_enabled(debug_enabled),
    config(load_configuration(this)),
    max_gzipped_size(zutil::max_gzipped_size(config.chunk_size))
{
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        throw not_a_tsdb_root();
    throw;
}

void
tsdb::root::add_user(const std::string& username, const std::string& password)
{
    auto sr = tcp::ssl::pbkdf2_sha512(password,username + "tsdb75D8",10000);

    futil::file lock_fd(root_dir,"passwd.lock",O_RDONLY);
    lock_fd.flock(LOCK_EX);

    futil::file passwd_fd(root_dir,"passwd",O_RDWR);
    futil::file::line line;
    do
    {
        line = passwd_fd.read_line();
        if (line.text.empty())
            continue;

        std::vector<std::string> parts = str::split(line.text);
        if (parts[0] == username)
            throw user_exists_exception();
    } while (line);

    std::string user(username + " " + sr.to_string() + "\n");
    passwd_fd.write_all(user.c_str(),user.size());
    passwd_fd.fsync_and_flush();
}

bool
tsdb::root::verify_user(const std::string& username,
    const std::string& password)
{
    futil::file lock_fd(root_dir,"passwd.lock",O_RDONLY);
    lock_fd.flock(LOCK_SH);

    futil::file passwd_fd(root_dir,"passwd",O_RDONLY);
    futil::file::line line;
    do
    {
        line = passwd_fd.read_line();
        if (line.text.empty())
            continue;
        
        std::vector<std::string> parts = str::split(line.text);
        if (parts[0] != username)
            continue;

        auto sr = tcp::ssl::pbkdf2_sha512(password,username + "tsdb75D8",
                                          10000);
        return sr.to_string() == parts[1];
    } while (line);

    throw no_such_user_exception();
}

void
tsdb::root::create_database(const char* name) try
{
    futil::mkdir(databases_dir,name,0770);
    databases_dir.fsync();
}
catch (const futil::errno_exception& e)
{
    throw tsdb::create_database_io_error_exception(e.errnov);
}

std::vector<std::string>
tsdb::root::list_databases()
{
    auto v = databases_dir.listdirs();
    std::sort(v.begin(),v.end());
    return v;
}

bool
tsdb::root::database_exists(const futil::path& path) try
{
    futil::directory dir(databases_dir,path);
    return true;
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        return false;
    throw;
}

void
tsdb::root::replace_with_truncated(futil::file& src_fd,
    const futil::directory& dir, const futil::path& path, size_t new_len) const
{
    futil::xact_mktemp tmp_fd(tmp_dir,0660);
    auto_buf truncated_data(new_len);
    src_fd.lseek(0,SEEK_SET);
    src_fd.read_all(truncated_data.data,new_len);
    tmp_fd.write_all(truncated_data.data,new_len);
    tmp_fd.fsync();
    futil::rename(tmp_dir,tmp_fd.name,dir,path);
    tmp_fd.commit();
    dir.fsync_and_flush();
    tmp_dir.fsync_and_flush();
    src_fd.swap(tmp_fd);
}

int
tsdb::root::debugf(const char* fmt, ...) const
{
    va_list ap;
    va_start(ap,fmt);
    int rv = vdebugf(fmt,ap);
    va_end(ap);

    return rv;
}

int
tsdb::root::vdebugf(const char* fmt, va_list ap) const
{
    if (!debug_enabled)
        return 0;

    return vprintf(fmt,ap);
}

void
tsdb::create_root(const futil::path& path, const configuration& config) try
{
    auto config_str = to_string(config);
    if (!is_pow2(config.chunk_size) || config.chunk_size < MIN_CHUNK_SIZE)
        throw invalid_chunk_size_exception();

    futil::directory root_dir(path);
    futil::xact_creat passwd_lock_fd(root_dir,"passwd.lock",O_RDWR | O_CREAT,
                                     0660);
    futil::xact_creat passwd_fd(root_dir,"passwd",O_RDWR | O_CREAT,0660);
    futil::xact_mkdir tmp_dir(root_dir,"tmp",0770);
    futil::xact_mkdir databases_dir(root_dir,"databases",0770);
    futil::xact_creat config_fd(root_dir,"config.txt",O_RDWR | O_CREAT,0440);
    config_fd.write_all(&config_str[0],config_str.size());
    config_fd.fsync();
    root_dir.fsync();
    config_fd.commit();
    databases_dir.commit();
    tmp_dir.commit();
    passwd_fd.commit();
    passwd_lock_fd.commit();
}
catch (const futil::errno_exception& e)
{
    throw tsdb::init_io_error_exception(e.errnov);
}
