// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "root.h"
#include "exception.h"
#include <futil/xact.h>
#include <futil/ssl.h>
#include <strutil/strutil.h>

tsdb::root::root(const futil::path& root_path) try :
    root_dir(root_path),
    tmp_dir(root_dir,"tmp"),
    databases_dir(root_dir,"databases")
{
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        throw not_a_tsdb_root();
    throw;
}

tsdb::root::root() try :
    root_dir(AT_FDCWD,"."),
    tmp_dir(root_dir,"tmp"),
    databases_dir(root_dir,"databases")
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
    for (;;)
    {
        auto line = passwd_fd.read_line();
        if (line.empty())
            break;

        std::vector<std::string> parts = str::split(line);
        if (parts[0] == username)
            throw user_exists_exception();
    }

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
    for (;;)
    {
        auto line = passwd_fd.read_line();
        if (line.empty())
            throw no_such_user_exception();
        
        std::vector<std::string> parts = str::split(line);
        if (parts[0] != username)
            continue;

        auto sr = tcp::ssl::pbkdf2_sha512(password,username + "tsdb75D8",
                                          10000);
        return sr.to_string() == parts[1];
    }
}

void
tsdb::root::create_database(const char* name) try
{
    futil::mkdir(databases_dir,name,0770);
}
catch (const futil::errno_exception& e)
{
    throw tsdb::create_database_io_error_exception(e.errnov);
}

std::vector<std::string>
tsdb::root::list_databases()
{
    return databases_dir.listdirs();
}

void
tsdb::create_root(const futil::path& path) try
{
    futil::directory root_dir(path);
    futil::xact_creat passwd_lock_fd(root_dir,"passwd.lock",O_RDWR | O_CREAT,
                                     0660);
    futil::xact_creat passwd_fd(root_dir,"passwd",O_RDWR | O_CREAT,0660);
    futil::xact_mkdir tmp_dir(root_dir,"tmp",0770);
    futil::xact_mkdir databases_dir(root_dir,"databases",0770);
    databases_dir.commit();
    tmp_dir.commit();
    passwd_fd.commit();
    passwd_lock_fd.commit();
}
catch (const futil::errno_exception& e)
{
    throw tsdb::init_io_error_exception(e.errnov);
}
