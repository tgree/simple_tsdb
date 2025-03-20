// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "tsdb.h"

tsdb::database::database(const futil::path& path) try:
    dir(futil::path("databases",path))
{
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        throw tsdb::no_such_database_exception();
    throw;
}

void
tsdb::create_database(const char* name) try
{
    futil::mkdir(futil::path("databases",name),0770);
}
catch (const futil::errno_exception& e)
{
    throw tsdb::create_database_io_error_exception(e.errnov);
}

std::vector<std::string>
tsdb::list_databases()
{
    return futil::directory("databases").listdirs();
}
