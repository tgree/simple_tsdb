// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "tsdb.h"

std::vector<std::string>
tsdb::list_series(const measurement& m)
{
    return m.dir.listdirs();
}

std::vector<std::string>
tsdb::list_measurements(const database& db)
{
    return db.dir.listdirs();
}

std::vector<std::string>
tsdb::list_databases()
{
    return futil::directory("databases").listdirs();
}

void
tsdb::init() try
{
    futil::mkdir("databases",0770);
}
catch (const futil::errno_exception& e)
{
    throw tsdb::init_io_error_exception(e.errnov);
}
