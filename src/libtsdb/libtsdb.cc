// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "tsdb.h"

void
tsdb::init() try
{
    futil::mkdir("databases",0770);
}
catch (const futil::errno_exception& e)
{
    throw tsdb::init_io_error_exception(e.errnov);
}
