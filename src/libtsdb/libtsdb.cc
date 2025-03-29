// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "tsdb.h"
#include <futil/xact.h>

void
tsdb::init() try
{
    futil::xact_mkdir databases_dir("databases",0770);
    databases_dir.commit();
}
catch (const futil::errno_exception& e)
{
    throw tsdb::init_io_error_exception(e.errnov);
}
