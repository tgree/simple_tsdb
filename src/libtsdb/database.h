// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_DATABASE_H
#define __SRC_LIBTSDB_DATABASE_H

#include "exception.h"
#include "root.h"

namespace tsdb
{
    struct database
    {
        const struct root&  root;
        futil::directory    dir;

        // Lists all the measurements in the database.
        std::vector<std::string> list_measurements() const
        {
            auto v = dir.listdirs();
            std::sort(v.begin(),v.end());
            return v;
        }

        database(const struct root& root, const futil::path& path) try :
            root(root),
            dir(root.databases_dir,path)
        {
        }
        catch (const futil::errno_exception& e)
        {
            if (e.errnov == ENOENT)
                throw tsdb::no_such_database_exception();
            throw;
        }
    };
}

#endif /* __SRC_LIBTSDB_DATABASE_H */
