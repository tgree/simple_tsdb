// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_DATABASE_H
#define __SRC_LIBTSDB_DATABASE_H

#include <futil/futil.h>

namespace tsdb
{
    struct database
    {
        futil::directory    dir;

        // Lists all the measurements in the database.
        std::vector<std::string> list_measurements() const
        {
            return dir.listdirs();
        }

        database(const futil::path& path);
    };

    // Creates a new database in the TSDB instance rooted at the current working
    // directory.
    void create_database(const char* name);

    // Returns a list of all databases.
    std::vector<std::string> list_databases();
}

#endif /* __SRC_LIBTSDB_DATABASE_H */
