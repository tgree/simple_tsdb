// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBROOT_ROOT_H
#define __SRC_LIBROOT_ROOT_H

#include <futil/futil.h>

namespace tsdb
{
    // Root object representing a ROOT instance.  A root object has multiple
    // sub-databases, a single set of users and a single configuration file.
    // A temporary directory also exists to hold files and sub-directories that
    // we are building up for atomic filesystem swap operations.
    struct root
    {
        // Various directories.
        futil::directory    root_dir;
        futil::directory    tmp_dir;
        futil::directory    databases_dir;

        // Adds a new user to the passwd file.
        void add_user(const std::string& username,
                      const std::string& password);

        // Verifies a username and password against the passwd file.
        bool verify_user(const std::string& username,
                         const std::string& password);

        // Creates a new database.
        void create_database(const char* name);

        // Returns a list of all databases.
        std::vector<std::string> list_databases();

        // Root at the specified path.
        root(const futil::path& root_path);

        // Root in the current working directory.
        root();
    };

    // Creates a new ROOT root in the specified directory.
    void create_root(const futil::path& path);
}

#endif /* __SRC_LIBROOT_ROOT_H */
