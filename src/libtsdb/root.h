// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBROOT_ROOT_H
#define __SRC_LIBROOT_ROOT_H

#include <futil/futil.h>
#include <strutil/strutil.h>
#include <hdr/kassert.h>

namespace tsdb
{
    // Configuration as loaded from the config file.
    struct configuration
    {
        size_t      chunk_size;
        size_t      wal_max_entries;
        size_t      write_throttle_ns;
    };
    static inline std::string to_string(const configuration& c)
    {
        std::string s;
        s += "chunk_size        ";
        s += str::encode_number_units_pow2(c.chunk_size) + "\n";
        s += "wal_max_entries   ";
        s += std::to_string(c.wal_max_entries) + "\n";
        s += "write_throttle_ns ";
        s += std::to_string(c.write_throttle_ns) + "\n";
        return s;
    }

    extern const configuration default_configuration;

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

        // Whether or not to enable debug printfs.
        bool                debug_enabled;

        // Configuration.
        configuration       config;
        const size_t        max_gzipped_size;

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

        // Returns true if a given databases exists.
        bool database_exists(const futil::path& path);

        // Atomically replaces some file with a truncated version of a source
        // file (possibly the same file).  This allows the source file to
        // remain open in other processes without losing data.  The source file
        // descriptor is swapped with the new one, so on exit src_fd refers to
        // the replacement file.  The file position will be at the end of the
        // truncated file.  All files and directories are fsync'd.
        void replace_with_truncated(futil::file& src_fd,
                                    const futil::directory& dst_dir,
                                    const futil::path& dst_path,
                                    size_t new_len) const;

        // Prints a debug message.
        int debugf(const char* fmt, ...) const __PRINTF__(2,3);
        int vdebugf(const char* fmt, va_list ap) const;

        // Root at the specified path.
        root(const futil::path& root_path, bool debug_enabled);

        // Root in the current working directory.
        explicit root(bool debug_enabled);
    };

    // Creates a new ROOT root in the specified directory.
    void create_root(const futil::path& path, const configuration& config);
}

#endif /* __SRC_LIBROOT_ROOT_H */
