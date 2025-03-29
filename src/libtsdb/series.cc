// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "series.h"

tsdb::series_write_lock
tsdb::open_or_create_and_lock_series(const measurement& m,
    const futil::path& series) try
{
    // Try to acquire a write lock on the series.
    futil::file time_last_fd;
    try
    {
        futil::directory series_dir(m.dir,series);
        time_last_fd.open_if_exists(series_dir,"time_last",O_RDWR | O_EXLOCK);
    }
    catch (const futil::errno_exception& e)
    {
        if (e.errnov != ENOENT)
            throw e;
    }

    // If the time_last file doesn't exist, we need to create the series.
    if (time_last_fd.fd == -1)
    {
        // Acquire the lock to create the series.
        futil::file create_series_lock_fd(m.dir,"create_series_lock",
                                          O_WRONLY | O_EXLOCK);

        // Create and open the series directory.
        futil::mkdir_if_not_exists(m.dir,series,0770);
        futil::directory series_dir(m.dir,series);

        // Someone else may have created it in the meantime.
        time_last_fd.open_if_exists(series_dir,"time_last",O_RDWR | O_EXLOCK);
        if (time_last_fd.fd == -1)
        {
            // Create the time sub-directory.
            futil::mkdir_if_not_exists(series_dir,"time_ns",0770);

            // Create the fields sub-directory.
            futil::mkdir_if_not_exists(series_dir,"fields",0770);
            futil::directory fields_dir(series_dir,"fields");

            // Create the bitmaps sub-directory.
            futil::mkdir_if_not_exists(series_dir,"bitmaps",0770);
            futil::directory bitmaps_dir(series_dir,"bitmaps");

            // Create all of the field and bitmap sub-directories.
            for (const auto& f : m.fields)
            {
                futil::mkdir_if_not_exists(fields_dir,f.name,0770);
                futil::mkdir_if_not_exists(bitmaps_dir,f.name,0770);
            }

            // Create the time_first file and populate it with the value 1.
            futil::file time_first_fd(series_dir,"time_first",
                                      O_CREAT | O_TRUNC | O_WRONLY | O_EXLOCK,
                                      0660);
            uint64_t one = 1;
            time_first_fd.write_all(&one,sizeof(uint64_t));
            time_first_fd.fsync();
            time_first_fd.close();

            // Create an empty index file.  No lock is needed since you can't
            // delete anything from an empty file!
            futil::file index_fd(series_dir,"index",O_CREAT | O_TRUNC | O_RDWR,
                                 0660);
            index_fd.fsync();

            // Write barrier so that time_last_fd is the last thing to go out.
            // TODO: fsync() series_dir.
            index_fd.fsync_and_barrier();

            // Create the time_last file and populate it with 0.
            time_last_fd.open(series_dir,"time_last",
                              O_RDWR | O_EXCL | O_CREAT | O_EXLOCK,0660);
            uint64_t zero = 0;
            time_last_fd.write_all(&zero,sizeof(uint64_t));
            time_last_fd.lseek(0,SEEK_SET);
            // TODO: fsync() series_dir.
        }
    }

    return series_write_lock(m,series,std::move(time_last_fd));
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == ENOENT)
        throw tsdb::no_such_measurement_exception();
    throw;
}
