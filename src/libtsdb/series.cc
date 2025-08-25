// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "series.h"
#include "database.h"
#include <futil/xact.h>

static tsdb::series_write_lock
open_and_lock_series(const tsdb::measurement& m, const futil::path& series)
{
    futil::directory series_dir(m.dir,series);
    futil::file time_first_fd(series_dir,"time_first",O_RDWR);
    time_first_fd.flock(LOCK_SH);
    futil::file time_last_fd(series_dir,"time_last",O_RDWR);
    time_last_fd.flock(LOCK_EX);
    futil::file wal_fd(series_dir,"wal",O_RDWR);
    return tsdb::series_write_lock(m,series,std::move(time_first_fd),
                                   std::move(wal_fd),std::move(time_last_fd));
}

tsdb::series_write_lock
tsdb::open_or_create_and_lock_series(const measurement& m,
    const futil::path& series)
{
    // Try to acquire a write lock on the series.
    try
    {
        return open_and_lock_series(m,series);
    }
    catch (const futil::errno_exception& e)
    {
        if (e.errnov != ENOENT)
            throw e;
    }

    // If the time_last file doesn't exist, we need to create the series.
    futil::xact_mkdtemp series_dir(m.db.root.tmp_dir,0770);
    futil::xact_mkdir time_ns_dir(series_dir,"time_ns",0770);
    futil::xact_mkdir fields_dir(series_dir,"fields",0770);
    futil::xact_mkdir bitmaps_dir(series_dir,"bitmaps",0770);

    // Create all of the field and bitmap sub-directories.
    field_vector<futil::xact_mkdir> field_and_bitmap_dirs;
    for (const auto& f : m.fields)
    {
        field_and_bitmap_dirs.emplace_back(fields_dir,f.name,0770);
        field_and_bitmap_dirs.emplace_back(bitmaps_dir,f.name,0770);
    }

    // Create empty index and write-ahead log files.
    futil::xact_creat index_fd(series_dir,"index",O_CREAT | O_EXCL | O_RDWR,
                               0660);
    futil::xact_creat wal_fd(series_dir,"wal",O_CREAT | O_EXCL | O_RDWR,0660);

    // Create the time_first file and populate it with the value 1.
    futil::xact_creat time_first_fd(series_dir,"time_first",
                                    O_CREAT | O_EXCL | O_RDWR,0660);
    uint64_t one = 1;
    time_first_fd.write_all(&one,sizeof(uint64_t));
    time_first_fd.lseek(0,SEEK_SET);
    time_first_fd.flock(LOCK_SH);

    // Create the time_last file, populate it with 0 and acquire an exclusive
    // lock on it.
    futil::xact_creat time_last_fd(series_dir,"time_last",
                                   O_CREAT | O_EXCL | O_RDWR,0660);
    uint64_t zero = 0;
    time_last_fd.write_all(&zero,sizeof(uint64_t));
    time_last_fd.lseek(0,SEEK_SET);
    time_last_fd.flock(LOCK_EX);

    // Sync everything.
    fields_dir.fsync();
    bitmaps_dir.fsync();
    time_first_fd.fsync();
    index_fd.fsync();
    wal_fd.fsync();
    time_last_fd.fsync();
    series_dir.fsync();

    // Try and move it in to place.  If we fail, this indicates that someone
    // else beat us to the punch and created the series for us.
    if (!futil::rename_if_not_exists(m.db.root.tmp_dir,
                                     (const char*)series_dir.name,
                                     m.dir,series))
    {
        return open_and_lock_series(m,series);
    }

    // Commit all the changes so we don't delete everything we just made.
    time_last_fd.commit();
    wal_fd.commit();
    index_fd.commit();
    time_first_fd.commit();
    for (auto& dir : field_and_bitmap_dirs)
        dir.commit();
    bitmaps_dir.commit();
    fields_dir.commit();
    time_ns_dir.commit();
    series_dir.commit();

    // Sync the new directory and return the locked series.
    m.dir.fsync_and_flush();
    m.db.root.tmp_dir.fsync_and_flush();
    return series_write_lock(m,series,std::move(time_first_fd),
                             std::move(wal_fd),std::move(time_last_fd));
}
