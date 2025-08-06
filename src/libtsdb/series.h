// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_SERIES_H
#define __SRC_LIBTSDB_SERIES_H

#include "exception.h"
#include "measurement.h"

namespace tsdb
{
#define TIMESTAMP_FILE_NAME_LEN 24
    struct index_entry
    {
        uint64_t    time_ns;
        char        timestamp_file[TIMESTAMP_FILE_NAME_LEN];
    };
    inline bool operator<(const index_entry& lhs, const index_entry& rhs)
    {
        return lhs.time_ns < rhs.time_ns;
    }
    inline bool operator<(uint64_t time_ns, const index_entry& rhs)
    {
        return time_ns < rhs.time_ns;
    }
    inline bool operator<(const index_entry& lhs, uint64_t time_ns)
    {
        return lhs.time_ns < time_ns;
    }

    constexpr uint64_t get_bitmap_bit(const uint64_t* bitmap, size_t index)
    {
        return (bitmap[index / 64] >> (index % 64)) & 1;
    }
    constexpr void set_bitmap_bit(uint64_t* bitmap, size_t index, bool v)
    {
        if (v)
            bitmap[index / 64] |= (1ULL << (index % 64));
        else
            bitmap[index / 64] &= ~(1ULL << (index % 64));
    }

    struct _series_lock
    {
        const measurement&  m;
        futil::directory    series_dir;
        futil::file         time_first_fd;
        uint64_t            time_first;

        _series_lock(const measurement& m, const futil::path& series,
                     int oflag) try:
            m(m),
            series_dir(m.dir,series),
            time_first_fd(series_dir,"time_first",oflag),
            time_first(time_first_fd.flock(LOCK_SH).read_u64())
        {
        }
        catch (const futil::errno_exception& e)
        {
            if (e.errnov == ENOENT)
                throw tsdb::no_such_series_exception();
            throw;
        }

        _series_lock(const measurement& m, const futil::path& series,
                     futil::file&& _time_first_fd) try:
            m(m),
            series_dir(m.dir,series),
            time_first_fd(std::move(_time_first_fd)),
            time_first(time_first_fd.read_u64())
        {
        }
        catch (const futil::errno_exception& e)
        {
            if (e.errnov == ENOENT)
                throw tsdb::no_such_series_exception();
            throw;
        }
    };

    // Obtains a read lock on a series.
    // This acquires a shared lock on time_first so that no delete operation
    // can happen while we are reading and a shared lock on time_last so that
    // no commit/truncate WAL operation can happen while we may be accessing it.
    // This means that we can have many readers on a series (concurrent queries)
    // but that any reader will be mutally exclusive with a writer, and a long
    // write operation will block all readers.
    //
    // Order of operations:
    //  1. Acquire shared lock on time_first_fd [RD].
    //  2. Acquire shared lock on time_last_fd [RD].
    struct series_read_lock : public _series_lock
    {
        futil::file time_last_fd;
        uint64_t    time_last;

        series_read_lock(const measurement& m, const futil::path& series):
            _series_lock(m,series,O_RDONLY),
            time_last_fd(series_dir,"time_last",O_RDONLY),
            time_last(time_last_fd.flock(LOCK_SH).read_u64())
        {
        }

    protected:
        // Passing up an already-opened time_last_fd that has an exclusive
        // lock - this is so that write_lock can also be a read_lock.
        series_read_lock(const measurement& m, const futil::path& series,
                         futil::file&& _time_first_fd,
                         futil::file&& _time_last_fd):
            _series_lock(m,series,std::move(_time_first_fd)),
            time_last_fd(std::move(_time_last_fd)),
            time_last(time_last_fd.read_u64())
        {
        }
    };

    // Obtains a write lock on a series.
    // This acquires a shared lock on time_first so that no delete operation
    // can happen while we are writing, and it acquires an exclusive lock on
    // time_last so that nobody else can write new data points while we are
    // (not that that should ever happen).
    //
    // A write lock can be substituted in calls where only a read lock is
    // needed; this allows for instance a select operation to take place inside
    // a write method to handle overwrite.
    //
    // Order of operations:
    //  1. Acquire shared lock on time_first_fd [RDWR].
    //  2. Acquire exclusive lock on time_last_fd [RDWR].
    struct series_write_lock : public series_read_lock
    {
        // Transfer ownership of an exclusive lock on time_last to us.
        series_write_lock(const measurement& m, const futil::path& series,
                          futil::file&& _time_first_fd,
                          futil::file&& _time_last_fd):
            series_read_lock(m,series,std::move(_time_first_fd),
                             std::move(_time_last_fd))
        {
        }
    };

    // If the series exists, obtains a write lock on it.  If the series doesn't
    // exist, atomically create it and return the write lock on it.
    series_write_lock open_or_create_and_lock_series(const measurement& m,
                                                     const futil::path& series);
}

#endif /* __SRC_LIBTSDB_SERIES_H */
