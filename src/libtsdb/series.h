// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_SERIES_H
#define __SRC_LIBTSDB_SERIES_H

#include "exception.h"
#include "measurement.h"

namespace tsdb
{
    struct index_entry
    {
        uint64_t    time_ns;
        char        timestamp_file[24];
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
            bitmap[index / 64] |= (((uint64_t)v) << (index % 64));
        else
            bitmap[index / 64] &= ~(((uint64_t)v) << (index % 64));
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
    struct series_read_lock : public _series_lock
    {
        series_read_lock(const measurement& m, const futil::path& series):
            _series_lock(m,series,O_RDONLY | O_SHLOCK)
        {
        }

    protected:
        series_read_lock(const measurement& m, const futil::path& series,
                         int oflag):
            _series_lock(m,series,oflag)
        {
        }
    };

    // Obtains a write lock on a series.
    struct series_write_lock : public series_read_lock
    {
        futil::file time_last_fd;
        uint64_t    time_last;

        // Transfer ownership of an O_EXLOCK exclusive lock fd on time_last to
        // us.
        series_write_lock(const measurement& m, const futil::path& series,
                          futil::file&& _time_last_fd):
            series_read_lock(m,series,O_RDWR | O_SHLOCK),
            time_last_fd(std::move(_time_last_fd)),
            time_last(time_last_fd.read_u64())
        {
        }
    };

    // If the series exists, obtains a write lock on it.  If the series doesn't
    // exist, atomically create it and return the write lock on it.
    series_write_lock open_or_create_and_lock_series(const measurement& m,
                                                     const futil::path& series);
}

#endif /* __SRC_LIBTSDB_SERIES_H */
