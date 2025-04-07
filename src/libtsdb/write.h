// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_WRITE_H
#define __SRC_LIBTSDB_WRITE_H

#include "series.h"

namespace tsdb
{
    struct series_write_lock;
    struct measurement;

    struct write_field_info
    {
        const uint64_t* bitmap_ptr;
        const char*     data_ptr;
    };

    // An index for finding information inside a block of write data.
    struct write_chunk_index
    {
        size_t                          npoints;
        size_t                          bitmap_offset;
        const uint64_t*                 timestamps;
        std::vector<write_field_info>   fields;

        constexpr bool get_bitmap_bit(size_t field_index, size_t i) const
        {
            return tsdb::get_bitmap_bit(fields[field_index].bitmap_ptr,
                                        bitmap_offset + i);
        }

        write_chunk_index(const measurement& m, size_t npoints,
                          size_t bitmap_offset, size_t data_len,
                          const void* data);
    };

    // Writes data points to the specified series.  This should not be called
    // directly and is instead invoked from committing the write-ahead log.
    void write_series(series_write_lock& write_lock, size_t npoints,
                      size_t bitmap_offset, size_t data_len, const void* data);
}

#endif /* __SRC_LIBTSDB_WRITE_H */
