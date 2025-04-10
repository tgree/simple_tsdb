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
        uint64_t* bitmap_ptr;
        char*     data_ptr;
    };

    // An index for working with data chunks used in write operations.
    struct write_chunk_index
    {
        size_t                          npoints;
        size_t                          bitmap_offset;
        uint64_t*                       timestamps;
        field_vector<write_field_info>  fields;

        constexpr uint64_t get_bitmap_bit(size_t field_index, size_t i) const
        {
            kassert(field_index < fields.size());
            kassert(i < npoints);
            return tsdb::get_bitmap_bit(fields[field_index].bitmap_ptr,
                                        bitmap_offset + i);
        }

        void set_bitmap_bit(size_t field_index, size_t i, bool v)
        {
            kassert(field_index < fields.size());
            kassert(i < npoints);
            tsdb::set_bitmap_bit(fields[field_index].bitmap_ptr,
                                 bitmap_offset + i,v);
        }

        write_chunk_index(const measurement& m, size_t npoints,
                          size_t bitmap_offset, size_t data_len,
                          void* data);
        write_chunk_index(const measurement& m, size_t npoints,
                          size_t bitmap_offset, size_t data_len,
                          const void* data):
            write_chunk_index(m,npoints,bitmap_offset,data_len,
                              (void*)data)
        {
        }
    };

    // Writes data points to the specified series.  This should not be called
    // directly and is instead invoked from committing the write-ahead log.
    // The wci argument is consumed in this operation.
    void write_series(series_write_lock& write_lock,
                      write_chunk_index& wci);
}

#endif /* __SRC_LIBTSDB_WRITE_H */
