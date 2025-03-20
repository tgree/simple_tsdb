// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_TSDB_H
#define __SRC_LIBTSDB_TSDB_H

#include "exception.h"
#include "database.h"
#include "measurement.h"
#include "series.h"
#include <futil/futil.h>
#include <hdr/fixed_vector.h>
#include <hdr/kmath.h>
#include <vector>
#include <span>

// Constants defining how the database is stored in disk.
#define CHUNK_FILE_SIZE     (2*1024*1024)
#define CHUNK_NPOINTS       (CHUNK_FILE_SIZE/8)
#define BITMAP_FILE_SIZE    (CHUNK_NPOINTS/8)

namespace tsdb
{
    // Class used to get data from a series.
    struct select_op
    {
        // Snapshot of the series when the query was initiated.  We keep
        // time_first_fd open with a shared lock to prevent anyone else from
        // deleting points during the query.  For time_last, we don't care if
        // someone adds points later, we only return the points from our
        // current snapshot of the live range.
        const series_read_lock& read_lock;
        const uint64_t          time_last;
        futil::file             index_fd;
        futil::mapping          index_mapping;
        const index_entry*      index_begin;
        const index_entry*      index_end;

        // Query parameters.
        uint64_t                    t0;
        uint64_t                    t1;
        uint64_t                    rem_limit;
        fixed_vector<schema_entry>  fields;

        // Mapping objects to track mmap()-ed files.
        const index_entry*              index_slot;
        futil::mapping                  timestamp_mapping;
        fixed_vector<futil::mapping>    field_mappings;
        fixed_vector<futil::mapping>    bitmap_mappings;

        // State of the current set of results.
        bool                            is_last;
        size_t                          npoints;
        size_t                          bitmap_offset;
        const uint64_t*                 timestamp_data;
        fixed_vector<const void*>       field_data;

        inline void advance()
        {
            _advance(false);
        }

        constexpr size_t compute_chunk_len() const
        {
            size_t N = npoints;
            size_t M = fields.size();
            size_t S = 0;
            for (const auto& f : fields)
            {
                const auto* fti = &ftinfos[f.type];
                S += round_up_pow2(N*fti->nbytes,8);
            }
            size_t bitmap_begin = bitmap_offset / 64;
            size_t bitmap_end = ceil_div<size_t>(bitmap_offset + N,64);
            size_t bitmap_n = bitmap_end - bitmap_begin;
            return 8*(N + bitmap_n*M) + S;
        }

        constexpr bool get_bitmap_bit(size_t field_index, size_t i) const
        {
            return tsdb::get_bitmap_bit(
                (const uint64_t*)bitmap_mappings[field_index].addr,
                bitmap_offset + i);
        }

        constexpr bool is_field_null(size_t field_index, size_t i) const
        {
            return !get_bitmap_bit(field_index,i);
        }

        template<typename T, size_t FieldIndex>
        T get_field(size_t i) const
        {
            return ((const T*)field_data[FieldIndex])[i];
        }

    protected:
        void _advance(bool is_first);

        select_op(const series_read_lock& read_lock,
                  const std::vector<std::string>& field_names,
                  uint64_t t0, uint64_t t1, uint64_t limit);
    };

    struct select_op_first : public select_op
    {
        select_op_first(const series_read_lock& read_lock,
                        const futil::path& series_id,
                        const std::vector<std::string>& field_names,
                        uint64_t t0, uint64_t t1, uint64_t limit);
    };

    struct select_op_last : public select_op
    {
        select_op_last(const series_read_lock& read_lock,
                       const futil::path& series_id,
                       const std::vector<std::string>& field_names,
                       uint64_t t0, uint64_t t1, uint64_t limit);
    };

    // Counts the number of points between t0 and t1, inclusive.
    size_t count_points(const series_read_lock& read_lock, uint64_t t0,
                        uint64_t t1);

    // Deletes points from the series, up to and including timestamp t.
    void delete_points(const measurement& m, const futil::path& series,
                       uint64_t t);

    // Writes data points to the specified series.  If the series does not
    // exist (but the measurement does exist) then the series will be created.
    // The path argument should have the form:
    //
    //      database_name/measurement_name/series_name
    //
    // The series name should be a tag that uniquely identifies the data source,
    // such as a sensor's serial number.
    //
    // Data points can only be written past the end of a series; however, you
    // can write the tail points of the series multiple times (tsdb will verify
    // that the data from the later writes matches the data already stored in
    // the series) to allow for sane failure recovery.  In the event of a
    // mismatch, the entire write operation will be rejected.
    //
    // The data to be written is in a flat format.  Data is chunked by field,
    // in the order specified in the schema file for this measurement.  First,
    // an array of 64-bit timestamps is present.  The data for each field
    // follow and begins with a bitmap to specify non-NULL (1) or NULL (0)
    // entries, and then the raw point data for the field follows.  This is
    // repeated for each field in the schema.  The bitmap and raw point data
    // will always be 8-byte aligned, padded with 0 bytes to maintain that
    // alignment as necessary.
    //
    // Example writing 9 bytes to an xtalx_data series:
    //
    //      9 u64 timestamps
    //      1 u64 bitmap for pressure_psi (only 9 bits used)
    //      9 f64 pressure_psi values
    //      1 u64 bitmap for temp_c (only 9 bits used)
    //      9 f32 temp_c values
    //      1 32-bit padding
    //      1 u64 bitmap for pressure_hz (only 9 bits used)
    //      9 f64 pressure_hz values
    //      1 u64 bitmap for temp_hz (only 9 bits used)
    //      9 f64 temp_hz values
    //
    //      Total: 360 bytes
    void write_series(series_write_lock& write_lock, size_t npoints,
                      size_t bitmap_offset, size_t data_len, const void* data);

    // Creates a new TSDB instance rooted at the current working directory.
    void init();
}

#endif /* __SRC_LIBTSDB_TSDB_H */
