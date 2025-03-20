// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_TSDB_H
#define __SRC_LIBTSDB_TSDB_H

#include "exception.h"
#include "database.h"
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
    enum field_type : uint8_t
    {
        FT_BOOL = 1,
        FT_U32  = 2,
        FT_U64  = 3,
        FT_F32  = 4,
        FT_F64  = 5,
        FT_I32  = 6,
        FT_I64  = 7,
    };
#define LAST_FIELD_TYPE     7

    struct field_type_info
    {
        field_type  type;
        uint8_t     nbytes;
        char        name[5];
    };

    constexpr const field_type_info ftinfos[] =
    {
        [0]             = {(field_type)0,0,""},
        [tsdb::FT_BOOL] = {tsdb::FT_BOOL,1,"bool"},
        [tsdb::FT_U32]  = {tsdb::FT_U32,4,"u32"},
        [tsdb::FT_U64]  = {tsdb::FT_U64,8,"u64"},
        [tsdb::FT_F32]  = {tsdb::FT_F32,4,"f32"},
        [tsdb::FT_F64]  = {tsdb::FT_F64,8,"f64"},
        [tsdb::FT_I32]  = {tsdb::FT_I32,4,"i32"},
        [tsdb::FT_I64]  = {tsdb::FT_I64,8,"i64"},
    };

    struct schema_entry
    {
        field_type  type;
        uint8_t     rsrv[3];
        char        name[124];
    };
    KASSERT(sizeof(schema_entry) == 128);

    struct measurement
    {
        futil::directory                dir;
        futil::file                     schema_fd;
        futil::mapping                  schema_mapping;
        std::span<const schema_entry>   fields;

        std::vector<std::string> list_series() const
        {
            return dir.listdirs();
        }

        measurement(const database& db, const futil::path& path);
    };

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

    // Creates a new measurement in the specified database.
    void create_measurement(const database& db, const futil::path& name,
                            const std::vector<schema_entry>& fields);

    // Creates a new TSDB instance rooted at the current working directory.
    void init();
}

#endif /* __SRC_LIBTSDB_TSDB_H */
