// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_TSDB_H
#define __SRC_LIBTSDB_TSDB_H

#include <futil/futil.h>
#include <hdr/fixed_vector.h>
#include <vector>

namespace tsdb
{
    enum field_type
    {
        FT_BOOL = 0,
        FT_U32  = 1,
        FT_U64  = 2,
        FT_F32  = 3,
        FT_F64  = 4,
    };

    struct field
    {
        std::string name;
        field_type  type;
    };

    struct field_type_info
    {
        field_type  type;
        uint8_t     nbytes;
        char        ascii_type;
        char        name[5];
    };

    constexpr const field_type_info ftinfos[] =
    {
        [tsdb::FT_BOOL] = {tsdb::FT_BOOL,1,'0',"bool"},
        [tsdb::FT_U32]  = {tsdb::FT_U32,4,'1',"u32"},
        [tsdb::FT_U64]  = {tsdb::FT_U64,8,'2',"u64"},
        [tsdb::FT_F32]  = {tsdb::FT_F32,4,'3',"f32"},
        [tsdb::FT_F64]  = {tsdb::FT_F64,8,'4',"f64"},
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

    // Class used to get data from a series.
    struct select_op
    {
        // Snapshot of the series when the query was initiated.  We keep
        // time_first_fd open with a shared lock to prevent anyone else from
        // deleting points during the query.  For time_last, we don't care if
        // someone adds points later, we only return the points from our
        // current snapshot of the live range.
        futil::file         time_first_fd;
        const uint64_t      time_first;
        const uint64_t      time_last;
        futil::file         index_fd;
        futil::mapping      index_mapping;
        const index_entry*  index_begin;
        const index_entry*  index_end;

        // Query parameters.
        const futil::path   series;
        uint64_t            t0;
        uint64_t            t1;
        uint64_t            rem_limit;
        fixed_vector<field> fields;

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

        select_op(const futil::path& series,
                  const std::vector<std::string>& field_names,
                  uint64_t t0, uint64_t t1, uint64_t limit);
    };

    struct select_op_first : public select_op
    {
        select_op_first(const futil::path& series,
                        const std::vector<std::string>& field_names,
                        uint64_t t0, uint64_t t1, uint64_t limit);
    };

    struct select_op_last : public select_op
    {
        select_op_last(const futil::path& series,
                       const std::vector<std::string>& field_names,
                       uint64_t t0, uint64_t t1, uint64_t limit);
    };

    // Given a series name of the form:
    //
    //      database_name/measurement_name/series_name
    //
    // extract the measurement:
    //
    //      database_name/measurement_name
    futil::path series_to_measurement(const futil::path& series);

    // Parses a schema file into a vector of fields.
    void parse_schema(const futil::path& schema_path,
                      std::vector<field>& fields);
    void parse_schema_for_series(const futil::path& series,
                                 std::vector<field>& fields);

    // Returns the timestamp stored in the time_last file.
    uint64_t get_time_last(const futil::path& series);

    // Deletes points from the series, up to and including timestamp t.
    void delete_points(const futil::path& series, uint64_t t);

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
    void write_series(const futil::path& path, size_t npoints, size_t data_len,
                      const void* data);

    // Creates a new measurement in the TSDB instance rooted at the current
    // working directory.  The path argument should have the form:
    //
    //      database_name/measurement_name
    //
    // and it creates a new measurement called "measurement_name" in the
    // database "database_name".
    void create_measurement(const futil::path& path,
                            const std::vector<field>& fields);

    // Creates a new database in the TSDB instance rooted at the current working
    // directory.
    void create_database(const char* name);

    // Creates a new TSDB instance rooted at the current working directory.
    void init();
}

#endif /* __SRC_LIBTSDB_TSDB_H */
