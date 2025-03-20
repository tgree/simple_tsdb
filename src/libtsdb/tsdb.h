// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_TSDB_H
#define __SRC_LIBTSDB_TSDB_H

#include "exception.h"
#include "database.h"
#include "measurement.h"
#include "series.h"
#include "select_op.h"

// Constants defining how the database is stored in disk.
#define CHUNK_FILE_SIZE     (2*1024*1024)
#define CHUNK_NPOINTS       (CHUNK_FILE_SIZE/8)
#define BITMAP_FILE_SIZE    (CHUNK_NPOINTS/8)

namespace tsdb
{
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
