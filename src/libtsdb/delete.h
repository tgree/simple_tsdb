// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_DELETE_H
#define __SRC_LIBTSDB_DELETE_H

#include "series.h"
#include <futil/futil.h>

namespace tsdb
{
    struct measurement;

    // Deletes points from the series, up to and including timestamp t.
    // Order of operations:
    //  1. Acquires exclusive lock on time_first_fd.
    //
    // Danger!  We must not attempt to lock time_last_fd at all after acquiring
    // the time_first_fd exclusive lock, otherwise we will deadlock with write
    // locking.
    void delete_points(const tsdb::series_total_lock& stl, uint64_t t);
    void delete_points(const measurement& m, const futil::path& series,
                       uint64_t t);
}

#endif /* __SRC_LIBTSDB_DELETE_H */
