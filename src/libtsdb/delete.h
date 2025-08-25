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
    void delete_points(tsdb::series_total_lock& stl, uint64_t t);
}

#endif /* __SRC_LIBTSDB_DELETE_H */
