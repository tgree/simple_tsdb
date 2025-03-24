// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_DELETE_H
#define __SRC_LIBTSDB_DELETE_H

#include <futil/futil.h>

namespace tsdb
{
    struct measurement;

    // Deletes points from the series, up to and including timestamp t.
    void delete_points(const measurement& m, const futil::path& series,
                       uint64_t t);
}

#endif /* __SRC_LIBTSDB_DELETE_H */
