// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_TSDB_H
#define __SRC_LIBTSDB_TSDB_H

#include "exception.h"
#include "database.h"
#include "measurement.h"
#include "series.h"
#include "select_op.h"
#include "count.h"
#include "write.h"
#include "delete.h"
#include "sum_op.h"
#include "wal.h"

// Whether or not to enable compression.
#define ENABLE_COMPRESSION  1

// Constants defining how the database is stored in disk.
#define CHUNK_FILE_SIZE     (32*1024*1024)
#define CHUNK_NPOINTS       (CHUNK_FILE_SIZE/8)
#define BITMAP_FILE_SIZE    (CHUNK_NPOINTS/8)

namespace tsdb
{
    // Adds a new user to the passwd file.
    void add_user(const std::string& username, const std::string& password);

    // Verifies a username and password against the passwd file.
    bool verify_user(const std::string& username, const std::string& password);

    // Creates a new TSDB instance rooted at the current working directory.
    void init();
}

#endif /* __SRC_LIBTSDB_TSDB_H */
