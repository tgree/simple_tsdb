// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_CONSTANTS_H
#define __SRC_LIBTSDB_CONSTANTS_H

// Constants defining how the database is stored in disk.
#define CHUNK_FILE_SIZE     (32*1024*1024)
#define CHUNK_NPOINTS       (CHUNK_FILE_SIZE/8)
#define BITMAP_FILE_SIZE    (CHUNK_NPOINTS/8)

// Maximum number of fields in a measurement.  This is partly defined by things
// like the size of the index field in a schema_entry, so increasing this past
// 64 here is insufficient; reducing it below 64 should be fine.
#define MAX_FIELDS          64

// Maximum number of entries in the WAL.
#define WAL_MAX_ENTRIES     128

#endif /* __SRC_LIBTSDB_CONSTANTS_H */
