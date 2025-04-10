// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_CONSTANTS_H
#define __SRC_LIBTSDB_CONSTANTS_H

#include <hdr/kmath.h>

// Constants defining how the database is stored in disk.
#define DEFAULT_CHUNK_SIZE_M        2
KASSERT(is_pow2(DEFAULT_CHUNK_SIZE_M));

// Maximum number of entries in the WAL.
#define DEFAULT_WAL_MAX_ENTRIES     128

// How long for tsdbserver to delay between write requests on the same
// connection.  This reduces wear on the SSD by delaying acknowledgements back
// to the source, forcing the source to buffer data.
#define DEFAULT_WRITE_THROTTLE_NS   0

// Maximum number of fields in a measurement.  This is partly defined by things
// like the size of the index field in a schema_entry, so increasing this past
// 64 here is insufficient; reducing it below 64 should be fine.
#define MAX_FIELDS                  64

#endif /* __SRC_LIBTSDB_CONSTANTS_H */
