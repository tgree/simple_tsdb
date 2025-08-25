// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __LIBTSDB_UNITTEST_FAKE_DB_H
#define __LIBTSDB_UNITTEST_FAKE_DB_H

#include "../series.h"

struct data_point
{
    uint32_t    field1;
    double      field2;
    uint32_t    field3;
    bool        is_non_null[3];
};

extern const std::array<data_point,1024> dps;

void write_points(tsdb::series_write_lock& write_lock, size_t npoints,
                  uint64_t t0, uint64_t dt, size_t offset);
void init_db(size_t wal_max_entries, size_t chunk_size = 1024);
size_t populate_db(int64_t t0, uint64_t dt, const std::vector<size_t>& nvec);
size_t validate_points(uint64_t t0, uint64_t dt);

#endif /* __LIBTSDB_UNITTEST_FAKE_DB_H */
