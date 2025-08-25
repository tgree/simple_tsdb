// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "fake_db.h"
#include "../write.h"
#include "../wal.h"
#include "../database.h"
#include "../count.h"
#include "../select_op.h"
#include <futil/fakefs/fakefs.h>
#include <zutil/zutil.h>
#include <hdr/auto_buf.h>
#include <hdr/types.h>
#include <hdr/prng.h>
#include <tmock/tmock.h>

const std::vector<tsdb::schema_entry> test_fields =
{
    {tsdb::FT_U32,SCHEMA_VERSION,0, 0,"field1"},
    {tsdb::FT_F64,SCHEMA_VERSION,1, 4,"field2"},
    {tsdb::FT_F32,SCHEMA_VERSION,2,12,"field3"},
};

static constexpr std::array<data_point,1024>
gen_random_points()
{
    std::array<data_point,1024> arr;
    prng::minstd r;
    for (size_t i=0; i<arr.size(); ++i)
    {
        arr[i].field1 = r.next();
        arr[i].field2 = (double)r.next() / r._Range;
        arr[i].field3 = (float)r.next() / r._Range;
        arr[i].is_non_null[0] = (r.next() > (r._Range / 4));
        arr[i].is_non_null[1] = (r.next() > (r._Range / 4));
        arr[i].is_non_null[2] = (r.next() > (r._Range / 4));
    }
    return arr;
}

constexpr const std::array<data_point,1024> dps = gen_random_points();

void
write_points(tsdb::series_write_lock& write_lock, size_t npoints, uint64_t t0,
    uint64_t dt, size_t offset)
{
    TASSERT(offset + npoints <= dps.size());
    size_t chunk_len = write_lock.m.compute_write_chunk_len(npoints);
    auto_buf chunk_data(chunk_len);
    tsdb::write_chunk_index wci(write_lock.m,npoints,0,chunk_len,
                                chunk_data.data);
    uint64_t t = t0;
    auto* dp = &dps[offset];
    for (size_t i=0; i<npoints; ++i)
    {
        wci.timestamps[i] = t;
        wci.set_bitmap_bit(0,i,dp[i].is_non_null[0]);
        wci.set_bitmap_bit(1,i,dp[i].is_non_null[1]);
        wci.set_bitmap_bit(2,i,dp[i].is_non_null[2]);
        ((uint32_t*)wci.fields[0].data_ptr)[i] = dp[i].field1;
        ((double*)wci.fields[1].data_ptr)[i] = dp[i].field2;
        ((uint32_t*)wci.fields[2].data_ptr)[i] = dp[i].field3;
        t += dt;
    }

    tsdb::write_wal(write_lock,npoints,0,chunk_len,chunk_data.data);
    assert_tree_fsynced(fs_root);
}

void
init_db(size_t wal_max_entries, size_t chunk_size)
{
    tsdb::configuration c = {
        .chunk_size = chunk_size,
        .wal_max_entries = wal_max_entries,
        .write_throttle_ns = 1000000000,
    };
    tsdb::create_root(".",c);
    assert_tree_fsynced(fs_root);

    tsdb::root root(".",false);
    root.create_database("db1");
    assert_tree_fsynced(fs_root);
    tsdb::database db1(root,"db1");
    tsdb::create_measurement(db1,"measurement1",test_fields);
    assert_tree_fsynced(fs_root);
    tsdb::measurement m1(db1,"measurement1");
    tsdb::open_or_create_and_lock_series(m1,"series1");
    assert_tree_fsynced(fs_root);
}

size_t
populate_db(int64_t t0, uint64_t dt, const std::vector<size_t>& nvec)
{
    tsdb::root root(".",false);
    tsdb::database db1(root,"db1");
    tsdb::measurement m1(db1,"measurement1");
    auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");

    // Write points to the WAL, triggering a chunk store commit partway through.
    size_t offset = 0;
    for (size_t n : nvec)
    {
        write_points(swl,n,t0 + dt*offset,dt,offset);
        offset += n;
    }

    return offset;
}

size_t
validate_points(uint64_t t0, uint64_t dt)
{
    tsdb::root root(".",false);
    tsdb::database db1(root,"db1");
    tsdb::measurement m1(db1,"measurement1");
    tsdb::series_read_lock srl(m1,"series1");

    // Query all the points.
    tsdb::select_op_first op(srl,"series1",{"field1","field2","field3"},0,-1,
                             -1);
    tsdb::wal_query wq(srl,0,-1);
    size_t offset = 0;
    while (op.npoints)
    {
        TASSERT(offset + op.npoints <= NELEMS(dps));
        for (size_t i=0; i<op.npoints; ++i)
        {
            tmock::assert_equiv(op.timestamps_begin[i],
                                t0 + (i + offset)*dt);
            tmock::assert_equiv(!op.is_field_null(0,i),
                                dps[i + offset].is_non_null[0]);
            tmock::assert_equiv(!op.is_field_null(1,i),
                                dps[i + offset].is_non_null[1]);
            tmock::assert_equiv(!op.is_field_null(2,i),
                                dps[i + offset].is_non_null[2]);
            tmock::assert_equiv(op.get_field<uint32_t,0>(i),
                                dps[i + offset].field1);
            tmock::assert_equiv(op.get_field<double,1>(i),
                                dps[i + offset].field2);
            tmock::assert_equiv(op.get_field<float,2>(i),
                                dps[i + offset].field3);
        }
        offset += op.npoints;
        op.next();
    }
    
    for (auto i = wq.begin(); i != wq.end(); ++i)
    {
        TASSERT(offset < NELEMS(dps));
        tmock::assert_equiv(i->time_ns,t0 + offset*dt);
        tmock::assert_equiv(!i->is_field_null(0),
                            dps[offset].is_non_null[0]);
        tmock::assert_equiv(!i->is_field_null(1),
                            dps[offset].is_non_null[1]);
        tmock::assert_equiv(!i->is_field_null(2),
                            dps[offset].is_non_null[2]);
        tmock::assert_equiv(i->get_field<uint32_t>(0),
                            dps[offset].field1);
        tmock::assert_equiv(i->get_field<double>(1),
                            dps[offset].field2);
        tmock::assert_equiv(i->get_field<float>(2),
                            dps[offset].field3);
        ++offset;
    }

    return offset;
}
