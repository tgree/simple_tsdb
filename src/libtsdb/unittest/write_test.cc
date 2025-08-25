// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../write.h"
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

struct data_point
{
    uint32_t    field1;
    double      field2;
    uint32_t    field3;
    bool        is_non_null[3];
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

static constexpr const auto dps = gen_random_points();

static void
write_points(tsdb::series_write_lock& write_lock, size_t npoints, uint64_t t0,
    uint64_t dt, size_t offset)
{
    TASSERT(npoints <= dps.size());
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

    tsdb::write_series(write_lock,wci);
    assert_tree_fsynced(fs_root);
}

static tsdb::count_result
validate_points()
{
    tsdb::root root(".",false);
    tsdb::database db1(root,"db1");
    tsdb::measurement m1(db1,"measurement1");
    tsdb::series_read_lock srl(m1,"series1");

    auto cr = tsdb::count_committed_points(srl,0,-1);

    tsdb::select_op_first op(srl,"series1",{"field1","field2","field3"},0,-1,
                             -1);
    size_t rem_points = 1024;
    size_t offset = 0;
    while (op.npoints)
    {
        TASSERT(rem_points >= op.npoints);
        for (size_t i=0; i<op.npoints; ++i)
        {
            tmock::assert_equiv(op.timestamps_begin[i],
                                1000 + (i + offset)*100);
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
        rem_points -= op.npoints;
        op.next();
    }
    tmock::assert_equiv(offset,cr.npoints);

    return cr;
}

static std::vector<file_node*>
get_dangling_chunk_files(dir_node* sdn)
{
    std::set<std::string> bitmap_chunk_names;

    size_t nentries = sdn->get_file("index")->data.size() /
        sizeof(tsdb::index_entry);
    const auto* entries = sdn->get_file("index")
        ->as_array<tsdb::index_entry>();
    for (size_t i=0; i<nentries; ++i)
        bitmap_chunk_names.insert(entries[i].timestamp_file);

    std::set<std::string> field_chunk_names(bitmap_chunk_names);
    for (const auto& n : bitmap_chunk_names)
        field_chunk_names.insert(n + ".gz");

    dir_node* fields_dir = sdn->get_dir("fields");
    dir_node* bitmaps_dir = sdn->get_dir("bitmaps");
    const char* fields[] = {"field1", "field2", "field3"};
    std::vector<file_node*> dangling_files;
    for (auto* f : fields)
    {
        dir_node* field_dir = fields_dir->get_dir(f);
        for (auto iter : field_dir->files)
        {
            if (!field_chunk_names.contains(iter.first))
                dangling_files.push_back(iter.second);
        }
        dir_node* bitmap_dir = bitmaps_dir->get_dir(f);
        for (auto iter : bitmap_dir->files)
        {
            if (!bitmap_chunk_names.contains(iter.first))
                dangling_files.push_back(iter.second);
        }
    }

    return dangling_files;
}

class tmock_test
{
    TMOCK_TEST(test_write_chunk_index)
    {
        tsdb::configuration c = {
            .chunk_size = 2*1024*1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",false);
        root.create_database("db1");
        tsdb::database db1(root,"db1");
        tsdb::create_measurement(db1,"measurement1",test_fields);
        tsdb::measurement m1(db1,"measurement1");

        // A write chunk is constructed as follows:
        //  1. Timestamps
        //  2. Fields
        //      1. Bitmap
        //          1. Bitmap offset bits
        //          2. Bitmap bits
        //          3. Padding to 64 bits
        //      2. Field data, padded to 64 bits
        //
        // Construct an index using a NULL pointer just so we can validate
        // offsets.
        size_t chunk_len = m1.compute_write_chunk_len(15);

        try
        {
            tsdb::write_chunk_index wci(m1,15,0,chunk_len-4,(const void*)NULL);
            tmock::abort("Expected incorrect write chunk len exception!");
        }
        catch (const tsdb::incorrect_write_chunk_len_exception&)
        {
        }

        try
        {
            tsdb::write_chunk_index wci(m1,15,0,chunk_len+4,(const void*)NULL);
            tmock::abort("Expected incorrect write chunk len exception!");
        }
        catch (const tsdb::incorrect_write_chunk_len_exception&)
        {
        }

        tsdb::write_chunk_index wci(m1,15,0,chunk_len,(const void*)NULL);
        tmock::assert_equiv(wci.npoints,15UL);
        tmock::assert_equiv(wci.bitmap_offset,0UL);
        tmock::assert_equiv(
            (uintptr_t)wci.timestamps,(uintptr_t)0x00000000);
        tmock::assert_equiv(wci.fields.size(),3UL);
        tmock::assert_equiv(
            (uintptr_t)wci.fields[0].bitmap_ptr,(uintptr_t)0x00000078);
        tmock::assert_equiv(
            (uintptr_t)wci.fields[0].data_ptr,(uintptr_t)0x00000080);
        tmock::assert_equiv(
            (uintptr_t)wci.fields[1].bitmap_ptr,(uintptr_t)0x000000C0);
        tmock::assert_equiv(
            (uintptr_t)wci.fields[1].data_ptr,(uintptr_t)0x000000C8);
        tmock::assert_equiv(
            (uintptr_t)wci.fields[2].bitmap_ptr,(uintptr_t)0x00000140);
        tmock::assert_equiv(
            (uintptr_t)wci.fields[2].data_ptr,(uintptr_t)0x00000148);
        tmock::assert_equiv(chunk_len,392UL);
    }

    TMOCK_TEST(test_write_few)
    {
        tsdb::configuration c = {
            .chunk_size = 1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);
        tsdb::root root(".",false);
        root.create_database("db1");
        tsdb::database db1(root,"db1");
        tsdb::create_measurement(db1,"measurement1",test_fields);
        tsdb::measurement m1(db1,"measurement1");
        auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
        auto sdn             = fd_table[swl.series_dir.fd].directory;
        auto time_first_fn   = sdn->get_file("time_first");
        auto time_last_fn    = sdn->get_file("time_last");
        auto time_ns_dn      = sdn->get_dir("time_ns");
        auto fields_dn       = sdn->get_dir("fields");
        auto field1_dn       = fields_dn->get_dir("field1");
        auto field2_dn       = fields_dn->get_dir("field2");
        auto field3_dn       = fields_dn->get_dir("field3");
        auto bitmaps_dn      = sdn->get_dir("bitmaps");
        auto bitmap1_dn      = bitmaps_dn->get_dir("field1");
        auto bitmap2_dn      = bitmaps_dn->get_dir("field2");
        auto bitmap3_dn      = bitmaps_dn->get_dir("field3");

        // Write 32 points.  This will only result in an 256-byte chunk size
        // and so not trigger a compression.
        write_points(swl,32,1000,100,0);
        auto index_fn        = sdn->get_file("index");
        auto time_1000_fn    = time_ns_dn->get_file("1000");
        auto field1_1000_fn  = field1_dn->get_file("1000");
        auto field2_1000_fn  = field2_dn->get_file("1000");
        auto field3_1000_fn  = field3_dn->get_file("1000");
        auto bitmap1_1000_fn = bitmap1_dn->get_file("1000");
        auto bitmap2_1000_fn = bitmap2_dn->get_file("1000");
        auto bitmap3_1000_fn = bitmap3_dn->get_file("1000");
        tmock::assert_equiv(index_fn->data.size(),sizeof(tsdb::index_entry));
        tmock::assert_equiv(time_first_fn->get_data<uint64_t>(),1000UL);
        tmock::assert_equiv(time_last_fn->get_data<uint64_t>(),4100UL);
        tmock::assert_equiv(time_1000_fn->data.size(),32UL*8UL);
        for (size_t i=0; i<32; ++i)
        {
            tmock::assert_equiv(time_1000_fn->get_data<uint64_t>(i*8),
                                1000 + 100*i);
            tmock::assert_equiv(field1_1000_fn->get_data<uint32_t>(i*4),
                                dps[i].field1);
            tmock::assert_equiv(field2_1000_fn->get_data<double>(i*8),
                                dps[i].field2);
            tmock::assert_equiv(field3_1000_fn->get_data<float>(i*4),
                                dps[i].field3);
            tmock::assert_equiv(
                (bitmap1_1000_fn->get_data<uint64_t>(i/64) >> (i % 64)) & 1,
                (uint64_t)dps[i].is_non_null[0]);
            tmock::assert_equiv(
                (bitmap2_1000_fn->get_data<uint64_t>(i/64) >> (i % 64)) & 1,
                (uint64_t)dps[i].is_non_null[1]);
            tmock::assert_equiv(
                (bitmap3_1000_fn->get_data<uint64_t>(i/64) >> (i % 64)) & 1,
                (uint64_t)dps[i].is_non_null[2]);
        }

        // Write 128 points.  This will result in a 1024-byte chunk and
        // trigger a compression.
        write_points(swl,128,4200,100,32);
        index_fn = sdn->get_file("index");
        tmock::assert_equiv(index_fn->data.size(),2*sizeof(tsdb::index_entry));
        tmock::assert_equiv(time_first_fn->get_data<uint64_t>(),1000UL);
        tmock::assert_equiv(time_last_fn->get_data<uint64_t>(),16900UL);
        tmock::assert_equiv(time_1000_fn->data.size(),1024UL);
        TASSERT(!field1_dn->files.contains("1000"));
        TASSERT(!field2_dn->files.contains("1000"));
        TASSERT(!field3_dn->files.contains("1000"));
        auto field1_1000_gz_fn = field1_dn->get_file("1000.gz");
        auto field2_1000_gz_fn = field2_dn->get_file("1000.gz");
        auto field3_1000_gz_fn = field3_dn->get_file("1000.gz");
        auto_buf field1_buf(root.config.chunk_size/2);
        auto_buf field2_buf(root.config.chunk_size);
        auto_buf field3_buf(root.config.chunk_size/2);
        zutil::gzip_decompress(field1_buf,root.config.chunk_size/2,
                               &field1_1000_gz_fn->data[0],
                               field1_1000_gz_fn->data.size());
        zutil::gzip_decompress(field2_buf,root.config.chunk_size,
                               &field2_1000_gz_fn->data[0],
                               field2_1000_gz_fn->data.size());
        zutil::gzip_decompress(field3_buf,root.config.chunk_size/2,
                               &field3_1000_gz_fn->data[0],
                               field3_1000_gz_fn->data.size());
        for (size_t i=0; i<128; ++i)
        {
            tmock::assert_equiv(time_1000_fn->get_data<uint64_t>(i*8),
                                1000 + 100*i);
            tmock::assert_equiv(
                (bitmap1_1000_fn->get_data<uint64_t>((i/64)*8) >> (i % 64)) & 1,
                (uint64_t)dps[i].is_non_null[0]);
            tmock::assert_equiv(
                (bitmap2_1000_fn->get_data<uint64_t>((i/64)*8) >> (i % 64)) & 1,
                (uint64_t)dps[i].is_non_null[1]);
            tmock::assert_equiv(
                (bitmap3_1000_fn->get_data<uint64_t>((i/64)*8) >> (i % 64)) & 1,
                (uint64_t)dps[i].is_non_null[2]);
        }

        // Verify the new chunk files.
        auto time_13800_fn    = time_ns_dn->get_file("13800");
        auto field1_13800_fn  = field1_dn->get_file("13800");
        auto field2_13800_fn  = field2_dn->get_file("13800");
        auto field3_13800_fn  = field3_dn->get_file("13800");
        auto bitmap1_13800_fn = bitmap1_dn->get_file("13800");
        auto bitmap2_13800_fn = bitmap2_dn->get_file("13800");
        auto bitmap3_13800_fn = bitmap3_dn->get_file("13800");
        tmock::assert_equiv(time_13800_fn->data.size(),32UL*8UL);
        for (size_t i=0; i<32; ++i)
        {
            tmock::assert_equiv(time_13800_fn->get_data<uint64_t>(i*8),
                                13800 + 100*i);
            tmock::assert_equiv(field1_13800_fn->get_data<uint32_t>(i*4),
                                dps[i+128].field1);
            tmock::assert_equiv(field2_13800_fn->get_data<double>(i*8),
                                dps[i+128].field2);
            tmock::assert_equiv(field3_13800_fn->get_data<float>(i*4),
                                dps[i+128].field3);
            tmock::assert_equiv(
                (bitmap1_13800_fn->get_data<uint64_t>(i/64) >> (i % 64)) & 1,
                (uint64_t)dps[i+128].is_non_null[0]);
            tmock::assert_equiv(
                (bitmap2_13800_fn->get_data<uint64_t>(i/64) >> (i % 64)) & 1,
                (uint64_t)dps[i+128].is_non_null[1]);
            tmock::assert_equiv(
                (bitmap3_13800_fn->get_data<uint64_t>(i/64) >> (i % 64)) & 1,
                (uint64_t)dps[i+128].is_non_null[2]);
        }

        // Everything seems in order.  Let's do a count_op.
        auto cr = tsdb::count_committed_points(swl,0,-1);
        tmock::assert_equiv(cr.npoints,128UL + 32UL);
        tmock::assert_equiv(cr.time_first,1000UL);
        tmock::assert_equiv(cr.time_last,16900UL);

        // Try a select op.  Mix up the order of the fields.
        tsdb::select_op_first op(swl,"series1",{"field3","field2","field1"},0,
                                 -1,-1);
        size_t rem_points = 128 + 32;
        size_t offset = 0;
        while (op.npoints)
        {
            TASSERT(rem_points >= op.npoints);
            for (size_t i=0; i<op.npoints; ++i)
            {
                tmock::assert_equiv(op.timestamps_begin[i],
                                    1000 + (i + offset)*100);
                tmock::assert_equiv(!op.is_field_null(0,i),
                                    dps[i + offset].is_non_null[2]);
                tmock::assert_equiv(!op.is_field_null(1,i),
                                    dps[i + offset].is_non_null[1]);
                tmock::assert_equiv(!op.is_field_null(2,i),
                                    dps[i + offset].is_non_null[0]);
                tmock::assert_equiv(op.get_field<float,0>(i),
                                    dps[i + offset].field3);
                tmock::assert_equiv(op.get_field<double,1>(i),
                                    dps[i + offset].field2);
                tmock::assert_equiv(op.get_field<uint32_t,2>(i),
                                    dps[i + offset].field1);
            }
            offset += op.npoints;
            rem_points -= op.npoints;
            op.next();
        }
    }

    TMOCK_TEST(test_write_consistency)
    {
        tsdb::configuration c = {
            .chunk_size = 1024,
            .wal_max_entries = 10240,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);

        {
            tsdb::root root(".",false);
            root.create_database("db1");
            tsdb::database db1(root,"db1");
            tsdb::create_measurement(db1,"measurement1",test_fields);
            tsdb::measurement m1(db1,"measurement1");
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");

            // We are going to write a total of 1024 points.  We break it up
            // into a bunch of weird sizes.
            snapshot_auto_begin();
            size_t offset = 0;
            for (size_t n : {76, 128, 93, 1, 1, 2, 700, 12, 11})
            {
                write_points(swl,n,1000 + 100*offset,100,offset);
                offset += n;
            }
            snapshot_auto_end();
            tmock::assert_equiv(offset,NELEMS(dps));
        }

        auto dn_final __UNUSED__ = fs_root;
        tsdb::count_result prev_cr = {};
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            // Validate whatever points did make it to disk.
            auto cr = validate_points();
            TASSERT(cr.npoints >= prev_cr.npoints);
            TASSERT(cr.time_first == 1 || cr.time_first == 1000);
            TASSERT(cr.time_first >= prev_cr.time_first);
            TASSERT(cr.time_last <= 103300);
            TASSERT(cr.time_last >= prev_cr.time_last);
            prev_cr = cr;

            if (cr.npoints == NELEMS(dps))
                continue;

            // Finish populating the series from whatever intermediate state it
            // was interrupted in.
            {
                tsdb::root root(".",false);
                TASSERT(fd_table[root.root_dir.fd].directory == dn);
                tsdb::database db1(root,"db1");
                tsdb::measurement m1(db1,"measurement1");
                auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
                size_t total = cr.npoints;
                write_points(swl,NELEMS(dps)-total,1000 + 100*total,100,total);
            }

            // Validate it all again to make sure it is sane.
            tmock::assert_equiv(validate_points().npoints,NELEMS(dps));
        }
    }

    TMOCK_TEST(test_write_consistency_small_chunks)
    {
        tsdb::configuration c = {
            .chunk_size = 128,
            .wal_max_entries = 102400,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);

        {
            tsdb::root root(".",false);
            root.create_database("db1");
            tsdb::database db1(root,"db1");
            tsdb::create_measurement(db1,"measurement1",test_fields);
            tsdb::measurement m1(db1,"measurement1");
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");

            // We are going to write a total of 73 points, all at once.
            snapshot_auto_begin();
            write_points(swl,73,1000,100,0);
            snapshot_auto_end();
        }

        tsdb::count_result prev_cr = {};
        bool found_dangling = false;
        for (size_t i = 0; i < snapshots.size(); ++i)
        {
            auto* dn = snapshots[i];
            activate_and_fsync_snapshot(dn);

            // Validate whatever points did make it to disk.
            auto cr = validate_points();
            TASSERT(cr.npoints >= prev_cr.npoints);
            TASSERT(cr.time_first == 1 || cr.time_first == 1000);
            TASSERT(cr.time_first >= prev_cr.time_first);
            TASSERT(cr.time_last <= 8200);
            TASSERT(cr.time_last >= prev_cr.time_last);
            prev_cr = cr;

            // Finish populating the series from whatever intermediate state it
            // was interrupted in.  This ensures that we clean up any dangling
            // files as long as we resume the rewrite from the timestamp that
            // was interrupted.
            {
                tsdb::root root(".",false);
                TASSERT(fd_table[root.root_dir.fd].directory == dn);
                tsdb::database db1(root,"db1");
                tsdb::measurement m1(db1,"measurement1");
                auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
                auto sdn = fd_table[swl.series_dir.fd].directory;
                bool dangling = !get_dangling_chunk_files(sdn).empty();
                size_t total = cr.npoints;
                if (total < 73)
                    write_points(swl,73-total,1000 + 100*total,100,total);
                else
                    TASSERT(!dangling);
                if (dangling)
                    TASSERT(get_dangling_chunk_files(sdn).empty());
                found_dangling |= dangling;
            }

            // Validate it all again to make sure it is sane.
            tmock::assert_equiv(validate_points().npoints,73UL);
        }
        TASSERT(found_dangling);
    }

    // We would prefer for there not to be any dangling files if we crash while
    // doing a write operation.  For now, we want point reading to be easy,
    // which means that the index file entries always point at valid chunk
    // files.  That means the index file is the last thing to get updated and
    // that means that we can create chunk files for a write operation before
    // the index file knows about them.  A crash at that point leaves them
    // empty but completely dangling, and if no subsequent write operation
    // tries to use the same starting timestamp (maybe an entire system hard-
    // crashed and that epoch timestamp is now in the past) then the empty
    // files never get recycled.
    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(test_no_dangling_files)
    {
        tsdb::configuration c = {
            .chunk_size = 128,
            .wal_max_entries = 64,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);

        {
            tsdb::root root(".",false);
            root.create_database("db1");
            tsdb::database db1(root,"db1");
            tsdb::create_measurement(db1,"measurement1",test_fields);
            tsdb::measurement m1(db1,"measurement1");
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");

            // We are going to write a total of 300 points, all at once.
            snapshot_auto_begin();
            write_points(swl,300,1000,100,0);
            snapshot_auto_end();
        }

        for (auto* dn : snapshots)
        {
            auto* sdn = dn
                ->get_dir("databases")
                ->get_dir("db1")
                ->get_dir("measurement1")
                ->get_dir("series1");
            TASSERT(get_dangling_chunk_files(sdn).empty());
        }
    }

    // If we have dangling files (see previous unit test), they should all be
    // for time stamps that come after time_last (and thus are not live yet).
    //
    // This presents a possible solution: we could insert the new chunk
    // timestamp into the index file before we create the chunk files.  Then,
    // we create the files and populate them with the new data points, and
    // finally we update time_last.  We would no longer have any dangling chunk
    // files since the timestamp is allocated to the index file first.  The
    // read/select operation would have to ignore index entries that come after
    // time_last.  Future write operations would clean up the dangling entry
    // automatically which they already do.
    TMOCK_TEST(test_dangling_files_come_after_time_last)
    {
        tsdb::configuration c = {
            .chunk_size = 128,
            .wal_max_entries = 64,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);

        {
            tsdb::root root(".",false);
            root.create_database("db1");
            tsdb::database db1(root,"db1");
            tsdb::create_measurement(db1,"measurement1",test_fields);
            tsdb::measurement m1(db1,"measurement1");
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");

            // We are going to write a total of 300 points, all at once.
            snapshot_auto_begin();
            write_points(swl,300,1000,100,0);
            snapshot_auto_end();
        }

        bool found_danglers = false;
        for (auto* dn : snapshots)
        {
            auto* sdn = dn
                ->get_dir("databases")
                ->get_dir("db1")
                ->get_dir("measurement1")
                ->get_dir("series1");
            uint64_t time_last =
                sdn->get_file("time_last")->get_data<uint64_t>();
            for (auto* fn : get_dangling_chunk_files(sdn))
            {
                found_danglers = true;

                auto name = fn->name;
                if (name.ends_with(".gz"))
                    name.resize(name.size() - 3);
                uint64_t chunk_time_ns = std::stoull(name);
                TASSERT(chunk_time_ns > time_last);
            }
        }
        TASSERT(found_danglers);
    }

    TMOCK_TEST(test_no_empty_index_entries)
    {
        tsdb::configuration c = {
            .chunk_size = 128,
            .wal_max_entries = 64,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);

        {
            tsdb::root root(".",false);
            root.create_database("db1");
            tsdb::database db1(root,"db1");
            tsdb::create_measurement(db1,"measurement1",test_fields);
            tsdb::measurement m1(db1,"measurement1");
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");

            // We are going to write a total of 300 points, all at once.
            snapshot_auto_begin();
            write_points(swl,300,1000,100,0);
            snapshot_auto_end();
        }

        for (auto* dn : snapshots)
        {
            auto* sdn = dn
                ->get_dir("databases")
                ->get_dir("db1")
                ->get_dir("measurement1")
                ->get_dir("series1");
            auto* index_fn = sdn->get_file("index");

            size_t nindices = index_fn->data.size() / sizeof(tsdb::index_entry);
            auto* ies = index_fn->as_array<tsdb::index_entry>();
            auto* time_ns_dn = sdn->get_dir("time_ns");
            for (size_t i=0; i<nindices; ++i)
                TASSERT(time_ns_dn->files.contains(ies[i].timestamp_file));
        }
    }

    TMOCK_TEST(test_no_tailfd_write_truncate_while_selecting)
    {
        tsdb::configuration c = {
            .chunk_size = 128,
            .wal_max_entries = 16,
            .write_throttle_ns = 1000000000,
        };
        tsdb::create_root(".",c);

        {
            tsdb::root root(".",false);
            root.create_database("db1");
            tsdb::database db1(root,"db1");
            tsdb::create_measurement(db1,"measurement1",test_fields);
            tsdb::measurement m1(db1,"measurement1");
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");

            // We are going to write a total of 45 points, all at once.
            snapshot_auto_begin();
            write_points(swl,45,1000,100,0);
            snapshot_auto_end();
        }

        size_t last_total_points = 0;
        for (auto* dn : snapshots)
        {
            activate_and_fsync_snapshot(dn);

            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            auto swl = tsdb::open_or_create_and_lock_series(m1,"series1");
            if (swl.time_last == 0)
                continue;

            // Give space to the index file backing so that it can be grown
            // while under fakefs mmap.
            auto index_fn = fd_table[swl.series_dir.fd].directory
                                ->get_file("index");
            index_fn->data.reserve(index_fn->data.size() + 1024);

            // Start a select op.
            auto srl = tsdb::series_read_lock(m1,"series1");
            tsdb::select_op_first op(srl,"series1",{"field1"},0,-1,-1);
            tmock::assert_equiv(op.npoints,16UL);
            tmock::assert_equiv(op.timestamps_begin[0],1000UL);
            tmock::assert_equiv(op.timestamps_end[-1],2500UL);

            // Start a write.
            write_points(swl,45,100000,100,0);

            // Work through the select.
            size_t total_points = 0;
            while (op.npoints)
            {
                total_points += op.npoints;
                op.next();
            }
            TASSERT(total_points >= last_total_points);
            last_total_points = total_points;
        }
    }
};

TMOCK_MAIN();
