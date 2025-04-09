// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include <version.h>
#include <hdr/kmath.h>
#include <hdr/types.h>
#include <strutil/strutil.h>
#include <libtsdb/tsdb.h>

#include <algorithm>
#include <random>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#if IS_MACOS
#define NSERIES 10
#elif IS_LINUX
#define NSERIES 6
#else
#error Unknown platform!
#endif

constexpr const char* databases[] =
{
    "test1",
    "test2",
    "test3",
};

constexpr const char* measurements[] =
{
    "measurement1",
    "measurement2",
    "measurement3",
};

std::vector<tsdb::schema_entry> fields =
{
    {tsdb::FT_BOOL,{},"field_bool"},
    {tsdb::FT_U32,{},"field_u32_1"},
    {tsdb::FT_U32,{},"field_u32_2"},
    {tsdb::FT_U64,{},"field_u64"},
    {tsdb::FT_F32,{},"field_f32"},
    {tsdb::FT_F64,{},"field_f64"},
    {tsdb::FT_I32,{},"field_i32"},
    {tsdb::FT_I64,{},"field_i64"},
};

std::vector<tsdb::schema_entry> bad_fields =
{
    {tsdb::FT_I64,{},"field_i64"},
    {tsdb::FT_BOOL,{},"field_bool"},
    {tsdb::FT_U32,{},"field_u32_1"},
    {tsdb::FT_U32,{},"field_u32_2"},
    {tsdb::FT_U64,{},"field_u64"},
    {tsdb::FT_F32,{},"field_f32"},
    {tsdb::FT_F64,{},"field_f64"},
    {tsdb::FT_I32,{},"field_i32"},
};

struct data_point
{
    uint64_t    time_ns;

    bool        field_bool;
    uint32_t    field_u32_1;
    uint32_t    field_u32_2;
    uint64_t    field_u64;
    float       field_f32;
    double      field_f64;
    int32_t     field_i32;
    int64_t     field_i64;

    bool        field_bool_is_null;
    bool        field_u32_1_is_null;
    bool        field_u32_2_is_null;
    bool        field_u64_is_null;
    bool        field_f32_is_null;
    bool        field_f64_is_null;
    bool        field_i32_is_null;
    bool        field_i64_is_null;
};
bool operator<(const data_point& lhs, uint64_t time_ns)
{
    return lhs.time_ns < time_ns;
}
bool operator<(uint64_t time_ns, const data_point& rhs)
{
    return time_ns < rhs.time_ns;
}

struct series_state
{
    std::string             database;
    std::string             measurement;
    std::string             series;
    futil::path             dms_path;
    std::vector<data_point> points;
};

std::mt19937_64 mt(std::mt19937_64::default_seed + 1);
std::vector<series_state> states;
std::vector<std::string> field_names;

static void
push_bitmap(std::vector<uint64_t>& data, const bool* is_null, size_t npoints)
{
    for (size_t i=0; i<npoints; ++i)
    {
        if (i % 64 == 0)
            data.push_back(0xFFFFFFFFFFFFFFFFULL);
        if (*is_null)
            data.back() &= ~(1ULL << (i % 64));
        is_null += sizeof(data_point);
    }
}

static void
push_bool(std::vector<uint64_t>& data, const bool* v, size_t npoints)
{
    for (size_t i=0; i<npoints; ++i)
    {
        if (i % 8 == 0)
            data.push_back(0);
        if (*v)
            data.back() |= ((uint64_t)*v) << (8*(i % 8));
        v += sizeof(data_point);
    }
}

template<typename T>
static void
push_32(std::vector<uint64_t>& data, const T* v, size_t npoints)
{
    KASSERT(sizeof(T) == 4);
    for (size_t i=0; i<npoints; ++i)
    {
        if (i % 2 == 0)
            data.push_back(0);
        data.back() |= ((uint64_t)*(const uint32_t*)v) << (32*(i % 2));
        v += sizeof(data_point)/4;
    }
}

template<typename T>
static void
push_64(std::vector<uint64_t>& data, const T* v, size_t npoints)
{
    KASSERT(sizeof(T) == 8);
    for (size_t i=0; i<npoints; ++i)
    {
        data.push_back(*(const uint64_t*)v);
        v += sizeof(data_point)/8;
    }
}

static void
write_series(series_state& ss, size_t offset, size_t npoints)
{
    // Acquire the write lock.
    tsdb::database db(ss.database);
    tsdb::measurement m(db,ss.measurement);
    auto write_lock = tsdb::open_or_create_and_lock_series(m,ss.series);

    while (npoints)
    {
        // Figure out how many to write in this op.
        size_t cpoints = (npoints > 1000 ? npoints - 1000 : npoints);

        // Compute the expected length.
        size_t expected_len = cpoints*8; // Timestamps
        for (auto& f : fields)
        {
            const auto* fti = &tsdb::ftinfos[f.type];
            expected_len   += ceil_div<size_t>(cpoints,64)*8; // Bitmap
            expected_len   += ceil_div<size_t>(cpoints*fti->nbytes,8)*8; // Data
        }

        // Create the data vector.
        std::vector<uint64_t> data;
        data.reserve(expected_len/8);

        // Push the timestamps.
        auto* p0 = &ss.points[offset];
        for (size_t i=0; i<cpoints; ++i)
            data.push_back(p0[i].time_ns);

        // Push each field, first the bitmap then the data.
        push_bitmap(data,&p0->field_bool_is_null,cpoints);
        push_bool(data,&p0->field_bool,cpoints);
        push_bitmap(data,&p0->field_u32_1_is_null,cpoints);
        push_32(data,&p0->field_u32_1,cpoints);
        push_bitmap(data,&p0->field_u32_2_is_null,cpoints);
        push_32(data,&p0->field_u32_2,cpoints);
        push_bitmap(data,&p0->field_u64_is_null,cpoints);
        push_64(data,&p0->field_u64,cpoints);
        push_bitmap(data,&p0->field_f32_is_null,cpoints);
        push_32(data,&p0->field_f32,cpoints);
        push_bitmap(data,&p0->field_f64_is_null,cpoints);
        push_64(data,&p0->field_f64,cpoints);
        push_bitmap(data,&p0->field_i32_is_null,cpoints);
        push_32(data,&p0->field_i32,cpoints);
        push_bitmap(data,&p0->field_i64_is_null,cpoints);
        push_64(data,&p0->field_i64,cpoints);
        kassert(data.size()*8 == expected_len);

        // Write the series to disk.
        printf("Writing %zu points to %s...\n",cpoints,ss.dms_path.c_str());
        tsdb::write_series(write_lock,cpoints,0,expected_len,&data[0]);
        printf("Done\n");

        npoints -= cpoints;
        offset  += cpoints;
    }
}

void
validate_points(std::vector<data_point>::iterator fp, tsdb::select_op* op,
    size_t npoints)
{
    size_t total_points = 0;
    while (op->npoints)
    {
        printf("CHUNK %zu\n",op->npoints);
        total_points += op->npoints;
        kassert(total_points <= npoints);
        for (size_t i=0; i<op->npoints; ++i)
        {
            data_point p =
            {
                op->timestamps_begin[i],
                op->get_field<bool,0>(i),
                op->get_field<uint32_t,1>(i),
                op->get_field<uint32_t,2>(i),
                op->get_field<uint64_t,3>(i),
                op->get_field<float,4>(i),
                op->get_field<double,5>(i),
                op->get_field<int32_t,6>(i),
                op->get_field<int64_t,7>(i),
                op->is_field_null(0,i),
                op->is_field_null(1,i),
                op->is_field_null(2,i),
                op->is_field_null(3,i),
                op->is_field_null(4,i),
                op->is_field_null(5,i),
                op->is_field_null(6,i),
                op->is_field_null(7,i),
            };
            kassert(p.time_ns             == fp->time_ns);
            kassert(p.field_bool          == fp->field_bool);
            kassert(p.field_u32_1         == fp->field_u32_1);
            kassert(p.field_u32_2         == fp->field_u32_2);
            kassert(p.field_u64           == fp->field_u64);
            kassert(p.field_f32           == fp->field_f32);
            kassert(p.field_f64           == fp->field_f64);
            kassert(p.field_i32           == fp->field_i32);
            kassert(p.field_i64           == fp->field_i64);
            kassert(p.field_bool_is_null  == fp->field_bool_is_null);
            kassert(p.field_u32_1_is_null == fp->field_u32_1_is_null);
            kassert(p.field_u32_2_is_null == fp->field_u32_2_is_null);
            kassert(p.field_u64_is_null   == fp->field_u64_is_null);
            kassert(p.field_f32_is_null   == fp->field_f32_is_null);
            kassert(p.field_f64_is_null   == fp->field_f64_is_null);
            kassert(p.field_i32_is_null   == fp->field_i32_is_null);
            kassert(p.field_i64_is_null   == fp->field_i64_is_null);
            ++fp;
        }
        op->next();
    }
    kassert(total_points == npoints);
}

void
select_test()
{
    // Get a random series.
    size_t ss_index = rand() % states.size();
    auto& ss = states[ss_index];

    // Get a random range.
    uint64_t t[2];
    uint32_t t_type[2];
    for (size_t i=0; i<NELEMS(t); ++i)
    {
        t_type[i] = rand() % 3;
        if (t_type[i] == 0 && ss.points.front().time_ns == 0)
            t_type[i] = 1;
        else if (t_type[i] == 2 && ss.points.back().time_ns == (uint64_t)-1)
            t_type[i] = 1;

        uint64_t t_min;
        uint64_t t_max;
        if (t_type[i] == 0)
        {
            t_min = 0;
            t_max = ss.points.front().time_ns - 1;
        }
        else if (t_type[i] == 1)
        {
            t_min = ss.points.front().time_ns;
            t_max = ss.points.back().time_ns;
        }
        else
        {
            t_min = ss.points.back().time_ns + 1;
            t_max = -1;
        }
        t[i] = std::uniform_int_distribution<uint64_t>(t_min,t_max)(mt);
    }
    uint64_t t0 = MIN(t[0],t[1]);
    uint64_t t1 = MAX(t[0],t[1]);

    // Find our expected first point.
    auto fp = std::lower_bound(ss.points.begin(),ss.points.end(),t0);
    auto lp = std::upper_bound(ss.points.begin(),ss.points.end(),t1);
    size_t npoints = lp - fp;

    // Perform the query.
    tsdb::database db(ss.database);
    tsdb::measurement m(db,ss.measurement);
    tsdb::series_read_lock read_lock(m,ss.series);
    tsdb::select_op* op;
    size_t N;
    switch (rand() % 3)
    {
        case 0:
            // No limit.
            printf("QUERY %s %" PRIu64 " %" PRIu64 " "
                   "FROM %" PRIu64 " %" PRIu64 " TYPE %u %u "
                   "EXPECT %zu\n",
                   ss.dms_path.c_str(),t0,t1,
                   ss.points.front().time_ns,
                   ss.points.back().time_ns,
                   t_type[0],t_type[1],npoints);
            op = new tsdb::select_op_first(read_lock,ss.dms_path,
                                           field_names,t0,t1,-1);
        break;

        case 1:
            // LIMIT N.
            N = rand() % 1000000;
            npoints = MIN(N,npoints);
            printf("QUERY %s %" PRIu64 " %" PRIu64 " "
                   "FROM %" PRIu64 " %" PRIu64 " TYPE %u %u "
                   "LIMIT %zu EXPECT %zu\n",
                   ss.dms_path.c_str(),t0,t1,
                   ss.points.front().time_ns,
                   ss.points.back().time_ns,
                   t_type[0],t_type[1],N,npoints);
            op = new tsdb::select_op_first(read_lock,ss.dms_path,
                                           field_names,t0,t1,N);
        break;

        case 2:
            // LAST N.
            N = rand() % 1000000;
            npoints = MIN(N,npoints);
            fp = lp - npoints;
            printf("QUERY %s %" PRIu64 " %" PRIu64 " "
                   "FROM %" PRIu64 " %" PRIu64 " TYPE %u %u "
                   "LAST %zu EXPECT %zu\n",
                   ss.dms_path.c_str(),t0,t1,
                   ss.points.front().time_ns,
                   ss.points.back().time_ns,
                   t_type[0],t_type[1],N,npoints);
            op = new tsdb::select_op_last(read_lock,ss.dms_path,
                                          field_names,t0,t1,N);
        break;

        default:
            kabort();
        break;
    }

    // Validate.
    validate_points(fp,op,npoints);
    delete op;
}

void
rotate_test()
{
    // Delete some points from the front and append them to the end.

    // Get a random series.
    size_t ss_index = rand() % states.size();
    auto& ss = states[ss_index];

    // Get a timestamp in the live range.
    uint64_t t_min = ss.points.front().time_ns;
    uint64_t t_max = ss.points.back().time_ns;
    uint64_t t = std::uniform_int_distribution<uint64_t>(t_min,t_max)(mt);

    // We will delete everything up to and including t.
    auto fp = ss.points.begin();
    auto mp = std::lower_bound(ss.points.begin(),ss.points.end(),t + 1);
    size_t npoints = mp - fp;
    if (!npoints)
        return;

    // Delete from the front of the database.
    tsdb::database db(ss.database);
    tsdb::measurement m(db,ss.measurement);
    printf("DELETE FROM %s WHERE time_ns < %" PRIu64 "\n",
           ss.dms_path.c_str(),t);
    tsdb::delete_points(m,ss.series,t);

    // Incrementing the timestamp for everything that will be rotated.
    uint64_t dt = ss.points.back().time_ns - ss.points.front().time_ns + 1;
    while (fp != mp)
    {
        fp->time_ns += dt;
        ++fp;
    }

    // Write the points that we are about to rotate.
    printf("WRITE %s T %" PRIu64 " NPOINTS %zu\n",
           ss.dms_path.c_str(),ss.points.front().time_ns,npoints);
    write_series(ss,0,npoints);

    // Now rotate.
    std::rotate(ss.points.begin(),mp,ss.points.end());

    // Finally, validate the entire series.
    tsdb::series_read_lock read_lock(m,ss.series);
    auto cr = tsdb::count_points(read_lock,0,-1);
    kassert(cr.npoints == ss.points.size());
    kassert(cr.time_first == ss.points.front().time_ns);
    kassert(cr.time_last == ss.points.back().time_ns);
    tsdb::select_op_first op(read_lock,ss.dms_path,field_names,0,-1,-1);
    validate_points(ss.points.begin(),&op,ss.points.size());
}

int
main(int argc, const char* argv[])
{
    // Create a temporary directory for our database.
#if IS_MACOS
    char tmp[] = "/Volumes/ram_disk/tsdbtest.XXXXXX";
#elif IS_LINUX
    char tmp[] = "/tmp/tsdbtest.XXXXXX";
#endif
    futil::mkdtemp(tmp);
    futil::chdir(tmp);

    // Initialize the database directory.
    printf("Initializing test databases in %s...\n",tmp);
    tsdb::init();

    // Create some test databases.
    for (const auto* db : databases)
    {
        tsdb::create_database(db);
        tsdb::database _db(db);
        for (const auto* m : measurements)
        {
            tsdb::create_measurement(_db,m,fields);
            tsdb::create_measurement(_db,m,fields);
            try
            {
                tsdb::create_measurement(_db,m,bad_fields);
            }
            catch (const tsdb::measurement_exists_exception&)
            {
            }
        }
    }

    for (auto& f : fields)
        field_names.push_back(f.name);

    // Create some random series.
    printf("Generating random points...\n");
    srand(2);
    uint64_t time_ns = rand();
    for (size_t i=0; i<NSERIES; ++i)
    {
        const auto* db = databases[rand() % NELEMS(databases)];
        const auto* m  = measurements[rand() % NELEMS(measurements)];
        std::string s  = "series_" + std::to_string(i);
        states.emplace_back(series_state{db,m,s,futil::path(db,m,s)});

        auto& ss = states.back();
        size_t nelems = (rand() % 10000000) + 1;
        ss.points.reserve(nelems);
        for (size_t j=0; j<nelems; ++j)
        {
            ss.points.emplace_back(data_point{
                time_ns,
                (bool)(rand() % 2),
                (uint32_t)rand(),
                (uint32_t)rand(),
                ((uint64_t)rand() << 32) | rand(),
                (float)rand() / RAND_MAX,
                (double)rand() / RAND_MAX,
                (int32_t)(rand() - (RAND_MAX/2)),
                (int64_t)(rand() - (RAND_MAX/2)),
                rand() < (RAND_MAX / 1000),
                rand() < (RAND_MAX / 1000),
                rand() < (RAND_MAX / 1000),
                rand() < (RAND_MAX / 1000),
                rand() < (RAND_MAX / 1000),
                rand() < (RAND_MAX / 1000),
                rand() < (RAND_MAX / 1000),
                rand() < (RAND_MAX / 1000),
            });
            time_ns += (rand() % 1000) + 1;
        }
    }

    // Populate all of the series.
    for (auto& ss : states)
    {
        printf("Populating series %s...\n",ss.dms_path.c_str());
        if(ss.points.size() >= 4096*1024/8)
        {
            // The series is large.  Write two 2M chunks, test a 100% overlap
            // write crossing the chunks, and finally complete the write but
            // start at offset 100 to do it.
            printf("Testing dual-chunk overlap...\n");
            write_series(ss,0,4096*1024/8);
            write_series(ss,10,4096*1024/8 - 20);
            write_series(ss,100,ss.points.size() - 100);
        }
        else
        {
            // The series is not large, so just write the whole thing.
            write_series(ss,0,ss.points.size());
        }
    }

    for (;;)
    {
        // Figure out what action to take.
        uint8_t p = std::uniform_int_distribution<uint8_t>(0,100)(mt);
        if (p <= 5)
            rotate_test();
        else if (p <= 100)
            select_test();
    }
}

