// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "tsdb.h"
#include <strutil/strutil.h>
#include <hdr/kmath.h>
#include <algorithm>

futil::path
tsdb::series_to_measurement(const futil::path& series)
{
    if (series.empty() || series[0] == '/')
        throw futil::errno_exception(EINVAL);

    auto parts = series.decompose();
    if (parts.size() != 3)
        throw futil::errno_exception(EINVAL);

    return futil::path(parts[0],parts[1]);
}

void
tsdb::parse_schema(const futil::path& path, std::vector<field>& _fields)
{
    futil::file fd(path,O_RDONLY | O_SHLOCK);
    off_t f_size = fd.lseek(0,SEEK_END);
    auto m = fd.mmap(0,f_size,PROT_READ,MAP_SHARED,0);

    std::vector<field> fields;
    auto lines = str::split((const char*)m.addr);
    for (const auto& l : lines)
    {
        if (l.size() < 3 || !str::isprint(l) || l[1] != '/')
            throw futil::errno_exception(EINVAL);

        field f;
        f.name = std::string(l,2);
        switch (l[0])
        {
            case '0':   f.type = FT_BOOL;   break;
            case '1':   f.type = FT_U32;    break;
            case '2':   f.type = FT_U64;    break;
            case '3':   f.type = FT_F32;    break;
            case '4':   f.type = FT_F64;    break;

            default:
                throw futil::errno_exception(EINVAL);
        }
        fields.push_back(f);
    }
    _fields.swap(fields);
}

void
tsdb::parse_schema_for_series(const futil::path& series,
    std::vector<field>& fields)
{
    futil::path schema_path("databases",series_to_measurement(series),
                            "schema.txt");
    tsdb::parse_schema(schema_path,fields);
}

uint64_t
tsdb::get_time_last(const futil::path& series)
{
    uint64_t time_last;
    futil::path time_last_path("databases",series,"time_last");
    futil::file(time_last_path,O_RDONLY).read_all(&time_last,8);
    return time_last;
}

void
tsdb::delete_points(const futil::path& series, uint64_t t)
{
    // We need to find the first valid timestamp > t.  This becomes the new
    // value for time_first and then we drop all points before it.  If no such
    // timestamp exists, then time_first becomes time_last + 1 and we drop
    // everything except the last chunk, which only drop if it is full.  This
    // all has to be done while holding the time_first lock.
    futil::path series_path("databases",series);
    futil::path time_ns_path(series_path,"time_ns");

    // Parse the schema for this series to find the field names.
    std::vector<field> fields;
    parse_schema_for_series(series,fields);

    // We need to get time_first and time_last.  We lock time_first so that
    // nobody else tries to access the series while we are adjusting things.
    futil::file time_first_fd(series_path + "time_first",O_RDWR | O_EXLOCK);
    futil::file time_last_fd(series_path + "time_last",O_RDONLY);
    uint64_t time_first = time_first_fd.read_u64();
    uint64_t time_last = time_last_fd.read_u64();
    if (t < time_first)
        return;

    // Map the index file and find the beginning and end.
    futil::file index_fd(series_path + "index",O_RDWR);
    auto index_m = index_fd.mmap(NULL,index_fd.lseek(0,SEEK_END),
                                 PROT_READ | PROT_WRITE,MAP_SHARED,0);
    auto* index_begin = (index_entry*)index_m.addr;
    auto* index_end = index_begin + index_m.len / sizeof(index_entry);
    auto* index_slot = index_begin;

    // Find the target slot.  std::upper_bound returns the first slot greater
    // than the requested value, meaning that t must appear in the slot
    // preceding it - IF t appears at all.  It could be the case that t is
    // a value in the gap between slots.
    index_slot = std::upper_bound(index_begin,index_end,t);
    kassert(index_slot != index_begin);
    --index_slot;

    // Map the timestamp chunk and search it for t.  The std::upper_bound will
    // find the timestamp after t, unless it is the end of the chunk.
    futil::file timestamp_fd(time_ns_path + index_slot->timestamp_file,
                             O_RDONLY);
    auto timestamp_m = timestamp_fd.mmap(NULL,timestamp_fd.lseek(0,SEEK_END),
                                         PROT_READ,MAP_SHARED,0);
    auto* timestamps_begin = (const uint64_t*)timestamp_m.addr;
    auto* timestamps_end = timestamps_begin + timestamp_m.len / 8;
    auto* timestamp_slot = std::upper_bound(timestamps_begin,timestamps_end,t);
    if (timestamp_slot < timestamps_end)
    {
        // We can delete all chunks before this one.
        time_first = *timestamp_slot;
    }
    else if (index_slot < index_end - 1)
    {
        // This is not the last chunk, so it must be full.  We can delete all
        // chunks up to and including this one.
        ++index_slot;
        time_first = index_slot->time_ns;
    }
    else
    {
        // This is equivalent to t >= time_last (which maybe we could have
        // checked above?).  We can delete every single chunk in the series.
        ++index_slot;
        time_first = time_last + 1;
    }

    // Update time_first.
    time_first_fd.lseek(0,SEEK_SET);
    time_first_fd.write_all(&time_first,8);
    time_first_fd.fcntl(F_BARRIERFSYNC);

    // Delete all orphaned chunks.
    for (auto* slot = index_begin; slot < index_slot; ++slot)
    {
        futil::unlink_if_exists(time_ns_path + slot->timestamp_file);
        for (const auto& f : fields)
        {
            futil::unlink_if_exists(series_path + "fields" + f.name +
                                    slot->timestamp_file);
            futil::unlink_if_exists(series_path + "bitmaps" + f.name +
                                    slot->timestamp_file);
        }
    }

    // Shift the index file appropriately.
    memmove(index_begin,index_slot,
            (index_end - index_slot)*sizeof(index_entry));
    index_m.msync();
    index_fd.truncate((index_end - index_slot)*sizeof(index_entry));
    index_fd.fcntl(F_BARRIERFSYNC);
    printf("Deleted %zu slots from the start of the index file.\n",
           index_slot - index_begin);
}

// TODO: This sets index_begin as the start of the index file.  Instead, it
// should search for the first slot that could hold time_first since a delete
// operation could have incremented time_first and then crashed before deleting
// the chunk files or shifting the index file up.
tsdb::select_op::select_op(const futil::path& series,
    const std::vector<std::string>& field_names, uint64_t _t0, uint64_t _t1,
    uint64_t limit):
        time_first_fd(futil::path("databases",series,"time_first"),
                      O_RDONLY | O_SHLOCK),
        time_first(time_first_fd.read_u64()),
        time_last(get_time_last(series)),
        index_begin(NULL),
        index_end(NULL),
        series(series),
        t0(MAX(_t0,time_first)),
        t1(MIN(_t1,time_last)),
        rem_limit(limit),
        fields(field_names.size()),
        index_slot(NULL),
        timestamp_mapping(NULL,2048*1024,PROT_NONE,MAP_ANONYMOUS | MAP_PRIVATE,
                          -1,0),
        field_mappings(field_names.size()),
        bitmap_mappings(field_names.size()),
        is_last(true),
        npoints(0),
        bitmap_offset(0),
        timestamp_data(NULL),
        field_data(field_names.size())
{
    // Validate the time and limit ranges.
    if (time_first > time_last)
        return;
    if (_t0 > _t1)
        return;
    if (_t1 < time_first)
        return;
    if (_t0 > time_last)
        return;
    if (rem_limit == 0)
        return;

    // Map the index.  Since time_first <= time_last, there must be at least one
    // measurement, which means there must be at least one chunk, which means
    // there must be at least one slot in the index file - so we won't attempt
    // to mmap() an empty file which always fails.
    index_fd.open(futil::path("databases",series,"index"),O_RDONLY);
    index_mapping.map(NULL,index_fd.lseek(0,SEEK_END),PROT_READ,MAP_SHARED,
                      index_fd.fd,0),
    index_begin = (const index_entry*)index_mapping.addr;
    index_end = index_begin + index_mapping.len / sizeof(index_entry);

    // Fetch the schema and figure out which fields we are going to return.
    // We are sure to keep the fields vector in the same order as the field
    // names that were passed in; this is the order in which we will return
    // the data points.
    std::vector<field> schema_fields;
    parse_schema_for_series(series,schema_fields);
    for (const auto& fn : field_names)
    {
        bool found = false;
        for (const auto& sf : schema_fields)
        {
            if (sf.name == fn)
            {
                fields.emplace_back(sf);
                found = true;
                break;
            }
        }
        if (!found)
            throw futil::errno_exception(ENOENT);
    }
}

void
tsdb::select_op::_advance(bool is_first)
{
    if (!is_first)
    {
        if (is_last)
            throw futil::errno_exception(EFAULT);
        ++index_slot;
    }

    // Map the timestamp file associated with the target slot.
    futil::path time_ns_path("databases",series,"time_ns");
    futil::file timestamp_fd(time_ns_path + index_slot->timestamp_file,
                             O_RDONLY);
    off_t timestamp_fd_size = timestamp_fd.lseek(0,SEEK_END);
    futil::mmap(timestamp_mapping.addr,timestamp_fd_size,PROT_READ,
                MAP_SHARED | MAP_FIXED,timestamp_fd.fd,0);

    // Find the first timestamp >= t0.
    size_t start_index;
    const auto* timestamps_end = (const uint64_t*)timestamp_mapping.addr +
                                 timestamp_fd_size/8;
    if (!is_first)
    {
        // The first timestamp in our range is right at the start of the file.
        timestamp_data = (const uint64_t*)timestamp_mapping.addr;
        start_index = 0;
    }
    else
    {
        // The first timestamp could be partway through the file.
        auto* timestamps_begin = (const uint64_t*)timestamp_mapping.addr;
        timestamp_data = std::lower_bound(timestamps_begin,timestamps_end,t0);

        // The first timestamp could even be in the gap between this slot
        // and the next one, which is yuck.  We can't know that until we've
        // mapped everything and done the search though.
        if (timestamp_data == timestamps_end)
        {
            // Increment to the next slot (which we know must exist since we
            // know that there is still data to return), then open and map it,
            // overwriting the previous mapping.
            ++index_slot;
            timestamp_fd.open(time_ns_path + index_slot->timestamp_file,
                              O_RDONLY);
            timestamp_fd_size = timestamp_fd.lseek(0,SEEK_END);
            futil::mmap(timestamp_mapping.addr,timestamp_fd_size,PROT_READ,
                        MAP_SHARED | MAP_FIXED,timestamp_fd.fd,0);
            timestamp_data = timestamps_begin;
            timestamps_end = (const uint64_t*)timestamp_mapping.addr +
                             timestamp_fd_size/8;
        }

        start_index = timestamp_data - timestamps_begin;
    }

    // Find the first timestamp > t1.
    auto* last_timestamp = std::upper_bound(timestamp_data,timestamps_end,t1);
    npoints = last_timestamp - timestamp_data;

    // Figure out if this is the last chunk of data.
    if (npoints < rem_limit && last_timestamp == timestamps_end &&
        index_slot + 1 != index_end && index_slot[1].time_ns <= t1)
    {
        is_last = false;
    }
    else
        is_last = true;

    // Cap the number of returned points.
    npoints = MIN(npoints,rem_limit);
    rem_limit -= npoints;

    // Map the data points.
    for (size_t i=0; i<fields.size(); ++i)
    {
        const auto& f = fields[i];
        futil::path field_path("databases",series,"fields",f.name,
                               index_slot->timestamp_file);
        futil::file field_fd(field_path,O_RDONLY);
        const auto* fti = &ftinfos[f.type];
        size_t len = (timestamp_fd_size/8)*fti->nbytes;
        if (is_first)
        {
            field_mappings.emplace_back((void*)NULL,len,PROT_READ,MAP_SHARED,
                                        field_fd.fd,0);
            field_data.emplace_back(
                (const char*)field_mappings[i].addr + start_index*fti->nbytes);
        }
        else
        {
            futil::mmap(field_mappings[i].addr,len,PROT_READ,
                        MAP_SHARED | MAP_FIXED,field_fd.fd,0);
            field_data[i] = field_mappings[i].addr;
        }
    }

    // Map the bitmap points.  Bitmap files are always 32K in size.
    bitmap_offset = start_index;
    for (size_t i=0; i<fields.size(); ++i)
    {
        const auto& f = fields[i];
        futil::path bitmap_path("databases",series,"bitmaps",f.name,
                                index_slot->timestamp_file);
        futil::file bitmap_fd(bitmap_path,O_RDONLY);
        if (is_first)
        {
            bitmap_mappings.emplace_back((void*)NULL,32*1024,PROT_READ,
                                         MAP_SHARED,bitmap_fd.fd,0);
        }
        else
        {
            futil::mmap(bitmap_mappings[i].addr,32*1024,PROT_READ,
                        MAP_SHARED | MAP_FIXED,bitmap_fd.fd,0);
        }
    }
}

tsdb::select_op_first::select_op_first(const futil::path& series,
    const std::vector<std::string>& field_names, uint64_t _t0, uint64_t _t1,
    uint64_t limit):
        select_op(series,field_names,_t0,_t1,limit)
{
    // If we have no work to do, return.
    if (fields.empty())
        return;

    // Log what we are going to do.
    printf("SELECT [ ");
    for (const auto& f : fields)
    {
        const auto* ftinfo = &ftinfos[f.type];
        printf("%s/%s ",f.name.c_str(),ftinfo->name);
    }
    printf("] FROM %s WHERE %llu <= time_ns <= %llu LIMIT %llu\n",
           series.c_str(),t0,t1,limit);

    // Find the target slot.  std::upper_bound returns the first slot greater
    // than the requested value, meaning that t0 must appear in the slot
    // preceding it - IF t0 appears at all.  It could be the case that t0 is
    // a value in the gap between slots.
    index_slot = std::upper_bound(index_begin,index_end,t0);
    kassert(index_slot != index_begin);
    --index_slot;

    // And map it all.
    _advance(true);
}

tsdb::select_op_last::select_op_last(const futil::path& series,
    const std::vector<std::string>& field_names, uint64_t _t0, uint64_t _t1,
    uint64_t limit):
        select_op(series,field_names,_t0,_t1,limit)
{
    // If we have no work to do, return.
    if (fields.empty())
        return;

    // Log what we are going to do.
    printf("SELECT [ ");
    for (const auto& f : fields)
    {
        const auto* ftinfo = &ftinfos[f.type];
        printf("%s/%s ",f.name.c_str(),ftinfo->name);
    }
    printf("] FROM %s WHERE %llu <= time_ns <= %llu LAST %llu\n",
           series.c_str(),t0,t1,limit);

    // Find the location of both t0 and t1.
    auto* t0_index_slot = std::upper_bound(index_begin,index_end,t0);
    auto* t1_index_slot = std::upper_bound(index_begin,index_end,t1);
    kassert(t0_index_slot != index_begin);
    kassert(t1_index_slot != index_begin);
    --t0_index_slot;
    --t1_index_slot;

    // Figure out how many full middle slots there are.
    size_t n_middle_slots;
    if (t0_index_slot + 1 < t1_index_slot)
        n_middle_slots = t1_index_slot - t0_index_slot - 1;
    else
        n_middle_slots = 0;

    // Now, we need to map both slots and find the beginning and ending of the
    // target range.
    futil::path time_ns_path("databases",series,"time_ns");
    futil::file t0_file(time_ns_path + t0_index_slot->timestamp_file,O_RDONLY);
    futil::file t1_file(time_ns_path + t1_index_slot->timestamp_file,O_RDONLY);
    auto t0_mmap = t0_file.mmap(NULL,t0_file.lseek(0,SEEK_END),PROT_READ,
                                MAP_SHARED,0);
    auto t1_mmap = t1_file.mmap(NULL,t1_file.lseek(0,SEEK_END),PROT_READ,
                                MAP_SHARED,0);

    // Search for the beginning and end timestamps.
    auto* t0_data_begin = (const uint64_t*)t0_mmap.addr;
    auto* t1_data_begin = (const uint64_t*)t1_mmap.addr;
    auto* t0_data_end = t0_data_begin + t0_mmap.len/8;
    auto* t1_data_end = t1_data_begin + t1_mmap.len/8;
    auto* t0_lower = std::lower_bound(t0_data_begin,t0_data_end,t0);
    auto* t1_upper = std::upper_bound(t1_data_begin,t1_data_end,t1);
    size_t t0_index = t0_lower - t0_data_begin;
    size_t t1_index = t1_upper - t1_data_begin;

    // Figure out how many points are actually available.
    size_t t0_avail_points;
    size_t t1_avail_points;
    size_t avail_points;
    if (t0_index_slot != t1_index_slot)
    {
        t0_avail_points = t0_mmap.len/8 - (t0_lower - t0_data_begin);
        t1_avail_points = t1_upper - t1_data_begin;
        avail_points = t0_avail_points + t1_avail_points +
            (2048*1024/8)*n_middle_slots;
    }
    else
        t0_avail_points = t1_avail_points = avail_points = t1_index - t0_index;

    // If there are more points available than requested, then we need to
    // seek forward.
    if (avail_points > rem_limit)
    {
        // Consume points until we are in the right slot.
        size_t seek_points = avail_points - rem_limit;
        while (seek_points >= t0_avail_points)
        {
            kassert(t0_index_slot != t1_index_slot);
            seek_points -= t0_avail_points;
            if (++t0_index_slot == t1_index_slot)
                t0_avail_points = t1_avail_points;
            else
                t0_avail_points = 2048*1024/8;
        }

        // Extract the timestamp from the right index.
        size_t t1_index = t1_upper - t1_data_begin;
        size_t index = (t1_index - rem_limit) % (2048*1024/8);
        t0_file.open(time_ns_path + t0_index_slot->timestamp_file,O_RDONLY);
        t0_file.lseek(index*8,SEEK_SET);
        t0 = t0_file.read_u64();
    }
    else
        rem_limit = avail_points;

    // Log what we are going to do.
    printf("=> SELECT [ ");
    for (const auto& f : fields)
    {
        const auto* ftinfo = &ftinfos[f.type];
        printf("%s/%s ",f.name.c_str(),ftinfo->name);
    }
    printf("] FROM %s WHERE %llu <= time_ns <= %llu LIMIT %llu\n",
           series.c_str(),t0,t1,rem_limit);

    // And map it all.
    index_slot = t0_index_slot;
    _advance(true);
}

void
tsdb::write_series(const futil::path& series, size_t npoints, size_t data_len,
    const void* data)
{
    // Predicates when writing a series:
    //  1. If the series/time_last file exists then the series is fully
    //     constructed and at least the following paths exist:
    //
    //      series/index
    //      series/time_ns
    //      series/time_first
    //      series/time_last
    //      series/<field>/ for all fields
    //
    //  2. Points stored in time_ns and field data files are valid up to the
    //     timestamp stored in the time_last file.  The time_ns and field data
    //     files may contain partial points past time_last if a write operation
    //     was interrupted.  That is:
    //
    //      time_last <= last indexed timestamp
    //
    //  3. If an entry exists in the index file, then the time_ns and data
    //     files for that entry are guaranteed to exist (but may be empty).
    //
    //  4. There may be random garbage at the end of timestamp and field files
    //     for indices that would correspond to points past time_last.  These
    //     are points that would have been partially written before a crash.
    //     This garbage data will eventually be overwritten if new points come
    //     in, but we will never access it otherwise.
    futil::path measurement = series_to_measurement(series);

    std::vector<field> fields;
    futil::path schema_path("databases",measurement,"schema.txt");
    parse_schema(schema_path,fields);

    // Compute the expected data length, starting with the timestamps and then
    // each field.
    std::vector<const uint64_t*> field_bitmap_ptrs;
    std::vector<const char*> field_data_ptrs;
    auto* data_ptr = (const char*)data + npoints*8;
    for (auto& f : fields)
    {
        const auto* fti     = &ftinfos[f.type];
        size_t bitmap_words = ceil_div<size_t>(npoints,64);
        size_t data_words   = ceil_div<size_t>(npoints*fti->nbytes,8);

        field_bitmap_ptrs.push_back((const uint64_t*)data_ptr);
        data_ptr += bitmap_words*8;
        field_data_ptrs.push_back(data_ptr);
        data_ptr += data_words*8;
    }
    size_t expected_len = data_ptr - (const char*)data;
    if (data_len != expected_len)
    {
        printf("Expected %zu bytes of data, got %zu bytes.\n",
               expected_len,data_len);
        throw futil::errno_exception(EINVAL);
    }

    // Find the first and last time stamps and ensure that the timestamps are
    // in strictly-increasing order.
    const auto* time_data = (const uint64_t*)data;
    uint64_t time_first   = time_data[0];
    uint64_t time_last    = time_data[npoints-1];
    for (size_t i=1; i<npoints; ++i)
    {
        if (time_data[i] <= time_data[i-1])
            throw futil::errno_exception(EINVAL);
    }

    // Paths of interest.
    futil::path series_path("databases",series);
    futil::path index_path(series_path,"index");
    futil::path time_first_path(series_path,"time_first");
    futil::path time_last_path(series_path,"time_last");
    futil::path time_ns_path(series_path,"time_ns");
    futil::path fields_path(series_path,"fields");
    futil::path bitmaps_path(series_path,"bitmaps");

    // Files we use throughout this routine.
    futil::file index_fd;
    futil::file time_last_fd;

    // Try to acquire a write lock on the series.
    time_last_fd.open_if_exists(time_last_path,O_RDWR | O_EXLOCK);

    // If the time_last file doesn't exist, we need to create the series.
    if (time_last_fd.fd == -1)
    {
        // Acquire the lock to create the series.
        futil::path create_series_lock_path("databases",
                                            measurement,
                                            "create_series_lock");
        futil::file create_series_lock_fd(create_series_lock_path,
                                          O_WRONLY | O_EXLOCK);

        // Someone else may have created it in the meantime.
        time_last_fd.open_if_exists(time_last_path,O_RDWR | O_EXLOCK);
        if (time_last_fd.fd == -1)
        {
            // Create the series directory.
            futil::mkdir_if_not_exists(series_path,0770);

            // Create the time sub-directory.
            futil::mkdir_if_not_exists(time_ns_path,0770);

            // Create the fields sub-directory.
            futil::mkdir_if_not_exists(fields_path,0770);

            // Create the bitmaps sub-directory.
            futil::mkdir_if_not_exists(bitmaps_path,0770);

            // Create all of the field and bitmap sub-directories.
            for (const auto& f : fields)
            {
                futil::mkdir_if_not_exists(fields_path + f.name,0770);
                futil::mkdir_if_not_exists(bitmaps_path + f.name,0770);
            }

            // Create the time_first file and populate it with the first
            // timestamp.
            futil::file time_first_fd(time_first_path,
                                      O_CREAT | O_TRUNC | O_WRONLY,0660);
            time_first_fd.write_all(&time_first,sizeof(uint64_t));
            time_first_fd.fsync();
            time_first_fd.close();

            // Create an empty index file, and take a shared lock on it to
            // prevent any other client from deleting from the file.
            index_fd.open(index_path,O_CREAT | O_TRUNC | O_RDWR | O_SHLOCK,
                          0660);
            index_fd.fsync();

            // Write barrier so that time_last_fd is the last thing to go out.
            index_fd.fcntl(F_BARRIERFSYNC);

            // Create the time_last file and populate it with 0.
            time_last_fd.open(time_last_path,
                              O_RDWR | O_EXCL | O_CREAT | O_EXLOCK,0660);
            uint64_t zero = 0;
            time_last_fd.write_all(&zero,sizeof(uint64_t));
            time_last_fd.lseek(0,SEEK_SET);
        }
    }

    // If we don't have the index file open yet (it may be open already if we
    // just created it above), do so now.
    if (index_fd.fd == -1)
        index_fd.open(index_path,O_RDWR | O_SHLOCK);

    // The series exists and we hold time_last_fd with an exclusive lock,
    // preventing anyone else from writing to the series until we are done.
    // All of which is kind of insane, since two writers to the same series,
    // when we can only append new data, conflict with each other by definition.

    // Read the last stored timestamp.
    uint64_t time_last_stored;
    time_last_fd.read_all(&time_last_stored,sizeof(uint64_t));
    time_last_fd.lseek(0,SEEK_SET);

    // If the timestamps we are appending start before our last stored
    // timestamp, we should validate and discard the overlapping part and then
    // only write the new part.
    size_t n_overlap_points = 0;
    if (time_first <= time_last_stored)
    {
        // TODO: We should get time_first_stored and silently discard any
        // points before that point in time - those are points that have
        // presumably been deleted and we have no way to verify if they are
        // proper overwrites or not.  This could happen in the following
        // scenario:
        //
        //  1. Client has a store of local points.
        //  2. Client transmits points to server.
        //  3. Server writes points but crashes before transmitting ACK back to
        //     client.
        //  4. Client crashes and goes away.
        //  5. Server is restored, (lots of?) time passes, someone deletes old
        //     points, maybe as part of an automated cleanup script, leaving
        //     some of the client's later points behind but purging the earlier
        //     ones.
        //  6. Client is finally restored and tries to retransmit its entire
        //     store of local points.
        //
        // In step 6, the client will be transmitting points from its local
        // store that precede the server's time_first value.  The client has no
        // way of knowing (without doing a select op which we'd rather avoid
        // for the client) what time_first is on the server.  There's no
        // possible way for the client to even store those old points on the
        // server now since that would be rewriting history which is forbidden,
        // so the options are to either halt the client which now has points it
        // can't write anywhere, or to just discard the very-old points.
        //
        // Select all data points from the overlap.
        uint64_t overlap_time_last = MIN(time_last,time_last_stored);
        std::vector<std::string> field_names;
        field_names.reserve(fields.size());
        for (const auto& f : fields)
            field_names.emplace_back(f.name);
        select_op_first op(series,field_names,time_first,overlap_time_last,-1);
        kassert(op.npoints);
        
        // Compare all the points.
        for (;;)
        {
            // Start by doing a memcmp of all the timestamps.
            if (memcmp(time_data,op.timestamp_data,op.npoints*8))
            {
                printf("Overwrite mismatch in timestamps.\n");
                throw futil::errno_exception(EINVAL);
            }
            time_data += op.npoints;

            // Do a memcmp on all of the fields.
            for (size_t i=0; i<fields.size(); ++i)
            {
                const auto* fti = &ftinfos[fields[i].type];
                size_t len = op.npoints*fti->nbytes;
                if (memcmp(field_data_ptrs[i],op.field_data[i],len))
                {
                    printf("Overwrite mismatch in field %s.\n",
                           fields[i].name.c_str());
                    throw futil::errno_exception(EINVAL);
                }
                field_data_ptrs[i] += len;
            }

            // Manually check all the bitmaps.
            for (size_t i=0; i<fields.size(); ++i)
            {
                for (size_t j=0; j<op.npoints; ++j)
                {
                    bool new_bitmap = get_bitmap_bit(field_bitmap_ptrs[i],
                                                     n_overlap_points + j);
                    if (new_bitmap != op.get_bitmap_bit(i,j))
                    {
                        printf("Overwrite mismatch in bitmap %s.\n",
                               fields[i].name.c_str());
                        throw futil::errno_exception(EINVAL);
                    }
                }
            }

            // Count how many points we drop.
            n_overlap_points += op.npoints;

            if (op.is_last)
                break;

            op.advance();
        }

        npoints -= n_overlap_points;
        if (!npoints)
        {
            printf("100%% overwrite, abandoning write op.\n");
            return;
        }
        else
            printf("Dropping %zu overlapping points.\n",n_overlap_points);

        time_first = time_data[0];
    }

    // Open the most recent timestamp file if it exists, and perform validation
    // checking on it if found.
    futil::file tail_fd;
    std::vector<futil::path> field_file_paths;
    std::vector<futil::file> field_fds(fields.size());
    std::vector<futil::path> bitmap_file_paths;
    std::vector<futil::file> bitmap_fds(fields.size());
    off_t index_len = index_fd.lseek(0,SEEK_END);
    size_t nindices = index_len / sizeof(index_entry);
    size_t avail_points = 0;
    off_t pos;
    for (size_t i=0; i<nindices; ++i)
    {
        // Work from the back.
        size_t index = nindices - i - 1;

        // Open the file pointed to by the last entry in the index.
        index_entry ie;
        index_fd.lseek(index*sizeof(index_entry),SEEK_SET);
        index_fd.read_all(&ie,sizeof(ie));

        // Generate all the paths
        futil::path timestamp_file_path(time_ns_path,ie.timestamp_file);
        field_file_paths.clear();
        bitmap_file_paths.clear();
        for (const auto& field : fields)
        {
            field_file_paths.emplace_back(fields_path,field.name,
                                          ie.timestamp_file);
            bitmap_file_paths.emplace_back(bitmaps_path,field.name,
                                           ie.timestamp_file);
        }

        // Open the tail file.
        tail_fd.open(timestamp_file_path,O_RDWR);

        // Validate the file size.
        pos = tail_fd.lseek(0,SEEK_END);
        if (pos > 2*1024*1024)
            throw futil::errno_exception(EFBIG);
        if (pos % sizeof(uint64_t))
            throw futil::errno_exception(EINVAL);

        // For a non-empty timestamp file, try to find the time_last entry so
        // we can append.
        if (pos >= 8)
        {
            // Read the last entry, and leave the file position at the end of
            // the file.
            uint64_t tail_fd_first_entry;
            uint64_t tail_fd_last_entry;
            tail_fd.lseek(0,SEEK_SET);
            tail_fd.read_all(&tail_fd_first_entry,sizeof(uint64_t));
            tail_fd.lseek((pos & ~7) - 8,SEEK_SET);
            tail_fd.read_all(&tail_fd_last_entry,sizeof(uint64_t));

            // Fast path - if the last entry in the last timestamp file matches
            // the time_last file, then the series is consistent and we can
            // break out to writing points.
            if (tail_fd_last_entry == time_last_stored)
            {
                // Open all of the field and bitmap files with the same name as
                // the timestamp file.  They are guaranteed to exist.
                for (size_t j=0; j<field_fds.size(); ++j)
                {
                    field_fds[j].open(field_file_paths[j],O_WRONLY);
                    bitmap_fds[j].open(bitmap_file_paths[j],O_RDWR);
                }
                avail_points = (2*1024*1024 - pos) / sizeof(uint64_t);
                break;
            }

            // Slow path.  The time_last file and the last timestamp stored in
            // the file pointed to by the last index entry differ.  This
            // indicates that we are recovering from a previous crash.

            // Consistency check - the following predicate should always be
            // held:
            //
            //      time_last <= last indexed timestamp
            //
            // This is because incrementing time_last is the very last step
            // when writing data points.  So, if the value we see stored in
            // the time_last file is larger than the value we found in the
            // index, the series is corrupt.
            if (tail_fd_last_entry < time_last_stored)
                throw futil::errno_exception(EINVAL);

            // There are extra timestamp entries compared to the value stored
            // in the time_last file.  This means a write_points operation was
            // previously interrupted and we need to clean up the series.
            // First, check if there is any live data at all in this timestamp
            // file.
            if (tail_fd_first_entry <= time_last_stored)
            {
                // We have:
                //
                //  tail_fd_first_entry <= time_last_stored <=
                //      tail_fd_last_entry
                //
                // so, the first timestamp in this file is a live value.  We
                // need to search the file for the index of the timestamp that
                // has the time_last value.  When found, we should set pos to
                // immediately after that entry, lseek() there, ftruncate() and
                // exit with success.
                auto m = tail_fd.mmap(0,pos,PROT_READ,MAP_SHARED,0);
                const auto* iter_first = &((const uint64_t*)m.addr)[0];
                const auto* iter_last = &((const uint64_t*)m.addr)[pos/8];
                const auto iter = std::lower_bound(iter_first,iter_last,
                                                   time_last_stored);

                // Sanity check: we already know that some position in this
                // file should satisfy the lower_bound criteria, so
                // something is massively broken if this fails.
                kassert(iter < iter_last);

                // If we don't have a match, the series is corrupt.
                if (*iter != time_last_stored)
                    throw futil::errno_exception(EINVAL);

                // We have found the time_last value in the timestamp file!
                // Truncate the file as a convenience for future self and exit
                // with success.
                pos = (const char*)iter - (const char*)iter_first + 8;
                tail_fd.lseek(pos,SEEK_SET);
                tail_fd.truncate(pos);
                avail_points = (2*1024*1024 - pos) / sizeof(uint64_t);
                break;
            }

            // There are timestamps in the file, but none of them are live
            // because all of them are after the stored time_last value.  We
            // just discard this file completely.
        }

        // Slow path - the index entry points to an empty file, or we
        // determined that it is entirely full of timestamp values that haven't
        // gone live yet.  Delete the file and remove the index entry.
        for (auto& fd : field_fds)
            fd.close();
        for (auto& fd : bitmap_fds)
            fd.close();
        for (const auto& field_file_path : field_file_paths)
            futil::unlink_if_exists(field_file_path);
        for (const auto& bitmap_file_path : bitmap_file_paths)
            futil::unlink_if_exists(bitmap_file_path);
        tail_fd.fcntl(F_BARRIERFSYNC);
        tail_fd.close();
        futil::unlink(timestamp_file_path);
        index_fd.flock(LOCK_EX);
        index_fd.truncate(index*sizeof(index_entry));
        index_fd.flock(LOCK_SH);
    }

    // Write points.  The variable pos is the absolute position in the
    // timestamp file; this should be used to calculate the index for other
    // field sizes.
    size_t rem_points = npoints;
    uint64_t last_written_timestamp = 0;
    size_t src_bitmap_offset = n_overlap_points;
    while (rem_points)
    {
        // If we have overflowed the timestamp file, or we have an empty index,
        // we need to grow into a new timestamp file.
        if (!avail_points)
        {
            // Figure out what to name the new files.
            std::string time_data_str = std::to_string(time_data[0]);

            // Create and open all new field and bitmap files.  We fully-
            // populate the bitmap files with zeroes even though they don't
            // have any data yet.  This barely wastes any space and makes
            // bitmap accounting via mmap() much simpler.
            field_file_paths.clear();
            bitmap_file_paths.clear();
            for (size_t i=0; i<fields.size(); ++i)
            {
                field_file_paths.emplace_back(fields_path,fields[i].name,
                                              time_data_str);
                field_fds[i].open(field_file_paths[i],
                                  O_CREAT | O_TRUNC | O_WRONLY,0660);
                field_fds[i].fsync();

                bitmap_file_paths.emplace_back(bitmaps_path,fields[i].name,
                                               time_data_str);
                bitmap_fds[i].open(bitmap_file_paths[i],
                                   O_CREAT | O_TRUNC | O_RDWR,0660);
                bitmap_fds[i].truncate(2048*1024/8/8);
                bitmap_fds[i].fsync();
            }

            // Create the new timestamp file.  As when first creating the
            // series, it is possible that someone got this far when growing
            // the series but crashed before updating the index file.  Truncate
            // the file if it exists.
            futil::path time_path(series_path,"time_ns",time_data_str);
            tail_fd.open(time_path,O_CREAT | O_TRUNC | O_WRONLY,0660);
            avail_points = 2*1024*1024 / sizeof(uint64_t);
            pos = 0;

            // TODO: If we crash here, there is now a dangling, empty timestamp
            // file.  It isn't in the index, so will never be accessed, but it
            // will still exist.  If someone tries to rewrite the timestamp
            // that created this file in the future then we will truncate/reuse
            // it.  However, if the client doesn't try to rewrite this (maybe
            // the client goes away before our database can recover) then it
            // will sit here forever.  We may wish to periodically scan for
            // orphaned files and delete them.

            // Barrier before we update the index file.
            tail_fd.fcntl(F_BARRIERFSYNC);

            // Add the timestamp file to the index.
            index_entry ie;
            ie.time_ns = time_data[0];
            strcpy(ie.timestamp_file,time_data_str.c_str());
            index_fd.write_all(&ie,sizeof(ie));
        }

        // Compute how many points we can write.
        size_t write_points = MIN(rem_points,avail_points);

        // Write out all the field files first.
        size_t timestamp_index = pos / 8;
        for (size_t i=0; i<fields.size(); ++i)
        {
            const auto* fti = &ftinfos[fields[i].type];
            size_t nbytes = write_points*fti->nbytes;
            field_fds[i].lseek(timestamp_index*fti->nbytes,SEEK_SET);
            field_fds[i].write_all(field_data_ptrs[i],nbytes);
            field_data_ptrs[i] += nbytes;
        }

        // Update all the bitmap files.
        for (size_t i=0; i<fields.size(); ++i)
        {
            auto m = bitmap_fds[i].mmap(0,2048*1024/8/8,PROT_READ | PROT_WRITE,
                                        MAP_SHARED,0);
            auto* bitmap = (uint64_t*)m.addr;
            size_t bitmap_index = timestamp_index;
            for (size_t j=0; j<write_points; ++j)
            {
                if (get_bitmap_bit(field_bitmap_ptrs[i],src_bitmap_offset + j))
                    set_bitmap_bit(bitmap,bitmap_index,1);
                ++bitmap_index;
            }
            m.msync();
        }

        // Update the timestamp file.
        tail_fd.write_all(time_data,write_points*sizeof(uint64_t));
        last_written_timestamp = time_data[write_points - 1];
        time_data             += write_points;
        rem_points            -= write_points;
        pos                   += write_points*sizeof(uint64_t);
        avail_points          -= write_points;
        src_bitmap_offset     += write_points;

        // Synchronize the points we have written.
        for (auto& fd : field_fds)
        {
            fd.fsync();
            fd.close();
        }
        for (auto& fd : bitmap_fds)
        {
            fd.fsync();
            fd.close();
        }
        tail_fd.fsync();
        tail_fd.close();

        // Issue a barrier and write the end timestamp.
        time_last_fd.fcntl(F_BARRIERFSYNC);
        if (last_written_timestamp)
        {
            time_last_fd.write_all(&last_written_timestamp,
                                   sizeof(uint64_t));
            time_last_fd.lseek(0,SEEK_SET);
        }
    }
}

void
tsdb::create_measurement(const futil::path& path,
    const std::vector<field>& fields)
{
    if (path.empty() || path[0] == '/' || path.count_components() != 2)
        throw futil::errno_exception(EINVAL);

    futil::path dir_path("databases",path);
    futil::mkdir(dir_path,0770);

    futil::path create_series_path(dir_path,"create_series_lock");
    futil::file fd;
    try
    {
        fd.open(create_series_path,O_WRONLY | O_CREAT | O_EXCL,0660);
    }
    catch (const futil::exception&)
    {
        rmdir(dir_path);
        throw;
    }
    fd.close();

    futil::path schema_path(dir_path,"schema.txt");
    try
    {
        fd.open(schema_path,O_WRONLY | O_CREAT | O_EXCL | O_EXLOCK,0440);
    }
    catch (const futil::exception&)
    {
        ::unlink(create_series_path);
        rmdir(dir_path);
        throw;
    }
    
    try
    {
        for (auto& f : fields)
        {
            futil::path line(std::to_string(f.type),f.name);
            fd.write_all((line._path + "\n").c_str(),line.size() + 1);
        }
    }
    catch (const futil::exception&)
    {
        ::unlink(create_series_path);
        ::unlink(schema_path);
        rmdir(dir_path);
        throw;
    }
}

void
tsdb::create_database(const char* name)
{
    futil::mkdir(futil::path("databases",name),0770);
}

void
tsdb::init()
{
    futil::mkdir("databases",0770);
}
