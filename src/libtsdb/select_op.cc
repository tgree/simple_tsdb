// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "select_op.h"
#include "tsdb.h"
#include <inttypes.h>
#include <algorithm>

#define WITH_GZFILEOP
#include <zlib-ng/zlib-ng.h>

tsdb::select_op::select_op(const series_read_lock& read_lock,
    const std::vector<std::string>& field_names, uint64_t _t0, uint64_t _t1,
    uint64_t limit, bool mmap_timestamps):
        mmap_timestamps(mmap_timestamps),
        read_lock(read_lock),
        time_last(futil::file(read_lock.series_dir,"time_last",
                              O_RDONLY).read_u64()),
        index_begin(NULL),
        index_end(NULL),
        t0(MAX(_t0,read_lock.time_first)),
        t1(MIN(_t1,time_last)),
        rem_limit(limit),
        fields(field_names.size() ?: read_lock.m.fields.size()),
        index_slot(NULL),
        timestamp_mapping(NULL,CHUNK_FILE_SIZE,PROT_NONE,
                          MAP_ANONYMOUS | MAP_PRIVATE,-1,0),
        timestamp_buf(mmap_timestamps ? 0 : CHUNK_FILE_SIZE),
        field_mappings(field_names.size() ?: read_lock.m.fields.size()),
        bitmap_mappings(field_names.size() ?: read_lock.m.fields.size()),
        is_last(true),
        npoints(0),
        bitmap_offset(0),
        timestamp_data(NULL),
        field_data(field_names.size() ?: read_lock.m.fields.size())
{
    // Validate the time and limit ranges.
    if (read_lock.time_first > time_last)
        return;
    if (_t0 > _t1)
        return;
    if (_t1 < read_lock.time_first)
        return;
    if (_t0 > time_last)
        return;
    if (rem_limit == 0)
        return;

    // Map the index.  Since time_first <= time_last, there must be at least one
    // measurement, which means there must be at least one chunk, which means
    // there must be at least one slot in the index file - so we won't attempt
    // to mmap() an empty file which always fails.
    //
    // Note: if we crashed during a delete operation, it is possible that the
    // first index entries in the index file point to chunks that all precede
    // time_first.  We must not access those chunks.  Since we force t0 to be
    // in the range of time_first and time_last, we should be safe and the
    // extra slots will just make our binary searched on the index file a bit
    // slower.
    index_fd.open(read_lock.series_dir,"index",O_RDONLY);
    index_mapping.map(NULL,index_fd.lseek(0,SEEK_END),PROT_READ,MAP_SHARED,
                      index_fd.fd,0),
    index_begin = (const index_entry*)index_mapping.addr;
    index_end = index_begin + index_mapping.len / sizeof(index_entry);

    // Fetch the schema and figure out which fields we are going to return.
    // We are sure to keep the fields vector in the same order as the field
    // names that were passed in; this is the order in which we will return
    // the data points.
    if (!field_names.empty())
    {
        for (const auto& fn : field_names)
        {
            bool found = false;
            for (const auto& sf : read_lock.m.fields)
            {
                if (sf.name == fn)
                {
                    fields.emplace_back(sf);
                    found = true;
                    break;
                }
            }
            if (!found)
                throw tsdb::no_such_field_exception();
        }
    }
    else
    {
        for (const auto& sf : read_lock.m.fields)
            fields.emplace_back(sf);
    }
}

void
tsdb::select_op::_advance(bool is_first)
{
    if (!is_first)
    {
        if (is_last)
            throw tsdb::end_of_select_exception();
        ++index_slot;
    }

    // Map the timestamp file associated with the target slot.
    futil::directory time_ns_dir(read_lock.series_dir,"time_ns");
    futil::file timestamp_fd(time_ns_dir,index_slot->timestamp_file,O_RDONLY);
    off_t timestamp_fd_size = timestamp_fd.lseek(0,SEEK_END);
    const uint64_t* timestamps_begin;
    if (mmap_timestamps)
    {
        futil::mmap(timestamp_mapping.addr,timestamp_fd_size,PROT_READ,
                    MAP_SHARED | MAP_FIXED,timestamp_fd.fd,0);
        timestamps_begin = (const uint64_t*)timestamp_mapping.addr;
    }
    else
    {
        timestamp_fd.lseek(0,SEEK_SET);
        timestamp_fd.read_all(timestamp_buf,timestamp_fd_size);
        timestamps_begin = (const uint64_t*)timestamp_buf.data;
    }
    const auto* timestamps_end = timestamps_begin + timestamp_fd_size/8;

    // Find the first timestamp >= t0.
    size_t start_index;
    if (!is_first)
    {
        // The first timestamp in our range is right at the start of the file.
        timestamp_data = timestamps_begin;
        start_index = 0;
    }
    else
    {
        // The first timestamp could be partway through the file.
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
            timestamp_fd.open(time_ns_dir,index_slot->timestamp_file,O_RDONLY);
            timestamp_fd_size = timestamp_fd.lseek(0,SEEK_END);
            if (mmap_timestamps)
            {
                futil::mmap(timestamp_mapping.addr,timestamp_fd_size,PROT_READ,
                            MAP_SHARED | MAP_FIXED,timestamp_fd.fd,0);
            }
            else
            {
                timestamp_fd.lseek(0,SEEK_SET);
                timestamp_fd.read_all(timestamp_buf,timestamp_fd_size);
            }
            timestamp_data = timestamps_begin;
            timestamps_end = timestamps_begin + timestamp_fd_size/8;
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

    // Map or decompress the data points.
    futil::directory fields_dir(read_lock.series_dir,"fields");
    for (size_t i=0; i<fields.size(); ++i)
    {
        const auto& f = fields[i];
        const auto* fti = &ftinfos[f.type];
        size_t len = (timestamp_fd_size/8)*fti->nbytes;

        //  Try opening an uncompressed file first.  If one exists, we must use
        //  it; any compressed file that exists could be the result of a
        //  partial compression operation that then crashed before completing.
        try
        {
            futil::file field_fd(fields_dir,
                                 futil::path(f.name,index_slot->timestamp_file),
                                 O_RDONLY);

            if (is_first)
            {
                field_mappings.emplace_back((void*)NULL,len,PROT_READ,
                                            MAP_SHARED,field_fd.fd,0);
                field_data.emplace_back((const char*)field_mappings[i].addr +
                                        start_index*fti->nbytes);
            }
            else
            {
                futil::mmap(field_mappings[i].addr,len,PROT_READ,
                            MAP_SHARED | MAP_FIXED,field_fd.fd,0);
                field_data[i] = field_mappings[i].addr;
            }

            continue;
        }
        catch (const futil::errno_exception& e)
        {
            if (e.errnov != ENOENT)
                throw;
        }

        // No uncompressed file exists.  Try with a compressed file.  First,
        // make an anonymous backing region to hold the uncompressed data.
        if (is_first)
        {
            field_mappings.emplace_back((void*)NULL,len,PROT_READ | PROT_WRITE,
                                        MAP_ANONYMOUS | MAP_PRIVATE,-1,0);
            field_data.emplace_back((const char*)field_mappings[i].addr +
                                    start_index*fti->nbytes);
        }
        else
        {
            futil::mmap(field_mappings[i].addr,len,PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED,-1,0);
            field_data[i] = field_mappings[i].addr;
        }

        // Now, try and open the file and unzip it into the backing region.
        char gz_name[TIMESTAMP_FILE_NAME_LEN + 3];
        auto* p = stpcpy(gz_name,index_slot->timestamp_file);
        strcpy(p,".gz");
        int field_fd = futil::openat(fields_dir,futil::path(f.name,gz_name),
                                     O_RDONLY);
        gzFile gzf = zng_gzdopen(field_fd,"rb");
        int32_t zlen = zng_gzread(gzf,field_mappings[i].addr,len);
        kassert((size_t)zlen == len);
        zng_gzclose_r(gzf);
    }

    // Map the bitmap points.  Bitmap files are always fixed size.
    futil::directory bitmaps_dir(read_lock.series_dir,"bitmaps");
    bitmap_offset = start_index;
    for (size_t i=0; i<fields.size(); ++i)
    {
        const auto& f = fields[i];
        futil::path bitmap_path(f.name,index_slot->timestamp_file);
        futil::file bitmap_fd(bitmaps_dir,bitmap_path,O_RDONLY);
        if (is_first)
        {
            bitmap_mappings.emplace_back((void*)NULL,BITMAP_FILE_SIZE,PROT_READ,
                                         MAP_SHARED,bitmap_fd.fd,0);
        }
        else
        {
            futil::mmap(bitmap_mappings[i].addr,BITMAP_FILE_SIZE,PROT_READ,
                        MAP_SHARED | MAP_FIXED,bitmap_fd.fd,0);
        }
    }
}

tsdb::select_op_first::select_op_first(const series_read_lock& read_lock,
    const futil::path& series_id, const std::vector<std::string>& field_names,
    uint64_t _t0, uint64_t _t1, uint64_t limit):
        select_op(read_lock,field_names,_t0,_t1,limit)
{
    // If we have no work to do, return.
    if (fields.empty())
        return;

    // Log what we are going to do.
    printf("SELECT [ ");
    for (const auto& f : fields)
    {
        const auto* ftinfo = &ftinfos[f.type];
        printf("%s/%s ",f.name,ftinfo->name);
    }
    printf("] FROM %s WHERE %" PRIu64 " <= time_ns <= %" PRIu64
           " LIMIT %" PRIu64 "\n",
           series_id.c_str(),t0,t1,limit);

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

tsdb::select_op_last::select_op_last(const series_read_lock& read_lock,
    const futil::path& series_id, const std::vector<std::string>& field_names,
    uint64_t _t0, uint64_t _t1, uint64_t limit):
        select_op(read_lock,field_names,_t0,_t1,limit)
{
    // If we have no work to do, return.
    if (fields.empty())
        return;

    // Log what we are going to do.
    printf("SELECT [ ");
    for (const auto& f : fields)
    {
        const auto* ftinfo = &ftinfos[f.type];
        printf("%s/%s ",f.name,ftinfo->name);
    }
    printf("] FROM %s WHERE %" PRIu64 " <= time_ns <= %" PRIu64
           " LAST %" PRIu64 "\n",
           series_id.c_str(),t0,t1,limit);

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
    futil::directory time_ns_dir(read_lock.series_dir,"time_ns");
    futil::file t0_file(time_ns_dir,t0_index_slot->timestamp_file,O_RDONLY);
    futil::file t1_file(time_ns_dir,t1_index_slot->timestamp_file,O_RDONLY);
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
            CHUNK_NPOINTS*n_middle_slots;
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
                t0_avail_points = CHUNK_NPOINTS;
        }

        // Extract the timestamp from the right index.
        size_t t1_index = t1_upper - t1_data_begin;
        size_t index = (t1_index - rem_limit) % CHUNK_NPOINTS;
        t0_file.open(time_ns_dir,t0_index_slot->timestamp_file,O_RDONLY);
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
        printf("%s/%s ",f.name,ftinfo->name);
    }
    printf("] FROM %s WHERE %" PRIu64 " <= time_ns <= %" PRIu64
           " LIMIT %" PRIu64 "\n",
           series_id.c_str(),t0,t1,rem_limit);

    // And map it all.
    index_slot = t0_index_slot;
    _advance(true);
}
