// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_SELECT_OP_H
#define __SRC_LIBTSDB_SELECT_OP_H

#include "series.h"
#include <hdr/fixed_vector.h>
#include <hdr/auto_buf.h>
#include <hdr/kmath.h>

namespace tsdb
{
    // Class used to get data from a series.
    struct select_op
    {
        // Snapshot of the series when the query was initiated.  We keep
        // time_first_fd open with a shared lock to prevent anyone else from
        // deleting points during the query.  For time_last, we don't care if
        // someone adds points later, we only return the points from our
        // current snapshot of the live range.
        const bool              mmap_timestamps;
        const series_read_lock& read_lock;
        const uint64_t          time_last;
        futil::file             index_fd;
        futil::mapping          index_mapping;
        const index_entry*      index_begin;
        const index_entry*      index_end;

        // Query parameters.
        uint64_t                    t0;
        uint64_t                    t1;
        uint64_t                    rem_limit;
        fixed_vector<schema_entry>  fields;

        // Mapping objects to track mmap()-ed files.
        const index_entry*              index_slot;
        futil::mapping                  timestamp_mapping;
        auto_buf                        timestamp_buf;
        fixed_vector<futil::mapping>    field_mappings;
        fixed_vector<futil::mapping>    bitmap_mappings;

        // State of the current set of results.
        bool                            is_last;
        size_t                          npoints;
        size_t                          bitmap_offset;
        const uint64_t*                 timestamp_data;
        fixed_vector<const void*>       field_data;

        inline void advance()
        {
            _advance(false);
        }

        constexpr size_t compute_chunk_len() const
        {
            size_t N = npoints;
            size_t M = fields.size();
            size_t S = 0;
            for (size_t i=0; i<M; ++i)
            {
                const auto& f = fields[i];
                const auto* fti = &ftinfos[f.type];
                S += round_up_pow2(N*fti->nbytes,8);
            }
            size_t bitmap_begin = bitmap_offset / 64;
            size_t bitmap_end = ceil_div<size_t>(bitmap_offset + N,64);
            size_t bitmap_n = bitmap_end - bitmap_begin;
            return 8*(N + bitmap_n*M) + S;
        }

        constexpr bool get_bitmap_bit(size_t field_index, size_t i) const
        {
            return tsdb::get_bitmap_bit(
                (const uint64_t*)bitmap_mappings[field_index].addr,
                bitmap_offset + i);
        }

        constexpr bool is_field_null(size_t field_index, size_t i) const
        {
            return !get_bitmap_bit(field_index,i);
        }

        template<typename T, size_t FieldIndex>
        T get_field(size_t i) const
        {
            return ((const T*)field_data[FieldIndex])[i];
        }

    protected:
        void _advance(bool is_first);

        select_op(const series_read_lock& read_lock,
                  const std::vector<std::string>& field_names,
                  uint64_t t0, uint64_t t1, uint64_t limit,
                  bool mmap_timestamps = false);
    };

    struct select_op_first : public select_op
    {
        select_op_first(const series_read_lock& read_lock,
                        const futil::path& series_id,
                        const std::vector<std::string>& field_names,
                        uint64_t t0, uint64_t t1, uint64_t limit);
    };

    struct select_op_last : public select_op
    {
        select_op_last(const series_read_lock& read_lock,
                       const futil::path& series_id,
                       const std::vector<std::string>& field_names,
                       uint64_t t0, uint64_t t1, uint64_t limit);
    };
}

#endif /* __SRC_LIBTSDB_SELECT_OP_H */
