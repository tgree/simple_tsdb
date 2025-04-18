// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_MEASUREMENT_H
#define __SRC_LIBTSDB_MEASUREMENT_H

#include "exception.h"
#include "constants.h"
#include <futil/futil.h>
#include <span>
#include <hdr/kassert.h>
#include <hdr/kmath.h>
#include <hdr/static_vector.h>

namespace tsdb
{
    struct database;

    template<typename T>
    using field_vector = static_vector<T,MAX_FIELDS>;

    enum field_type : uint8_t
    {
        FT_BOOL = 1,
        FT_U32  = 2,
        FT_U64  = 3,
        FT_F32  = 4,
        FT_F64  = 5,
        FT_I32  = 6,
        FT_I64  = 7,
    };
#define LAST_FIELD_TYPE     7

    struct field_type_info
    {
        field_type  type;
        uint8_t     nbytes;
        char        name[5];
    };

    constexpr const field_type_info ftinfos[] =
    {
        [0]             = {(field_type)0,0,""},
        [tsdb::FT_BOOL] = {tsdb::FT_BOOL,1,"bool"},
        [tsdb::FT_U32]  = {tsdb::FT_U32,4,"u32"},
        [tsdb::FT_U64]  = {tsdb::FT_U64,8,"u64"},
        [tsdb::FT_F32]  = {tsdb::FT_F32,4,"f32"},
        [tsdb::FT_F64]  = {tsdb::FT_F64,8,"f64"},
        [tsdb::FT_I32]  = {tsdb::FT_I32,4,"i32"},
        [tsdb::FT_I64]  = {tsdb::FT_I64,8,"i64"},
    };

#define SCHEMA_VERSION  1
    struct schema_entry
    {
        field_type  type;
        uint8_t     version;
        uint16_t    index : 6,
                    offset : 10;
        char        name[124];
    };
    KASSERT(sizeof(schema_entry) == 128);

    struct measurement
    {
        const database&                 db;
        futil::directory                dir;
        futil::file                     schema_fd;
        futil::mapping                  schema_mapping;
        std::span<const schema_entry>   fields;

        std::vector<std::string> list_series() const
        {
            auto v = dir.listdirs();
            std::sort(v.begin(),v.end());
            return v;
        }

        size_t compute_write_chunk_len(size_t npoints,
                                       size_t bitmap_offset = 0) const
        {
            // Timestamps first.
            size_t len = npoints*8;

            // For each field: bitmap comes first, padded to 64-bits, then
            // field data comes next, padded to 64-bits.
            for (auto& f : fields)
            {
                const auto* fti = &ftinfos[f.type];
                len += ceil_div<size_t>(npoints + bitmap_offset,64)*8;
                len += ceil_div<size_t>(npoints*fti->nbytes,8)*8;
            }

            return len;
        }

        field_vector<const schema_entry*> gen_entries(
            const std::vector<std::string>& field_names) const
        {
            // Generate a lookup table for the specified field names.  An empty
            // list is assumed to mean all fields in their natural order.
            field_vector<const schema_entry*> entries;
            if (!field_names.empty())
            {
                uint64_t field_mask = 0;
                for (auto& field_name : field_names)
                {
                    bool found = false;
                    for (auto& f : fields)
                    {
                        if (field_name == f.name)
                        {
                            if (field_mask & (1 << f.index))
                                throw duplicate_field_exception();
                            field_mask |= (1 << f.index);
                            entries.emplace_back(&f);
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                        throw no_such_field_exception();
                }
            }
            else
            {
                for (auto& f : fields)
                    entries.emplace_back(&f);
            }
            return entries;
        }

        measurement(const database& db, const futil::path& path);
    };

    // Creates a new measurement in the specified database.
    void create_measurement(const database& db, const futil::path& name,
                            const std::vector<schema_entry>& fields);
}

#endif /* __SRC_LIBTSDB_MEASUREMENT_H */
