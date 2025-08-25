// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_WAL_H
#define __SRC_LIBTSDB_WAL_H

#include "series.h"
#include <hdr/kmath.h>
#include <algorithm>

namespace tsdb
{
    union wal_field
    {
        uint64_t    u64;
        uint32_t    u32;
        uint8_t     u8;
        double      f64;
        float       f32;
        int64_t     i64;
        int32_t     i32;
    };

    struct wal_entry
    {
        uint64_t    time_ns;
        uint64_t    bitmap;
        wal_field   fields[];

        constexpr uint64_t get_bitmap_bit(size_t i) const
        {
            return (bitmap >> i) & 1;
        }

        constexpr bool is_field_null(size_t i) const
        {
            return !get_bitmap_bit(i);
        }

        template<typename T>
        T get_field(size_t i) const
        {
            return *(const T*)(&fields[i]);
        }

        template<typename T>
        constexpr T cast_field(size_t i, const tsdb::field_type ft)
        {
            switch (ft)
            {
                case tsdb::FT_BOOL: return fields[i].u8;
                case tsdb::FT_U32:  return fields[i].u32;
                case tsdb::FT_U64:  return fields[i].u64;
                case tsdb::FT_F32:  return fields[i].f32;
                case tsdb::FT_F64:  return fields[i].f64;
                case tsdb::FT_I32:  return fields[i].i32;
                case tsdb::FT_I64:  return fields[i].i64;
            }
            kabort();
        }
    };
    inline bool operator<(const wal_entry& lhs, const wal_entry& rhs)
    {
        return lhs.time_ns < rhs.time_ns;
    }
    inline bool operator<(const wal_entry& lhs, uint64_t rhs)
    {
        return lhs.time_ns < rhs;
    }
    inline bool operator<(uint64_t lhs, const wal_entry& rhs)
    {
        return lhs < rhs.time_ns;
    }

    class wal_entry_iterator
    {
        wal_entry*      we;

    public:
        using iterator_category = std::random_access_iterator_tag;
        using difference_type   = std::ptrdiff_t;
        using value_type        = wal_entry;
        using pointer           = wal_entry*;
        using reference         = wal_entry&;

        const size_t    entry_size;

        reference operator*() const {return *we;}
        pointer operator->() const {return we;}

        reference operator[](difference_type n) const
        {
            return *(*this + n);
        }

        wal_entry_iterator& operator+=(difference_type n)
        {
            we = (wal_entry*)((char*)we + n*entry_size);
            return *this;
        }

        wal_entry_iterator& operator-=(difference_type n)
        {
            return (*this += -n);
        }

        wal_entry_iterator& operator++()
        {
            return (*this += 1);
        }

        wal_entry_iterator operator++(int)
        {
            auto tmp(*this);
            ++(*this);
            return tmp;
        }

        wal_entry_iterator operator--()
        {
            return (*this -= 1);
        }

        friend wal_entry_iterator operator+(const wal_entry_iterator& a,
                                            difference_type n)
        {
            wal_entry_iterator it(a);
            it += n;
            return it;
        }

        friend wal_entry_iterator operator+(difference_type n,
                                            const wal_entry_iterator& a)
        {
            wal_entry_iterator it(a);
            it += n;
            return it;
        }

        friend difference_type operator-(const wal_entry_iterator& a,
                                         const wal_entry_iterator& b)
        {
            ptrdiff_t nbytes = ((char*)a.we - (char*)b.we);
            return nbytes / a.entry_size;
        }

        friend wal_entry_iterator operator-(const wal_entry_iterator& a,
                                            difference_type n)
        {
            wal_entry_iterator it(a);
            it -= n;
            return it;
        }

        friend bool operator==(const wal_entry_iterator& a,
                               const wal_entry_iterator& b)
        {
            return a.we == b.we;
        }

        friend bool operator!=(const wal_entry_iterator& a,
                               const wal_entry_iterator& b)
        {
            return a.we != b.we;
        }

        friend bool operator<(const wal_entry_iterator& a,
                              const wal_entry_iterator& b)
        {
            return a.we < b.we;
        }

        friend bool operator>(const wal_entry_iterator& a,
                              const wal_entry_iterator& b)
        {
            return a.we > b.we;
        }

        friend bool operator>=(const wal_entry_iterator& a,
                               const wal_entry_iterator& b)
        {
            return !(a < b);
        }

        friend bool operator<=(const wal_entry_iterator& a,
                               const wal_entry_iterator& b)
        {
            return !(a > b);
        }

        wal_entry_iterator& operator=(const wal_entry_iterator& a)
        {
            we = a.we;
            return *this;
        }

        constexpr wal_entry_iterator(wal_entry* we, size_t entry_size):
            we(we),
            entry_size(entry_size)
        {
        }

        constexpr wal_entry_iterator(const wal_entry_iterator& other):
            we(other.we),
            entry_size(other.entry_size)
        {
        }
    };

    // Map the write-ahead log and generate iterators to the target query range.
    struct wal_query
    {
        const series_read_lock&     read_lock;
        const size_t                entry_size;
        const uint64_t              t0;
        const uint64_t              t1;
        futil::mapping              wal_mm;
        wal_entry_iterator          _begin;
        wal_entry_iterator          _end;
        size_t                      nentries;

        wal_entry_iterator begin() const {return _begin;}
        wal_entry_iterator end() const {return _end;}
        wal_entry_iterator front() const {return _begin;}
        wal_entry_iterator back() const {return _end - 1;}
        const wal_entry& operator[](size_t i) const {return begin()[i];}

        // Return all WAL points in the range [t0, t1] that have not been
        // comitted to the chunk store.
        //
        // A WAL query is a bit complicated, because it has to skip points that
        // exist in the chunk store.  This means we need to increment t0 and t1
        // past the chunk store before doing the query.  time_last is the last
        // timestamp in the chunk store and time_first is the first valid
        // series timestamp. If time_first is past time_last, the chunk store
        // is empty and we may have deleted into the future and the first valid
        // point in the WAL would be time_first.  Otherwise, there is a live
        // range in the chunk store and the first valid point in the WAL would
        // be just past time_last.  So:
        //
        //  1. If time_first <= time_last, we should only return points in
        //     [time_last + 1, ...].
        //  2. If time_first > time_last, we should only return points in
        //     [time_first, ...].
        //
        // This reduces to:
        //
        //      [MAX(time_last + 1, time_first), ...]
        //
        // Now, t0 could already be past that minimum value, so we set it as
        // follows:
        //
        //      t0' = MAX(t0, time_last + 1, time_first)
        //
        // We also need to increment t1 past any points in the chunk store.  If
        // an increment is necessary, then that means that the entire query
        // range was in the chunk store already and we should return an empty
        // result.  If the query range was also backwards (t0 > t1) then we
        // want to return an empty result.  The easiest way to ensure all of
        // this is to set t1 as follows:
        //
        //      t1' = MAX(t1,t0' - 1)
        // 
        // If an increment is necessary, (t0' - 1) is the furthest we should
        // go.  If no increment is necessary then t1 is fine as-is.
        wal_query(const series_read_lock& read_lock, uint64_t _t0, uint64_t _t1,
                  int oflag = O_RDONLY):
            read_lock(read_lock),
            entry_size(sizeof(wal_entry) + read_lock.m.fields.size()*8),
            t0(MAX(_t0,read_lock.time_first,read_lock.time_last + 1)),
            t1(MAX(_t1,t0 - 1)),
            wal_mm(0,read_lock.wal_fd.lseek(0,SEEK_END),PROT_READ,MAP_SHARED,
                   read_lock.wal_fd.fd,0),
            _begin(std::lower_bound(
                wal_entry_iterator((wal_entry*)wal_mm.addr,entry_size),
                wal_entry_iterator((wal_entry*)wal_mm.addr,entry_size) +
                                   wal_mm.len/entry_size,
                t0)),
            _end(std::upper_bound(
                wal_entry_iterator((wal_entry*)wal_mm.addr,entry_size),
                wal_entry_iterator((wal_entry*)wal_mm.addr,entry_size) +
                                   wal_mm.len/entry_size,
                t1)),
            nentries(_end - _begin)
        {
        }
    };

    // Write data points to the write-ahead log.
    //
    // Data points can only be written past the end of a series; however, you
    // can write the tail points of the series multiple times (tsdb will verify
    // that the data from the later writes matches the data already stored in
    // the series) to allow for sane failure recovery.  In the event of a
    // mismatch, the entire write operation will be rejected.
    //
    // The data to be written is in a flat format.  Data is chunked by field,
    // in the order specified in the schema file for this measurement.  First,
    // an array of 64-bit timestamps is present.  The data for each field
    // follow and begins with a bitmap to specify non-NULL (1) or NULL (0)
    // entries, and then the raw point data for the field follows.  This is
    // repeated for each field in the schema.  The bitmap and raw point data
    // will always be 8-byte aligned, padded with 0 bytes to maintain that
    // alignment as necessary.
    //
    // Example writing 9 points to an xtalx_data series:
    //
    //      9 u64 timestamps
    //      1 u64 bitmap for pressure_psi (only 9 bits used)
    //      9 f64 pressure_psi values
    //      1 u64 bitmap for temp_c (only 9 bits used)
    //      9 f32 temp_c values
    //      1 32-bit padding
    //      1 u64 bitmap for pressure_hz (only 9 bits used)
    //      9 f64 pressure_hz values
    //      1 u64 bitmap for temp_hz (only 9 bits used)
    //      9 f64 temp_hz values
    //
    //      Total: 360 bytes
    void write_wal(series_write_lock& write_lock, size_t npoints,
                   size_t bitmap_offset, size_t data_len, const void* data);
}

#endif /* __SRC_LIBTSDB_WAL_H */
