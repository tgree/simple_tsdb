// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_LIBTSDB_WAL_H
#define __SRC_LIBTSDB_WAL_H

#include "series.h"
#include <hdr/kmath.h>

namespace tsdb
{
    union wal_field
    {
        uint8_t     u8;
        uint32_t    u32;
        uint64_t    u64;
        float       f32;
        double      f64;
        int32_t     i32;
        int64_t     i64;
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
        futil::file                 wal_fd;
        futil::mapping              wal_mm;
        wal_entry_iterator          _begin;
        wal_entry_iterator          _end;
        size_t                      nentries;

        wal_entry_iterator begin() const {return _begin;}
        wal_entry_iterator end() const {return _end;}
        wal_entry_iterator front() const {return _begin;}
        wal_entry_iterator back() const {return _end - 1;}
        const wal_entry& operator[](size_t i) const {return begin()[i];}

        wal_query(const series_read_lock& read_lock, uint64_t t0, uint64_t t1):
            read_lock(read_lock),
            entry_size(sizeof(wal_entry) + read_lock.m.fields.size()*8),
            wal_fd(read_lock.series_dir,"wal",O_RDONLY),
            wal_mm(0,wal_fd.lseek(0,SEEK_END),PROT_READ,MAP_SHARED,wal_fd.fd,0),
            _begin(std::lower_bound(
                wal_entry_iterator((wal_entry*)wal_mm.addr,entry_size),
                wal_entry_iterator((wal_entry*)wal_mm.addr,entry_size) +
                                   wal_mm.len/entry_size,
                MAX(t0,read_lock.time_first))),
            _end(std::upper_bound(
                wal_entry_iterator((wal_entry*)wal_mm.addr,entry_size),
                wal_entry_iterator((wal_entry*)wal_mm.addr,entry_size) +
                                   wal_mm.len/entry_size,
                t1)),
            nentries(_end >= _begin ? _end - _begin : 0)
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
