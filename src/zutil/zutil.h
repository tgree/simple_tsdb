// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __ZUTIL_ZUTIL_H
#define __ZUTIL_ZUTIL_H

#include <futil/futil.h>
#include <zlib-ng/zlib-ng.h>

// Constants from the zlib-ng/zutil.h header.  We don't want to pull in that
// header because it is kind of internal to zlib and redefines things like MIN
// and MAX.  We can use push_macro/pop_macro to handle that, but it is simpler
// to just not pollute everybody that includes us.
//
// We have a unittest that compares these against the zlib values for sanity.
#define _GZIP_WRAPLEN                   18
#define _DEFLATE_HEADER_BITS            3
#define _DEFLATE_EOBS_BITS              15
#define _DEFLATE_PAD_BITS               6
#define _DEFLATE_QUICK_LIT_MAX_BITS     9
#define _DEFLATE_BLOCK_OVERHEAD \
    ((_DEFLATE_HEADER_BITS + \
      _DEFLATE_EOBS_BITS + \
      _DEFLATE_PAD_BITS) >> 3)
#define _DEFLATE_QUICK_OVERHEAD(x) \
    (((x) * (_DEFLATE_QUICK_LIT_MAX_BITS - 8) + 7) >> 3)

namespace zutil
{
    constexpr size_t max_gzipped_size(size_t src_len)
    {
        // Returns an upper bound on what the final compressed file size would
        // be for an input of size src_len.  Only valid if we are doing
        // MAX_WBITS windows.
        return src_len
          + (src_len == 0 ? 1 : 0)
          + (src_len < 9 ? 1 : 0)
          + _DEFLATE_QUICK_OVERHEAD(src_len)
          + _DEFLATE_BLOCK_OVERHEAD
          + _GZIP_WRAPLEN;
    }

    inline void gzip_decompress(void* dst, size_t dst_len, const void* src,
                                size_t src_len)
    {
        // Initialize the deflate stream.
        zng_stream zs = {};
        zs.next_in    = (const uint8_t*)src;
        zs.avail_in   = src_len;
        zs.next_out   = (uint8_t*)dst;
        zs.avail_out  = dst_len;

        // Decompress.  We get Z_BUF_ERROR if dst_len wasn't enough to
        // decompress all of src_len.  That's fine; sometimes we only want to
        // decompress the front of a database chunk instead of the entire thing.
        if (zng_inflateInit2(&zs,16 + MAX_WBITS) != Z_OK)
            throw futil::errno_exception(EIO);
        int err = zng_inflate(&zs,Z_FINISH);
        zng_inflateEnd(&zs);
        if (err != Z_BUF_ERROR && err != Z_STREAM_END)
            throw futil::errno_exception(EIO);

        // We didn't get too many bytes, but make sure we got enough.
        if (zs.total_out != dst_len)
            throw futil::errno_exception(EIO);
    }

    inline size_t gzip_compress(void* dst, size_t dst_len, const void* src,
                                size_t src_len,
                                int32_t level = Z_DEFAULT_COMPRESSION)
    {
        // Initialize the deflate stream.
        zng_stream zs = {};
        zs.next_in    = (const uint8_t*)src;
        zs.avail_in   = src_len;
        zs.next_out   = (uint8_t*)dst;
        zs.avail_out  = dst_len;

        // Compress.
        if (zng_deflateInit2(&zs,level,Z_DEFLATED,16 + MAX_WBITS,9,
                             Z_DEFAULT_STRATEGY) != Z_OK)
        {
            throw futil::errno_exception(EIO);
        }
        int err = zng_deflate(&zs,Z_FINISH);
        zng_deflateEnd(&zs);
        if (err != Z_STREAM_END)
            throw futil::errno_exception(EIO);

        // Return the actual compressed length.
        return zs.total_out;
    }
}

#endif /* __ZUTIL_ZUTIL_H */
