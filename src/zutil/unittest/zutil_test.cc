// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../zutil.h"
#include <tmock/tmock.h>

static size_t
slow_get_max_gzipped_size(size_t src_len)
{
    // Returns the maximum compressed size of a stream of the specified
    // source length.  It is recommended to only call this once and cache
    // the result for a given size.
    zng_stream zs = {};

    tmock::assert_equiv(
        zng_deflateInit2(&zs,9,Z_DEFLATED,16 + MAX_WBITS,9,Z_DEFAULT_STRATEGY),
        Z_OK);

    size_t len = zng_deflateBound(&zs,src_len);

    zng_deflateEnd(&zs);

    return len;
}

class tmock_test
{
    TMOCK_TEST(test_gzip_size)
    {
        // Test all powers of 2, since that's what libtsdb allows for chunk
        // sizes (well, technically they have to be at least 128 bytes).
        size_t prev_size = 0;
        for (uint32_t v = 16; v; v <<= 1)
        {
            size_t size = zutil::max_gzipped_size(v);
            tmock::assert_equiv(size,slow_get_max_gzipped_size(v));
            TASSERT(size >= prev_size);
            prev_size = size;
        }
    }

    TMOCK_TEST(test_compress_decompress)
    {
        const uint8_t data[] =
            {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};
        uint8_t comp_data[zutil::max_gzipped_size(sizeof(data))] = {};
        uint8_t decomp_data[sizeof(data)] = {};
        size_t size = zutil::gzip_compress(comp_data,sizeof(comp_data),
                                           data,sizeof(data));
        zutil::gzip_decompress(decomp_data,sizeof(decomp_data),comp_data,size);
        tmock::assert_mem_same(data,decomp_data);
    }
};

TMOCK_MAIN();

#undef MIN
#undef MAX
#include <zlib-ng/zutil.h>

KASSERT(_GZIP_WRAPLEN               == GZIP_WRAPLEN);
KASSERT(_DEFLATE_HEADER_BITS        == DEFLATE_HEADER_BITS);
KASSERT(_DEFLATE_EOBS_BITS          == DEFLATE_EOBS_BITS);
KASSERT(_DEFLATE_PAD_BITS           == DEFLATE_PAD_BITS);
KASSERT(_DEFLATE_QUICK_LIT_MAX_BITS == DEFLATE_QUICK_LIT_MAX_BITS);
KASSERT(_DEFLATE_BLOCK_OVERHEAD     == DEFLATE_BLOCK_OVERHEAD);
