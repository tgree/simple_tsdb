// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../series.h"
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(bitmap_zeroes_test)
    {
        uint64_t bitmap[4] = {0,0,0,0};
        for (size_t i=5; i<= 101; ++i)
            tsdb::set_bitmap_bit(bitmap,i,i % 2);
        tmock::assert_equiv(bitmap[0],0xAAAAAAAAAAAAAAA0ULL);
        tmock::assert_equiv(bitmap[1],0x0000002AAAAAAAAAULL);
        tmock::assert_equiv(bitmap[2],0x0000000000000000ULL);
        tmock::assert_equiv(bitmap[3],0x0000000000000000ULL);
    }

    TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(bitmap_ones_test)
    {
        uint64_t bitmap[4] = {0xFFFFFFFFFFFFFFFFULL,
                              0xFFFFFFFFFFFFFFFFULL,
                              0xFFFFFFFFFFFFFFFFULL,
                              0xFFFFFFFFFFFFFFFFULL};
        for (size_t i=5; i<= 101; ++i)
            tsdb::set_bitmap_bit(bitmap,i,i % 2);
        tmock::assert_equiv(bitmap[0],0xAAAAAAAAAAAAAABFULL);
        tmock::assert_equiv(bitmap[1],0xFFFFFFEAAAAAAAAAULL);
        tmock::assert_equiv(bitmap[2],0xFFFFFFFFFFFFFFFFULL);
        tmock::assert_equiv(bitmap[3],0xFFFFFFFFFFFFFFFFULL);
    }

    TMOCK_TEST(get_bitmap_test)
    {
        uint64_t bitmap[4] = {0x123456789ABCDEF0ULL,
                              0,
                              0x9876543210987654ULL,
                              0};
        for (size_t i=0; i<64; ++i)
        {
            tmock::assert_equiv(tsdb::get_bitmap_bit(bitmap,i),
                                (bitmap[0] >> i) & 1);
        }
        for (size_t i=64; i<128; ++i)
        {
            tmock::assert_equiv(tsdb::get_bitmap_bit(bitmap,i),
                                (bitmap[1] >> (i - 64)) & 1);
        }
        for (size_t i=128; i<192; ++i)
        {
            tmock::assert_equiv(tsdb::get_bitmap_bit(bitmap,i),
                                (bitmap[2] >> (i - 128)) & 1);
        }
        for (size_t i=192; i<256; ++i)
        {
            tmock::assert_equiv(tsdb::get_bitmap_bit(bitmap,i),
                                (bitmap[3] >> (i - 192)) & 1);
        }
    }
};

TMOCK_MAIN();
