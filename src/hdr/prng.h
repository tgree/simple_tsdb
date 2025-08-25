// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __HDR_PRNG_H
#define __HDR_PRNG_H

#include "kassert.h"
#include <type_traits>

namespace prng
{
    template<typename T, T a, T c, T m>
    struct linear_congruential
    {
        KASSERT(!m || a < m);
        KASSERT(!m || c < m);
        KASSERT(std::is_unsigned<T>::value);
        static constexpr const T _Min = (c ? 1 : 0);
        static constexpr const T _Max = m - 1;
        KASSERT(_Min < _Max);
        static constexpr const T _Range = (_Max - _Min);

        T   x;

        constexpr uint32_t next()
        {
            x = ((a * x) + c) % m;
            return x;
        }

        constexpr linear_congruential(T seed = 1):x(seed) {}
    };

    typedef linear_congruential<uint32_t,48271,0,0x7FFFFFFF> minstd;
}

#endif /* __HDR_PRNG_H */
