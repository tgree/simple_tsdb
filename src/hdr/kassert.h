// Copyright (c) 2018-2020 by Terry Greeniaus.  All rights reserved.
#ifndef __KERNEL_ASSERT_H
#define __KERNEL_ASSERT_H

#include <hdr/kassert.h>
#include <hdr/compiler.h>
#include <hdr/fileline.h>
#include <stdint.h>

// Compile-time assertion.
#define KASSERT(exp) static_assert(exp, #exp)

void kabort(const char* f = __builtin_FILE(),
            unsigned int l = __builtin_LINE()) noexcept __NORETURN__;

inline void kassert(bool expr,
                     const char* f = __builtin_FILE(),
                     unsigned int l = __builtin_LINE())
{
    if (!expr)
        kabort(f,l);
}

#endif /* __KERNEL_ASSERT_H */
