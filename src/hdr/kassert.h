// Copyright (c) 2018-2020 by Terry Greeniaus.  All rights reserved.
#ifndef __KERNEL_ASSERT_H
#define __KERNEL_ASSERT_H

#include <hdr/kassert.h>
#include <hdr/compiler.h>
#include <hdr/fileline.h>
#include <stdint.h>

// Compile-time assertion.
#define KASSERT(exp) static_assert(exp, #exp)

void _kabort(const char* f, unsigned int l) noexcept __NORETURN__;
#define kabort() _kabort(__FILE__,__LINE__)

inline void _kassert(const char* f, unsigned int l, bool expr)
{
    if (!expr)
        _kabort(f,l);
}
#define kassert(...) _kassert(__FILE__,__LINE__,__VA_ARGS__)

#endif /* __KERNEL_ASSERT_H */
