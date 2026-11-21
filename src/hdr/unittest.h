// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_HDR_UNITTEST_H
#define __SRC_HDR_UNITTEST_H

#include <hdr/compiler.h>
#include <stdarg.h>
#include <stdio.h>

// Use prod_static instead of static for a function or variable that a
// unittest needs to reach.  It is static only in production builds; in
// UNITTEST builds it has external linkage.
#ifdef UNITTEST
#define prod_static
#else
#define prod_static static
#endif

// Use prod_printf() instead of printf() for log messages that production
// builds print but unittests shouldn't, such as errors that a test provokes
// on purpose.  In UNITTEST builds it prints nothing, but its arguments are
// still evaluated and format-checked.
static inline int prod_printf(const char* fmt, ...) __PRINTF__(1,2);

static inline int
prod_printf(const char* fmt, ...)
{
#ifdef UNITTEST
    return 0;
#else
    va_list ap;
    va_start(ap,fmt);
    int rv = vprintf(fmt,ap);
    va_end(ap);
    return rv;
#endif
}

#endif /* __SRC_HDR_UNITTEST_H */
