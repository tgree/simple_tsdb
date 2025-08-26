// Copyright (c) 2018-2019 Terry Greeniaus.
// All rights reserved.
#include "tmock.h"
#include "tcolor.h"
#include <string.h>
#include <stdio.h>
#include <math.h>

void
tmock::mem_dump(const void* v, size_t len, const char* file, unsigned int l)
{
    if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
    {
        printf("%s:%u:",file,l);
        for (size_t i=0; i<len; ++i)
            printf(" %02X",((const uint8_t*)v)[i]);
        printf("\n");
    }
}

void
tmock::abort(const char* s, const char* f, unsigned int l)
{
    if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
        printf("%s:%u: %s\n",f,l,s);
    ::abort();
}

void
tmock::_tassert(bool expr, const char* s, const char* f, unsigned int l)
{
    if (!expr)
        tmock::abort(s,f,l);
}

void
tmock::abort_mem_dump(const void* v, const void* expected, size_t len,
    const char* file, size_t line)
{
    if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
    {
        printf("%s:%zu:\n",file,line);
        printf(" Expected:");
        for (size_t i=0; i<len; ++i)
            printf(" %02X",((const uint8_t*)expected)[i]);
        printf("\n      Got:");
        for (size_t i=0; i<len; ++i)
        {
            if (((const uint8_t*)expected)[i] == ((const uint8_t*)v)[i])
                printf(" %02X",((const uint8_t*)v)[i]);
            else
                printf(RED " %02X" RESET,((const uint8_t*)v)[i]);
        }
        printf("\n");
    }
    ::abort();
}

void
tmock::abort_not_equiv(long long v, long long expected, const char* file,
    size_t line)
{
    if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
    {
        printf("%s:%zu:\n",file,line);
        printf(" Expected: %lld\n",expected);
        printf("      Got: %lld\n",v);
    }
    ::abort();
}

void
tmock::abort_not_equiv(unsigned long long v, unsigned long long expected,
    const char* file, size_t line)
{
    if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
    {
        printf("%s:%zu:\n",file,line);
        printf(" Expected: %llu\n",expected);
        printf("      Got: %llu\n",v);
    }
    ::abort();
}

void
tmock::abort_not_equiv(const char* s, const char* expected, const char* file,
    size_t line)
{
    if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
    {
        printf("%s:%zu:\n",file,line);
        printf(" Expected: '%s'\n",expected);
        printf("      Got: '%s'\n",s);
    }
    ::abort();
}

void
tmock::assert_float_similar(float v, float expected, float tolerance,
    const char* file, size_t line)
{
    if (fabsf(v - expected) > tolerance)
    {
        if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
        {
            printf("%s:%zu:\n",file,line);
            printf("Float value %.10f not within %.10f of expected value "
                   "%.10f\n",v,tolerance,expected);
        }
        ::abort();
    }
}

void
tmock::assert_double_similar(double v, double expected, double tolerance,
    const char* file, size_t line)
{
    if (fabs(v - expected) > tolerance)
    {
        if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
        {
            printf("%s:%zu:\n",file,line);
            printf("Double value %.10f not within %.10f of expected value "
                   "%.10f\n",v,tolerance,expected);
        }
        ::abort();
    }
}

void
tmock::vprintf(const char* fmt, va_list ap)
{
    if (tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT)
        return;

    ::vprintf(fmt,ap);
}
