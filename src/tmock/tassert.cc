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
    exit(-1);
}

void
tmock::assert_equiv(const char* s, const char* expected, const char* file,
    size_t line)
{
    if (strcmp(s,expected))
    {
        if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
        {
            printf("%s:%zu:\n",file,line);
            printf(" Expected: '%s'\n",expected);
            printf("      Got: '%s'\n",s);
        }
        exit(-1);
    }
}

void
tmock::assert_equiv(uint16_t v, uint16_t expected, const char* file,
    size_t line)
{
    if (v != expected)
    {
        if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
        {
            printf("%s:%zu:\n",file,line);
            printf(" Expected: 0x%04X\n",expected);
            printf("      Got: 0x%04X\n",v);
        }
        exit(-1);
    }
}

void
tmock::assert_equiv(uint32_t v, uint32_t expected, const char* file,
    size_t line)
{
    if (v != expected)
    {
        if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
        {
            printf("%s:%zu:\n",file,line);
            printf(" Expected: 0x%08X\n",expected);
            printf("      Got: 0x%08X\n",v);
        }
        exit(-1);
    }
}

void
tmock::assert_equiv(uint64_t v, uint64_t expected, const char* file,
    size_t line)
{
    if (v != expected)
    {
        if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
        {
            printf("%s:%zu:\n",file,line);
            printf(" Expected: 0x%016lX\n",expected);
            printf("      Got: 0x%016lX\n",v);
        }
        exit(-1);
    }
}

void
tmock::assert_equiv(int32_t v, int32_t expected, const char* file,
    size_t line)
{
    if (v != expected)
    {
        if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
        {
            printf("%s:%zu:\n",file,line);
            printf(" Expected: %u\n",expected);
            printf("      Got: %u\n",v);
        }
        exit(-1);
    }
}

void
tmock::assert_equiv(int64_t v, int64_t expected, const char* file,
    size_t line)
{
    if (v != expected)
    {
        if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
        {
            printf("%s:%zu:\n",file,line);
            printf(" Expected: %lu\n",expected);
            printf("      Got: %lu\n",v);
        }
        exit(-1);
    }
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
        exit(-1);
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
        exit(-1);
    }
}

void
tmock::vprintf(const char* fmt, va_list ap)
{
    if (tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT)
        return;

    ::vprintf(fmt,ap);
}
