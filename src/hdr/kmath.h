// Copyright (c) 2018-2019 Terry Greeniaus.
// All rights reserved.
#ifndef __KERNEL_MATH_H
#define __KERNEL_MATH_H

#include "kassert.h"
#include <stddef.h>
#include <stdint.h>

template<typename T>
constexpr T _min(T v)
{
    return v;
}
template<typename L, typename R, typename ...Args>
constexpr auto _min(L l, R r, Args... args)
{
    return (l < r) ? _min(l,args...) : _min(r,args...);
}
#define MIN _min
KASSERT(MIN(1,2,3,4,5)   == 1);
KASSERT(MIN(1,-2,3,4,5)  == -2);
KASSERT(MIN(1,-2,3,4,-5) == -5);
KASSERT(MIN((int16_t)-1,(int32_t)0x80000000) == (int32_t)0x80000000);
KASSERT(MIN((int32_t)0x80000000,(int16_t)-1) == (int32_t)0x80000000);
KASSERT(MIN((int32_t)0x80000000,(uint16_t)1) == (int32_t)0x80000000);
KASSERT(MIN((int16_t)-1,(uint16_t)5) == (int16_t)-1);

template<typename T>
constexpr T _max(T v)
{
    return v;
}
template<typename L, typename R, typename ...Args>
constexpr auto _max(L l, R r, Args... args)
{
    return (l > r) ? _max(l,args...) : _max(r,args...);
}
#define MAX _max
KASSERT(MAX(1,2,3,4,5)     == 5);
KASSERT(MAX(1,2,3,4,-5)    == 4);
KASSERT(MAX(1,-2,-3,-4,-5) == 1);
KASSERT(MAX((int16_t)-1,(int32_t)0x80000000) == -1);
KASSERT(MAX((int32_t)0x80000000,(int16_t)-1) == -1);
KASSERT(MAX((int32_t)0x80000000,(uint16_t)1) == 1);
KASSERT(MAX((int16_t)-1,(uint16_t)5) == 5);

template<typename T>
constexpr T _clamp(T lower, T val, T upper)
{
    return (val < lower ? lower :
            val > upper ? upper :
            val);
}
#define CLAMP _clamp
KASSERT(CLAMP(10,5,30) == 10);
KASSERT(CLAMP(10,10,30) == 10);
KASSERT(CLAMP(10,15,30) == 15);
KASSERT(CLAMP(10,20,30) == 20);
KASSERT(CLAMP(10,25,30) == 25);
KASSERT(CLAMP(10,30,30) == 30);
KASSERT(CLAMP(10,35,30) == 30);

template<typename T>
constexpr bool is_pow2(T v)
{
    return (v != 0) && ((v & (v-1)) == 0);
}

// Returns the floor of the log2().
constexpr unsigned int ulog2(unsigned int v)
{
    return sizeof(v)*8 - __builtin_clz(v) - 1;
}

constexpr unsigned int ulog2(unsigned long v)
{
    return sizeof(v)*8 - __builtin_clzl(v) - 1;
}

constexpr unsigned int ulog2(unsigned long long v)
{
    return sizeof(v)*8 - __builtin_clzll(v) - 1;
}
KASSERT(ulog2(8U) == 3);
KASSERT(ulog2(9U) == 3);

// Returns the ceil of the log2().
template<typename T>
constexpr unsigned int ceil_ulog2(T v)
{
    return ulog2(v) + !is_pow2(v);
}
KASSERT(ceil_ulog2(8U) == 3);
KASSERT(ceil_ulog2(9U) == 4);

constexpr int ffs(unsigned char v)
{
    return __builtin_ffs(v) - 1;
}

constexpr int ffs(unsigned short v)
{
    return __builtin_ffs(v) - 1;
}

constexpr int ffs(unsigned int v)
{
    return __builtin_ffs(v) - 1;
}

constexpr int ffs(unsigned long v)
{
    return __builtin_ffsl(v) - 1;
}

constexpr int ffs(unsigned long long v)
{
    return __builtin_ffsll(v) - 1;
}
KASSERT(ffs(0x12345678U) == 3);
KASSERT(ffs(0x12345678UL) == 3);
KASSERT(ffs(0x1234567800000000ULL) == 35);

constexpr int popcount(unsigned char v)
{
    return __builtin_popcount(v);
}
constexpr int popcount(unsigned short v)
{
    return __builtin_popcount(v);
}
constexpr int popcount(unsigned int v)
{
    return __builtin_popcount(v);
}
constexpr int popcount(unsigned long v)
{
    return __builtin_popcountl(v);
}
constexpr int popcount(unsigned long long v)
{
    return __builtin_popcountll(v);
}
KASSERT(popcount(0x11111111U) == 8);
KASSERT(popcount(0x33333333UL) == 16);
KASSERT(popcount(0x3333333344444444ULL) == 24);

constexpr uint32_t parity(uint32_t v)
{
    v = ((v >> 16) ^ v);
    v = ((v >>  8) ^ v);
    v = ((v >>  4) ^ v);
    v = ((v >>  2) ^ v);
    v = ((v >>  1) ^ v);
    return (v & 1);
}
KASSERT(parity(0x00000000) == 0);
KASSERT(parity(0x08000001) == 0);
KASSERT(parity(0x12345678) == 1);
KASSERT(parity(0x32345678) == 0);
KASSERT(parity(0xFFFFFFFF) == 0);

template<typename T>
constexpr T round_up_pow2(T v, uint64_t p2)
{
    kassert(is_pow2(p2));
    return (T)(((uint64_t)v + p2 - 1) & ~(p2 - 1));
}

template<typename T>
constexpr T round_down_pow2(T v, uint64_t p2)
{
    kassert(is_pow2(p2));
    return (T)(((uint64_t)v) & ~(p2 - 1));
}

template<typename T>
constexpr T round_to_nearest_multiple(T v, T base)
{
    return (T)(((v + (base/2))/base)*base);
}
KASSERT(round_to_nearest_multiple(12345,100) == 12300);
KASSERT(round_to_nearest_multiple(12349,100) == 12300);
KASSERT(round_to_nearest_multiple(12350,100) == 12400);
KASSERT(round_to_nearest_multiple(12351,100) == 12400);
KASSERT(round_to_nearest_multiple(12399,100) == 12400);
KASSERT(round_to_nearest_multiple(12400,100) == 12400);
KASSERT(round_to_nearest_multiple(12401,100) == 12400);

template<typename T>
constexpr T round_down_to_nearest_multiple(T v, T base)
{
    return (T)((v/base)*base);
}
KASSERT(round_down_to_nearest_multiple(12300,100) == 12300);
KASSERT(round_down_to_nearest_multiple(12301,100) == 12300);
KASSERT(round_down_to_nearest_multiple(12345,100) == 12300);
KASSERT(round_down_to_nearest_multiple(12399,100) == 12300);
KASSERT(round_down_to_nearest_multiple(12400,100) == 12400);

template<typename T>
constexpr T round_up_to_nearest_multiple(T v, T base)
{
    return (T)(((v + (base - 1))/base)*base);
}
KASSERT(round_up_to_nearest_multiple(12300,100) == 12300);
KASSERT(round_up_to_nearest_multiple(12301,100) == 12400);
KASSERT(round_up_to_nearest_multiple(12345,100) == 12400);
KASSERT(round_up_to_nearest_multiple(12399,100) == 12400);
KASSERT(round_up_to_nearest_multiple(12400,100) == 12400);

// Find the next power-of-2 that is greater than or equal to v.  Note that
// this will wrap back to 0 if your high bit and some other bit are set.
constexpr unsigned int ceil_pow2(unsigned int v)
{
    return (!v ? 0 : (is_pow2(v) ? v : (2U << ulog2(v))));
}

constexpr unsigned long ceil_pow2(unsigned long v)
{
    return (!v ? 0 : (is_pow2(v) ? v : (2UL << ulog2(v))));
}

constexpr unsigned long long ceil_pow2(unsigned long long v)
{
    return (!v ? 0 : (is_pow2(v) ? v : (2ULL << ulog2(v))));
}
KASSERT(ceil_pow2(0U) == 0);
KASSERT(ceil_pow2(1U) == 1);
KASSERT(ceil_pow2(2U) == 2);
KASSERT(ceil_pow2(3U) == 4);
KASSERT(ceil_pow2(4U) == 4);
KASSERT(ceil_pow2(5U) == 8);
KASSERT(ceil_pow2(7U) == 8);
KASSERT(ceil_pow2(8U) == 8);
KASSERT(ceil_pow2(9U) == 16);

constexpr unsigned int floor_pow2(unsigned int v)
{
    return (!v ? 0 : (is_pow2(v) ? v : (1U << ulog2(v))));
}

constexpr unsigned long floor_pow2(unsigned long v)
{
    return (!v ? 0 : (is_pow2(v) ? v : (1UL << ulog2(v))));
}

constexpr unsigned long long floor_pow2(unsigned long long v)
{
    return (!v ? 0 : (is_pow2(v) ? v : (1ULL << ulog2(v))));
}
KASSERT(floor_pow2(0U) == 0);
KASSERT(floor_pow2(1U) == 1);
KASSERT(floor_pow2(2U) == 2);
KASSERT(floor_pow2(3U) == 2);
KASSERT(floor_pow2(4U) == 4);
KASSERT(floor_pow2(5U) == 4);
KASSERT(floor_pow2(7U) == 4);
KASSERT(floor_pow2(8U) == 8);
KASSERT(floor_pow2(9U) == 8);

template<typename T>
constexpr T ceil_div(T num, T denom)
{
    return (T)((num + denom - 1)/denom);
}

template<typename T>
constexpr T floor_div(T num, T denom)
{
    return (T)(num/denom);
}

template<typename T>
constexpr T round_div(T num, T denom)
{
    return (T)((num + denom/2)/denom);
}
KASSERT(round_div(5,5)  == 1);
KASSERT(round_div(6,5)  == 1);
KASSERT(round_div(7,5)  == 1);
KASSERT(round_div(8,5)  == 2);
KASSERT(round_div(9,5)  == 2);
KASSERT(round_div(6,6)  == 1);
KASSERT(round_div(7,6)  == 1);
KASSERT(round_div(8,6)  == 1);
KASSERT(round_div(9,6)  == 2);
KASSERT(round_div(10,6) == 2);
KASSERT(round_div(11,6) == 2);

// Implements the modulo operator such that the result is *always* positive,
// regardless of the signs of either num or denom.  Loosely speaking:
//
//      kmod(N,D) == kmod(N,-D)
//
// So the sign of the denominator is of no consequence here.  This operator
// is mainly useful for calculating padding lengths; if you have to fill up
// a buffer such that its length is a multiple of L bytes, and you currently
// have N bytes in the buffer, then you need P padding bytes:
//
//      P = kmod(-N,L)
//
// Note that this works even if N >= L.
template<typename T>
constexpr T kmod(T num, T denom)
{
    T m = num % denom;
    if (m < 0)
        return (denom < 0) ? m - denom : m + denom;
    return m;
}
KASSERT(kmod(10, 3) == 1);
KASSERT(kmod( 1, 3) == 1);
KASSERT(kmod(-1, 3) == 2);
KASSERT(kmod( 1,-3) == 1);
KASSERT(kmod(-1,-3) == 2);
KASSERT(kmod(-10,4) == 2);

#ifndef PHASE_NO_FP
template<>
constexpr float kmod(float num, float denom)
{
    return num - ((float)(long long)(num/denom))*denom;
}

template<>
constexpr double kmod(double num, double denom)
{
    return num - ((double)(long long)(num/denom))*denom;
}
KASSERT(kmod(1.f,0.5f) == 0.f);
KASSERT(kmod(2.5f,1.f) == 0.5f);
#endif /* PHASE_NO_FP */

// Given two points (x0, y0) and (x1, y1), interpolate the line between those
// points to find the y-coordinate for a given x-coordinate.
template<typename T>
constexpr T linterp(T x, T x0, T y0, T x1, T y1)
{
    return y0 + (x - x0)*(y1 - y0)/(x1 - x0);
}
KASSERT(linterp(10,5,0,15,10) == 5);
KASSERT(linterp(20,5,0,15,10) == 15);

// Given a source range (s0, s1) and a target range (t0, t1), rescale a value v
// from the source range into the target range.
template<typename T>
constexpr T rescale(T v, T s0, T s1, T t0, T t1)
{
    return linterp(v,s0,t0,s1,t1);
}
KASSERT(rescale(2,0,4,0,2) == 1);
KASSERT(rescale(10,5,20,1,4) == 2);

// Compute the GCD of two values.
template<typename T>
constexpr T gcd(T t0, T t1)
{
    return (t1 == 0) ? t0 : gcd<T>(t1,t0 % t1);
}
KASSERT(gcd(12,15) == 3);

// Compute the LCM of two values.
template<typename T>
constexpr T lcm(T t0, T t1)
{
    return t0*t1/gcd(t0,t1);
}
KASSERT(lcm(10,15) == 30);

#define PI_F    3.1415926535897932384626433832795f
#define PI2_F   6.2831853071795864769252867665590f
#define PI_G    3.1415926535897932384626433832795
#define PI2_G   6.2831853071795864769252867665590

// Convert the value to a decimal string.
static inline void
num_to_dec(uint32_t n, char (&s)[11])
{
    s[10] = '\0';

    unsigned int i = 10;
    do
    {
        s[--i] = '0' + (n % 10);
        n     /= 10;
    } while (n && i);

    if (!i)
        return;

    char* p = &s[0];
    for(; i < 11; ++i)
        *p++ = s[i];
}

// Perform summation.
template<typename U, typename T>
static constexpr U
SUM(const T* t, size_t N)
{
    U sum = 0;
    while (N--)
        sum += *t++;
    return sum;
}

template<typename U, typename T, size_t N>
static constexpr U
SUM(const T (&t)[N])
{
    return SUM<U>(t,N);
}

// Slow but constexpr evaluation of integer square root.
// Based on: https://stackoverflow.com/a/63457507
template<typename T>
static constexpr T
isqrt(T v)
{
    if (v == 0)
        return 0;

    T result = 0;
    for (size_t shift = round_to_nearest_multiple(ulog2(v) + 1U,2U);
         shift != 0;
         shift -= 2)
    {
        result  = (result << 1) | 1;
        if (result*result > (v >> (shift - 2)))
            result ^= 1;
    }

    return result;
}
KASSERT(isqrt(0U)     == 0);
KASSERT(isqrt(1U)     == 1);
KASSERT(isqrt(3U)     == 1);
KASSERT(isqrt(4U)     == 2);
KASSERT(isqrt(8U)     == 2);
KASSERT(isqrt(9U)     == 3);
KASSERT(isqrt(15U)    == 3);
KASSERT(isqrt(16U)    == 4);
KASSERT(isqrt(24U)    == 4);
KASSERT(isqrt(25U)    == 5);
KASSERT(isqrt(35U)    == 5);
KASSERT(isqrt(36U)    == 6);
KASSERT(isqrt(99U)    == 9);
KASSERT(isqrt(100U)   == 10);
KASSERT(isqrt(9999U)  == 99);
KASSERT(isqrt(10000U) == 100);

#ifdef PHASE_CORTEX_M0P
extern "C" uint64_t _mul_32x32_64(uint32_t a, uint32_t b);
#else
static constexpr uint64_t
_mul_32x32_64(uint32_t a, uint32_t b)
{
    return ((uint64_t)a)*b;
}
KASSERT(_mul_32x32_64(0xFFFFFFFFU,0xFFFFFFFFU) == 0xFFFFFFFE00000001);
#endif

#endif /* __KERNEL_MATH_H */
