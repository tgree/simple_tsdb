// Copyright (c) 2018-2020 by Terry Greeniaus.  All rights reserved.
#ifndef __ARM_DEV_ENVIRONMENT_COMPILER_H
#define __ARM_DEV_ENVIRONMENT_COMPILER_H

#define __UNUSED__      __attribute__((unused))
#define __PACKED__      __attribute__((packed))
#define __ALIGNED__(n)  __attribute__((aligned(n)))
#define __PRINTF__(a,b) __attribute__((format(printf,a,b)))
#define __NORETURN__    __attribute__((noreturn))
#define __FASTCALL__    __attribute__((fastcall))
#define __REG_PARMS__   __attribute__((regparm(3)))
#define __NOINLINE__    __attribute__((noinline))
#define __WEAK__        __attribute__((weak))
#define __SECTION__(s)  __attribute__((section(s)))
#define __NOINIT__      __SECTION__(".noinit")
#define __NOINITP__(n)  __SECTION__(".noinit" #n)
#define __NOSTRIP__     __SECTION__(".nostrip")
#define __NAKED__       __attribute__((naked))
#define __ALIAS__(f)    __attribute__((alias(f)))
#define __WALIAS__(f)   __attribute__((weak,alias(f)))
#define __NOTHROW__     __attribute__((nothrow))
#define __OPT__(n)      __attribute__((optimize(n)))
#define __ASSUME__(e)   do {if (!(e)) __builtin_unreachable();} while (0)
#define __MUST_CHECK__  __attribute__((warn_unused_result))

#define __CACHE_ALIGNED__   __ALIGNED__(64)

#define __EXPECT_ALWAYS__(e)    __builtin_expect((e),true)
#define __EXPECT_NEVER__(e)     __builtin_expect((e),false)


#if __SIZEOF_POINTER__ == 8
typedef __uint128_t uint128_t;
#endif

#define __COMPILER_BARRIER__()  asm volatile("":::"memory")

#define DO_PRAGMA(x) _Pragma(#x)
#define TODO(x) DO_PRAGMA(message("\033[31mTODO: " x "\033[0m"))

#define _STRINGIFY(s) #s
#define STRINGIFY(s)  _STRINGIFY(s)

#endif /* __ARM_DEV_ENVIRONMENT_COMPILER_H */
