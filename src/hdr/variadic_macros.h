// Copyright (c) 2018-2020 by Terry Greeniaus.  All rights reserved.
#ifndef __ARM_DEV_ENVIRONMENT_VARIADIC_MACROS_H
#define __ARM_DEV_ENVIRONMENT_VARIADIC_MACROS_H

#define VA_NARGS(...) VA_NARGS_(,##__VA_ARGS__, \
                                20, 19, 18, 17, 16, 15, 14, 13, 12, 11, \
                                10,  9,  8,  7,  6,  5,  4,  3,  2,  1, \
                                 0)
#define VA_NARGS_(_00,_01,_02,_03,_04,_05,_06,_07,_08,_09, \
                  _10,_11,_12,_13,_14,_15,_16,_17,_18,_19, \
                  _20, \
                  N,...) N
static_assert(VA_NARGS(1,2,3,4) == 4,"VA_NARGS of 4 failed");
static_assert(VA_NARGS() == 0,"VA_NARGS of 0 failed");

#define VA_FIRST(VAL,...) VAL

static_assert(VA_FIRST(1,2,3,4) == 1,"VA_FIRST of 4 failed");

#define _CAT(x,y) x ## y
#define CAT(x,y) _CAT(x,y)
#define VA_APPLY(FN,...) CAT(VA_APPLY_,VA_NARGS(__VA_ARGS__))(FN,__VA_ARGS__)
#define VA_APPLY_0(FN,...)
#define VA_APPLY_1(FN,VAL,...)  FN(VAL)  VA_APPLY_0(FN,__VA_ARGS__)
#define VA_APPLY_2(FN,VAL,...)  FN(VAL)  VA_APPLY_1(FN,__VA_ARGS__)
#define VA_APPLY_3(FN,VAL,...)  FN(VAL)  VA_APPLY_2(FN,__VA_ARGS__)
#define VA_APPLY_4(FN,VAL,...)  FN(VAL)  VA_APPLY_3(FN,__VA_ARGS__)
#define VA_APPLY_5(FN,VAL,...)  FN(VAL)  VA_APPLY_4(FN,__VA_ARGS__)
#define VA_APPLY_6(FN,VAL,...)  FN(VAL)  VA_APPLY_5(FN,__VA_ARGS__)
#define VA_APPLY_7(FN,VAL,...)  FN(VAL)  VA_APPLY_6(FN,__VA_ARGS__)
#define VA_APPLY_8(FN,VAL,...)  FN(VAL)  VA_APPLY_7(FN,__VA_ARGS__)
#define VA_APPLY_9(FN,VAL,...)  FN(VAL)  VA_APPLY_8(FN,__VA_ARGS__)
#define VA_APPLY_10(FN,VAL,...) FN(VAL)  VA_APPLY_9(FN,__VA_ARGS__)
#define VA_APPLY_11(FN,VAL,...) FN(VAL) VA_APPLY_10(FN,__VA_ARGS__)
#define VA_APPLY_12(FN,VAL,...) FN(VAL) VA_APPLY_11(FN,__VA_ARGS__)
#define VA_APPLY_13(FN,VAL,...) FN(VAL) VA_APPLY_12(FN,__VA_ARGS__)
#define VA_APPLY_14(FN,VAL,...) FN(VAL) VA_APPLY_13(FN,__VA_ARGS__)
#define VA_APPLY_15(FN,VAL,...) FN(VAL) VA_APPLY_14(FN,__VA_ARGS__)
#define VA_APPLY_16(FN,VAL,...) FN(VAL) VA_APPLY_15(FN,__VA_ARGS__)
#define VA_APPLY_17(FN,VAL,...) FN(VAL) VA_APPLY_16(FN,__VA_ARGS__)
#define VA_APPLY_18(FN,VAL,...) FN(VAL) VA_APPLY_17(FN,__VA_ARGS__)
#define VA_APPLY_19(FN,VAL,...) FN(VAL) VA_APPLY_18(FN,__VA_ARGS__)
#define VA_APPLY_20(FN,VAL,...) FN(VAL) VA_APPLY_19(FN,__VA_ARGS__)

#endif /* __ARM_DEV_ENVIRONMENT_VARIADIC_MACROS_H */
