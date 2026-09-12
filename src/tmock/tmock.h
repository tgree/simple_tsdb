// Copyright (c) 2018-2019 Terry Greeniaus.
// All rights reserved.
#ifndef __TMOCK_H
#define __TMOCK_H

#include "texpectation.h"
#include <hdr/fileline.h>
#include <hdr/compiler.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <string>

namespace tmock
{
    // Dump a region of memory.
    void mem_dump(const char* file, unsigned int l, const void* v, size_t len);
#define TMOCK_MEM_DUMP(...) tmock::mem_dump(__FILE__,__LINE__,__VA_ARGS__)

    // Abort with an error message.
    void abort(const char* s, const char* f,
               unsigned int l) __attribute__((noreturn));
#define TABORT(s) tmock::abort((s),__FILE__,__LINE__)
#define TASSERT(...) \
    do {if (!(__VA_ARGS__)) tmock::abort(#__VA_ARGS__,__FILE__,__LINE__);} while(0)

    void abort_mem_dump(const char* file, size_t line, const void* v,
                        const void* expected, size_t len) __NORETURN__;

    // Assert that memory contents are the same.
    inline void assert_mem_same(const char* file, size_t line, const void* v,
                                const void* expected, size_t len)
    {
        if (!memcmp(v,expected,len))
            return;
        abort_mem_dump(file,line,v,expected,len);
    }
    template<typename T>
    inline void assert_mem_same(const char* file, size_t line,
                                const T& v, const T& expected)
    {
        assert_mem_same(file,line,&v,&expected,sizeof(T));
    }
#define TASSERT_MEM_SAME(...) \
    tmock::assert_mem_same(__FILE__,__LINE__,__VA_ARGS__)

    // Assert that two objects are equivalent.  For values it is just a simple
    // comparison with ==.  For things like const char* it will do a string
    // comparison.
    void abort_not_equiv(const char* file, size_t line, const char* v,
                         const char* expected) __NORETURN__;
    void abort_not_equiv(const char* file, size_t line, long long v,
                         long long expected) __NORETURN__;
    void abort_not_equiv(const char* file, size_t line, unsigned long long v,
                         unsigned long long expected) __NORETURN__;
    void abort_not_equiv(const char* file, size_t line, double v,
                         double expected) __NORETURN__;
#define ABORT_NOT_EQUIV_FUNC(T,U) \
    inline void abort_not_equiv(const char* file, size_t line, \
                                T v, T expected) \
    { \
        abort_not_equiv(file,line,(U)v,(U)expected); \
    }
    ABORT_NOT_EQUIV_FUNC(char,long long);
    ABORT_NOT_EQUIV_FUNC(signed char,long long);
    ABORT_NOT_EQUIV_FUNC(signed short,long long);
    ABORT_NOT_EQUIV_FUNC(signed int,long long);
    ABORT_NOT_EQUIV_FUNC(signed long,long long);
    ABORT_NOT_EQUIV_FUNC(unsigned char,unsigned long long);
    ABORT_NOT_EQUIV_FUNC(unsigned short,unsigned long long);
    ABORT_NOT_EQUIV_FUNC(unsigned int,unsigned long long);
    ABORT_NOT_EQUIV_FUNC(unsigned long,unsigned long long);
    ABORT_NOT_EQUIV_FUNC(float,double);
    template<typename T> __NORETURN__
    void abort_not_equiv(const char* file, size_t line, const T& v,
                         const T& expected)
    {
        abort_mem_dump(file,line,&v,&expected,sizeof(T));
    }
    inline void assert_equiv(const char* file, size_t line,
                             const char* s, const char* expected)
    {
        if (!strcmp(s,expected))
            return;
        abort_not_equiv(file,line,s,expected);
    }
    inline void assert_equiv(const char* file, size_t line,
                             char* s, const char* expected)
    {
        assert_equiv(file,line,(const char*)s,(const char*)expected);
    }
    inline void assert_equiv(const char* file, size_t line,
                             const std::string& s, const char* expected)
    {
        assert_equiv(file,line,s.c_str(),(const char*)expected);
    }
    void assert_equiv(const char* file, size_t line,
                      const char* s, char* expected) = delete;
    template<typename T, typename U>
    inline void assert_equiv(const char* file, size_t line,
                             const T& v, const U& expected)
    {
        if (v == (T)expected)
            return;
        abort_not_equiv(file,line,v,(T)expected);
    }
#define TASSERT_EQUIV(...) tmock::assert_equiv(__FILE__,__LINE__,__VA_ARGS__)

    void assert_float_similar(const char* file, size_t line, float v,
                              float expected, float tolerance);
#define TASSERT_FLOAT_SIMILAR(...) \
    tmock::assert_float_similar(__FILE__,__LINE__,__VA_ARGS__)

    void assert_double_similar(const char* file, size_t line, double v,
                               double expected, double tolerance);
#define TASSERT_DOUBLE_SIMILAR(...) \
    tmock::assert_double_similar(__FILE__,__LINE__,__VA_ARGS__)

    void vprintf(const char* fmt, va_list ap);
    inline void printf(const char* fmt, ...)
    {
        va_list ap;
        va_start(ap,fmt);
        tmock::vprintf(fmt,ap);
        va_end(ap);
    }

    int run_tests(int argc, const char* argv[]);

    namespace internal
    {
#define TCI_FLAG_FAILURE_EXPECTED   (1<<0)
#define TCI_FLAG_SHOULD_FAIL        (1<<1)
#define TCI_FLAG_DID_FAIL           (1<<2)
#define TCI_RESULT_FLAGS (TCI_FLAG_FAILURE_EXPECTED | TCI_FLAG_SHOULD_FAIL)
        struct test_case_info
        {
            test_case_info*     next;
            void                (*fn)();
            const char*         name;
            unsigned long       flags;
            pid_t               pid;
        };

        void register_test_case(test_case_info* tci);

        struct test_case_registrar
        {
            inline test_case_registrar(test_case_info* tci)
            {
                register_test_case(tci);
            }
        };

#define TMOCK_MODE_FLAG_SILENT  (1<<0)  // Don't print expected vs. actual
        extern uint64_t mode_flags;
    }
}

#define _TMOCK_TEST(fn,flags) \
    __attribute__((constructor))                                            \
    static void fn ## _test_case_registrar()                                \
    {                                                                       \
        static tmock::internal::test_case_info tci = {NULL,fn,#fn,flags};   \
        tmock::internal::register_test_case(&tci);                          \
    }                                                                       \
    static void fn()

// Define tests.  There is actually a matrix of test results and the defines
// below help you set up the expected behaviour of your test case.  You may
// have any of the following cases:
//
//  1. A test case that we expect will pass and SHOULD pass.  (Testing normal
//     functionality).
//  2. A test case that we expect will fail and SHOULD fail.  (Testing the
//     assert/abort paths actually assert/abort).
//  3. A test case that we expect will pass but SHOULDN'T pass.  (The code
//     under test is missing some sort of precondition check that should
//     trigger an abort or some other failure).
//  4. A test case that we expect will fail but SHOULDN'T fail.  (The code
//     under test is missing functionality and just aborts/asserts if you try
//     to exercise that functionality).
//
// We differentiate between what we expect the test to do and what the code
// SHOULD do so that we can track missing/incomplete functionality as part of
// our unittest output.
#define PASS_EXPECTED_SHOULD_PASS  0
#define FAIL_EXPECTED_SHOULD_FAIL \
    (TCI_FLAG_FAILURE_EXPECTED | TCI_FLAG_SHOULD_FAIL)
#define PASS_EXPECTED_SHOULD_FAIL TCI_FLAG_SHOULD_FAIL
#define FAIL_EXPECTED_SHOULD_PASS TCI_FLAG_FAILURE_EXPECTED

#define TMOCK_TEST(fn) \
    _TMOCK_TEST(fn,PASS_EXPECTED_SHOULD_PASS)
#define TMOCK_TEST_EXPECT_FAILURE(fn) \
    _TMOCK_TEST(fn,FAIL_EXPECTED_SHOULD_FAIL)
#define TMOCK_TEST_EXPECT_PASS_SHOULD_FAIL(fn) \
    _TMOCK_TEST(fn,PASS_EXPECTED_SHOULD_FAIL)
#define TMOCK_TEST_EXPECT_FAILURE_SHOULD_PASS(fn) \
    _TMOCK_TEST(fn,FAIL_EXPECTED_SHOULD_PASS)

#define TMOCK_MAIN() \
    int \
    main(int argc, const char* argv[]) \
    { \
        return tmock::run_tests(argc,argv); \
    }

#endif /* __TMOCK_H */
