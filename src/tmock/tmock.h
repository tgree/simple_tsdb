// Copyright (c) 2018-2019 Terry Greeniaus.
// All rights reserved.
#ifndef __TMOCK_H
#define __TMOCK_H

#include "texpectation.h"
#include <hdr/fileline.h>
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
    void mem_dump(const void* v, size_t len,
                  const char* file = __builtin_FILE(),
                  unsigned int l = __builtin_LINE());

    // Abort with an error message.
    void abort(const char* s, const char* f = __builtin_FILE(),
               unsigned int l = __builtin_LINE()) __attribute__((noreturn));
#define TASSERT(expr) do {if (!(expr)) tmock::abort(#expr);} while(0)

    void abort_mem_dump(const void* v, const void* expected, size_t len,
                        const char* file, size_t line);

    // Assert that memory contents are the same.
    inline void assert_mem_same(const void* v, const void* expected, size_t len,
                                const char* file = __builtin_FILE(),
                                size_t line = __builtin_LINE())
    {
        if (!memcmp(v,expected,len))
            return;
        abort_mem_dump(v,expected,len,file,line);
    }
    template<typename T>
    inline void assert_mem_same(const T& v, const T& expected,
                                const char* file = __builtin_FILE(),
                                size_t line = __builtin_LINE())
    {
        assert_mem_same(&v,&expected,sizeof(T),file,line);
    }

    // Assert that two objects are equivalent.  For values it is just a simple
    // comparison with ==.  For things like const char* it will do a string
    // comparison.
    void assert_equiv(const char* s, const char* expected,
                      const char* file = __builtin_FILE(),
                      size_t line = __builtin_LINE());
    inline void assert_equiv(char* s, char* expected,
                             const char* file = __builtin_FILE(),
                             size_t line = __builtin_LINE())
    {
        assert_equiv((const char*)s,(const char*)expected,file,line);
    }
    inline void assert_equiv(char* s, const char* expected,
                             const char* file = __builtin_FILE(),
                             size_t line = __builtin_LINE())
    {
        assert_equiv((const char*)s,(const char*)expected,file,line);
    }
    inline void assert_equiv(const std::string& s, const char* expected,
                             const char* file = __builtin_FILE(),
                             size_t line = __builtin_LINE())
    {
        assert_equiv(s.c_str(),(const char*)expected,file,line);
    }
    void assert_equiv(const char* s, char* expected,
                      const char* file = __builtin_FILE(),
                      size_t line = __builtin_LINE()) = delete;
    void assert_equiv(uint16_t v, uint16_t expected,
                      const char* file = __builtin_FILE(),
                      size_t line = __builtin_LINE());
    void assert_equiv(uint32_t v, uint32_t expected,
                      const char* file = __builtin_FILE(),
                      size_t line = __builtin_LINE());
    void assert_equiv(uint64_t v, uint64_t expected,
                      const char* file = __builtin_FILE(),
                      size_t line = __builtin_LINE());
    void assert_equiv(int32_t v, int32_t expected,
                      const char* file = __builtin_FILE(),
                      size_t line = __builtin_LINE());
    void assert_equiv(int64_t v, int64_t expected,
                      const char* file = __builtin_FILE(),
                      size_t line = __builtin_LINE());
    template<typename T>
    inline void assert_equiv(const T& v, const T& expected,
                             const char* file = __builtin_FILE(),
                             size_t line = __builtin_LINE())
    {
        if (v == expected)
            return;
        abort_mem_dump(&v,&expected,sizeof(T),file,line);
    }

    void assert_float_similar(float v, float expected,
                              float tolerance = 0.00000001,
                              const char* file = __builtin_FILE(),
                              size_t line = __builtin_LINE());
    void assert_double_similar(double v, double expected,
                               double tolerance = 0.00000001,
                               const char* file = __builtin_FILE(),
                               size_t line = __builtin_LINE());

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
