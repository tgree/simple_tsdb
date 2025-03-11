// Copyright (c) 2018-2019 Terry Greeniaus.
// All rights reserved.
#include "../tmock.h"

static int
_mock_foo(int a, int b)
{
    return mock("_mock_foo",a,b);
}

class tmock_test
{
    TMOCK_TEST_EXPECT_FAILURE(test_mock_not_called)
    {
        texpect("_mock_foo",want(a,1),want(b,2));
    }

    TMOCK_TEST(test_mock)
    {
        texpect("_mock_foo",want(a,1),want(b,2),returns(4));
        TASSERT(_mock_foo(1,2) == 4);
    }

    TMOCK_TEST(test_mock_missing_params_ok)
    {
        texpect("_mock_foo",want(b,2),returns(4));
        TASSERT(_mock_foo(123,2) == 4);
    }

    TMOCK_TEST_EXPECT_FAILURE(test_mock_missing_params_validates_passed_ones)
    {
        texpect("_mock_foo",want(b,2));
        _mock_foo(1,3);
    }

    TMOCK_TEST(test_mock_multiple)
    {
        texpect("_mock_foo",want(a,1),want(b,2),returns(4));
        texpect("_mock_foo",want(a,2),returns(10));
        texpect("_mock_foo",want(b,5),returns(314));
        TASSERT(_mock_foo(1,2) == 4);
        TASSERT(_mock_foo(2,1234) == 10);
        TASSERT(_mock_foo(1234,5) == 314);
    }

    TMOCK_TEST_EXPECT_FAILURE(test_multiple_arm_fails)
    {
        for (size_t i=0; i<2; ++i)
            texpect("_mock_foo");
    }

    TMOCK_TEST(test_multiple_arm_fire_works)
    {
        for (size_t i=0; i<2; ++i)
        {
            texpect("_mock_foo");
            _mock_foo(0,0);
        }
    }
};

TMOCK_MAIN();
