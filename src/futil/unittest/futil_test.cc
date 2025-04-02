// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../futil.h"
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_constructor)
    {
        tmock::assert_equiv((futil::path("a/b/","c/d/"))._path,"a/b/c/d/");
    }

    TMOCK_TEST(test_path_count_components)
    {
        tmock::assert_equiv(futil::path("////a///b/c///").count_components(),
                            (size_t)3);
        tmock::assert_equiv(futil::path("aaa/bb/cccc").count_components(),
                            (size_t)3);
        tmock::assert_equiv(futil::path("aaa/bb/").count_components(),
                            (size_t)2);
        tmock::assert_equiv(futil::path("/aaa/bb/").count_components(),
                            (size_t)2);
        tmock::assert_equiv(futil::path("/aaa/bb").count_components(),
                            (size_t)2);
        tmock::assert_equiv(futil::path("/aaa/b").count_components(),(size_t)2);
        tmock::assert_equiv(futil::path("/a/").count_components(),(size_t)1);
        tmock::assert_equiv(futil::path("/aa/").count_components(),(size_t)1);
        tmock::assert_equiv(futil::path("a/").count_components(),(size_t)1);
        tmock::assert_equiv(futil::path("aa/").count_components(),(size_t)1);
        tmock::assert_equiv(futil::path("aa").count_components(),(size_t)1);
        tmock::assert_equiv(futil::path("a").count_components(),(size_t)1);
    }

    TMOCK_TEST(test_path_join)
    {
        tmock::assert_equiv((futil::path("a/b/") + futil::path("c/d/"))._path,
                            "a/b/c/d/");
        tmock::assert_equiv((futil::path("a/b") + futil::path("c/d/"))._path,
                            "a/b/c/d/");
        try
        {
            tmock::assert_equiv((futil::path("a/b/") +
                                 futil::path("/c/d/"))._path,
                                "a/b/c/d/");
            tmock::abort("Expected exception!");
        }
        catch (const futil::invalid_join_exception&)
        {
        }
        tmock::assert_equiv((futil::path("/a/b") + futil::path("c/d/"))._path,
                            "/a/b/c/d/");
    }

    TMOCK_TEST(test_decompose)
    {
        auto p = futil::path("aaa/bb/cccc");
        auto v = p.decompose();
        tmock::assert_equiv(v.size(),(size_t)3);
        tmock::assert_equiv(v[0],"aaa");
        tmock::assert_equiv(v[1],"bb");
        tmock::assert_equiv(v[2],"cccc");

        p = futil::path("/aaa/bb/");
        v = p.decompose();
        tmock::assert_equiv(v.size(),(size_t)3);
        tmock::assert_equiv(v[0],"/");
        tmock::assert_equiv(v[1],"aaa");
        tmock::assert_equiv(v[2],"bb");

        p = futil::path("////a///b/c///");
        v = p.decompose();
        tmock::assert_equiv(v.size(),(size_t)4);
        tmock::assert_equiv(v[0],"/");
        tmock::assert_equiv(v[1],"a");
        tmock::assert_equiv(v[2],"b");
        tmock::assert_equiv(v[3],"c");
    }
};

TMOCK_MAIN();
