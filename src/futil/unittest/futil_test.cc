// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../futil.h"
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_constructor)
    {
        TASSERT_EQUIV((futil::path("a/b/","c/d/"))._path,"a/b/c/d/");
    }

    TMOCK_TEST(test_path_count_components)
    {
        TASSERT_EQUIV(futil::path("////a///b/c///").count_components(),
                      (size_t)3);
        TASSERT_EQUIV(futil::path("aaa/bb/cccc").count_components(),
                      (size_t)3);
        TASSERT_EQUIV(futil::path("aaa/bb/").count_components(),
                      (size_t)2);
        TASSERT_EQUIV(futil::path("/aaa/bb/").count_components(),
                      (size_t)2);
        TASSERT_EQUIV(futil::path("/aaa/bb").count_components(),
                      (size_t)2);
        TASSERT_EQUIV(futil::path("/aaa/b").count_components(),(size_t)2);
        TASSERT_EQUIV(futil::path("/a/").count_components(),(size_t)1);
        TASSERT_EQUIV(futil::path("/aa/").count_components(),(size_t)1);
        TASSERT_EQUIV(futil::path("a/").count_components(),(size_t)1);
        TASSERT_EQUIV(futil::path("aa/").count_components(),(size_t)1);
        TASSERT_EQUIV(futil::path("aa").count_components(),(size_t)1);
        TASSERT_EQUIV(futil::path("a").count_components(),(size_t)1);
    }

    TMOCK_TEST(test_path_join)
    {
        TASSERT_EQUIV((futil::path("a/b/") + futil::path("c/d/"))._path,
                      "a/b/c/d/");
        TASSERT_EQUIV((futil::path("a/b") + futil::path("c/d/"))._path,
                      "a/b/c/d/");
        try
        {
            TASSERT_EQUIV((futil::path("a/b/") + futil::path("/c/d/"))._path,
                          "a/b/c/d/");
            TABORT("Expected exception!");
        }
        catch (const futil::invalid_join_exception&)
        {
        }
        TASSERT_EQUIV((futil::path("/a/b") + futil::path("c/d/"))._path,
                      "/a/b/c/d/");
    }

    TMOCK_TEST(test_decompose)
    {
        auto p = futil::path("aaa/bb/cccc");
        auto v = p.decompose();
        TASSERT_EQUIV(v.size(),(size_t)3);
        TASSERT_EQUIV(v[0],"aaa");
        TASSERT_EQUIV(v[1],"bb");
        TASSERT_EQUIV(v[2],"cccc");

        p = futil::path("/aaa/bb/");
        v = p.decompose();
        TASSERT_EQUIV(v.size(),(size_t)3);
        TASSERT_EQUIV(v[0],"/");
        TASSERT_EQUIV(v[1],"aaa");
        TASSERT_EQUIV(v[2],"bb");

        p = futil::path("////a///b/c///");
        v = p.decompose();
        TASSERT_EQUIV(v.size(),(size_t)4);
        TASSERT_EQUIV(v[0],"/");
        TASSERT_EQUIV(v[1],"a");
        TASSERT_EQUIV(v[2],"b");
        TASSERT_EQUIV(v[3],"c");
    }
};

TMOCK_MAIN();
