// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../strutil.h"
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_strslice)
    {
        std::string s("0123456789");
        tmock::assert_equiv(str::slice(s).c_str(),s.c_str());
        tmock::assert_equiv(str::slice(s,1).c_str(),"123456789");
        tmock::assert_equiv(str::slice(s,2).c_str(),"23456789");
        tmock::assert_equiv(str::slice(s,3).c_str(),"3456789");
        tmock::assert_equiv(str::slice(s,4).c_str(),"456789");
        tmock::assert_equiv(str::slice(s,5).c_str(),"56789");
        tmock::assert_equiv(str::slice(s,6).c_str(),"6789");
        tmock::assert_equiv(str::slice(s,7).c_str(),"789");
        tmock::assert_equiv(str::slice(s,8).c_str(),"89");
        tmock::assert_equiv(str::slice(s,9).c_str(),"9");
        tmock::assert_equiv(str::slice(s,10).c_str(),"");
        tmock::assert_equiv(str::slice(s,11).c_str(),"");
        tmock::assert_equiv(str::slice(s,12).c_str(),"");

        tmock::assert_equiv(str::slice(s,1,4).c_str(),"123");
        tmock::assert_equiv(str::slice(s,2,4).c_str(),"23");
        tmock::assert_equiv(str::slice(s,3,4).c_str(),"3");
        tmock::assert_equiv(str::slice(s,4,4).c_str(),"");
        tmock::assert_equiv(str::slice(s,5,4).c_str(),"");
        tmock::assert_equiv(str::slice(s,50,4).c_str(),"");

        tmock::assert_equiv(str::slice(s,3,-1).c_str(),"345678");
        tmock::assert_equiv(str::slice(s,4,-2).c_str(),"4567");
        tmock::assert_equiv(str::slice(s,5,-3).c_str(),"56");
        tmock::assert_equiv(str::slice(s,5,-4).c_str(),"5");
        tmock::assert_equiv(str::slice(s,5,-5).c_str(),"");
        tmock::assert_equiv(str::slice(s,6,-7).c_str(),"");
        tmock::assert_equiv(str::slice(s,60,-7).c_str(),"");

        tmock::assert_equiv(str::slice(s,-7,-3).c_str(),"3456");
        tmock::assert_equiv(str::slice(s,-7,-4).c_str(),"345");
        tmock::assert_equiv(str::slice(s,-7,-5).c_str(),"34");
        tmock::assert_equiv(str::slice(s,-7,-6).c_str(),"3");
        tmock::assert_equiv(str::slice(s,-8,-7).c_str(),"2");
        tmock::assert_equiv(str::slice(s,-9,-8).c_str(),"1");
        tmock::assert_equiv(str::slice(s,-10,-9).c_str(),"0");
        tmock::assert_equiv(str::slice(s,-11,-9).c_str(),"0");
        tmock::assert_equiv(str::slice(s,-12,-9).c_str(),"0");
        tmock::assert_equiv(str::slice(s,-12,-10).c_str(),"");
        tmock::assert_equiv(str::slice(s,-12,-11).c_str(),"");
        tmock::assert_equiv(str::slice(s,-12,-12).c_str(),"");
        tmock::assert_equiv(str::slice(s,-12,-13).c_str(),"");

        tmock::assert_equiv(str::slice(s,8,12).c_str(),"89");
        tmock::assert_equiv(str::slice(s,9,12).c_str(),"9");
        tmock::assert_equiv(str::slice(s,10,12).c_str(),"");
        tmock::assert_equiv(str::slice(s,1000,1200).c_str(),"");
    }

    TMOCK_TEST(test_strip)
    {
        tmock::assert_equiv(str::strip("  123  ").c_str(),"123");
        tmock::assert_equiv(str::strip("  123  \n").c_str(),"123");
        tmock::assert_equiv(str::strip("123  \n").c_str(),"123");
        tmock::assert_equiv(str::strip("123\n").c_str(),"123");
        tmock::assert_equiv(str::strip("123").c_str(),"123");
        tmock::assert_equiv(str::strip("\t123").c_str(),"123");
        tmock::assert_equiv(str::strip("\t\n123").c_str(),"123");
    }

    TMOCK_TEST(test_split_sep)
    {
        std::vector<std::string> v;

        v = str::split("123,45,6789,10",",");
        tmock::assert_equiv(v.size(),(size_t)4);
        tmock::assert_equiv(v[0].c_str(),"123");
        tmock::assert_equiv(v[1].c_str(),"45");
        tmock::assert_equiv(v[2].c_str(),"6789");
        tmock::assert_equiv(v[3].c_str(),"10");

        v = str::split("123,45,6789,10,",",");
        tmock::assert_equiv(v.size(),(size_t)5);
        tmock::assert_equiv(v[0].c_str(),"123");
        tmock::assert_equiv(v[1].c_str(),"45");
        tmock::assert_equiv(v[2].c_str(),"6789");
        tmock::assert_equiv(v[3].c_str(),"10");
        tmock::assert_equiv(v[4].c_str(),"");

        v = str::split("123a,a45a,a6789a,a10a,a","a,a");
        tmock::assert_equiv(v.size(),(size_t)5);
        tmock::assert_equiv(v[0].c_str(),"123");
        tmock::assert_equiv(v[1].c_str(),"45");
        tmock::assert_equiv(v[2].c_str(),"6789");
        tmock::assert_equiv(v[3].c_str(),"10");
        tmock::assert_equiv(v[4].c_str(),"");
        
        v = str::split("  1  2  3  "," ");
        tmock::assert_equiv(v.size(),(size_t)9);
        tmock::assert_equiv(v[0].c_str(),"");
        tmock::assert_equiv(v[1].c_str(),"");
        tmock::assert_equiv(v[2].c_str(),"1");
        tmock::assert_equiv(v[3].c_str(),"");
        tmock::assert_equiv(v[4].c_str(),"2");
        tmock::assert_equiv(v[5].c_str(),"");
        tmock::assert_equiv(v[6].c_str(),"3");
        tmock::assert_equiv(v[7].c_str(),"");
        tmock::assert_equiv(v[8].c_str(),"");
    }

    TMOCK_TEST(test_split_whitespace)
    {
        std::vector<std::string> v;

        v = str::split("123 45 6789 10");
        tmock::assert_equiv(v.size(),(size_t)4);
        tmock::assert_equiv(v[0].c_str(),"123");
        tmock::assert_equiv(v[1].c_str(),"45");
        tmock::assert_equiv(v[2].c_str(),"6789");
        tmock::assert_equiv(v[3].c_str(),"10");

        v = str::split("123 45 6789 10 ");
        tmock::assert_equiv(v.size(),(size_t)4);
        tmock::assert_equiv(v[0].c_str(),"123");
        tmock::assert_equiv(v[1].c_str(),"45");
        tmock::assert_equiv(v[2].c_str(),"6789");
        tmock::assert_equiv(v[3].c_str(),"10");

        v = str::split("    123   45  6789  10");
        tmock::assert_equiv(v.size(),(size_t)4);
        tmock::assert_equiv(v[0].c_str(),"123");
        tmock::assert_equiv(v[1].c_str(),"45");
        tmock::assert_equiv(v[2].c_str(),"6789");
        tmock::assert_equiv(v[3].c_str(),"10");

        v = str::split("    123   45  6789  10       ");
        tmock::assert_equiv(v.size(),(size_t)4);
        tmock::assert_equiv(v[0].c_str(),"123");
        tmock::assert_equiv(v[1].c_str(),"45");
        tmock::assert_equiv(v[2].c_str(),"6789");
        tmock::assert_equiv(v[3].c_str(),"10");
    }
};

TMOCK_MAIN();
