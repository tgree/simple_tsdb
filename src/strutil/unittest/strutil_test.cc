// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../strutil.h"
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_strslice)
    {
        std::string s("0123456789");
        TASSERT_EQUIV(str::slice(s).c_str(),s.c_str());
        TASSERT_EQUIV(str::slice(s,1).c_str(),"123456789");
        TASSERT_EQUIV(str::slice(s,2).c_str(),"23456789");
        TASSERT_EQUIV(str::slice(s,3).c_str(),"3456789");
        TASSERT_EQUIV(str::slice(s,4).c_str(),"456789");
        TASSERT_EQUIV(str::slice(s,5).c_str(),"56789");
        TASSERT_EQUIV(str::slice(s,6).c_str(),"6789");
        TASSERT_EQUIV(str::slice(s,7).c_str(),"789");
        TASSERT_EQUIV(str::slice(s,8).c_str(),"89");
        TASSERT_EQUIV(str::slice(s,9).c_str(),"9");
        TASSERT_EQUIV(str::slice(s,10).c_str(),"");
        TASSERT_EQUIV(str::slice(s,11).c_str(),"");
        TASSERT_EQUIV(str::slice(s,12).c_str(),"");

        TASSERT_EQUIV(str::slice(s,1,4).c_str(),"123");
        TASSERT_EQUIV(str::slice(s,2,4).c_str(),"23");
        TASSERT_EQUIV(str::slice(s,3,4).c_str(),"3");
        TASSERT_EQUIV(str::slice(s,4,4).c_str(),"");
        TASSERT_EQUIV(str::slice(s,5,4).c_str(),"");
        TASSERT_EQUIV(str::slice(s,50,4).c_str(),"");

        TASSERT_EQUIV(str::slice(s,3,-1).c_str(),"345678");
        TASSERT_EQUIV(str::slice(s,4,-2).c_str(),"4567");
        TASSERT_EQUIV(str::slice(s,5,-3).c_str(),"56");
        TASSERT_EQUIV(str::slice(s,5,-4).c_str(),"5");
        TASSERT_EQUIV(str::slice(s,5,-5).c_str(),"");
        TASSERT_EQUIV(str::slice(s,6,-7).c_str(),"");
        TASSERT_EQUIV(str::slice(s,60,-7).c_str(),"");

        TASSERT_EQUIV(str::slice(s,-7,-3).c_str(),"3456");
        TASSERT_EQUIV(str::slice(s,-7,-4).c_str(),"345");
        TASSERT_EQUIV(str::slice(s,-7,-5).c_str(),"34");
        TASSERT_EQUIV(str::slice(s,-7,-6).c_str(),"3");
        TASSERT_EQUIV(str::slice(s,-8,-7).c_str(),"2");
        TASSERT_EQUIV(str::slice(s,-9,-8).c_str(),"1");
        TASSERT_EQUIV(str::slice(s,-10,-9).c_str(),"0");
        TASSERT_EQUIV(str::slice(s,-11,-9).c_str(),"0");
        TASSERT_EQUIV(str::slice(s,-12,-9).c_str(),"0");
        TASSERT_EQUIV(str::slice(s,-12,-10).c_str(),"");
        TASSERT_EQUIV(str::slice(s,-12,-11).c_str(),"");
        TASSERT_EQUIV(str::slice(s,-12,-12).c_str(),"");
        TASSERT_EQUIV(str::slice(s,-12,-13).c_str(),"");

        TASSERT_EQUIV(str::slice(s,8,12).c_str(),"89");
        TASSERT_EQUIV(str::slice(s,9,12).c_str(),"9");
        TASSERT_EQUIV(str::slice(s,10,12).c_str(),"");
        TASSERT_EQUIV(str::slice(s,1000,1200).c_str(),"");
    }

    TMOCK_TEST(test_strip)
    {
        TASSERT_EQUIV(str::strip("  123  ").c_str(),"123");
        TASSERT_EQUIV(str::strip("  123  \n").c_str(),"123");
        TASSERT_EQUIV(str::strip("123  \n").c_str(),"123");
        TASSERT_EQUIV(str::strip("123\n").c_str(),"123");
        TASSERT_EQUIV(str::strip("123").c_str(),"123");
        TASSERT_EQUIV(str::strip("\t123").c_str(),"123");
        TASSERT_EQUIV(str::strip("\t\n123").c_str(),"123");
    }

    TMOCK_TEST(test_split_sep)
    {
        std::vector<std::string> v;

        v = str::split("123,45,6789,10",",");
        TASSERT_EQUIV(v.size(),(size_t)4);
        TASSERT_EQUIV(v[0].c_str(),"123");
        TASSERT_EQUIV(v[1].c_str(),"45");
        TASSERT_EQUIV(v[2].c_str(),"6789");
        TASSERT_EQUIV(v[3].c_str(),"10");

        v = str::split("123,45,6789,10,",",");
        TASSERT_EQUIV(v.size(),(size_t)5);
        TASSERT_EQUIV(v[0].c_str(),"123");
        TASSERT_EQUIV(v[1].c_str(),"45");
        TASSERT_EQUIV(v[2].c_str(),"6789");
        TASSERT_EQUIV(v[3].c_str(),"10");
        TASSERT_EQUIV(v[4].c_str(),"");

        v = str::split("123a,a45a,a6789a,a10a,a","a,a");
        TASSERT_EQUIV(v.size(),(size_t)5);
        TASSERT_EQUIV(v[0].c_str(),"123");
        TASSERT_EQUIV(v[1].c_str(),"45");
        TASSERT_EQUIV(v[2].c_str(),"6789");
        TASSERT_EQUIV(v[3].c_str(),"10");
        TASSERT_EQUIV(v[4].c_str(),"");
        
        v = str::split("  1  2  3  "," ");
        TASSERT_EQUIV(v.size(),(size_t)9);
        TASSERT_EQUIV(v[0].c_str(),"");
        TASSERT_EQUIV(v[1].c_str(),"");
        TASSERT_EQUIV(v[2].c_str(),"1");
        TASSERT_EQUIV(v[3].c_str(),"");
        TASSERT_EQUIV(v[4].c_str(),"2");
        TASSERT_EQUIV(v[5].c_str(),"");
        TASSERT_EQUIV(v[6].c_str(),"3");
        TASSERT_EQUIV(v[7].c_str(),"");
        TASSERT_EQUIV(v[8].c_str(),"");
    }

    TMOCK_TEST(test_split_whitespace)
    {
        std::vector<std::string> v;

        v = str::split("123 45 6789 10");
        TASSERT_EQUIV(v.size(),(size_t)4);
        TASSERT_EQUIV(v[0].c_str(),"123");
        TASSERT_EQUIV(v[1].c_str(),"45");
        TASSERT_EQUIV(v[2].c_str(),"6789");
        TASSERT_EQUIV(v[3].c_str(),"10");

        v = str::split("123 45 6789 10 ");
        TASSERT_EQUIV(v.size(),(size_t)4);
        TASSERT_EQUIV(v[0].c_str(),"123");
        TASSERT_EQUIV(v[1].c_str(),"45");
        TASSERT_EQUIV(v[2].c_str(),"6789");
        TASSERT_EQUIV(v[3].c_str(),"10");

        v = str::split("    123   45  6789  10");
        TASSERT_EQUIV(v.size(),(size_t)4);
        TASSERT_EQUIV(v[0].c_str(),"123");
        TASSERT_EQUIV(v[1].c_str(),"45");
        TASSERT_EQUIV(v[2].c_str(),"6789");
        TASSERT_EQUIV(v[3].c_str(),"10");

        v = str::split("    123   45  6789  10       ");
        TASSERT_EQUIV(v.size(),(size_t)4);
        TASSERT_EQUIV(v[0].c_str(),"123");
        TASSERT_EQUIV(v[1].c_str(),"45");
        TASSERT_EQUIV(v[2].c_str(),"6789");
        TASSERT_EQUIV(v[3].c_str(),"10");
    }

    TMOCK_TEST(test_decode_units_pow2)
    {
        TASSERT_EQUIV(str::decode_number_units_pow2("12345678"),12345678UL);
        TASSERT_EQUIV(str::decode_number_units_pow2("12K"),12UL*1024UL);
        TASSERT_EQUIV(str::decode_number_units_pow2("12M"),12UL*1024UL*1024UL);
        TASSERT_EQUIV(str::decode_number_units_pow2("12G"),
                      12UL*1024UL*1024UL*1024UL);
        TASSERT_EQUIV(str::decode_number_units_pow2("12T"),
                      12UL*1024UL*1024UL*1024UL*1024UL);

        try
        {
            str::decode_number_units_pow2("");
            TABORT("Expected invalid_argument exception for empty "
                         "string.");
        }
        catch (std::invalid_argument&)
        {
        }

        try
        {
            str::decode_number_units_pow2("asdf");
            TABORT("Expected invalid_argument exception for text "
                         "string.");
        }
        catch (std::invalid_argument&)
        {
        }

        try
        {
            str::decode_number_units_pow2("12asdf");
            TABORT("Expected invalid_argument exception for garbage "
                         "string.");
        }
        catch (std::invalid_argument&)
        {
        }

        try
        {
            str::decode_number_units_pow2("12asdfM");
            TABORT("Expected invalid_argument exception for garbage "
                         "M string.");
        }
        catch (std::invalid_argument&)
        {
        }
    }
};

TMOCK_MAIN();
