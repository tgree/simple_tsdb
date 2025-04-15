// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_STRUTIL_STRUTIL_H
#define __SRC_STRUTIL_STRUTIL_H

#include <hdr/compiler.h>
#include <string.h>
#include <string>
#include <vector>
#include <ctype.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdexcept>

namespace str
{
    struct slice_indices
    {
        size_t i;
        size_t j;

        constexpr slice_indices(ssize_t size, ssize_t _i, ssize_t _j):
            i(_i >= 0 ? ( _i >= size ? size : _i       ) :
                        (-_i >= size ? 0    : _i + size)),
            j(_j >= 0 ? ( _j >= size ? size : _j       ) :
                        (-_j >= size ? 0    : _j + size))
        {
        }
    };

    // Returns a slice of the string, from index i to index j-1, inclusive.
    // Negative indices are measured from the end of the string with index -1
    // being the last character, -2 the second-last character, etc.
    inline std::string
    slice(const std::string& s, ssize_t i = 0, ssize_t j = 0x7FFFFFFFFFFFFFFFLL)
    {
        slice_indices si(s.size(),i,j);
        return (si.j <= si.i ? "" : std::string(s,si.i,si.j-si.i));
    }

    // Strips whitespace from either end of the string.
    inline std::string
    strip(const std::string& s)
    {
        size_t i = s.find_first_not_of("\n\r\t\f ");
        if (i == std::string::npos)
            return "";

        size_t j = s.find_last_not_of("\n\r\t\f ");
        return std::string(s,i,j-i+1);
    }

    // Splits the string using whitespace as the separator.  Note that this is
    // quite different from doing a str::split(s," "), even in the base case
    // where s is separated by single spaces between words.  For instance:
    //
    //      split_whitespace("  1  2  3  ") = ["1", "2", "3"]
    //
    // whereas:
    //
    //      split("  1  2  3  "," ") = ["", "", "1", "", "2", "", "3", "", ""]
    //
    // This is clearer if you replace the separator in the split() case with a
    // comma instead.
    inline std::vector<std::string>
    split_whitespace(const std::string& s)
    {
        std::vector<std::string> v;

        size_t i = s.find_first_not_of("\n\r\t\f ");
        while (i != std::string::npos)
        {
            size_t j = s.find_first_of("\n\r\t\f ",i);
            if (j == std::string::npos)
            {
                v.push_back(slice(s,i));
                break;
            }
            v.push_back(slice(s,i,j));

            i = s.find_first_not_of("\n\r\t\f ",j);
        }

        return v;
    }

    // Splits the string.  If sep is specified, then it is used as the
    // separator and may be more than one character long.  If sep is empty or
    // NULL, then any kind of whitespace is used as the separator.
    inline std::vector<std::string>
    split(const std::string& s, const char* sep = NULL)
    {
        if (!sep || sep[0] == '\0')
            return split_whitespace(s);

        std::vector<std::string> v;
        size_t sep_len = strlen(sep);
        for (size_t i = 0;;)
        {
            size_t j = s.find(sep,i);
            if (j == std::string::npos)
            {
                v.push_back(slice(s,i));
                break;
            }
            v.push_back(slice(s,i,j));
            i = j + sep_len;
        }

        return v;
    }

    // Returns true if all charactes in the string are printable.
    inline bool
    isprint(const std::string& s)
    {
        for (char c: s)
        {
            if (!::isprint(c))
                return false;
        }
        return true;
    }

    // Returns a printf-style formatted string.
    inline std::string __PRINTF__(1,2)
    printf(const char* fmt, ...)
    {
        char* buf;

        va_list ap;
        va_start(ap,fmt);
        vasprintf(&buf,fmt,ap);
        va_end(ap);

        if (!buf)
            return "";

        std::string s(buf);
        free(buf);
        return s;
    }

    // Decodes a string that has a suffix in power-of-2 style.
    inline uint64_t
    decode_number_units_pow2(const std::string& s)
    {
        if (s.empty())
            throw std::invalid_argument("Empty string");

        uint64_t multiplier = 1;
        uint64_t expected_pos = s.size();
        switch (s[s.size()-1])
        {
            case 'T':
                multiplier = 1024ULL*1024ULL*1024ULL*1024ULL;
                --expected_pos;
            break;

            case 'G':
                multiplier = 1024ULL*1024ULL*1024ULL;
                --expected_pos;
            break;

            case 'M':
                multiplier = 1024ULL*1024ULL;
                --expected_pos;
            break;

            case 'K':
                multiplier = 1024ULL;
                --expected_pos;
            break;
        }

        size_t pos;
        uint64_t v = std::stoul(s,&pos);
        if (pos != expected_pos)
            throw std::invalid_argument("Extra characters");

        return multiplier*v;
    }
}

#endif /* __SRC_STRUTIL_STRUTIL_H */
