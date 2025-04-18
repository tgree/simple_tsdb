// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "parse_types.h"
#include "parse.h"
#include <futil/futil.h>
#include <strutil/strutil.h>

template<> fields_list
parse_type<fields_list>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    if (begin == end)
        throw parse_exception("Expected fields specifier.");

    std::vector<std::string> fields;

    if (*begin != "*")
    {
        while (begin != end)
        {
            auto v = str::split(*begin,",");
            size_t n = begin->ends_with(",") ? v.size() - 1 : v.size();
            for (size_t i=0; i<n; ++i)
                fields.emplace_back(std::move(v[i]));
            
            if (!(*begin++).ends_with(","))
                break;
        }
        if (fields.empty())
            throw parse_exception("Expected fields specifier.");
    }
    else
        ++begin;

    return fields_list{std::move(fields)};
}

template<> database_specifier
parse_type<database_specifier>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    if (begin == end)
        throw parse_exception("Expected <database>.");

    auto& specifier = *begin;
    if (std::count(specifier.begin(),specifier.end(),'/') > 0)
        throw parse_exception("Not a <database> specifier.");

    ++begin;
    return database_specifier{specifier};
}

template<> measurement_specifier
parse_type<measurement_specifier>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    if (begin == end)
        throw parse_exception("Expected <database/measurement>.");

    auto& specifier = begin[0];
    if (std::count(specifier.begin(),specifier.end(),'/') != 1)
        throw parse_exception("Not a <database/measurement> specifier.");

    auto components = futil::path(specifier).decompose();
    ++begin;
    return measurement_specifier{components[0],components[1]};
}

template<> series_specifier
parse_type<series_specifier>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    if (begin == end)
        throw parse_exception("Expected <database/measurement/series>.");

    auto& specifier = *begin;
    if (std::count(specifier.begin(),specifier.end(),'/') != 2)
        throw parse_exception("Not a <database/measurement/series> specifier.");

    auto components = futil::path(specifier).decompose();
    ++begin;
    return series_specifier{components[0],components[1],components[2]};
}

template<> fields_specifier
parse_type<fields_specifier>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    if (end - begin < 3 || strcasecmp(begin[0].c_str(),"with") ||
        strcasecmp(begin[1].c_str(),"fields"))
    {
        throw parse_exception("Expected 'WITH FIELDS <field_spec>'.");
    }

    std::vector<tsdb::schema_entry> fields;
    auto field_specifiers = str::split(begin[2],",");
    size_t offset = 0;
    for (size_t i=0; i<field_specifiers.size(); ++i)
    {
        const auto& fs = field_specifiers[i];
        auto field_specifier = str::split(fs,"/");
        if (field_specifier.size() != 2 || field_specifier[0].empty() ||
            field_specifier[1].empty())
        {
            throw parse_exception("Invalid field specifier.");
        }
        if (field_specifier[0].size() >= 124)
        {
            throw parse_exception(
                str::printf("Field name '%s' too long.",
                            field_specifier[0].c_str()));
        }

        tsdb::schema_entry se{};
        se.version = SCHEMA_VERSION;
        se.index = i;
        se.offset = offset;
        strcpy(se.name,field_specifier[0].c_str());
        if (field_specifier[1] == "bool")
            se.type = tsdb::FT_BOOL;
        else if (field_specifier[1] == "u32")
            se.type = tsdb::FT_U32;
        else if (field_specifier[1] == "u64")
            se.type = tsdb::FT_U64;
        else if (field_specifier[1] == "f32")
            se.type = tsdb::FT_F32;
        else if (field_specifier[1] == "f64")
            se.type = tsdb::FT_F64;
        else if (field_specifier[1] == "i32")
            se.type = tsdb::FT_I32;
        else if (field_specifier[1] == "i64")
            se.type = tsdb::FT_I64;
        else
        {
            throw parse_exception(
                str::printf("Unrecognized field type '%s'.",
                            field_specifier[1].c_str()));
        }
        offset += tsdb::ftinfos[se.type].nbytes;

        fields.push_back(se);
    }

    begin += 3;
    return fields_specifier{std::move(fields)};
}

static select_time_range
parse_time_range_6_arg(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    // WHERE T0 LT_LE_OP time_ns LT_LE_OP T1
    if (strcasecmp(begin[0].c_str(),"where") || begin[3] != "time_ns")
        throw parse_exception("Expected 'WHERE T LT_LEOP time_ns LT_LEOP T'.");

    uint64_t t0;
    uint64_t t1;
    try
    {
        t0 = std::stoul(begin[1]);
        t1 = std::stoul(begin[5]);
    }
    catch (const std::invalid_argument&)
    {
        throw parse_exception("Expected integer time.");
    }

    if (begin[2] == "<")
        t0 += 1;
    else if (begin[2] != "<=")
        throw parse_exception("Expected 'WHERE T LT_LEOP time_ns LT_LEOP T'.");
    if (begin[4] == "<")
        t1 += 1;
    else if (begin[4] != "<=")
        throw parse_exception("Expected 'WHERE T LT_LEOP time_ns LT_LEOP T'.");

    begin += 6;
    return select_time_range{t0,t1};
}

static select_time_range
parse_time_range_4_arg(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    // WHERE time_ns OP T
    if (strcasecmp(begin[0].c_str(),"where") || begin[1] != "time_ns")
        throw parse_exception("Expected 'WHERE time_ns OP T'.");

    uint64_t t;
    try
    {
        t = std::stoul(begin[3]);
    }
    catch (const std::invalid_argument&)
    {
        throw parse_exception("Expected integer time.");
    }

    auto& op = begin[2];
    begin += 4;
    if (op == "<")
        return select_time_range{0,t-1};
    if (op == "<=")
        return select_time_range{0,t};
    if (op == "==")
        return select_time_range{t,t};
    if (op == ">=")
        return select_time_range{t,(uint64_t)-1};
    if (op == ">")
        return select_time_range{t+1,(uint64_t)-1};

    begin -= 4;
    throw parse_exception("Expected 'WHERE time_ns OP T'.");
}

template<> select_time_range
parse_type<select_time_range>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    // WHERE T0 LT_LE_OP time_ns LT_LE_OP T1
    // WHERE time_ns OP T
    // []
    size_t avail_args = end - begin;
    if (!avail_args || strcasecmp(begin[0].c_str(),"where"))
        return select_time_range{0,(uint64_t)-1};

    if (avail_args >= 6)
    {
        try
        {
            return parse_time_range_6_arg(begin,end);
        }
        catch (const parse_exception&)
        {
        }
    }
    if (avail_args >= 4)
    {
        try
        {
            return parse_time_range_4_arg(begin,end);
        }
        catch (const parse_exception&)
        {
        }
    }

    return select_time_range{0,(uint64_t)-1};
}

template<> delete_time_range
parse_type<delete_time_range>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    // WHERE time_ns {<, <=} T
    if (end - begin < 4 || strcasecmp(begin[0].c_str(),"where") ||
        begin[1] != "time_ns")
    {
        throw parse_exception("Expected 'WHERE time_ns {<, <=} T'.");
    }

    uint64_t t;
    try
    {
        t = std::stoul(begin[3]);
    }
    catch (const std::invalid_argument&)
    {
        throw parse_exception("Expected integer time.");
    }

    auto& op = begin[2];
    begin += 4;
    if (op == "<")
        return delete_time_range{t-1};
    if (op == "<=")
        return delete_time_range{t};

    begin -= 4;
    throw parse_exception("Expected 'WHERE time_ns {<, <=} T'.");
}

template<> select_limit
parse_type<select_limit>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    size_t avail_args = end - begin;
    if (avail_args < 2 || strcasecmp(begin[0].c_str(),"limit"))
        throw parse_exception("Expected 'LIMIT N'.");

    uint64_t n;
    try
    {
        n = std::stoul(begin[1]);
    }
    catch (const std::invalid_argument&)
    {
        throw parse_exception("Expected integer limit.");
    }

    begin += 2;
    return select_limit{n};
}

template<> select_last
parse_type<select_last>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    size_t avail_args = end - begin;
    if (avail_args < 2 || strcasecmp(begin[0].c_str(),"last"))
        throw parse_exception("Expected 'LAST N'.");

    uint64_t n;
    try
    {
        n = std::stoul(begin[1]);
    }
    catch (const std::invalid_argument&)
    {
        throw parse_exception("Expected integer last.");
    }
    
    begin += 2;
    return select_last{n};
}

template<> window_ns
parse_type<window_ns>(
    std::vector<std::string>::iterator& begin,
    std::vector<std::string>::iterator end)
{
    size_t avail_args = end - begin;
    if (avail_args < 2 || begin[0] != "window_ns")
        throw parse_exception("Expected 'window_ns N'.");

    uint64_t n;
    try
    {
        n = std::stoul(begin[1]);
    }
    catch (const std::invalid_argument&)
    {
        throw parse_exception("Expected integer window_ns.");
    }
    
    begin += 2;
    return window_ns{n};
}
