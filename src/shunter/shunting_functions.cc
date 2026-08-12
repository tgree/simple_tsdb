// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "shunting_functions.h"
#include <libtsdb/exception.h>
#include <unordered_map>
#include <math.h>

static void
sf_pow(std::vector<double>& stack)
{
    if (stack.size() < 2)
        throw tsdb::shunt_missing_func_arg_exception();
    double exponent = stack.back();
    stack.pop_back();
    double base = stack.back();
    stack.pop_back();
    stack.push_back(::pow(base,exponent));
}

static void
sf_exp(std::vector<double>& stack)
{
    if (stack.empty())
        throw tsdb::shunt_missing_func_arg_exception();
    double v = stack.back();
    stack.pop_back();
    stack.push_back(::exp(v));
}

static void
sf_ln(std::vector<double>& stack)
{
    if (stack.empty())
        throw tsdb::shunt_missing_func_arg_exception();
    double v = stack.back();
    stack.pop_back();
    stack.push_back(::log(v));
}

static void
sf_log2(std::vector<double>& stack)
{
    if (stack.empty())
        throw tsdb::shunt_missing_func_arg_exception();
    double v = stack.back();
    stack.pop_back();
    stack.push_back(::log2(v));
}

static void
sf_log10(std::vector<double>& stack)
{
    if (stack.empty())
        throw tsdb::shunt_missing_func_arg_exception();
    double v = stack.back();
    stack.pop_back();
    stack.push_back(::log10(v));
}

static void
sf_min(std::vector<double>& stack)
{
    if (stack.size() < 2)
        throw tsdb::shunt_missing_func_arg_exception();
    double rhs = stack.back();
    stack.pop_back();
    double lhs = stack.back();
    stack.pop_back();
    stack.push_back(rhs < lhs ? rhs : lhs);
}

static void
sf_max(std::vector<double>& stack)
{
    if (stack.size() < 2)
        throw tsdb::shunt_missing_func_arg_exception();
    double rhs = stack.back();
    stack.pop_back();
    double lhs = stack.back();
    stack.pop_back();
    stack.push_back(rhs > lhs ? rhs : lhs);
}

static void
sf_pi(std::vector<double>& stack)
{
    stack.push_back(M_PI);
}

static void
sf_e(std::vector<double>& stack)
{
    stack.push_back(2.7182818284590452353602);
}

static void
sf_sin(std::vector<double>& stack)
{
    if (stack.empty())
        throw tsdb::shunt_missing_func_arg_exception();
    double v = stack.back();
    stack.pop_back();
    stack.push_back(::sin(v));
}

static void
sf_cos(std::vector<double>& stack)
{
    if (stack.empty())
        throw tsdb::shunt_missing_func_arg_exception();
    double v = stack.back();
    stack.pop_back();
    stack.push_back(::cos(v));
}

static std::unordered_map<std::string,void (*)(std::vector<double>&)> func_map =
{
    {"pow",     sf_pow},
    {"exp",     sf_exp},
    {"ln",      sf_ln},
    {"log2",    sf_log2},
    {"log10",   sf_log10},
    {"min",     sf_min},
    {"max",     sf_max},
    {"pi",      sf_pi},
    {"e",       sf_e},
    {"sin",     sf_sin},
    {"cos",     sf_cos},
};

void
shunting_functions::populate(shunting_yard::shunter& s)
{
    for (auto& f : s.functions)
    {
        auto it = func_map.find(f.name);
        if (it != func_map.end())
            f.func = it->second;
    }
}
