// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_TSDBCLI_PARSE_H
#define __SRC_TSDBCLI_PARSE_H

#include <exception>
#include <string>
#include <vector>
#include <tuple>

struct parse_exception : public std::exception
{
    std::string msg;

    virtual const char* what() const noexcept override
    {
        return msg.c_str();
    }

    parse_exception(const std::string& msg):msg(msg) {}
};

template<typename T>
T parse_type(std::vector<std::string>::iterator& begin,
             const std::vector<std::string>::iterator end);

template<> std::string
inline parse_type<std::string>(
    std::vector<std::string>::iterator& begin,
    const std::vector<std::string>::iterator end)
{
    if (begin == end)
        throw parse_exception("Expected string.");
    return *begin++;
}

template<typename FuncSig, FuncSig& Func>
struct arg_translator;
template<typename... Args, void(&Func)(Args...)>
struct arg_translator<void(Args...),Func>
{
    static void do_it(std::vector<std::string>::iterator begin,
                      std::vector<std::string>::iterator end)
    {
        std::tuple<typename std::decay<Args>::type...> args{
            parse_type<typename std::decay<Args>::type>(begin,end)...};
        if (begin != end)
            throw parse_exception("Extra arguments.");

        std::apply(Func,args);
    }
};

typedef void (*parse_handler_func)(
    std::vector<std::string>::iterator begin,
    std::vector<std::string>::iterator end);

#define XLATE(F) arg_translator<decltype(F),F>::do_it

#endif /* __SRC_TSDBCLI_PARSE_H */
