// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "acl.h"
#include "root.h"
#include "exception.h"
#include <strutil/strutil.h>
#include <algorithm>
#include <strings.h>

static const struct
{
    const char* name;
    uint32_t    perm;
} perm_names[] =
{
    {"read",    tsdb::PERM_READ},
    {"write",   tsdb::PERM_WRITE},
    {"delete",  tsdb::PERM_DELETE},
};

uint32_t
tsdb::parse_permissions(const std::string& s)
{
    uint32_t perms = 0;
    for (const auto& name : str::split(s,","))
    {
        if (!strcasecmp(name.c_str(),"all"))
        {
            perms |= PERM_ALL;
            continue;
        }

        uint32_t perm = 0;
        for (const auto& pn : perm_names)
        {
            if (!strcasecmp(name.c_str(),pn.name))
                perm = pn.perm;
        }
        if (!perm)
            return 0;
        perms |= perm;
    }
    return perms;
}

std::string
tsdb::permissions_to_string(uint32_t perms)
{
    if (perms == PERM_ALL)
        return "all";

    std::vector<std::string> names;
    for (const auto& pn : perm_names)
    {
        if (perms & pn.perm)
            names.push_back(pn.name);
    }
    return str::join(names,",");
}

uint32_t
tsdb::acl::permissions(const std::string& user,
    const std::string& database) const
{
    uint32_t perms = 0;
    for (const auto& e : entries)
    {
        if (e.user == user && (e.database == database || e.database == "*"))
            perms |= e.perms;
    }
    return perms;
}

std::string
tsdb::acl::to_string() const
{
    size_t user_width = 0;
    size_t database_width = 0;
    for (const auto& e : entries)
    {
        user_width = std::max(user_width,e.user.size());
        database_width = std::max(database_width,e.database.size());
    }

    std::string s = std::string(acl_file_header) + "\n";
    for (const auto& e : entries)
    {
        s += str::printf("%-*s %-*s %s\n",
                         (int)user_width,e.user.c_str(),
                         (int)database_width,e.database.c_str(),
                         permissions_to_string(e.perms).c_str());
    }
    return s;
}

tsdb::acl
tsdb::acl::parse(const std::string& text)
{
    acl a;
    size_t line_num = 0;
    for (const auto& line : str::split(text,"\n"))
    {
        ++line_num;
        auto parts = str::split(line);
        if (parts.empty() || parts[0][0] == '#')
            continue;
        if (parts.size() != 3)
            throw invalid_acl_file_exception(line_num);

        acl_entry e{parts[0],parts[1],parse_permissions(parts[2])};
        if (e.database != "*" && !is_valid_name(e.database))
            throw invalid_acl_file_exception(line_num);
        if (!e.perms)
            throw invalid_acl_file_exception(line_num);
        for (const auto& other : a.entries)
        {
            if (other.user == e.user && other.database == e.database)
                throw invalid_acl_file_exception(line_num);
        }
        a.entries.push_back(e);
    }

    std::sort(a.entries.begin(),a.entries.end(),
        [](const acl_entry& l, const acl_entry& r)
        {
            if (l.user != r.user)
                return l.user < r.user;
            return l.database < r.database;
        });
    return a;
}

std::optional<tsdb::acl>
tsdb::root::load_acl() const
{
    std::string text;
    try
    {
        futil::file acl_fd(root_dir,"acl",O_RDONLY);
        size_t len = acl_fd.lseek(0,SEEK_END);
        acl_fd.lseek(0,SEEK_SET);
        text.resize(len);
        acl_fd.read_all(&text[0],len);
    }
    catch (const futil::errno_exception& e)
    {
        if (e.errnov == ENOENT)
            return std::nullopt;
        throw;
    }
    return acl::parse(text);
}
