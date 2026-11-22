// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "acl.h"
#include "root.h"
#include "exception.h"
#include <futil/xact.h>
#include <strutil/strutil.h>
#include <algorithm>
#include <functional>
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

static bool
entry_less(const tsdb::acl_entry& l, const tsdb::acl_entry& r)
{
    if (l.user != r.user)
        return l.user < r.user;
    return l.database < r.database;
}

void
tsdb::acl::grant(const std::string& user, const std::string& database,
    uint32_t perms)
{
    for (auto& e : entries)
    {
        if (e.user == user && e.database == database)
        {
            e.perms |= perms;
            return;
        }
    }
    entries.push_back(acl_entry{user,database,perms});
    std::sort(entries.begin(),entries.end(),entry_less);
}

void
tsdb::acl::revoke(const std::string& user, const std::string& database,
    uint32_t perms)
{
    for (auto iter = entries.begin(); iter != entries.end(); ++iter)
    {
        if (iter->user == user && iter->database == database)
        {
            iter->perms &= ~perms;
            if (!iter->perms)
                entries.erase(iter);
            return;
        }
    }
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

    std::sort(a.entries.begin(),a.entries.end(),entry_less);
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

static void
write_acl_file(const tsdb::root& r, const tsdb::acl& a)
{
    // Writes the new acl file in tmp/ and renames it into place, so that
    // tsdbserver, which reads it without locking, always sees a whole file.
    auto text = a.to_string();
    futil::xact_mktemp tmp_fd(r.tmp_dir,0660);
    tmp_fd.write_all(text.c_str(),text.size());
    tmp_fd.fsync();
    futil::rename(r.tmp_dir,tmp_fd.name,r.root_dir,"acl");
    tmp_fd.commit();
    r.root_dir.fsync_and_flush();
    r.tmp_dir.fsync_and_flush();
}

static bool
passwd_has_user(const tsdb::root& r, const std::string& user)
{
    // The caller holds passwd.lock.  Don't flock() it again here: a second
    // lock on another descriptor for the same file would wait for ours.
    futil::file passwd_fd(r.root_dir,"passwd",O_RDONLY);
    for (const std::string& line : passwd_fd.lines())
    {
        auto parts = str::split(line);
        if (!parts.empty() && parts[0] == user)
            return true;
    }
    return false;
}

static void
update_acl(tsdb::root& r, const std::function<void(tsdb::acl&)>& f)
{
    // Changes to the acl file are serialized by an exclusive lock on
    // passwd.lock, the same lock that add_user() takes, so that two
    // tsdbcli2 sessions can't both read the old file and have the second
    // rename discard the first one's change.
    futil::file lock_fd(r.root_dir,"passwd.lock",O_RDONLY);
    lock_fd.flock(LOCK_EX);

    auto a = r.load_acl();
    if (!a)
        throw tsdb::acls_disabled_exception();
    f(*a);
    write_acl_file(r,*a);
}

static void
validate_acl_database(const std::string& database)
{
    if (database != "*")
        tsdb::validate_database_name(database);
}

bool
tsdb::root::enable_acls()
{
    futil::file lock_fd(root_dir,"passwd.lock",O_RDONLY);
    lock_fd.flock(LOCK_EX);

    if (load_acl())
        return false;
    write_acl_file(*this,acl());
    return true;
}

void
tsdb::root::grant(const std::string& user, const std::string& database,
    uint32_t perms)
{
    kassert(perms && !(perms & ~PERM_ALL));
    validate_acl_database(database);
    update_acl(*this,[&](acl& a)
    {
        if (!passwd_has_user(*this,user))
            throw no_such_user_exception();
        a.grant(user,database,perms);
    });
}

void
tsdb::root::revoke(const std::string& user, const std::string& database,
    uint32_t perms)
{
    kassert(perms && !(perms & ~PERM_ALL));
    validate_acl_database(database);
    update_acl(*this,[&](acl& a)
    {
        a.revoke(user,database,perms);
    });
}
