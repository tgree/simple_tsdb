// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __FUTIL_FAKEFS_H
#define __FUTIL_FAKEFS_H

#include <hdr/kassert.h>
#include <string>
#include <vector>
#include <map>
#include <set>

struct file_node;

struct dir_node
{
    dir_node*                           parent;
    std::string                         name;
    mode_t                              mode;
    size_t                              refcount;
    bool                                meta_fsynced;
    std::set<std::string>               dirty_unlinks;
    std::map<std::string,file_node*>    files;
    std::map<std::string,dir_node*>     subdirs;

    file_node* get_file(const std::string& name) const
    {
        auto iter = files.find(name);
        kassert(iter != files.end());
        return iter->second;
    }

    dir_node* get_dir(const std::string& name) const
    {
        auto iter = subdirs.find(name);
        kassert(iter != subdirs.end());
        return iter->second;
    }

    std::string path() const
    {
        if (!parent)
            return "/";
        return parent->path() + name + "/";
    }
};

struct file_node
{
    dir_node*               parent;
    std::string             name;
    mode_t                  mode;
    size_t                  refcount;
    size_t                  shared_locks;
    size_t                  exclusive_locks;
    size_t                  mmap_count;
    bool                    data_fsynced;
    bool                    meta_fsynced;
    std::vector<uint8_t>    data;

    std::string data_as_string() const
    {
        return std::string(data.begin(),data.end());
    }
    template<size_t N>
    void set_data(const char (&d)[N])
    {
        data.resize(N);
        memcpy(&data[0],&d[0],N);
    }
    template<typename T>
    T& get_data(size_t offset = 0)
    {
        kassert(offset + sizeof(T) <= data.size());
        return *(T*)&data[offset];
    }
    template<typename T>
    T* as_array(size_t offset = 0)
    {
        return (T*)&data[offset];
    }

    std::string path() const
    {
        return parent->path() + name;
    }
};

enum fd_type
{
    FDT_FREE,
    FDT_FILE,
    FDT_DIRECTORY,
};

struct file_descriptor
{
    fd_type     type = FDT_FREE;
    off_t       pos = 0;
    size_t      shared_locks = 0;
    size_t      exclusive_locks = 0;
    union
    {
        file_node*  file;
        dir_node*   directory;
    };
};

extern void delete_tree(dir_node* dn);
extern dir_node* clone_tree(dir_node* dn);
extern void fsync_tree(dir_node* dn);
extern bool is_tree_fsynced(dir_node* dn);
extern void assert_tree_fsynced(dir_node* dn);
extern void assert_children_fsynced(dir_node* dn);
extern void snapshot_fs();
extern void snapshot_reset();
extern void snapshot_auto_begin();
extern void snapshot_auto_end();
extern void activate_and_fsync_snapshot(dir_node* dn);
extern void nuke_fs_root();

extern dir_node* fs_root;
extern std::vector<dir_node*> snapshots;
extern std::set<dir_node*> live_dirs;
extern std::set<file_node*> live_files;
extern file_descriptor fd_table[128];

#endif /* __FUTIL_FAKEFS_H */
