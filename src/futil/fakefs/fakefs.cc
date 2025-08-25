// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "fakefs.h"
#include "../futil_wrap.h"
#include <strutil/strutil.h>
#include <hdr/kassert.h>
#include <hdr/kmath.h>
#include <hdr/types.h>
#include <dirent.h>

struct ffs_DIR
{
    int at_fd;
    dir_node* dn;
    struct dirent de;
    std::map<std::string,file_node*>::iterator files_iter;
    size_t special_iter;
    std::map<std::string,dir_node*>::iterator subdirs_iter;

    ffs_DIR(int at_fd,dir_node* dn):
        at_fd(at_fd),
        dn(dn),
        files_iter(dn->files.begin()),
        special_iter(0),
        subdirs_iter(dn->subdirs.begin())
    {
    }
};

struct resolved_path
{
    dir_node*   directory;
    std::string name;
};

dir_node* fs_root = new dir_node{NULL,"",0,1,true};
std::vector<dir_node*> snapshots;
std::set<dir_node*> live_dirs{fs_root};
std::set<file_node*> live_files;
file_descriptor fd_table[128];
static dir_node* current_wd = fs_root;
static bool snapshot_auto = false;

static void
auto_snapshot_fs()
{
    if (snapshot_auto)
        snapshot_fs();
}

static int
find_free_fd()
{
    for (size_t i=1; i<NELEMS(fd_table); ++i)
    {
        if (fd_table[i].type == FDT_FREE)
            return i;
    }
    throw futil::errno_exception(ENFILE);
}

static dir_node*
find_at_fd_dir_node(int at_fd)
{
    if (at_fd == AT_FDCWD)
        return current_wd;
    if (at_fd >= (int)NELEMS(fd_table) || at_fd <= 0)
        throw futil::errno_exception(EBADF);
    if (fd_table[at_fd].type != FDT_DIRECTORY)
        throw futil::errno_exception(EBADF);
    return fd_table[at_fd].directory;
}

static file_node*
find_fd_file_node(int fd)
{
    if (fd >= (int)NELEMS(fd_table) || fd <= 0)
        throw futil::errno_exception(EBADF);
    if (fd_table[fd].type != FDT_FILE)
        throw futil::errno_exception(EBADF);
    return fd_table[fd].file;
}

static std::string __UNUSED__
dirpath(dir_node* d)
{
    if (!d->parent)
        return "/";
    return dirpath(d->parent) + d->name + "/";
}

static resolved_path
resolve_path(dir_node* base_dir, const std::string& path)
{
    if (path.empty())
        return resolved_path{base_dir,""};
    if (path[0] == '/')
        base_dir = fs_root;
    auto parts = str::split(path,"/");
    for (size_t i=0; i<parts.size() - 1; ++i)
    {
        if (parts[i] == ".")
            continue;
        if (parts[i] == "..")
        {
            base_dir = base_dir->parent ?: base_dir;
            continue;
        }
        auto iter = base_dir->subdirs.find(parts[i]);
        if (iter == base_dir->subdirs.end())
            throw futil::errno_exception(ENOENT);
        base_dir = iter->second;
    }

    auto& name = parts[parts.size() - 1];
    if (name == ".")
        return resolved_path{base_dir,""};
    if (name == "..")
        return resolved_path{base_dir->parent ?: base_dir,""};
    return resolved_path{base_dir,parts[parts.size() - 1]};
}

void*
futil::mmap(void* addr, size_t len, int prot, int flags, int fd, off_t offset)
{
    kassert(addr == NULL);
    kassert(flags == MAP_SHARED);
    kassert(offset == 0);

    // This works, but it is not great.  If someone else grows the file, the
    // std::vector<> may need to reallocate and this would move the underlying
    // storage around, leaving the mmap() client with a dangling pointer.
    auto fn = find_fd_file_node(fd);
    kassert(len <= fn->data.size());
    ++fn->mmap_count;
    if (prot & PROT_WRITE)
        fn->data_fsynced = false;
    return &fn->data[0];
}

void
futil::munmap(void* addr, size_t len)
{
    for (auto* fn : live_files)
    {
        if (&fn->data[0] <= addr && addr < &fn->data[0] + fn->data.size())
        {
            // Make sure we aren't unmapping past the end of the file.  In
            // realitly this is actually fine if the client has carefully laid
            // out multiple files back-to-back in memory, but we are stricter
            // here for simplicity.
            if ((uint8_t*)addr + len > &fn->data[0] + fn->data.size())
                throw futil::errno_exception(EINVAL);

            kassert(fn->mmap_count--);
            auto_snapshot_fs();
            return;
        }
    }
    throw futil::errno_exception(EINVAL);
}

void
futil::msync(void* addr, size_t len, int flags)
{
    kassert(flags == MS_SYNC);
    for (auto* fn : live_files)
    {
        if (&fn->data[0] <= addr && addr < &fn->data[0] + fn->data.size())
        {
            if ((uint8_t*)addr + len > &fn->data[0] + fn->data.size())
                throw futil::errno_exception(ENOMEM);

            auto_snapshot_fs();
            fn->data_fsynced = true;    // TODO: Yuck!
            return;
        }
    }
    throw futil::errno_exception(ENOMEM);
}

void
futil::fsync(int fd)
{
    if (fd >= (int)NELEMS(fd_table) || fd <= 0)
        throw futil::errno_exception(EBADF);
    switch (fd_table[fd].type)
    {
        case FDT_DIRECTORY:
            for (auto &[name, fn] : fd_table[fd].directory->files)
                fn->meta_fsynced = true;
            for (auto &[name, dn] : fd_table[fd].directory->subdirs)
                dn->meta_fsynced = true;
            fd_table[fd].directory->dirty_unlinks.clear();
        break;

        case FDT_FILE:
            fd_table[fd].file->data_fsynced = true;
        break;

        case FDT_FREE:
            throw futil::errno_exception(EBADF);
    }
}

void
futil::close(int fd)
{
    if (fd >= (int)NELEMS(fd_table) || fd <= 0)
        throw futil::errno_exception(EBADF);
    if (fd_table[fd].type == FDT_FILE)
    {
        fd_table[fd].file->shared_locks -= fd_table[fd].shared_locks;
        fd_table[fd].file->exclusive_locks -= fd_table[fd].exclusive_locks;
        fd_table[fd].shared_locks = 0;
        fd_table[fd].exclusive_locks = 0;
        if (!--fd_table[fd].file->refcount && !fd_table[fd].file->mmap_count)
        {
            kassert(live_files.contains(fd_table[fd].file));
            delete fd_table[fd].file;
            live_files.erase(fd_table[fd].file);
        }
        fd_table[fd].file = NULL;
    }
    else if (fd_table[fd].type == FDT_DIRECTORY)
    {
        if (!--fd_table[fd].directory->refcount)
        {
            kassert(live_dirs.contains(fd_table[fd].directory));
            delete fd_table[fd].directory;
            live_dirs.erase(fd_table[fd].directory);
        }
        fd_table[fd].directory = NULL;
    }
    else
        throw futil::errno_exception(EBADF);
    fd_table[fd].type = FDT_FREE;
}

void
futil::fsync_and_flush(int fd)
{
    futil::fsync(fd);
}

void
futil::fsync_and_barrier(int fd)
{
    futil::fsync(fd);
}

int
futil::openat(int at_fd, const char* path, int oflag)
{
    kassert(!(oflag & O_APPEND));
    kassert(!(oflag & O_CLOEXEC));
    kassert(!(oflag & O_CREAT));
    kassert(!(oflag & O_EXCL));
    kassert(!(oflag & O_NOFOLLOW));
    kassert(!(oflag & O_NONBLOCK));
    kassert(!(oflag & O_TRUNC));
#if IS_MACOS
    kassert(!(oflag & O_EXEC));
    kassert(!(oflag & O_SHLOCK));
    kassert(!(oflag & O_EXLOCK));
    kassert(!(oflag & O_SYMLINK));
    kassert(!(oflag & O_EVTONLY));
    kassert(!(oflag & O_NOFOLLOW_ANY));
#if 0
    kassert(!(oflag & O_RESOLVE_BENEATH));
#endif
#elif IS_LINUX
    kassert(!(oflag & O_ASYNC));
    kassert(!(oflag & O_DIRECT));
    kassert(!(oflag & O_DSYNC));
    kassert(!(oflag & O_LARGEFILE));
    kassert(!(oflag & O_NOATIME));
    kassert(!(oflag & O_NOCTTY));
    kassert(!(oflag & O_NDELAY));
    kassert(!(oflag & O_PATH));
    kassert(!(oflag & O_SYNC));
    // kassert(!(oflag & O_TMPFILE));  // O_TMPFILE == O_DIRECTORY????
#else
#error Unknown OS
#endif

    int fd = find_free_fd();
    dir_node* at_dir = find_at_fd_dir_node(at_fd);
    auto rp = resolve_path(at_dir,path);

    if (rp.name.empty())
    {
        kassert(!(oflag & O_TRUNC));
        fd_table[fd].type      = FDT_DIRECTORY;
        fd_table[fd].pos       = 0;
        fd_table[fd].directory = rp.directory;
        ++fd_table[fd].directory->refcount;
        return fd;
    }

    auto diter = rp.directory->subdirs.find(rp.name);
    if (diter != rp.directory->subdirs.end())
    {
        kassert(!(oflag & O_TRUNC));
        fd_table[fd].type      = FDT_DIRECTORY;
        fd_table[fd].pos       = 0;
        fd_table[fd].directory = diter->second;
        ++fd_table[fd].directory->refcount;
        return fd;
    }

    auto fiter = rp.directory->files.find(rp.name);
    if (fiter != rp.directory->files.end())
    {
        if (oflag & O_DIRECTORY)
            throw futil::errno_exception(ENOTDIR);
        if (oflag & O_TRUNC)
        {
            if (!fiter->second->data.empty())
            {
                fiter->second->data.clear();
                fiter->second->data_fsynced = false;
                auto_snapshot_fs();
            }
        }
        fd_table[fd].type = FDT_FILE;
        fd_table[fd].pos  = 0;
        fd_table[fd].file = fiter->second;
        ++fd_table[fd].file->refcount;
        return fd;
    }

    throw futil::errno_exception(ENOENT);
}

int
futil::openat(int at_fd, const char* path, int oflag, mode_t mode)
{
    kassert(!(oflag & O_APPEND));
    kassert(!(oflag & O_CLOEXEC));
    kassert(oflag & O_CREAT);
    kassert(!(oflag & O_NOFOLLOW));
    kassert(!(oflag & O_NONBLOCK));
#if IS_MACOS
    kassert(!(oflag & O_EXEC));
    kassert(!(oflag & O_SHLOCK));
    kassert(!(oflag & O_EXLOCK));
    kassert(!(oflag & O_DIRECTORY));
    kassert(!(oflag & O_SYMLINK));
    kassert(!(oflag & O_EVTONLY));
    kassert(!(oflag & O_NOFOLLOW_ANY));
#if 0
    kassert(!(oflag & O_RESOLVE_BENEATH));
#endif
#elif IS_LINUX
    kassert(!(oflag & O_ASYNC));
    kassert(!(oflag & O_DIRECT));
    kassert(!(oflag & O_DSYNC));
    kassert(!(oflag & O_LARGEFILE));
    kassert(!(oflag & O_NOATIME));
    kassert(!(oflag & O_NOCTTY));
    kassert(!(oflag & O_NDELAY));
    kassert(!(oflag & O_PATH));
    kassert(!(oflag & O_SYNC));
    // kassert(!(oflag & O_TMPFILE));  // O_TMPFILE == O_DIRECTORY????
#else
#error Unknown OS
#endif

    int fd = find_free_fd();
    dir_node* at_dir = find_at_fd_dir_node(at_fd);
    auto rp = resolve_path(at_dir,path);

    // What does it mean to O_CREAT an existing directory?
    kassert(!rp.directory->subdirs.contains(rp.name));

    // Find it if it exists, create it if it doesn't.
    file_node* fn;
    auto iter = rp.directory->files.find(rp.name);
    if (iter == rp.directory->files.end())
    {
        fn = new file_node{rp.directory,rp.name,mode,1,0,0,0,true,false};
        rp.directory->files.emplace(std::make_pair(rp.name,fn));
        ++rp.directory->refcount;
        live_files.insert(fn);
        auto_snapshot_fs();
    }
    else if (oflag & O_EXCL)
        throw futil::errno_exception(EEXIST);
    else
        fn = iter->second;

    // Truncate if necessary.
    if (oflag & O_TRUNC)
    {
        if (!fn->data.empty())
        {
            fn->data_fsynced = false;
            fn->data.clear();
        }
        auto_snapshot_fs();
    }

    // Allocate the descriptor and return.
    fd_table[fd].type = FDT_FILE;
    fd_table[fd].pos  = 0;
    fd_table[fd].file = fn;
    ++fn->refcount;

    return fd;
}

DIR*
futil::fdopendir(int at_fd)
{
    auto dn = find_at_fd_dir_node(at_fd);
    return reinterpret_cast<DIR*>(new ffs_DIR(at_fd,dn));
}

struct dirent*
futil::readdir(DIR* _dirp)
{
    auto dirp = reinterpret_cast<ffs_DIR*>(_dirp);
    if (dirp->files_iter != dirp->dn->files.end())
    {
        auto* fn = dirp->files_iter->second;
        memset(&dirp->de,0,sizeof(dirp->de));
        dirp->de.d_type = DT_REG;
#if IS_MACOS
        dirp->de.d_namlen = fn->name.size();
#endif
        strlcpy(dirp->de.d_name,fn->name.c_str(),sizeof(dirp->de.d_name));
        ++dirp->files_iter;
        return &dirp->de;
    }
    if (dirp->special_iter < 2)
    {
        memset(&dirp->de,0,sizeof(dirp->de));
        dirp->de.d_type = DT_DIR;
        switch (dirp->special_iter)
        {
            case 0:
#if IS_MACOS
                dirp->de.d_namlen = 1;
#endif
                dirp->de.d_name[0] = '.';
                dirp->de.d_name[1] = '\0';
            break;

            case 1:
#if IS_MACOS
                dirp->de.d_namlen = 2;
#endif
                dirp->de.d_name[0] = '.';
                dirp->de.d_name[1] = '.';
                dirp->de.d_name[2] = '\0';
            break;
        }
        ++dirp->special_iter;
        return &dirp->de;
    }
    if (dirp->subdirs_iter != dirp->dn->subdirs.end())
    {
        auto* dn = dirp->subdirs_iter->second;
        memset(&dirp->de,0,sizeof(dirp->de));
        dirp->de.d_type = DT_DIR;
#if IS_MACOS
        dirp->de.d_namlen = dn->name.size();
#endif
        strlcpy(dirp->de.d_name,dn->name.c_str(),sizeof(dirp->de.d_name));
        ++dirp->subdirs_iter;
        return &dirp->de;
    }
    return NULL;
}

void
futil::closedir(DIR* _dirp)
{
    auto dirp = reinterpret_cast<ffs_DIR*>(_dirp);
    int at_fd = dirp->at_fd;
    delete dirp;
    futil::close(at_fd);
}

int
futil::open(const char* path, int oflag)
{
    return futil::openat(AT_FDCWD,path,oflag);
}

ssize_t
futil::read(int fd, void* buf, size_t nbyte)
{
    file_node* file = find_fd_file_node(fd);
    if (fd_table[fd].pos >= (off_t)file->data.size())
        return 0;

    size_t limit = file->data.size() - fd_table[fd].pos;
    nbyte = MIN(nbyte,limit);
    memcpy(buf,&file->data[fd_table[fd].pos],nbyte);
    fd_table[fd].pos += nbyte;
    return nbyte;
}

ssize_t
futil::write(int fd, const void* buf, size_t nbyte)
{
    file_node* fn = find_fd_file_node(fd);

    // Zero-length writes do not extend the file length, even if the position
    // is past the end of the file.
    if (!nbyte)
        return 0;

    size_t limit = fd_table[fd].pos + nbyte;
    if (fn->data.size() < limit)
    {
        kassert(fn->mmap_count == 0);
        fn->data.resize(limit);
    }
    memcpy(&fn->data[fd_table[fd].pos],buf,nbyte);
    fn->data_fsynced = false;
    auto_snapshot_fs();
    fd_table[fd].pos += nbyte;
    return nbyte;
}

off_t
futil::lseek(int fd, off_t offset, int whence)
{
    file_node* fn = find_fd_file_node(fd);

    off_t new_pos = 0;
    switch (whence)
    {
        case SEEK_SET:
            new_pos = offset;
        break;

        case SEEK_CUR:
            new_pos = fd_table[fd].pos + offset;
        break;

        case SEEK_END:
            new_pos = fn->data.size() + offset;
        break;

        case SEEK_HOLE:
        case SEEK_DATA:
        default:
            kabort();
        break;
    }

    if (new_pos < 0)
        throw futil::errno_exception(EINVAL);

    fd_table[fd].pos = new_pos;
    return new_pos;
}

void
futil::flock(int fd, int operation)
{
    kassert(operation == LOCK_SH ||
            operation == (LOCK_SH | LOCK_NB) ||
            operation == LOCK_EX ||
            operation == (LOCK_EX | LOCK_NB) ||
            operation == LOCK_UN);

    file_node* file = find_fd_file_node(fd);
    if (operation & LOCK_SH)
    {
        kassert(file->exclusive_locks == 0);
        ++file->shared_locks;
        ++fd_table[fd].shared_locks;
    }
    else if (operation & LOCK_EX)
    {
        kassert(file->shared_locks == 0);
        kassert(file->exclusive_locks == 0);
        ++file->exclusive_locks;
        ++fd_table[fd].exclusive_locks;
    }
    else if (operation & LOCK_UN)
    {
        file->shared_locks -= fd_table[fd].shared_locks;
        file->exclusive_locks -= fd_table[fd].exclusive_locks;
        fd_table[fd].shared_locks = 0;
        fd_table[fd].exclusive_locks = 0;
    }
}

void
futil::mkdirat(int at_fd, const char* path, mode_t mode)
{
    dir_node* at_dir = find_at_fd_dir_node(at_fd);
    auto rp = resolve_path(at_dir,path);

    if (rp.name.empty())
        throw futil::errno_exception(EISDIR);
    if (rp.directory->subdirs.contains(rp.name) ||
        rp.directory->files.contains(rp.name))
    {
        throw futil::errno_exception(EEXIST);
    }

    auto dn = new dir_node{rp.directory,rp.name,mode,1,false};
    dn->parent->subdirs.emplace(std::make_pair(dn->name,dn));
    ++dn->parent->refcount;
    live_dirs.insert(dn);
    auto_snapshot_fs();
}

bool
futil::mkdirat_if_not_exists(int at_fd, const char* path, mode_t mode) try
{
    futil::mkdirat(at_fd,path,mode);
    return true;
}
catch (const futil::errno_exception& e)
{
    if (e.errnov == EEXIST)
        return false;
    throw;
}

void
futil::unlinkat(int at_fd, const char* path, int flag)
{
#if IS_MACOS
    kassert(!(flag & AT_SYMLINK_NOFOLLOW_ANY));
#endif

    dir_node* at_dir __UNUSED__ = find_at_fd_dir_node(at_fd);
    auto rp = resolve_path(at_dir,path);
    if (flag & AT_REMOVEDIR)
    {
        // Removing the root!
        if (rp.name.empty() && !rp.directory->parent)
            throw futil::errno_exception(EISDIR);

        dir_node* rem_dir;
        if (rp.name.empty())
            rem_dir = rp.directory;
        else if (rp.directory->subdirs.contains(rp.name))
            rem_dir = rp.directory->subdirs[rp.name];
        else if (rp.directory->files.contains(rp.name))
            throw futil::errno_exception(ENOTDIR);
        else
            throw futil::errno_exception(ENOENT);

        if (!rem_dir->subdirs.empty() || !rem_dir->files.empty())
            throw futil::errno_exception(ENOTEMPTY);

        kassert(rem_dir->parent);
        kassert(--rem_dir->parent->refcount);
        rem_dir->parent->subdirs.erase(rem_dir->name);
        rem_dir->parent->dirty_unlinks.insert(rem_dir->name);
        rem_dir->meta_fsynced = false;
        auto_snapshot_fs();
        if (!--rem_dir->refcount)
        {
            kassert(live_dirs.contains(rem_dir));
            live_dirs.erase(rem_dir);
            delete rem_dir;
        }
    }
    else
    {
        if (rp.name.empty())
            throw futil::errno_exception(ENOENT);

        auto iter = rp.directory->files.find(rp.name);
        if (iter == rp.directory->files.end())
            throw futil::errno_exception(ENOENT);

        file_node* rem_file = iter->second;
        kassert(rem_file->parent);
        kassert(--rem_file->parent->refcount);
        rem_file->parent->files.erase(rem_file->name);
        rem_file->parent->dirty_unlinks.insert(rem_file->name);
        rem_file->meta_fsynced = false;
        auto_snapshot_fs();
        if (!--rem_file->refcount && !rem_file->mmap_count)
        {
            kassert(live_files.contains(rem_file));
            live_files.erase(rem_file);
            delete rem_file;
        }
    }
}

void
futil::renameat(int fromfd, const char* from, int tofd, const char* to)
{
    auto fromdn = find_at_fd_dir_node(fromfd);
    auto todn   = find_at_fd_dir_node(tofd);
    auto fromrp = resolve_path(fromdn,from);
    auto torp   = resolve_path(todn,to);
    kassert(!fromrp.name.empty());
    kassert(!torp.name.empty());

    if (fromrp.directory->subdirs.contains(fromrp.name))
    {
        // Moving a directory.
        if (torp.directory->files.contains(torp.name))
            throw futil::errno_exception(ENOTDIR);
        auto dn = fromrp.directory->subdirs[fromrp.name];

        // Make sure the new location is not a subdirectory of the from
        // directory.
        for (auto tdn = torp.directory; tdn != NULL; tdn = tdn->parent)
        {
            if (tdn == dn)
                throw futil::errno_exception(EINVAL);
        }
        
        // Remove the target directory if it exists and is empty.  Do not
        // snapshot after removing the directory since the full rename is an
        // atomic operation.
        auto iter = torp.directory->subdirs.find(torp.name);
        if (iter != torp.directory->subdirs.end())
        {
            auto rem_dir = iter->second;
            if (dn == rem_dir)
                return;
            if (!rem_dir->files.empty() || !rem_dir->subdirs.empty())
                throw futil::errno_exception(ENOTEMPTY);
            kassert(--torp.directory->refcount);
            torp.directory->subdirs.erase(rem_dir->name);
            torp.directory->dirty_unlinks.insert(rem_dir->name);
            rem_dir->meta_fsynced = false;
            if (!--rem_dir->refcount)
            {
                kassert(live_dirs.contains(rem_dir));
                live_dirs.erase(rem_dir);
                delete rem_dir;
            }
        }

        // Move the source directory to the target.
        kassert(--fromrp.directory->refcount);
        fromrp.directory->subdirs.erase(fromrp.name);
        fromrp.directory->dirty_unlinks.insert(fromrp.name);
        dn->name = torp.name;
        dn->parent = torp.directory;
        dn->meta_fsynced = false;
        torp.directory->subdirs.insert(std::make_pair(dn->name,dn));
        ++torp.directory->refcount;
    }
    else if (fromrp.directory->files.contains(fromrp.name))
    {
        // Moving a file.
        if (torp.directory->subdirs.contains(torp.name))
            throw futil::errno_exception(EISDIR);
        auto fn = fromrp.directory->files[fromrp.name];

        // Remove the target file if it exists.  Do not snapshot after removing
        // the file since the full rename is an atomoic operation.
        auto iter = torp.directory->files.find(torp.name);
        if (iter != torp.directory->files.end())
        {
            auto rem_file = iter->second;
            if (fn == rem_file)
                return;
            kassert(--torp.directory->refcount);
            torp.directory->files.erase(rem_file->name);
            torp.directory->dirty_unlinks.insert(rem_file->name);
            rem_file->meta_fsynced = false;
            if (!--rem_file->refcount && !rem_file->mmap_count)
            {
                kassert(live_files.contains(rem_file));
                live_files.erase(rem_file);
                delete rem_file;
            }
        }

        // Move the source file to the target.
        kassert(--fromrp.directory->refcount);
        fromrp.directory->files.erase(fromrp.name);
        fromrp.directory->dirty_unlinks.insert(fromrp.name);
        fn->name = torp.name;
        fn->parent = torp.directory;
        fn->meta_fsynced = false;
        torp.directory->files.insert(std::make_pair(fn->name,fn));
        ++torp.directory->refcount;
    }
    else
        throw futil::errno_exception(ENOENT);

    auto_snapshot_fs();
}

bool
futil::renameat_if_not_exists(int fromfd, const char* from, int tofd,
    const char* to)
{
    auto todn = find_at_fd_dir_node(tofd);
    auto torp = resolve_path(todn,to);
    kassert(!torp.name.empty());
    if (torp.directory->files.contains(torp.name) ||
        torp.directory->subdirs.contains(torp.name))
    {
        return false;
    }

    futil::renameat(fromfd,from,tofd,to);
    return true;
}

void
delete_tree(dir_node* dn)
{
    while (!dn->subdirs.empty())
        delete_tree(dn->subdirs.begin()->second);

    while (!dn->files.empty())
    {
        auto iter = dn->files.begin();
        auto fn = iter->second;
        dn->files.erase(fn->name);
        kassert(--dn->refcount);
        if (!--fn->refcount && !fn->mmap_count)
        {
            kassert(live_files.contains(fn));
            live_files.erase(fn);
            delete fn;
        }
    }

    if (dn->parent)
    {
        dn->parent->subdirs.erase(dn->name);
        kassert(--dn->parent->refcount);
    }

    if (!--dn->refcount)
    {
        kassert(live_dirs.contains(dn));
        live_dirs.erase(dn);
        delete dn;
    }
}

dir_node*
clone_tree(dir_node* dn)
{
    dir_node* new_dn = new dir_node{NULL,dn->name,dn->mode,1,dn->meta_fsynced,
                                    dn->dirty_unlinks};
    live_dirs.insert(new_dn);

    for (auto const &[name, fn] : dn->files)
    {
        file_node* new_fn = new file_node{new_dn,fn->name,fn->mode,1,0,0,0,
                                          fn->data_fsynced,fn->meta_fsynced,
                                          fn->data};
        new_dn->files.emplace(std::make_pair(new_fn->name,new_fn));
        ++new_dn->refcount;
        live_files.insert(new_fn);
    }

    for (auto const &[name, subdn] : dn->subdirs)
    {
        dir_node* new_subdn = clone_tree(subdn);
        new_subdn->parent = new_dn;
        new_dn->subdirs.emplace(std::make_pair(new_subdn->name,new_subdn));
        ++new_dn->refcount;
    }

    return new_dn;
}

void
fsync_tree(dir_node* dn)
{
    dn->meta_fsynced = true;
    dn->dirty_unlinks.clear();

    for (auto iter : dn->subdirs)
        fsync_tree(iter.second);
    for (auto iter : dn->files)
    {
        iter.second->data_fsynced = true;
        iter.second->meta_fsynced = true;
    }
}

bool
is_tree_fsynced(dir_node* dn, bool check_root)
{
    if (check_root && (!dn->meta_fsynced || !dn->dirty_unlinks.empty()))
        return false;

    for (auto iter : dn->subdirs)
    {
        if (!is_tree_fsynced(iter.second,true))
            return false;
    }
    for (auto iter : dn->files)
    {
        if (!iter.second->data_fsynced)
            return false;
        if (!iter.second->meta_fsynced)
            return false;
    }

    return true;
}

static void
print_not_fsynced(dir_node* dn, bool print_root)
{
    auto dirname = dirpath(dn);
    if (print_root)
    {
        if (!dn->meta_fsynced)
            printf("%s not meta-fsynced.\n",dirname.c_str());
        for (const auto& s : dn->dirty_unlinks)
            printf("%s dirty unlink \"%s\".\n",dirname.c_str(),s.c_str());
    }

    for (auto iter : dn->files)
    {
        auto fname = dirname + "/" + iter.first;
        if (!iter.second->data_fsynced)
            printf("%s not data-fsynced.\n",fname.c_str());
        if (!iter.second->meta_fsynced)
            printf("%s not meta-fsynced.\n",fname.c_str());
    }
    for (auto iter : dn->subdirs)
        print_not_fsynced(iter.second,true);
}

void
assert_tree_fsynced(dir_node* dn)
{
    if (is_tree_fsynced(dn,true))
        return;
    print_not_fsynced(dn,true);
    kabort();
}

void
assert_children_fsynced(dir_node* dn)
{
    if (is_tree_fsynced(dn,false))
        return;
    print_not_fsynced(dn,false);
    kabort();
}

void
snapshot_fs()
{
    snapshots.push_back(clone_tree(fs_root));
}

void
snapshot_reset()
{
    for (auto* dn : snapshots)
        delete_tree(dn);
    snapshots.clear();
}

void
snapshot_auto_begin()
{
    snapshot_auto = true;
}

void
snapshot_auto_end()
{
    snapshot_auto = false;
}

void
activate_and_fsync_snapshot(dir_node* dn)
{
    for (size_t i=0; i<NELEMS(fd_table); ++i)
        kassert(fd_table[i].type == FDT_FREE);
    current_wd = fs_root = dn;
    fsync_tree(fs_root);
}

void
nuke_fs_root()
{
    for (size_t i=0; i<NELEMS(fd_table); ++i)
        kassert(fd_table[i].type == FDT_FREE);
    ++fs_root->refcount;
    delete_tree(fs_root);
    current_wd = fs_root;
    kassert(fs_root->refcount == 1);
}
