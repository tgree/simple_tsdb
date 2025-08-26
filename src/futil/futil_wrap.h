// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_FUTIL_WRAP_H
#define __SRC_FUTIL_FUTIL_WRAP_H

#include <hdr/compiler.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/errno.h>
#include <dirent.h>
#include <stdarg.h>
#include <string.h>
#include <inttypes.h>
#include <exception>
#include <functional>

#define FUTIL_WRAP_TRACE    0

namespace futil
{
    enum hook_id
    {
        HI_MMAP                     = 0x00010000,
        HI_MUNMAP                   = 0x00020000,
        HI_MSYNC                    = 0x00030000,
        HI_FSYNC                    = 0x00040000,
        HI_CLOSE                    = 0x00050000,
        HI_FCNTL1                   = 0x00060000,
        HI_FCNTL2                   = 0x00070000,
        HI_FSYNC_AND_FLUSH          = 0x00080000,
        HI_FSYNC_AND_BARRIER        = 0x00090000,
        HI_VDPRINTF                 = 0x000A0000,
        HI_OPENAT1                  = 0x000B0000,
        HI_OPENAT2                  = 0x000C0000,
        HI_OPENAT_IF_EXISTS         = 0x000D0000,
        HI_CREATEAT_IF_NOT_EXISTS   = 0x000E0000,
        HI_FDOPENDIR                = 0x000F0000,
        HI_READDIR                  = 0x00100000,
        HI_CLOSEDIR                 = 0x00110000,
        HI_OPEN                     = 0x00120000,
        HI_READ                     = 0x00130000,
        HI_WRITE                    = 0x00140000,
        HI_LSEEK                    = 0x00150000,
        HI_FLOCK                    = 0x00160000,
        HI_FTRUNCATE                = 0x00170000,
        HI_MKDIR                    = 0x00180000,
        HI_MKDIRAT                  = 0x00190000,
        HI_MKDIR_IF_NOT_EXISTS      = 0x001A0000,
        HI_MKDIRAT_IF_NOT_EXISTS    = 0x001B0000,
        HI_SYMLINK                  = 0x001C0000,
        HI_UNLINK                   = 0x001D0000,
        HI_UNLINKAT                 = 0x001E0000,
        HI_UNLINK_IF_EXISTS         = 0x001F0000,
        HI_UNLINKAT_IF_EXISTS       = 0x00200000,
        HI_RENAMEAT                 = 0x00210000,
        HI_RENAMEAT_IF_NOT_EXISTS   = 0x00220000,
        HI_MKDTEMP                  = 0x00230000,
        HI_CHDIR                    = 0x00240000,
        HI_FCHMOD                   = 0x00250000,
        HI_FCHMODAT                 = 0x00260000,
    };

    struct exception : public std::exception
    {
        exception() {}
    };

    struct errno_exception : public exception
    {
        int errnov;

        virtual const char* what() const noexcept override
        {
            return strerror(errnov);
        }

        errno_exception(int errnov):
            errnov(errnov)
        {
        }
    };

    // Exception thrown if passing inconsistent parameters to file constructor
    // (i.e. O_CREAT without a mode or a mode without O_CREAT).
    struct inconsistent_file_params : public exception
    {
        virtual const char* what() const noexcept override
        {
            return "Inconsistent arguments passed to file::file().";
        }

        inconsistent_file_params():
            exception()
        {
        }
    };

#ifdef UNITTEST
    extern std::function<void(uint32_t)> hook_func;
#endif

    struct _futil_probe
    {
        const hook_id id;
        _futil_probe(hook_id id, const char* fmt, ...) __PRINTF__(3,4) :
            id(id)
        {
#ifdef UNITTEST
            if (hook_func)
            {
                auto _hook_func = hook_func;
                hook_func = NULL;
                _hook_func(id | 0x0000);
                hook_func = _hook_func;
            }
#endif
            if (!FUTIL_WRAP_TRACE)
                return;

            va_list ap;
            va_start(ap,fmt);
            vprintf(fmt,ap);
            va_end(ap);
        }
        ~_futil_probe()
        {
#ifdef UNITTEST
            if (hook_func)
            {
                auto _hook_func = hook_func;
                hook_func = NULL;
                _hook_func(id | 0x0001);
                hook_func = _hook_func;
            }
#endif
        }
    };
#define FW_PROBE(id,fmt,...) _futil_probe(id,fmt "\n", ##__VA_ARGS__)
#define FW_TRACE(fmt,...) if (FUTIL_WRAP_TRACE) printf(fmt "\n", ##__VA_ARGS__)

    static inline void __NORETURN__ FW_THROW_ERRNO(int errnov)
    {
        if (FUTIL_WRAP_TRACE)
            printf("---> throw (%d): %s\n",errnov,strerror(errnov));
        throw futil::errno_exception(errnov);
    }
    static inline void __NORETURN__ FW_THROW_ERRNO()
    {
        FW_THROW_ERRNO(errno);
    }

#ifdef UNITTEST

    void* mmap(void* addr, size_t len, int prot, int flags, int fd,
               off_t offset);
    void munmap(void* addr, size_t len);
    void msync(void* addr, size_t len, int flags);
    void fsync(int fd);
    void close(int fd);
    int fcntl(int fd, int cmd);
    template<typename T> int fcntl(int fd, int cmd, T arg);
    void fsync_and_flush(int fd);
    void fsync_and_barrier(int fd);
    int vdprintf(int fd, const char* fmt, va_list ap);
    int openat(int at_fd, const char* path, int oflag);
    int openat(int at_fd, const char* path, int oflag, mode_t mode);
    int openat_if_exists(int at_fd, const char* path, int oflag);
    int createat_if_not_exists(int at_fd, const char* path, int oflag,
                               mode_t mode);
    DIR* fdopendir(int fd);
    struct dirent* readdir(DIR* dirp);
    void closedir(DIR* dirp);
    int open(const char* path, int oflag);
    ssize_t read(int fd, void* buf, size_t nbyte);
    ssize_t write(int fd, const void* buf, size_t nbyte);
    off_t lseek(int fd, off_t offset, int whence);
    void flock(int fd, int operation);
    void ftruncate(int fd, off_t length);
    void mkdir(const char* path, mode_t mode);
    void mkdirat(int at_fd, const char* path, mode_t mode);
    void mkdir_if_not_exists(const char* path, mode_t mode);
    bool mkdirat_if_not_exists(int at_fd, const char* path, mode_t mode);
    void symlink(const char* path1, const char* path2);
    void unlink(const char* path);
    void unlinkat(int at_fd, const char* path, int flag);
    void unlink_if_exists(const char* path);
    void unlinkat_if_exists(int at_fd, const char* path, int flag);
    void renameat(int fromfd, const char* from, int tofd, const char* to);
    bool renameat_if_not_exists(int fromfd, const char* from, int tofd,
                                const char* to);
    void mkdtemp(char* tmp);
    void chdir(const char* path);
    void fchmod(int fd, mode_t mode);
    void fchmodat(int fd, const char* path, mode_t mode, int flag);

#else

    inline void* mmap(void* addr, size_t len, int prot, int flags, int fd,
                      off_t offset)
    {
        FW_PROBE(HI_MMAP,"mmap(%p,%zu,%d,%d,%d,%" PRId64 ")",
                 addr,len,prot,flags,fd,offset);
        addr = ::mmap(addr,len,prot,flags,fd,offset);
        if (addr == MAP_FAILED)
            FW_THROW_ERRNO();
        return addr;
    }

    inline void munmap(void* addr, size_t len)
    {
        FW_PROBE(HI_MUNMAP,"munmap(%p,%zu)",addr,len);
        if (::munmap(addr,len) == -1)
            FW_THROW_ERRNO();
    }

    inline void msync(void* addr, size_t len, int flags)
    {
        FW_PROBE(HI_MSYNC,"msync(%p,%zu,%d)",addr,len,flags);
        if (::msync(addr,len,flags) == -1)
            FW_THROW_ERRNO();
    }

    inline void fsync(int fd)
    {
        FW_PROBE(HI_FSYNC,"fsync(%d)",fd);
        for (;;)
        {
#if IS_MACOS
            if (::fsync(fd) != -1)
                return;
#elif IS_LINUX
            if (::fdatasync(fd) != -1)
                return;
#endif
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline void close(int fd)
    {
        FW_PROBE(HI_CLOSE,"close(%d)",fd);
        for (;;)
        {
            if (::close(fd) != -1)
                return;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline int fcntl(int fd, int cmd)
    {
        FW_PROBE(HI_FCNTL1,"fcntl(%d,%d)",fd,cmd);
        for (;;)
        {
            int val = ::fcntl(fd,cmd);
            if (val != -1)
                return val;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    template<typename T>
    inline int fcntl(int fd, int cmd, T arg)
    {
        FW_PROBE(HI_FCNTL2,"fcntl(%d,%d,...)",fd,cmd);
        for (;;)
        {
            int val = ::fcntl(fd,cmd,arg);
            if (val != -1)
                return val;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline void fsync_and_flush(int fd)
    {
        FW_PROBE(HI_FSYNC_AND_FLUSH,"fsync_and_flush(%d)",fd);
#if IS_MACOS
        // Performs an fsync() and then flushes the disk controller's
        // buffers to the physical drive medium.
        futil::fcntl(fd,F_FULLFSYNC);
#elif IS_LINUX
        futil::fsync(fd);
#else
#error Unknown platform.
#endif
    }

    inline void fsync_and_barrier(int fd)
    {
        FW_PROBE(HI_FSYNC_AND_BARRIER,"fsync_and_barrier(%d)",fd);
#if IS_MACOS
        // Performs an fsync() and then inserts an IO barrier, preventing
        // IO reordering across the barrier.  This is available only on
        // macOS and only on some combinations of file system and specific
        // hard drives/SSDs.  Like fsync(), this doesn't guarantee that
        // data is flushed to the disk itself, just that if a flush happens
        // the data will be ordered correctly.
        // 
        // If the barrier operation is not supported, fall back to a full
        // flush.
        for (;;)
        {
            int val = ::fcntl(fd,F_BARRIERFSYNC);
            if (val != -1)
                return;
            if (errno != EINTR)
                break;
        }

        futil::fsync_and_flush(fd);
#elif IS_LINUX
        futil::fsync(fd);
#else
#error Unknown platform.
#endif
    }

    inline int vdprintf(int fd, const char* fmt, va_list ap)
    {
        FW_PROBE(HI_VDPRINTF,"vdprintf(%d,\"%s\",...)",fd,fmt);
        return ::vdprintf(fd,fmt,ap);
    }

    inline int openat(int at_fd, const char* path, int oflag)
    {
        FW_PROBE(HI_OPENAT1,"openat(%d,\"%s\",%d)",at_fd,path,oflag);
        if (oflag & O_CREAT)
        {
            FW_TRACE("---> throw inconsistent file params");
            throw inconsistent_file_params();
        }
        for (;;)
        {
            int fd = ::openat(at_fd,path,oflag);
            if (fd != -1)
                return fd;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline int openat(int at_fd, const char* path, int oflag, mode_t mode)
    {
        FW_PROBE(HI_OPENAT2,"openat(%d,\"%s\",%d,%d)",at_fd,path,oflag,mode);
        if (!(oflag & O_CREAT))
        {
            FW_TRACE("---> throw inconsistent file params");
            throw inconsistent_file_params();
        }
        for (;;)
        {
            int fd = ::openat(at_fd,path,oflag,mode);
            if (fd != -1)
                return fd;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline int openat_if_exists(int at_fd, const char* path, int oflag)
    {
        FW_PROBE(HI_OPENAT_IF_EXISTS,"openat_if_exists(%d,\"%s\",%d)",
                 at_fd,path,oflag);
        for (;;)
        {
            int fd = ::openat(at_fd,path,oflag);
            if (fd != -1 || errno == ENOENT)
                return fd;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline int createat_if_not_exists(int at_fd, const char* path, int oflag,
                                      mode_t mode)
    {
        FW_PROBE(HI_CREATEAT_IF_NOT_EXISTS,
                 "createat_if_not_exists(%d,\"%s\",%d,%d)",
                 at_fd,path,oflag,mode);
        for (;;)
        {
            int fd = ::openat(at_fd,path,oflag | O_CREAT | O_EXCL,mode);
            if (fd != -1)
                return fd;
            if (errno == EEXIST)
                return -1;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline DIR* fdopendir(int fd)
    {
        FW_PROBE(HI_FDOPENDIR,"fdopendir(%d)",fd);
        auto* dirp = ::fdopendir(fd);
        if (!dirp)
            FW_THROW_ERRNO(EBADF);
        return dirp;
    }

    inline struct dirent* readdir(DIR* dirp)
    {
        FW_PROBE(HI_READDIR,"readdir(%p)",dirp);
        return ::readdir(dirp);
    }

    inline void closedir(DIR* dirp)
    {
        FW_PROBE(HI_CLOSEDIR,"closedir(%p)",dirp);
        if (::closedir(dirp) == -1)
            FW_THROW_ERRNO();
    }

    inline int open(const char* path, int oflag)
    {
        FW_PROBE(HI_OPEN,"open(\"%s\",%d)",path,oflag);
        for (;;)
        {
            int fd = ::open(path,oflag);
            if (fd != -1)
                return fd;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }
    
    inline ssize_t read(int fd, void* buf, size_t nbyte)
    {
        FW_PROBE(HI_READ,"read(%d,%p,%zu)",fd,buf,nbyte);
        for (;;)
        {
            ssize_t v = ::read(fd,buf,nbyte);
            if (v != -1)
                return v;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline ssize_t write(int fd, const void* buf, size_t nbyte)
    {
        FW_PROBE(HI_WRITE,"write(%d,%p,%zu)",fd,buf,nbyte);
        for (;;)
        {
            ssize_t v = ::write(fd,buf,nbyte);
            if (v != -1)
                return v;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline off_t lseek(int fd, off_t offset, int whence)
    {
        FW_PROBE(HI_LSEEK,"lseek(%d,%" PRId64 ",%d)",fd,offset,whence);
        for (;;)
        {
            off_t pos = ::lseek(fd,offset,whence);
            if (pos != -1)
                return pos;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline void flock(int fd, int operation)
    {
        FW_PROBE(HI_FLOCK,"flock(%d,%d)",fd,operation);
        if (::flock(fd,operation) == -1)
            FW_THROW_ERRNO();
    }

    inline void ftruncate(int fd, off_t length)
    {
        FW_PROBE(HI_FTRUNCATE,"ftruncate(%d,%" PRId64 ")",fd,length);
        for (;;)
        {
            if (::ftruncate(fd,length) != -1)
                return;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline void mkdir(const char* path, mode_t mode)
    {
        FW_PROBE(HI_MKDIR,"mkdir(\"%s\",%d)",path,mode);
        // mkdir() doesn't seem to return EINTR.
        if (::mkdir(path,mode) == -1)
            FW_THROW_ERRNO();
    }

    inline void mkdirat(int at_fd, const char* path, mode_t mode)
    {
        FW_PROBE(HI_MKDIRAT,"mkdirat(%d,\"%s\",%d)",at_fd,path,mode);
        // mkdirat() doesn't seem to return EINTR.
        if (::mkdirat(at_fd,path,mode) == -1)
            FW_THROW_ERRNO();
    }

    inline bool mkdir_if_not_exists(const char* path, mode_t mode)
    {
        FW_PROBE(HI_MKDIR_IF_NOT_EXISTS,"mkdir_if_not_exists(\"%s\",%d)",
                 path,mode);
        // mkdir() doesn't seem to return EINTR.
        int v = ::mkdir(path,mode);
        if (v == -1 && errno != EEXIST)
            FW_THROW_ERRNO();
        return v == 0;
    }

    inline bool mkdirat_if_not_exists(int at_fd, const char* path, mode_t mode)
    {
        FW_PROBE(HI_MKDIRAT_IF_NOT_EXISTS,
                 "mkdirat_if_not_exists(%d,\"%s\",%d)",at_fd,path,mode);
        // mkdirat() doesn't seem to return EINTR.
        int v = ::mkdirat(at_fd,path,mode);
        if (v == -1 && errno != EEXIST)
            FW_THROW_ERRNO();
        return v == 0;
    }

    inline void symlink(const char* path1, const char* path2)
    {
        FW_PROBE(HI_SYMLINK,"symlink(\"%s\",\"%s\")",path1,path2);
        // symlink() doesn't seem to return EINTR.
        if (::symlink(path1,path2) == -1)
            FW_THROW_ERRNO();
    }

    inline void unlink(const char* path)
    {
        FW_PROBE(HI_UNLINK,"unlink(\"%s\")",path);
        // unlink() doesn't seem to return EINTR.
        if (::unlink(path) == -1)
            FW_THROW_ERRNO();
    }

    inline void unlinkat(int at_fd, const char* path, int flag)
    {
        FW_PROBE(HI_UNLINKAT,"unlinkat(%d,\"%s\",%d)",at_fd,path,flag);
        // unlinkat() doesn't seem to return EINTR.
        if (::unlinkat(at_fd,path,flag) == -1)
            FW_THROW_ERRNO();
    }

    inline void unlink_if_exists(const char* path)
    {
        FW_PROBE(HI_UNLINK_IF_EXISTS,"unlink_if_exists(\"%s\")",path);
        // unlink() doesn't seem to return EINTR.
        if (::unlink(path) == -1 && errno != ENOENT)
            FW_THROW_ERRNO();
    }

    inline void unlinkat_if_exists(int at_fd, const char* path, int flag)
    {
        FW_PROBE(HI_UNLINKAT_IF_EXISTS,"unlinkat_if_exists(%d,\"%s\",%d)",
                 at_fd,path,flag);
        // unlinkat() doesn't seem to return EINTR.
        if (::unlinkat(at_fd,path,flag) == -1 && errno != ENOENT)
            FW_THROW_ERRNO();
    }

    inline void renameat(int fromfd, const char* from, int tofd, const char* to)
    {
        FW_PROBE(HI_RENAMEAT,"renameat(%d,\"%s\",%d,\"%s\")",
                 fromfd,from,tofd,to);
        // renameat() doesn't seem to return EINTR.
        if (::renameat(fromfd,from,tofd,to) == -1)
            FW_THROW_ERRNO();
    }

    inline bool renameat_if_not_exists(int fromfd, const char* from, int tofd,
                                       const char* to)
    {
        FW_PROBE(HI_RENAMEAT_IF_NOT_EXISTS,
                 "renameat_if_not_exists(%d,\"%s\",%d,\"%s\")",
                 fromfd,from,tofd,to);
#if IS_MACOS
        if (!::renameatx_np(fromfd,from,tofd,to,RENAME_EXCL))
            return true;
#elif IS_LINUX
        if (!::renameat2(fromfd,from,tofd,to,RENAME_NOREPLACE))
            return true;
#else
#error Unknown platform.
#endif
        if (errno == EEXIST)
            return false;
        FW_THROW_ERRNO();
    }

    inline void mkdtemp(char* tmp)
    {
        FW_PROBE(HI_MKDTEMP,"mkdtemp(\"%s\")",tmp);
        if (::mkdtemp(tmp) == NULL)
            FW_THROW_ERRNO();
    }

    inline void chdir(const char* path)
    {
        FW_PROBE(HI_CHDIR,"chdir(\"%s\")",path);
        if (::chdir(path) == -1)
            FW_THROW_ERRNO();
    }

    inline void fchmod(int fd, mode_t mode)
    {
        FW_PROBE(HI_FCHMOD,"fchmod(%d,%d)",fd,mode);
        for (;;)
        {
            if (!::fchmod(fd,mode))
                return;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

    inline void fchmodat(int at_fd, const char* path, mode_t mode, int flag)
    {
        FW_PROBE(HI_FCHMODAT,"fchmodat(%d,\"%s\",%d,%d)",at_fd,path,mode,flag);
        for (;;)
        {
            if (!::fchmodat(at_fd,path,mode,flag))
                return;
            if (errno != EINTR)
                FW_THROW_ERRNO();
        }
    }

#endif
}

#endif /* __SRC_FUTIL_FUTIL_WRAP_H */
