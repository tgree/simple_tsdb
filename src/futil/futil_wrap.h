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
#include <exception>

namespace futil
{
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
    void mkdirat_if_not_exists(int at_fd, const char* path, mode_t mode);
    void symlink(const char* path1, const char* path2);
    void unlink(const char* path);
    void unlinkat(int at_fd, const char* path, int flag);
    void unlink_if_exists(const char* path);
    void unlinkat_if_exists(int at_fd, const char* path, int flag);
    void rename(const char* old, const char* _new);
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
        addr = ::mmap(addr,len,prot,flags,fd,offset);
        if (addr == MAP_FAILED)
            throw futil::errno_exception(errno);
        return addr;
    }

    inline void munmap(void* addr, size_t len)
    {
        if (::munmap(addr,len) == -1)
            throw futil::errno_exception(errno);
    }

    inline void msync(void* addr, size_t len, int flags)
    {
        if (::msync(addr,len,flags) == -1)
            throw futil::errno_exception(errno);
    }

    inline void fsync(int fd)
    {
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
                throw futil::errno_exception(errno);
        }
    }

    inline void close(int fd)
    {
        for (;;)
        {
            if (::close(fd) != -1)
                return;
            if (errno != EINTR)
                throw futil::errno_exception(errno);
        }
    }

    inline int fcntl(int fd, int cmd)
    {
        for (;;)
        {
            int val = ::fcntl(fd,cmd);
            if (val != -1)
                return val;
            if (errno != EINTR)
                throw futil::errno_exception(errno);
        }
    }

    template<typename T>
    inline int fcntl(int fd, int cmd, T arg)
    {
        for (;;)
        {
            int val = ::fcntl(fd,cmd,arg);
            if (val != -1)
                return val;
            if (errno != EINTR)
                throw futil::errno_exception(errno);
        }
    }

    inline void fsync_and_flush(int fd)
    {
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
        return ::vdprintf(fd,fmt,ap);
    }

    inline int openat(int at_fd, const char* path, int oflag)
    {
        if (oflag & O_CREAT)
            throw inconsistent_file_params();
        for (;;)
        {
            int fd = ::openat(at_fd,path,oflag);
            if (fd != -1)
                return fd;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }

    inline int openat(int at_fd, const char* path, int oflag, mode_t mode)
    {
        if (!(oflag & O_CREAT))
            throw inconsistent_file_params();
        for (;;)
        {
            int fd = ::openat(at_fd,path,oflag,mode);
            if (fd != -1)
                return fd;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }

    inline int openat_if_exists(int at_fd, const char* path, int oflag)
    {
        for (;;)
        {
            int fd = ::openat(at_fd,path,oflag);
            if (fd != -1 || errno == ENOENT)
                return fd;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }

    inline DIR* fdopendir(int fd)
    {
        auto* dirp = ::fdopendir(fd);
        if (!dirp)
            throw errno_exception(EBADF);
        return dirp;
    }

    inline struct dirent* readdir(DIR* dirp)
    {
        return ::readdir(dirp);
    }

    inline void closedir(DIR* dirp)
    {
        if (::closedir(dirp) == -1)
            throw errno_exception(errno);
    }

    inline int open(const char* path, int oflag)
    {
        for (;;)
        {
            int fd = ::open(path,oflag);
            if (fd != -1)
                return fd;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }
    
    inline ssize_t read(int fd, void* buf, size_t nbyte)
    {
        for (;;)
        {
            ssize_t v = ::read(fd,buf,nbyte);
            if (v != -1)
                return v;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }

    inline ssize_t write(int fd, const void* buf, size_t nbyte)
    {
        for (;;)
        {
            ssize_t v = ::write(fd,buf,nbyte);
            if (v != -1)
                return v;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }

    inline off_t lseek(int fd, off_t offset, int whence)
    {
        for (;;)
        {
            off_t pos = ::lseek(fd,offset,whence);
            if (pos != -1)
                return pos;
            if (errno != EINTR)
                throw futil::errno_exception(errno);
        }
    }

    inline void flock(int fd, int operation)
    {
        if (::flock(fd,operation) == -1)
            throw futil::errno_exception(errno);
    }

    inline void ftruncate(int fd, off_t length)
    {
        for (;;)
        {
            if (::ftruncate(fd,length) != -1)
                return;
            if (errno != EINTR)
                throw futil::errno_exception(errno);
        }
    }

    inline void mkdir(const char* path, mode_t mode)
    {
        // mkdir() doesn't seem to return EINTR.
        if (::mkdir(path,mode) == -1)
            throw errno_exception(errno);
    }

    inline void mkdirat(int at_fd, const char* path, mode_t mode)
    {
        // mkdirat() doesn't seem to return EINTR.
        if (::mkdirat(at_fd,path,mode) == -1)
            throw errno_exception(errno);
    }

    inline void mkdir_if_not_exists(const char* path, mode_t mode)
    {
        // mkdir() doesn't seem to return EINTR.
        if (::mkdir(path,mode) == -1 && errno != EEXIST)
            throw errno_exception(errno);
    }

    inline void mkdirat_if_not_exists(int at_fd, const char* path, mode_t mode)
    {
        // mkdirat() doesn't seem to return EINTR.
        if (::mkdirat(at_fd,path,mode) == -1 && errno != EEXIST)
            throw errno_exception(errno);
    }

    inline void symlink(const char* path1, const char* path2)
    {
        // symlink() doesn't seem to return EINTR.
        if (::symlink(path1,path2) == -1)
            throw errno_exception(errno);
    }

    inline void unlink(const char* path)
    {
        // unlink() doesn't seem to return EINTR.
        if (::unlink(path) == -1)
            throw errno_exception(errno);
    }

    inline void unlinkat(int at_fd, const char* path, int flag)
    {
        // unlinkat() doesn't seem to return EINTR.
        if (::unlinkat(at_fd,path,flag) == -1)
            throw errno_exception(errno);
    }

    inline void unlink_if_exists(const char* path)
    {
        // unlink() doesn't seem to return EINTR.
        if (::unlink(path) == -1 && errno != ENOENT)
            throw errno_exception(errno);
    }

    inline void unlinkat_if_exists(int at_fd, const char* path, int flag)
    {
        // unlinkat() doesn't seem to return EINTR.
        if (::unlinkat(at_fd,path,flag) == -1 && errno != ENOENT)
            throw errno_exception(errno);
    }

    inline void rename(const char* old, const char* _new)
    {
        // rename() doesn't seem to return EINTR.
        if (::rename(old,_new) == -1)
            throw errno_exception(errno);
    }

    inline void renameat(int fromfd, const char* from, int tofd, const char* to)
    {
        // renameat() doesn't seem to return EINTR.
        if (::renameat(fromfd,from,tofd,to) == -1)
            throw errno_exception(errno);
    }

    inline bool renameat_if_not_exists(int fromfd, const char* from, int tofd,
                                       const char* to)
    {
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
        throw errno_exception(errno);
    }

    inline void mkdtemp(char* tmp)
    {
        if (::mkdtemp(tmp) == NULL)
            throw errno_exception(errno);
    }

    inline void chdir(const char* path)
    {
        if (::chdir(path) == -1)
            throw errno_exception(errno);
    }

    inline void fchmod(int fd, mode_t mode)
    {
        for (;;)
        {
            if (!::fchmod(fd,mode))
                return;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }

    inline void fchmodat(int at_fd, const char* path, mode_t mode, int flag)
    {
        for (;;)
        {
            if (!::fchmodat(at_fd,path,mode,flag))
                return;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }

#endif
}

#endif /* __SRC_FUTIL_FUTIL_WRAP_H */
