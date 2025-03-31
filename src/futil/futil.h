// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_FUTIL_H
#define __SRC_FUTIL_FUTIL_H

#include <hdr/compiler.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <dirent.h>
#include <string.h>
#include <string>
#include <vector>

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
            exception(),
            errnov(errnov)
        {
        }
    };

    // Exception thrown when two paths cannot be joined.  This could be the
    // case if the second path is an absolute path, for instance.
    struct invalid_join_exception : public exception
    {
        virtual const char* what() const noexcept override
        {
            return "Cannot join paths.";
        }

        invalid_join_exception():
            exception()
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

    struct path
    {
        std::string _path;

        size_t size() const {return _path.size();}
        bool empty() const  {return _path.empty();}

        char operator[](size_t offset) const
        {
            return _path.at(offset);
        }

        const char* c_str() const
        {
            return _path.c_str();
        }
        operator const char*() const
        {
            return c_str();
        }

        // Returns the number of components in the path.  For instance, the
        // following path has three components, a, b, and c:
        //
        //      ///////a///b/c//////////
        size_t count_components() const
        {
            size_t n = 0;
            bool last_was_slash = true;
            for (char c : _path)
            {
                if (last_was_slash)
                {
                    if (c != '/')
                    {
                        last_was_slash = false;
                        ++n;
                    }
                }
                else
                {
                    if (c == '/')
                        last_was_slash = true;
                }
            }
            return n ?: 1;
        }

        // Decomposes the path into its constituent parts.  If the path is
        // absolute, the first part will be "/".
        std::vector<std::string> decompose() const
        {
            std::vector<std::string> v;
            if (_path.empty())
                return v;
            if (_path[0] == '/')
                v.push_back("/");

            std::string component;
            bool last_was_slash = true;
            for (char c : _path)
            {
                if (c == '/')
                {
                    if (!last_was_slash)
                    {
                        v.push_back(component);
                        component.clear();
                    }
                }
                else
                    component += c;
                last_was_slash = (c == '/');
            }
            if (!component.empty())
                v.push_back(component);
            
            return v;
        }

        inline futil::path
        operator+(const futil::path& rhs) const
        {
            if (rhs._path.empty())
                return *this;
            if (rhs[0] == '/')
                throw futil::invalid_join_exception();
            if (_path.empty())
                return rhs;
            if (_path[_path.size() - 1] == '/')
                return futil::path(_path + rhs._path);
            return futil::path(_path + "/" + rhs._path);
        }

        path(const char* _path):
            _path(_path)
        {
        }

        path(const std::string& _path):
            _path(_path)
        {
        }

        template<typename ...T>
        path(const path& _path, T... tail):
            _path((_path + ... + tail))
        {
        }
    };

    inline void* mmap(void* addr, size_t len, int prot, int flags, int fd,
                      off_t offset)
    {
        addr = ::mmap(addr,len,prot,flags,fd,offset);
        if (addr == MAP_FAILED)
            throw futil::errno_exception(errno);
        return addr;
    }

    struct mapping
    {
        void* addr;
        size_t len;

        void msync()
        {
            if (::msync(addr,len,MS_SYNC) == -1)
                throw futil::errno_exception(errno);
        }

        inline void map(void* _addr, size_t _len, int prot, int flags, int fd,
                        off_t offset)
        {
            unmap();
            addr = futil::mmap(_addr,_len,prot,flags,fd,offset);
            len = _len;
        }

        inline void unmap()
        {
            if (len)
            {
                munmap(addr,len);
                addr = MAP_FAILED;
                len = 0;
            }
        }
        
        // Link error if someone tries to copy us.
        mapping(const mapping&);

        mapping():
            addr(MAP_FAILED),
            len(0)
        {
        }

        mapping(void* _addr, size_t _len, int prot, int flags, int fd,
                off_t offset):
            addr(MAP_FAILED),
            len(0)
        {
            map(_addr,_len,prot,flags,fd,offset);
        }

        ~mapping()
        {
            unmap();
        }
    };

    inline void fsync(int fd)
    {
        for (;;)
        {
            if (::fsync(fd) != -1)
                return;
            if (errno != EINTR)
                throw futil::errno_exception(errno);
        }
    }

    struct file_descriptor
    {
        int fd;

        void swap(file_descriptor& other)
        {
            int temp = fd;
            fd = other.fd;
            other.fd = temp;
        }

        void close()
        {
            if (fd == -1)
                return;

            for (;;)
            {
                // Attempt the close.
                int err = ::close(fd);

                // If we closed successfully, or we failed to close with
                // anything other than EINTR, return.  close() can fail with
                // EIO if a preceding write() later failed due to an IO error.
                // The file descriptor is left in an unspecified state so you
                // cannot do ANYTHING with it.
                //
                // It may be desirable to throw an exception or something, but
                // this is a really nasty case.
                if (err != -1)
                {
                    fd = -1;
                    return;
                }
                if (errno != EINTR)
                    return;
            }
        }

        int fcntl(int cmd)
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
        int fcntl(int cmd, T arg)
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

        void fsync()
        {
            // Pushes dirty data out to the disk controller.  On macOS, this
            // doesn't flush to the disk itself; the dirty data could sit in
            // disk buffers.
            futil::fsync(fd);
        }

        void fsync_and_barrier()
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

            fsync_and_flush();
#elif IS_LINUX
            fsync();
#else
#error Unknown platform.
#endif
        }

        void fsync_and_flush()
        {
#if IS_MACOS
            // Performs an fasync() and then flushes the disk controller's
            // buffers to the physical drive medium.
            fcntl(F_FULLFSYNC);
#elif IS_LINUX
            fsync();
#else
#error Unknown platform.
#endif
        }

        constexpr file_descriptor():fd(-1) {}
        
        constexpr file_descriptor(int fd):fd(fd) {}

        file_descriptor(const file_descriptor&);  // Link error if invoked.

        file_descriptor(file_descriptor&& other):
            fd(other.fd)
        {
            other.fd = -1;
        }

        ~file_descriptor()
        {
            close();
        }
    };

    struct directory : public file_descriptor
    {
        std::vector<std::string> _listdir(uint32_t mask) const
        {
            int search_fd;
            for (;;)
            {
                search_fd = ::openat(fd,".",O_DIRECTORY);
                if (search_fd != -1)
                    break;
                if (errno != EINTR)
                    throw errno_exception(errno);
            }

            std::vector<std::string> dirs;
            auto* dirp = ::fdopendir(search_fd);
            if (dirp == NULL)
            {
                printf("fdopendir returned NULL.\n");
                throw errno_exception(EBADF);
            }
            struct dirent* dp;
            while ((dp = ::readdir(dirp)) != NULL)
            {
                if (!(mask & (1ULL << dp->d_type)))
                    continue;
                if (dp->d_name[0] == '.')
                {
                    if (dp->d_name[1] == '\0')
                        continue;
                    if (dp->d_name[1] == '.' && dp->d_name[2] == '\0')
                        continue;
                }

                dirs.push_back(dp->d_name);
            }

            ::closedir(dirp);

            return dirs;
        }

        inline std::vector<std::string> listdirs() const
        {
            return _listdir(1 << DT_DIR);
        }

        inline std::vector<std::string> listall() const
        {
            return _listdir(-1);
        }

        directory(const path& p)
        {
            for (;;)
            {
                fd = ::open(p,O_DIRECTORY | O_RDONLY);
                if (fd != -1)
                    return;
                if (errno != EINTR)
                    throw errno_exception(errno);
            }
        }

        directory(const directory& d, const path& p)
        {
            for (;;)
            {
                fd = ::openat(d.fd,p,O_DIRECTORY | O_RDONLY);
                if (fd != -1)
                    return;
                if (errno != EINTR)
                    throw errno_exception(errno);
            }
        }
    };

    struct file : public file_descriptor
    {
        void openat(int dir_fd, const path& p, int oflag)
        {
            close();
            if (oflag & O_CREAT)
                throw inconsistent_file_params();
            for (;;)
            {
                fd = ::openat(dir_fd,p,oflag);
                if (fd != -1)
                    return;
                if (errno != EINTR)
                    throw errno_exception(errno);
            }
        }

        void openat(int dir_fd, const path& p, int oflag, mode_t mode)
        {
            close();
            if (!(oflag & O_CREAT))
                throw inconsistent_file_params();
            for (;;)
            {
                fd = ::openat(dir_fd,p,oflag,mode);
                if (fd != -1)
                    return;
                if (errno != EINTR)
                    throw errno_exception(errno);
            }
        }

        void open(const path& p, int oflag)
        {
            openat(AT_FDCWD,p,oflag);
        }

        void open(const path& p, int oflag, mode_t mode)
        {
            openat(AT_FDCWD,p,oflag,mode);
        }

        void open(const directory& d, const path& p, int oflag)
        {
            openat(d.fd,p,oflag);
        }

        void open(const directory& d, const path& p, int oflag, mode_t mode)
        {
            openat(d.fd,p,oflag,mode);
        }
        
        void openat_if_exists(int dir_fd, const path& p, int oflag)
        {
            close();
            if (oflag & O_CREAT)
                throw inconsistent_file_params();
            for (;;)
            {
                fd = ::openat(dir_fd,p,oflag);
                if (fd != -1 || errno == ENOENT)
                    return;
                if (errno != EINTR)
                    throw errno_exception(errno);
            }
        }

        void open_if_exists(const path& p, int oflag)
        {
            openat_if_exists(AT_FDCWD,p,oflag);
        }

        void open_if_exists(const directory& d, const path& p, int oflag)
        {
            openat_if_exists(d.fd,p,oflag);
        }

        void read_all(void* _p, size_t n)
        {
            auto* p = (char*)_p;
            while (n)
            {
                ssize_t v = ::read(fd,p,n);
                if (v < -1 || v == 0)
                    throw futil::errno_exception(EIO);
                if (v == -1)
                {
                    if (errno == EINTR)
                        continue;
                    throw futil::errno_exception(errno);
                }
                p += v;
                n -= v;
            }
        }

        uint64_t read_u64()
        {
            uint64_t v;
            read_all(&v,sizeof(v));
            return v;
        }

        void write_all(const void* _p, size_t n)
        {
            auto* p = (const char*)_p;
            while (n)
            {
                ssize_t v = ::write(fd,p,n);
                if (v < -1)
                    throw futil::errno_exception(EIO);
                if (v == -1)
                {
                    if (errno == EINTR)
                        continue;
                    throw futil::errno_exception(errno);
                }
                p += v;
                n -= v;
            }
        }

        off_t lseek(off_t offset, int whence)
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

        void flock(int operation)
        {
            if (::flock(fd,operation) == -1)
                throw futil::errno_exception(errno);
        }

        mapping mmap(void* addr, size_t len, int prot, int flags, off_t offset)
        {
            return mapping(addr,len,prot,flags,fd,offset);
        }

        void truncate(off_t length)
        {
            for (;;)
            {
                if (::ftruncate(fd,length) != -1)
                    return;
                if (errno != EINTR)
                    throw futil::errno_exception(errno);
            }
        }

        constexpr file() {}

        file(const path& p, int oflag)
        {
            open(p,oflag);
        }

        file(const path& p, int oflag, mode_t mode)
        {
            open(p,oflag,mode);
        }

        file(const directory& d, const path& p, int oflag)
        {
            open(d,p,oflag);
        }

        file(const directory& d, const path& p, int oflag, mode_t mode)
        {
            open(d,p,oflag,mode);
        }

    protected:
        file(int fd):file_descriptor(fd) {}
    };

    struct file_write_watcher
    {
        const int fd;
        const int kqueue_fd;

        void wait_for_write() const;

        file_write_watcher(int fd);
        file_write_watcher(const file& f):file_write_watcher(f.fd) {}
        ~file_write_watcher();
    };

    inline void mkdir(const char* path, mode_t mode)
    {
        // mkdir() doesn't seem to return EINTR.
        if (::mkdir(path,mode) == -1)
            throw errno_exception(errno);
    }

    inline void mkdir(int at_fd, const char* path, mode_t mode)
    {
        if (::mkdirat(at_fd,path,mode) == -1)
            throw errno_exception(errno);
    }

    inline void mkdir(const directory& dir, const char* path, mode_t mode)
    {
        futil::mkdir(dir.fd,path,mode);
    }

    inline void mkdir_if_not_exists(const char* path, mode_t mode)
    {
        // mkdir() doesn't seem to return EINTR.
        if (::mkdir(path,mode) == -1 && errno != EEXIST)
            throw errno_exception(errno);
    }

    inline void mkdir_if_not_exists(const directory& dir, const char* path,
                                    mode_t mode)
    {
        if (::mkdirat(dir.fd,path,mode) == -1 && errno != EEXIST)
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

    inline void unlink(const directory& dir, const char* path)
    {
        if (::unlinkat(dir.fd,path,0) == -1)
            throw errno_exception(errno);
    }

    inline void unlink_if_exists(const char* path)
    {
        if (::unlink(path) == -1 && errno != ENOENT)
            throw errno_exception(errno);
    }

    inline void unlink_if_exists(const directory& dir, const char* path)
    {
        if (::unlinkat(dir.fd,path,0) == -1 && errno != ENOENT)
            throw errno_exception(errno);
    }

    inline void rename(const char* old, const char* _new)
    {
        // rename() doesn't seem to return EINTR.
        if (::rename(old,_new) == -1)
            throw errno_exception(errno);
    }

    inline void rename(const directory& old_dir, const char* old,
                       const directory& new_dir, const char* _new)
    {
        // Renames to the target location, atomically overwriting whatever was
        // there to begin with.
        if (::renameat(old_dir.fd,old,new_dir.fd,_new) == -1)
            throw errno_exception(errno);
    }

    inline bool rename_if_not_exists(const directory& old_dir, const char* old,
                                     const directory& new_dir, const char* _new)
    {
        // Renames to the target location, as long as the target doesn't exist.
        // Returns true if the rename was successful, false if the target
        // already existed, otherwise throws an exception upon error.
        if (!::renameatx_np(old_dir.fd,old,new_dir.fd,_new,RENAME_EXCL))
            return true;
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

    inline int openat(const directory& d, const path& p, int oflag)
    {
        if (oflag & O_CREAT)
            throw inconsistent_file_params();
        for (;;)
        {
            int fd = ::openat(d.fd,p,oflag);
            if (fd != -1)
                return fd;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }

    inline int openat(const directory& d, const path& p, int oflag,
                       mode_t mode)
    {
        if (!(oflag & O_CREAT))
            throw inconsistent_file_params();
        for (;;)
        {
            int fd = ::openat(d.fd,p,oflag,mode);
            if (fd != -1)
                return fd;
            if (errno != EINTR)
                throw errno_exception(errno);
        }
    }

    inline futil::path
    operator+(const std::string& lhs, const futil::path& rhs)
    {
        return futil::path(lhs,rhs);
    }

    inline futil::path
    operator+(const char* lhs, const futil::path& rhs)
    {
        return futil::path(lhs,rhs);
    }
}

#endif /* __SRC_FUTIL_FUTIL_H */
