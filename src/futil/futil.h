// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_FUTIL_H
#define __SRC_FUTIL_FUTIL_H

#include <hdr/kassert.h>
#include <hdr/compiler.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <dirent.h>
#include <string.h>
#include <stdarg.h>
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

        bool ends_with(const char* s) const
        {
            return _path.ends_with(s);
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

#ifndef UNITTEST
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
#else
    void* mmap(void* addr, size_t len, int prot, int flags, int fd,
               off_t offset);
    void munmap(void* addr, size_t len);
    void msync(void* addr, size_t len, int flags);
#endif

    struct mapping
    {
        void* addr;
        size_t len;

        void msync()
        {
            futil::msync(addr,len,MS_SYNC);
        }

        inline void map(void* _addr, size_t _len, int prot, int flags, int fd,
                        off_t offset)
        {
            unmap();
            if (_len)
            {
                addr = futil::mmap(_addr,_len,prot,flags,fd,offset);
                len = _len;
            }
        }

        inline void unmap()
        {
            if (len)
            {
                futil::munmap(addr,len);
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

#ifndef UNITTEST
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
        // Performs an fasync() and then flushes the disk controller's
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
#else
    void fsync(int fd);
    void close(int fd);
    int fcntl(int fd, int cmd);
    template<typename T> int fcntl(int fd, int cmd, T arg);
    void fsync_and_flush(int fd);
    void fsync_and_barrier(int fd);
    int vdprintf(int fd, const char* fmt, va_list ap);
#endif

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

            try
            {
                futil::close(fd);
                fd = -1;
            }
            catch (...)
            {
                // If we failed to close with anything other than EINTR, just
                // exit.  close() can fail with EIO if a preceding write()
                // later failed due to an IO error.  The file descriptor is
                // left in an unspecified state so you cannot do ANYTHING with
                // it.
                //
                // TODO: Shouldn't we be setting fd = -1 here so that we don't
                // try any future operations on the fd?
            }
        }

        int fcntl(int cmd) const
        {
            return futil::fcntl(fd,cmd);
        }

        template<typename T>
        int fcntl(int cmd, T arg) const
        {
            return futil::fcntl<T>(fd,cmd,arg);
        }

        void fsync() const
        {
            // Pushes dirty data out to the disk controller.  On macOS, this
            // doesn't flush to the disk itself; the dirty data could sit in
            // disk buffers.
            futil::fsync(fd);
        }

        void fsync_and_barrier() const
        {
            futil::fsync_and_barrier(fd);
        }

        void fsync_and_flush() const
        {
            futil::fsync_and_flush(fd);
        }

        int vprintf(const char* fmt, va_list ap)
        {
            return futil::vdprintf(fd,fmt,ap);
        }

        int printf(const char* fmt, ...) __PRINTF__(2,3)
        {
            va_list ap;
            va_start(ap,fmt);
            int rv = this->vprintf(fmt,ap);
            va_end(ap);

            return rv;
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

#ifndef UNITTEST
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
#else
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
#endif

    struct directory : public file_descriptor
    {
        std::vector<std::string> _listdir(uint32_t mask) const
        {
            int search_fd = futil::openat(fd,".",O_DIRECTORY);

            std::vector<std::string> dirs;
            auto* dirp = futil::fdopendir(search_fd);
            struct dirent* dp;
            while ((dp = futil::readdir(dirp)) != NULL)
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

            futil::closedir(dirp);

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

        directory(const path& p):
            file_descriptor(futil::open(p,O_DIRECTORY | O_RDONLY))
        {
        }

        directory(int at_fd, const path& p):
            file_descriptor(futil::openat(at_fd,p,O_DIRECTORY | O_RDONLY))
        {
        };

        directory(const directory& d, const path& p):
            directory(d.fd,p)
        {
        }
    };

    struct file : public file_descriptor
    {
        void openat(int dir_fd, const path& p, int oflag)
        {
            close();
            if (oflag & O_CREAT)
                throw inconsistent_file_params();
            fd = futil::openat(dir_fd,p,oflag);
        }

        void openat(int dir_fd, const path& p, int oflag, mode_t mode)
        {
            close();
            if (!(oflag & O_CREAT))
                throw inconsistent_file_params();
            fd = futil::openat(dir_fd,p,oflag,mode);
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
            fd = futil::openat_if_exists(dir_fd,p,oflag);
        }

        void open_if_exists(const path& p, int oflag)
        {
            openat_if_exists(AT_FDCWD,p,oflag);
        }

        void open_if_exists(const directory& d, const path& p, int oflag)
        {
            openat_if_exists(d.fd,p,oflag);
        }

        size_t read_all_or_eof(void* _p, size_t n)
        {
            auto* p = (char*)_p;
            while (n)
            {
                ssize_t v = futil::read(fd,p,n);
                if (v == 0)
                    break;
                p += v;
                n -= v;
            }
            return p - (char*)_p;
        }

        void read_all(void* p, size_t n)
        {
            if (read_all_or_eof(p,n) != n)
                throw futil::errno_exception(EIO);
        }

        uint64_t read_u64()
        {
            uint64_t v;
            read_all(&v,sizeof(v));
            return v;
        }

        struct line
        {
            std::string text;
            bool eof;

            operator bool() const {return !eof;}
        };

        line read_line(char terminator = '\n') const
        {
            // Reads lines, separated by the specified terminator string.
            // Strips the terminator string from the return value.
            std::string line;
            for (;;)
            {
                char c;
                ssize_t v = futil::read(fd,&c,1);
                if (v == 0)
                    return {line,true};
                if (v == -1)
                {
                    if (errno == EINTR)
                        continue;
                    throw futil::errno_exception(errno);
                }

                if (c == terminator)
                    return {line,false};

                line += c;
            }
        }

        struct lines_range
        {
            const file& f;
            const char terminator;

            struct lines_sentinel {};
            struct lines_iterator
            {
                const lines_range& lr;
                line l;
                bool at_eof;

                constexpr const std::string& operator*() const {return l.text;}
                lines_iterator& operator++()
                {
                    kassert(!at_eof);
                    at_eof = l.eof;
                    if (!at_eof)
                        l = lr.f.read_line(lr.terminator);
                    return *this;
                }

                constexpr bool operator==(const lines_sentinel&)
                {
                    return at_eof;
                }

                lines_iterator(const lines_range& lr):
                    lr(lr),
                    l(lr.f.read_line(lr.terminator)),
                    at_eof(false)
                {
                }
            };

            auto begin() const {return lines_iterator(*this);}
            constexpr auto end() const {return lines_sentinel{};}

            constexpr lines_range(const file& f, char terminator = '\n'):
                f(f),
                terminator(terminator)
            {
            }
        };

        constexpr lines_range lines(char terminator = '\n') const
        {
            return lines_range{*this,terminator};
        }

        void write_all(const void* _p, size_t n) const
        {
            auto* p = (const char*)_p;
            while (n)
            {
                ssize_t v = futil::write(fd,p,n);
                p += v;
                n -= v;
            }
        }

        off_t lseek(off_t offset, int whence) const
        {
            return futil::lseek(fd,offset,whence);
        }

        file& flock(int operation)
        {
            futil::flock(fd,operation);
            return *this;
        }

        mapping mmap(void* addr, size_t len, int prot, int flags, off_t offset)
        {
            return mapping(addr,len,prot,flags,fd,offset);
        }

        void truncate(off_t length)
        {
            futil::ftruncate(fd,length);
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

#ifndef UNITTEST
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

    inline void mkdir_if_not_exists(const char* path, mode_t mode)
    {
        // mkdir() doesn't seem to return EINTR.
        if (::mkdir(path,mode) == -1 && errno != EEXIST)
            throw errno_exception(errno);
    }

    inline void mkdirat_if_not_exists(int at_fd, const char* path, mode_t mode)
    {
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
        if (::unlinkat(at_fd,path,flag) == -1)
            throw errno_exception(errno);
    }

    inline void unlink_if_exists(const char* path)
    {
        if (::unlink(path) == -1 && errno != ENOENT)
            throw errno_exception(errno);
    }

    inline void unlinkat_if_exists(int at_fd, const char* path, int flag)
    {
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
#else
    void mkdir(const char* path, mode_t mode);
    void mkdir(int at_fd, const char* path, mode_t mode);
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
#endif

    inline void mkdir(const directory& dir, const char* path, mode_t mode)
    {
        futil::mkdir(dir.fd,path,mode);
    }

    inline void mkdir_if_not_exists(const directory& dir, const char* path,
                                    mode_t mode)
    {
        futil::mkdirat_if_not_exists(dir.fd,path,mode);
    }

    inline void unlink(const directory& dir, const char* path)
    {
        futil::unlinkat(dir.fd,path,0);
    }

    inline void unlink_if_exists(const directory& dir, const char* path)
    {
        futil::unlinkat_if_exists(dir.fd,path,0);
    }

    inline void rename(const directory& old_dir, const char* old,
                       const directory& new_dir, const char* _new)
    {
        // Renames to the target location, atomically overwriting whatever was
        // there to begin with.
        futil::renameat(old_dir.fd,old,new_dir.fd,_new);
    }

    inline bool rename_if_not_exists(const directory& old_dir, const char* old,
                                     const directory& new_dir, const char* _new)
    {
        // Renames to the target location, as long as the target doesn't exist.
        // Returns true if the rename was successful, false if the target
        // already existed, otherwise throws an exception upon error.
        return futil::renameat_if_not_exists(old_dir.fd,old,new_dir.fd,_new);
    }

    inline void fchmod(const directory& d, const path& p, mode_t mode)
    {
        futil::fchmodat(d.fd,p,mode,0);
    }

    inline int openat(const directory& d, const path& p, int oflag)
    {
        return futil::openat(d.fd,p,oflag);
    }

    inline int openat(const directory& d, const path& p, int oflag,
                       mode_t mode)
    {
        return futil::openat(d.fd,p,oflag,mode);
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
