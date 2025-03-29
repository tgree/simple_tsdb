// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_FUTIL_XACT_H
#define __SRC_FUTIL_XACT_H

#include "futil.h"
#include <hdr/auto_buf.h>

namespace futil
{
    struct xact
    {
        inline void commit() {committed = true;}

    protected:
        bool committed;

        constexpr xact():committed(false) {}
    };

    struct _xact_mkdir : public xact
    {
    protected:
        const int at_fd;
        const char* const path;

        _xact_mkdir(int at_fd, const char* path, mode_t mode):
            at_fd(at_fd),
            path(path)
        {
            futil::mkdir(at_fd,path,mode);
        }

        ~_xact_mkdir()
        {
            if (!committed)
                ::unlinkat(at_fd,path,AT_REMOVEDIR);
        }
    };

    struct xact_mkdir : public _xact_mkdir,
                        public directory
    {
        xact_mkdir(const directory& dir, const char* path, mode_t mode):
            _xact_mkdir(dir.fd,path,mode),
            directory(dir,path)
        {
        }

        xact_mkdir(const char* path, mode_t mode):
            _xact_mkdir(AT_FDCWD,path,mode),
            directory(path)
        {
        }
    };

    struct _xact_mkdtemp : public xact
    {
        const int at_fd;
        auto_chrbuf name;

    protected:
        _xact_mkdtemp(int at_fd, const char* templ):
            at_fd(at_fd),
            name(strlen(templ) + 1)
        {
            strcpy(name,templ);
            if (!::mkdtempat_np(at_fd,name))
                throw futil::errno_exception(errno);
        }

        ~_xact_mkdtemp()
        {
            if (!committed)
                ::unlinkat(at_fd,name,AT_REMOVEDIR);
        }
    };

    struct xact_mkdtemp : public _xact_mkdtemp,
                          public directory
    {
        xact_mkdtemp(const directory& dir, const char* templ, mode_t mode):
            _xact_mkdtemp(dir.fd,templ),
            directory(dir,(const char*)name)
        {
            futil::fchmod(dir.fd,mode);
        }
    };

    struct _xact_mktemp : public xact
    {
        const int at_fd;
        auto_chrbuf name;

    protected:
        int mk_fd;

        _xact_mktemp(int at_fd, const char* templ):
            at_fd(at_fd),
            name(strlen(templ) + 1)
        {
            strcpy(name,templ);
            mk_fd = ::mkstempsat_np(at_fd,name,0);
            if (mk_fd == -1)
                throw futil::errno_exception(errno);
        }

        ~_xact_mktemp()
        {
            if (!committed)
                ::unlinkat(at_fd,name,0);
        }
    };

    struct xact_mktemp : public _xact_mktemp,
                         public file
    {
        xact_mktemp(const directory& dir, const char* templ, mode_t mode):
            _xact_mktemp(dir.fd,templ),
            file(mk_fd)
        {
            futil::fchmod(mk_fd,mode);
        }
    };

    class xact_creat : public xact,
                       public file
    {
        const int at_fd;
        const char* const path;

    public:
        xact_creat(const directory& dir, const char* path, int oflag,
                   mode_t mode):
            file(dir,path,oflag,mode),
            at_fd(dir.fd),
            path(path)
        {
        }

        ~xact_creat()
        {
            if (!committed)
                ::unlinkat(at_fd,path,0);
        }
    };
}

#endif /* __SRC_FUTIL_FUTIL_H */
