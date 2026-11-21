// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../futil_wrap.h"
#include <tmock/tmock.h>

void*
futil::mmap(void* addr, size_t len, int prot, int flags, int fd,
    off_t offset)
{
    return (void*)mock("futil::mmap",addr,len,prot,flags,fd,offset);
}

void
futil::munmap(void* addr, size_t len)
{
    mock("futil::munmap",addr,len);
}

void
futil::close(int fd)
{
    mock("futil::close",fd);
}

int
futil::fcntl(int fd, int cmd)
{
    return (int)mock("futil::fcntl",fd,cmd);
}

template<>
int
futil::fcntl<int>(int fd, int cmd, int arg)
{
    return (int)mock("futil::fcntl",fd,cmd,arg);
}

int
futil::openat(int at_fd, const char* path, int oflag)
{
    return (int)mock("futil::openat",at_fd,path,oflag);
}

DIR*
futil::fdopendir(int fd)
{
    return (DIR*)mock("futil::fdopendir",fd);
}

struct dirent*
futil::readdir(DIR* dirp)
{
    return (struct dirent*)mock("futil::readdir",dirp);
}

void
futil::closedir(DIR* dirp)
{
    mock("futil::closedir",dirp);
}

ssize_t
futil::read(int fd, void* buf, size_t nbyte)
{
    return (ssize_t)mock("futil::read",fd,buf,nbyte);
}

off_t
futil::lseek(int fd, off_t offset, int whence)
{
    return (off_t)mock("futil::lseek",fd,offset,whence);
}

void
futil::flock(int fd, int operation)
{
    mock("futil::flock",fd,operation);
}
