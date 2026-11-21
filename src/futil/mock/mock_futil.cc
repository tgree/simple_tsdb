// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../futil_wrap.h"
#include <tmock/tmock.h>

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
