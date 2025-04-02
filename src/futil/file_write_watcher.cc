// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "futil.h"
#include <hdr/compiler.h>

#if IS_MACOS
#include <sys/event.h>

futil::file_write_watcher::file_write_watcher(int fd):
    fd(fd),
    kqueue_fd(kqueue())
{
    if (kqueue_fd == -1)
        throw errno_exception(errno);

    struct kevent events_to_monitor;
    EV_SET(&events_to_monitor,fd,EVFILT_VNODE,EV_ADD | EV_CLEAR,NOTE_WRITE,0,
           NULL);
    for (;;)
    {
        int count = kevent(kqueue_fd,&events_to_monitor,1,NULL,0,NULL);
        if (count != - 1)
            return;
        if (errno == EINTR)
            continue;
        throw errno_exception(errno);
    }
}

futil::file_write_watcher::~file_write_watcher()
{
    close(kqueue_fd);
}

void
futil::file_write_watcher::wait_for_write() const
{
    struct kevent event_data;
    for (;;)
    {
        int count = kevent(kqueue_fd,NULL,0,&event_data,1,NULL);
        if (count != -1)
            return;
        if (errno == EINTR)
            continue;
        throw errno_exception(errno);
    }
}
#endif
