// Copyright (c) 2025 by Terry Greeniaus.  All rights reserved.
#ifndef __HDR_AUTO_BUF_H
#define __HDR_AUTO_BUF_H

#include <futil/futil.h>

struct auto_buf
{
    void* const data;

    operator void*() const
    {
        return data;
    }

    auto_buf(size_t len):
        data(malloc(len))
    {
        if (!data)
            throw futil::errno_exception(ENOMEM);
    }
    ~auto_buf()
    {
        free(data);
    }
};

#endif /* __HDR_AUTO_BUF_H */
