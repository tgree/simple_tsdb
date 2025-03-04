// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include <hdr/kassert.h>
#include <stdlib.h>

struct
{
    const char* file;
    unsigned int line;
} static volatile abort_info;

void
kabort(const char* f, unsigned int l) noexcept
{
    abort_info.file = f;
    abort_info.line = l;
    abort();
}
