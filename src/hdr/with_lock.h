// Copyright (c) 2025 by Terry Greeniaus.  All rights reserved.
#ifndef __HDR_WITH_LOCK_H
#define __HDR_WITH_LOCK_H

#include <mutex>

#define with_lock_guard(l) \
    if constexpr( \
        const auto __with_lock_guard_helper = std::lock_guard(l); \
        true)

#endif /* __HDR_WITH_LOCK_H */
