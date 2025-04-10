// Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.
#ifndef __HDR_STATIC_VECTOR_H
#define __HDR_STATIC_VECTOR_H

#include "kassert.h"
#include <initializer_list>
#include <new>
#include <stddef.h>

template<typename T, size_t N>
struct static_vector
{
    size_t  n;
    union
    {
        char junk;
        T   elems[N];
    };

    constexpr bool full() const   {return n == N;}
    constexpr bool empty() const  {return n == 0;}
    constexpr size_t len() const  {return n;}
    constexpr size_t size() const {return len();}

    void clear()
    {
        for (size_t i=0; i<n; ++i)
            elems[n-i-1].~T();
        n = 0;
    }

    constexpr T& operator[](size_t i)
    {
        kassert(i < n);
        return elems[i];
    }

    constexpr const T& operator[](size_t i) const
    {
        kassert(i < n);
        return elems[i];
    }

    T* begin()             {return &elems[0];}
    const T* begin() const {return &elems[0];}
    T* end()               {return &elems[n];}
    const T* end() const   {return &elems[n];}

    void push_back(const T& v)
    {
        kassert(!full());
        new(&elems[n++]) T(v);
    }

    template<typename ...Args>
    void emplace_back(Args&& ...args)
    {
        kassert(!full());
        T* t = &elems[n];
        new((void*)t) T(std::forward<Args>(args)...);
        ++n;
    }

    constexpr static_vector():n(0),junk(0) {}

    static_vector(std::initializer_list<T> il):n(0)
    {
        for (const T& t : il)
            push_back(t);
    }

    ~static_vector()
    {
        clear();
    }
};

#endif /* __HDR_STATIC_VECTOR_H */
