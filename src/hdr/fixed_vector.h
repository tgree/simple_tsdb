// Copyright (c) 2020 by Terry Greeniaus.  All rights reserved.
#ifndef __HDR_FIXED_VECTOR_H
#define __HDR_FIXED_VECTOR_H

#include "kassert.h"
#include <initializer_list>
#include <utility>
#include <new>
#include <stddef.h>

template<typename T>
struct fixed_vector
{
    const size_t    N;
    size_t          n;
    T*              elems;

    constexpr bool full() const   {return n == N;}
    constexpr bool empty() const  {return n == 0;}
    constexpr size_t size() const {return n;}

    void clear()
    {
        for (size_t i=0; i<n; ++i)
            elems[n-i-1].~T();
        n = 0;
    }

    T& operator[](size_t i)
    {
        kassert(i < n);
        return elems[i];
    }

    const T& operator[](size_t i) const
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

    fixed_vector(const fixed_vector&);    // Link error if invoked.

    constexpr fixed_vector(size_t N):
        N(N),
        n(0),
        elems(NULL)
    {
        elems = (T*)malloc(sizeof(T)*N);
        if (!elems)
            throw std::bad_alloc();
    }

    ~fixed_vector()
    {
        clear();
        free(elems);
    }
};

#endif /* __HDR_FIXED_VECTOR_H */
