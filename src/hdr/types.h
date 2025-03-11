// Copyright (c) 2018-2019 Terry Greeniaus.
// All rights reserved.
#ifndef __TYPES_H
#define __TYPES_H

#include "kassert.h"
#include <stddef.h>

namespace types
{
    // Given a pointer to a field in an object of some type, extract a pointer
    // to the enclosing object.
#define container_of(p,type,field) \
        ((type*)((char*)(1 ? (p) : &((type *)0)->field) - offsetof(type,field)))

    // Extract the underlying type from a reference parameter.
    template<typename T> struct remove_reference      {typedef T type;};
    template<typename T> struct remove_reference<T&>  {typedef T type;};
    template<typename T> struct remove_reference<T&&> {typedef T type;};
    template<typename T>
    using remove_reference_t = typename remove_reference<T>::type;

    // Extract the underlying type from a pointer parameter.
    template<typename T>
    using remove_pointer_t = remove_reference_t<decltype(*(T)0)>;

    // Extract the underlying type from some pointer.
#define extract_type(p) types::remove_pointer_t<decltype(p)>

    // Extract the underlying type from the this pointer.
#define this_type extract_type(this)

    // Determine the attributes of a given field in a type.
#define typeof_field(T,f) types::remove_pointer_t<decltype(&((T*)0)->f)>
#define sizeof_field(T,f) sizeof(typeof_field(T,f))
#define nelmof_field(T,f) (sizeof(typeof_field(T,f))/ \
                           sizeof(typeof_field(T,f[0])))

    // Determine the attributes of an array.
#define nelmof_array(a) (sizeof(a)/sizeof((a)[0]))
#define NELEMS(a)       nelmof_array(a)

    // Forward a parameter.
    template<typename T>
    constexpr T&& forward(remove_reference_t<T>& t) noexcept
    {
        return static_cast<T&&>(t);
    }
    template<typename T>
    constexpr T&& forward(remove_reference_t<T>&& t) noexcept
    {
        return static_cast<T&&>(t);
    }

    // Type that cannot be copied if you subclass it.
    struct non_copyable
    {
        void operator=(const non_copyable&) = delete;

        non_copyable() = default;
        non_copyable(const non_copyable&) = delete;
    };
}

template<typename Dst, typename Src>
static constexpr Dst union_cast(Src v)
{
    KASSERT(sizeof(Dst) == sizeof(Src));
    union
    {
        Src orig;
        Dst casted;
    } u = {v};
    return u.casted;
}

#endif /* __TYPES_H */
