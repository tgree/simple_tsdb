// Copyright (c) 2018-2020 by Terry Greeniaus.  All rights reserved.
#ifndef __HDR_DELEGATE_H
#define __HDR_DELEGATE_H

#include "types.h"

template<typename Signature>
struct delegate;

template<typename RC, typename ...Args>
struct delegate<RC(Args...)>
{
    void*   obj;
    RC      (*bouncer)(void* obj, Args...);

    constexpr operator bool() const {return bouncer != NULL;}
    void clear()                    {bouncer = NULL;}

    inline RC operator()(Args... args) const
    {
        return (*bouncer)(obj,types::forward<Args>(args)...);
    }

    template<typename T, RC(T::*Method)(Args...)>
    static RC mbounce(void* obj, Args... args)
    {
        return (((T*)obj)->*Method)(types::forward<Args>(args)...);
    }

    template<RC (&Func)(Args...)>
    static RC fbounce(void* junk, Args... args)
    {
        return Func(types::forward<Args>(args)...);
    }

    template<typename T, RC(T::*Method)(Args...)>
    void _make_method_delegate(T* o)
    {
        obj     = (void*)o;
        bouncer = mbounce<T,Method>;
    }

    constexpr delegate(void* obj = NULL, typeof(bouncer) bouncer = NULL):
        obj(obj),bouncer(bouncer) {}
};

template<typename T, typename RC, typename ...Args>
delegate<RC(Args...)> delegate_convert(RC (T::*Method)(Args...));

template<typename RC, typename ...Args>
delegate<RC(Args...)> delegate_convert(RC (*Func)(Args...));

template<typename T, typename RC, typename ...Args>
T* ptmf_extract_type(RC (T::*Method)(Args...));

#define make_method_delegate(m) \
    _make_method_delegate<this_type,&this_type::m>(this)

#define method_delegate_ptmf(obj,ptmf) \
    decltype(delegate_convert(ptmf))((void*)obj, \
     decltype(delegate_convert(ptmf))::mbounce<extract_type(obj),ptmf>)

#define object_method_delegate(obj,m) \
    method_delegate_ptmf(obj,&extract_type(obj)::m)

#define method_delegate(m) \
    object_method_delegate(this,m)

#define super_delegate(fm) \
    method_delegate_ptmf(static_cast<decltype(ptmf_extract_type(&fm))>(this), \
                         &fm)

#define func_delegate(f) delegate<typeof(f)> \
    (NULL, \
     decltype(delegate_convert(f))::fbounce<f>)

struct nop_delegate
{
    template<typename RC, typename ...Args>
    static inline RC _nop_func(Args...)
    {
        return RC();
    }

    template<typename RC, typename ...Args>
    operator delegate<RC(Args...)>()
    {
        return delegate<RC(Args...)>(
            NULL,
            delegate<RC(Args...)>::template fbounce<_nop_func<RC,Args...>>);
    }
};

#endif /* __HDR_DELEGATE_H */
