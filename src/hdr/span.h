// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __HDR_SPAN_H
#define __HDR_SPAN_H

template<typename T>
class span
{
private:
    const T*        m_ptr;
    const size_t    m_size;

public:
    constexpr span():m_ptr(NULL),m_size(0) {}
    constexpr span(const T* m_ptr, size_t m_size):m_ptr(m_ptr),m_size(m_size) {}

    const T* begin() const              {return m_ptr;}
    const T* end() const                {return m_ptr + m_size;}
    size_t size() const                 {return m_size;}
    bool empty() const                  {return m_size == 0;}
    const T* data() const               {return m_ptr;}
    const T& operator[](size_t i) const {return m_ptr[i];}
};

#endif /* __HDR_SPAN_H */
