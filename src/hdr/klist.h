#ifndef __KERNEL_LIST_H
#define __KERNEL_LIST_H

#include "kassert.h"
#include <stddef.h>

namespace kernel
{
    template<typename L, size_t OFFSET>
    struct klist_iterator
    {
        typedef typename L::elem_type elem_type;
        typedef typename L::link_type link_type;

        const link_type*  pos;

        inline elem_type& operator*() const
        {
            return *(elem_type*)((char*)pos - OFFSET);
        }

        inline void operator++()
        {
            pos = pos->next;
        }

        constexpr bool operator==(const klist_iterator& other) const
        {
            return pos == other.pos;
        }

        constexpr bool operator!=(const klist_iterator& other) const
        {
            return pos != other.pos;
        }

        constexpr klist_iterator(const link_type* pos):pos(pos) {}
        constexpr klist_iterator(const klist_iterator& o):pos(o.pos) {}
    };

#define KLINK_NOT_IN_USE 0x4444444444444444U
    struct klink
    {
        union
        {
            uintptr_t   nextu;
            klink*      next;
        };

        constexpr klink():nextu(KLINK_NOT_IN_USE) {}
        inline ~klink() {kassert(nextu == KLINK_NOT_IN_USE);}
    };

    template<typename T>
    struct klist_leaks
    {
        typedef T elem_type;
        typedef klink link_type;

        klink*  head;
        klink*  tail;

        inline void clear() {head = tail = NULL;}
        constexpr bool empty() const {return head == NULL;}

        constexpr klink* first_link() const {return head;}
        constexpr const klink* sentinel_link() const {return NULL;}

        template<size_t OFFSET>
        constexpr auto begin() const
        {
            return klist_iterator<klist_leaks,OFFSET>(first_link());
        }

        template<size_t OFFSET>
        constexpr auto end() const
        {
            return klist_iterator<klist_leaks,OFFSET>(sentinel_link());
        }

        inline size_t size() const    
        {
            size_t n = 0;
            klink* l = head;
            while (l)
            {
                l = l->next;
                ++n;
            }
            return n;
        }

        constexpr klink* front() const {return first_link();}

        inline void push_front(klink* l)
        {
            kassert(l->nextu == KLINK_NOT_IN_USE);
            l->next = head;
            head    = l;
            tail    = tail ?: l;
        }

        inline void push_back(klink* l)
        {
            kassert(l->nextu == KLINK_NOT_IN_USE);
            l->next = NULL;
            if (!tail)
                head = l;
            else
                tail->next = l;
            tail = l;
        }

        inline void pop_front()
        {
            kassert(!empty());
            klink* curr = head;
            if (head == tail)
                head = tail = NULL;
            else
                head = head->next;
            curr->nextu = KLINK_NOT_IN_USE;
        }

        inline void pop_all()
        {
            while (!empty())
                pop_front();
        }

        inline void append(klist_leaks& other)
        {
            if (other.empty())
                return;

            if (tail)
                tail->next = other.head;
            else
                head = other.head;

            tail       = other.tail;
            other.head = NULL;
            other.tail = NULL;
        }

        constexpr klist_leaks():head(NULL),tail(NULL) {}
    };

    template<typename T>
    struct klist : public klist_leaks<T>
    {
        constexpr klist():klist_leaks<T>() {}
        inline ~klist() {kassert(this->head == NULL && this->tail == NULL);}
    };

    struct kdlink_leaks
    {
        union
        {
            struct
            {
                uintptr_t   nextu;
                uintptr_t   prevu;
            };
            struct
            {
                kdlink_leaks*   next;
                kdlink_leaks*   prev;
            };
        };

        inline void insert_before(kdlink_leaks* before)
        {
            // Insert this before before.
            kassert(nextu == KLINK_NOT_IN_USE);
            kassert(prevu == KLINK_NOT_IN_USE);
            next               = before;
            prev               = before->prev;
            before->prev->next = this;
            before->prev       = this;
        }

        inline void unlink()
        {
            prev->next = next;
            next->prev = prev;
        }

        constexpr kdlink_leaks():
            nextu(KLINK_NOT_IN_USE),
            prevu(KLINK_NOT_IN_USE)
        {
        }
        constexpr kdlink_leaks(kdlink_leaks* next,kdlink_leaks* prev):
            next(next),
            prev(prev)
        {
        }
    };

    struct kdlink : public kdlink_leaks
    {
        inline void unlink()
        {
            kdlink_leaks::unlink();
            nextu = prevu = KLINK_NOT_IN_USE;
        }

        constexpr kdlink():kdlink_leaks() {}
        inline ~kdlink()
        {
            kassert(nextu == KLINK_NOT_IN_USE && prevu == KLINK_NOT_IN_USE);
        }
    };

    template<typename T>
    struct kdlist_leaks
    {
        typedef T elem_type;
        typedef kdlink_leaks link_type;

        kdlink_leaks    sentinel;

        inline void clear() {sentinel.next = sentinel.prev = &sentinel;}
        constexpr bool empty() const {return sentinel.next == &sentinel;}

        constexpr kdlink_leaks* first_link() const {return sentinel.next;}
        constexpr const kdlink_leaks* sentinel_link() const {return &sentinel;}

        template<size_t OFFSET>
        constexpr auto begin() const
        {
            return klist_iterator<kdlist_leaks,OFFSET>(first_link());
        }

        template<size_t OFFSET>
        constexpr auto end() const
        {
            return klist_iterator<kdlist_leaks,OFFSET>(sentinel_link());
        }

        inline size_t size() const
        {
            size_t n = 0;
            auto* l  = sentinel.next;
            while (l != &sentinel)
            {
                l = l->next;
                ++n;
            }
            return n;
        }

        inline void push_front(kdlink_leaks* l)
        {
            l->insert_before(first_link());
        }

        inline void push_back(kdlink_leaks* l)
        {
            l->insert_before(&sentinel);
        }

        inline void pop_front()
        {
            kassert(!empty());
            auto* l = sentinel.next;
            l->next->prev = &sentinel;
            sentinel.next = l->next;
            l->nextu      = KLINK_NOT_IN_USE;
            l->prevu      = KLINK_NOT_IN_USE;
        }

        inline void pop_all()
        {
            while (!empty())
                pop_front();
        }

        inline void append(kdlist_leaks& other)
        {
            if (other.empty())
                return;

            sentinel.prev->next       = other.sentinel.next;
            other.sentinel.next->prev = sentinel.prev;
            other.sentinel.prev->next = &sentinel;
            sentinel.prev             = other.sentinel.prev;
            other.sentinel.next       = &other.sentinel;
            other.sentinel.prev       = &other.sentinel;
        }

        constexpr kdlist_leaks():sentinel(&sentinel,&sentinel) {}
    };

    template<typename T>
    struct kdlist : public kdlist_leaks<T>
    {
        constexpr kdlist():kdlist_leaks<T>() {}
        inline ~kdlist()
        {
            kassert(this->sentinel.next == &this->sentinel &&
                    this->sentinel.prev == &this->sentinel);
        }
    };

    template<typename L, size_t OFFSET>
    struct klist_rbfl_adapter
    {
        klist_iterator<L,OFFSET> pos;
        const klist_iterator<L,OFFSET> sentinel;

        inline auto& operator*() const {return *pos;}
        inline void operator++()       {++pos;}
        inline auto begin()            {return pos;}
        inline auto end() const        {return sentinel;}

        constexpr klist_rbfl_adapter(L& kl):
            pos(kl.first_link()),
            sentinel(kl.sentinel_link())
        {}
    };

#define klist_front(q,field) \
    (q.empty() ? NULL : \
     container_of(q.first_link(), \
              typename types::remove_reference_t<decltype(q)>::elem_type,field))

#define klist_elems(q,field) \
    kernel::klist_rbfl_adapter<types::remove_reference_t<decltype(q)>, \
        offsetof(typename types::remove_reference_t<decltype(q)>::elem_type, \
                 field)>(q)

#define klist_begin(q,field) \
    q.begin< \
        offsetof(typename types::remove_reference_t<decltype(q)>::elem_type, \
                 field)>()

#define klist_end(q,field) \
    q.end< \
        offsetof(typename types::remove_reference_t<decltype(q)>::elem_type, \
                 field)>()
}

#endif /* __KERNEL_LIST_H */
