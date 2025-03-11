// Copyright (c) 2018-2022 Terry Greeniaus.
// All rights reserved.
#include "texpectation.h"
#include "tmock.h"
#include <string.h>

static tmock::expectation* head;
static tmock::expectation* tail;

const tmock::targ*
tmock::call::find_arg(const char* name) const
{
    for (size_t i=0; i<nargs; ++i)
    {
        auto* a = &call_args[i];
        if (!strcmp(name,a->name))
            return a;
    }
    return NULL;
}

void
tmock::_expect(expectation* e)
{
    TASSERT(!e->armed);
    e->next = NULL;
    if (tail)
        tail->next = e;
    else
        head = e;
    tail = e;
    e->armed = true;
}

uintptr_t
tmock::_mock_call(const char* fname, const call* mc)
{
    if (head == NULL)
    {
        printf("Unexpected call (expected nothing): %s\n",fname);
        ::abort();
    }
    else if(strcmp(head->fname,fname))
    {
        printf("Unexpected call (expected %s): %s\n",head->fname,fname);
        ::abort();
    }
    
    bool end_test = false;
    uintptr_t rv = 0;
    for (size_t i=0; i<head->nconstraints; ++i)
    {
        const constraint* c = &head->constraints[i];
        switch (c->type)
        {
            case constraint::ARGUMENT:
            {
                auto* pc = mc->find_arg(c->want_arg.name);
                if (!pc)
                {
                    printf("Expected call to %s: no such argument \"%s\".\n",
                           fname,c->want_arg.name);
                    ::abort();
                }
                if (c->want_arg.value != pc->value)
                {
                    printf("Expected call to %s: expected argument %s to be %u "
                           "(actual %u)\n",fname,c->want_arg.name,
                           c->want_arg.value,pc->value);
                    ::abort();
                }
            }
            break;

            case constraint::STR_ARGUMENT:
            {
                auto* pc = mc->find_arg(c->want_str.name);
                if (!pc)
                {
                    printf("Expected call to %s: no such argument \"%s\".\n",
                           fname,c->want_str.name);
                    ::abort();
                }
                if (strcmp(c->want_str.str,(const char*)pc->value))
                {
                    printf("Expected call to %s: expected argument %s to be "
                           "\"%s\" "
                           "(actual %s)\n",fname,c->want_str.name,
                           c->want_str.str,(const char*)pc->value);
                    ::abort();
                }
            }
            break;

            case constraint::RETURN_VALUE:
                rv = c->return_value.value;
            break;

            case constraint::END_TEST:
                end_test = true;
            break;

            case constraint::CAPTURE:
            {
                auto* pc = mc->find_arg(c->capture_arg.name);
                TASSERT(pc != NULL);
                *c->capture_arg.dst = pc->value;
            }
            break;
        }
    }

    head->armed = false;
    head = head->next;
    if (!head)
        tail = NULL;

    if (end_test)
    {
        tmock::cleanup_expectations();
        exit(0);
    }

    return rv;
}

void
tmock::cleanup_expectations()
{
    if (head == NULL)
        return;

    if (!(tmock::internal::mode_flags & TMOCK_MODE_FLAG_SILENT))
    {
        for (auto* p = head; p; p = p->next)
        {
            printf("Unsatisfied expectation: %s:%zu:%s\n",
                   p->file,p->line,p->fname);
        }
    }
    ::abort();
}
