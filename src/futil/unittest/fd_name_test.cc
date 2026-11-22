// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../futil.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Checks futil::fd_open_was_exact() against the real file system that holds
// $TMPDIR (or /tmp).  Opening "Foo" as "Foo" is always exact.  On a
// case-insensitive file system, such as macOS's default APFS, opening "foo"
// finds the "Foo" directory, and fd_open_was_exact() must say that open
// wasn't exact.  On a case-sensitive one, "foo" doesn't exist.
// With --verbose, it says which kind of file system it found.
int
main(int argc, const char* argv[])
{
    bool verbose = false;
    for (int i=1; i<argc; ++i)
    {
        if (!strcmp(argv[i],"--verbose"))
            verbose = true;
    }

    const char* tmpdir = getenv("TMPDIR");
    std::string tmpl = std::string(tmpdir ? tmpdir : "/tmp") +
                       "/fd_name_test.XXXXXX";
    futil::mkdtemp(&tmpl[0]);

    {
        futil::directory dir(tmpl);
        futil::mkdir(dir,"Foo",0700);
        {
            futil::directory foo(dir,"Foo");
            assert(futil::fd_open_was_exact(foo.fd,"Foo"));

            try
            {
                futil::directory other(dir,"foo");
                assert(!futil::fd_open_was_exact(other.fd,"foo"));
                if (verbose)
                    printf("fd_name_test: case-insensitive file system.\n");
            }
            catch (const futil::errno_exception& e)
            {
                assert(e.errnov == ENOENT);
                if (verbose)
                    printf("fd_name_test: case-sensitive file system.\n");
            }
        }
        futil::unlinkat(dir.fd,"Foo",AT_REMOVEDIR);
    }
    futil::unlinkat(AT_FDCWD,tmpl.c_str(),AT_REMOVEDIR);
    return 0;
}
