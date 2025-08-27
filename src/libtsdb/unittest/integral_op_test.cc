// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../integral_op.h"
#include "../database.h"
#include "../series.h"
#include "fake_db.h"
#include <futil/fakefs/fakefs.h>
#include <hdr/types.h>
#include <tmock/tmock.h>
#include <limits>

static void
generate_db()
{
    init_db(16,128);

    // Populate the database.  We do a 45-point write followed by an 8-point
    // write, yielding 3 chunk entries and some WAL points:
    //
    //  CH0 100 - 250 [16]
    //  CH1 260 - 410 [16]
    //  CH2 420 - 540 [13]
    //  WAL 550 - 620 [8]
    //
    // Then we write 8 points, which will all end
    // up in the WAL.
    populate_db(100,10,{45,8});
}

class tmock_test
{
    TMOCK_TEST(test_integral_full)
    {
        generate_db();

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");
        tsdb::integral_op op(srl,"series1",{"field3"},0,-1);
    }
};

TMOCK_MAIN();
