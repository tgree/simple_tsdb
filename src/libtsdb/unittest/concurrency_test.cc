// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../delete.h"
#include "../write.h"
#include "../database.h"
#include "../count.h"
#include "../select_op.h"
#include "../wal.h"
#include "fake_db.h"
#include <futil/fakefs/fakefs.h>
#include <hdr/types.h>
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_write_while_selecting)
    {
        // Generate a DB with 3 chunk entries and 8 WAL points:
        //
        //  100 - 250 [CHUNK 16]
        //  260 - 410 [CHUNK 16]
        //  420 - 570 [CHUNK 16]
        //  580 - 620 [WAL    5]
        init_db(16,128);
        populate_db(100,10,{45,8});

        tsdb::root root(".",false);
        tsdb::database db1(root,"db1");
        tsdb::measurement m1(db1,"measurement1");
        tsdb::series_read_lock srl(m1,"series1");

        // The select_op mmaps stuff, so we need to provide backing storage
        // or else fakefs explodes.
        auto* sdn = fd_table[srl.series_dir.fd].directory;
        sdn->get_file("index")->data.reserve(1024*1024);

        // We are going to perform a 10-element write operation every time
        // our select_op does an futil call.
        uint64_t t = 630;
        futil::hook_func = [&t](uint32_t id)
        {
            populate_db(t,10,{10});
            t += 100;
        };

        // Do the select.
        tsdb::select_op_first op(srl,"series1",{"field1"},0,-1,-1);
        TASSERT(op.npoints != 0);
        while (op.npoints)
            op.next();

        // Disable the write hook.
        futil::hook_func = NULL;

        // Do a second select to figure out what is really there.
        tsdb::series_read_lock srl2(m1,"series1");
        tsdb::select_op_first op2(srl2,"series1",{"field1"},0,-1,-1);
        TASSERT(op2.npoints != 0);
        uint64_t t_first = *op2.timestamps_begin;
        uint64_t t_last = 0;
        while (op2.npoints)
        {
            t_last = *(op2.timestamps_end - 1);
            op2.next();
        }
        tsdb::wal_query wq(srl2,0,-1);
        if (wq.nentries)
            t_last = wq.back()->time_ns;
        tmock::assert_equiv(t_first,100ULL);
        tmock::assert_equiv(t_last,t - 10);
        TASSERT(t > 630);
    }

    TMOCK_TEST(test_select_while_writing)
    {
        // Generate a DB with 3 chunk entries and 8 WAL points:
        //
        //  100 - 250 [CHUNK 16]
        //  260 - 410 [CHUNK 16]
        //  420 - 570 [CHUNK 16]
        //  580 - 620 [WAL    5]
        init_db(16,128);
        populate_db(100,10,{45,8});

        // Queue up a select every time an futil call fires.
        uint64_t t = 620;
        futil::hook_func = [&t](uint32_t id)
        {
            tsdb::root root(".",false);
            tsdb::database db1(root,"db1");
            tsdb::measurement m1(db1,"measurement1");
            tsdb::series_read_lock srl(m1,"series1");
            tsdb::select_op_first op(srl,"series1",
                                     {"field1","field2","field3"},0,-1,-1);
            tsdb::wal_query wq(srl,0,-1);
            TASSERT(op.npoints != 0);
            tmock::assert_equiv(*op.timestamps_begin,100UL);
            uint64_t t_last;
            while (op.npoints)
            {
                t_last = *(op.timestamps_end - 1);
                op.next();
            }
            if (wq.nentries)
            {
                TASSERT(wq.front()->time_ns > t_last);
                t_last = wq.back()->time_ns;
            }
            TASSERT(t_last >= t);
            t = t_last;
        };

        // Do the write operation.
        populate_db(630,10,{33});

        // Disable the write hook.
        futil::hook_func = NULL;

        tmock::assert_equiv(t,950UL);
    }
};

TMOCK_MAIN();
