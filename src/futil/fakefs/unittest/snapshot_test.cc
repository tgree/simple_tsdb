// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../fakefs.h"
#include <futil/futil.h>
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_snapshot)
    {
        auto cwd = futil::directory(AT_FDCWD,"./");
        futil::mkdir(cwd,"dir1",0777);
        futil::mkdir(cwd,"dir2",0777);
        futil::mkdir(cwd,"dir1/subdir1",0777);
        futil::mkdir(cwd,"dir2/subdir2",0777);
        futil::mkdir(cwd,"dir1/subdir1/subdir3",0777);

        auto fd0 = futil::file(cwd,"fd0",O_CREAT | O_EXCL,0777);

        snapshot_fs();

        fd0.write_all("12345",5);
        futil::mkdir(cwd,"dir3",0777);

        snapshot_fs();

        fd0.write_all("6789",4);
        futil::unlinkat(cwd.fd,"dir3",AT_REMOVEDIR);

        snapshot_fs();

        for (auto* dn : snapshots)
        {
            TASSERT(dn->subdirs.contains("dir1"));
            TASSERT(dn->subdirs.contains("dir2"));
            TASSERT(dn->subdirs["dir1"]->subdirs.contains("subdir1"));
            TASSERT(dn->subdirs["dir2"]->subdirs.contains("subdir2"));
            TASSERT(dn->subdirs["dir1"]->subdirs["subdir1"]->subdirs.
                    contains("subdir3"));
            TASSERT(dn->files.contains("fd0"));
        }

        TASSERT(!snapshots[0]->subdirs.contains("dir3"));
        TASSERT(snapshots[1]->subdirs.contains("dir3"));
        TASSERT(!snapshots[2]->subdirs.contains("dir3"));
        tmock::assert_equiv(snapshots[0]->files["fd0"]->data_as_string(),"");
        tmock::assert_equiv(snapshots[1]->files["fd0"]->data_as_string(),
                            "12345");
        tmock::assert_equiv(snapshots[2]->files["fd0"]->data_as_string(),
                            "123456789");

        snapshot_reset();

        TASSERT(snapshots.empty());
        tmock::assert_equiv(live_dirs.size(),6UL);
        tmock::assert_equiv(live_files.size(),1UL);
    }

    TMOCK_TEST(test_auto_snapshot)
    {
        snapshot_auto_begin();

        auto cwd = futil::directory(AT_FDCWD,"./");
        tmock::assert_equiv(snapshots.size(),0UL);
        futil::mkdir(cwd,"dir1",0777);
        tmock::assert_equiv(snapshots.size(),1UL);
        futil::mkdir(cwd,"dir2",0777);
        tmock::assert_equiv(snapshots.size(),2UL);
        futil::mkdir(cwd,"dir1/subdir1",0777);
        tmock::assert_equiv(snapshots.size(),3UL);
        futil::mkdir(cwd,"dir2/subdir2",0777);
        tmock::assert_equiv(snapshots.size(),4UL);
        futil::mkdir(cwd,"dir1/subdir1/subdir3",0777);
        tmock::assert_equiv(snapshots.size(),5UL);

        auto fd0 = futil::file(cwd,"fd0",O_CREAT | O_EXCL,0777);
        tmock::assert_equiv(snapshots.size(),6UL);
        fd0.write_all("12345",5);
        tmock::assert_equiv(snapshots.size(),7UL);
        futil::mkdir(cwd,"dir3",0777);
        tmock::assert_equiv(snapshots.size(),8UL);
        fd0.write_all("6789",0);
        tmock::assert_equiv(snapshots.size(),8UL);  // 0-length: No snap!
        fd0.write_all("6789",4);
        tmock::assert_equiv(snapshots.size(),9UL);
        futil::unlinkat(cwd.fd,"dir3",AT_REMOVEDIR);
        tmock::assert_equiv(snapshots.size(),10UL);
        futil::unlink(cwd,"fd0");
        tmock::assert_equiv(snapshots.size(),11UL);

        for (size_t i=0; i<snapshots.size(); ++i)
        {
            auto* ss = snapshots[i];

            // dir1
            TASSERT(ss->subdirs.contains("dir1"));

            // dir2
            if (1 <= i && i < snapshots.size())
                TASSERT(ss->subdirs.contains("dir2"));
            else
                TASSERT(!ss->subdirs.contains("dir2"));

            // subdir1
            if (2 <= i && i < snapshots.size())
                TASSERT(ss->subdirs["dir1"]->subdirs.contains("subdir1"));
            else
            {
                TASSERT(!ss->subdirs.contains("dir1") ||
                        !ss->subdirs["dir1"]->subdirs.contains("subdir1"));
            }

            // subdir2
            if (3 <= i && i < snapshots.size())
                TASSERT(ss->subdirs["dir2"]->subdirs.contains("subdir2"));
            else
            {
                TASSERT(!ss->subdirs.contains("dir2") ||
                        !ss->subdirs["dir2"]->subdirs.contains("subdir2"));
            }

            // subdir3
            if (4 <= i && i < snapshots.size())
            {
                TASSERT(ss->subdirs["dir1"]->subdirs["subdir1"]->
                        subdirs.contains("subdir3"));
            }
            else
            {
                TASSERT(!ss->subdirs.contains("dir1") ||
                        !ss->subdirs["dir1"]->subdirs.contains("subdir1") ||
                        !ss->subdirs["dir1"]->subdirs["subdir1"]->
                         subdirs.contains("subdir3"));
            }

            // fd0
            if (5 <= i && i < 10)
            {
                TASSERT(ss->files.contains("fd0"));
                auto data = ss->files["fd0"]->data_as_string();
                if (i == 5)
                    tmock::assert_equiv(data,"");
                if (i == 6 || i == 7)
                    tmock::assert_equiv(data,"12345");
                if (i == 8 || i == 9)
                    tmock::assert_equiv(data,"123456789");
            }                       
            else
                TASSERT(!ss->files.contains("fd0"));

            // dir3
            if (7 <= i && i < 9)
                TASSERT(ss->subdirs.contains("dir3"));
            else
                TASSERT(!ss->subdirs.contains("dir3"));
        }
    }
};

TMOCK_MAIN();
