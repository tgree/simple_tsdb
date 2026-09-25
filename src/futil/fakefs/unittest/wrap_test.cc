// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#include "../fakefs.h"
#include <futil/futil.h>
#include <tmock/tmock.h>

class tmock_test
{
    TMOCK_TEST(test_creat_excl)
    {
        auto cwd = futil::directory(AT_FDCWD,"./");
        auto fd0 = futil::file(cwd,"fd0",O_CREAT | O_EXCL,0777);
        try
        {
            futil::file(cwd,"fd0",O_CREAT | O_EXCL,0777);
            TABORT("Expected exception trying to create exclusive file that "
                   "already exists!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT(e.errnov == EEXIST);
        }
    }

    TMOCK_TEST(test_paths)
    {
        auto cwd0 = futil::directory(AT_FDCWD,".");
        auto cwd1 = futil::directory(AT_FDCWD,"./");
        auto cwd2 = futil::directory(AT_FDCWD,"./.");
        auto cwd3 = futil::directory(AT_FDCWD,"././");
        TASSERT(fd_table[cwd0.fd].directory == fs_root);
        TASSERT(fd_table[cwd1.fd].directory == fs_root);
        TASSERT(fd_table[cwd2.fd].directory == fs_root);
        TASSERT(fd_table[cwd3.fd].directory == fs_root);

        futil::mkdir(cwd0,"dir1",0777);
        auto cwd4 = futil::directory(AT_FDCWD,"./dir1");
        auto cwd5 = futil::directory(AT_FDCWD,"././dir1/");
        auto cwd6 = futil::directory(AT_FDCWD,"./dir1/.");
        auto cwd7 = futil::directory(AT_FDCWD,"./dir1/./././");
        auto cwd8 = futil::directory(AT_FDCWD,"././dir1/");
        auto cwd9 = futil::directory(AT_FDCWD,"././dir1/../../../dir1");
        TASSERT(fd_table[cwd4.fd].directory == fs_root->subdirs["dir1"]);
        TASSERT(fd_table[cwd5.fd].directory == fs_root->subdirs["dir1"]);
        TASSERT(fd_table[cwd6.fd].directory == fs_root->subdirs["dir1"]);
        TASSERT(fd_table[cwd7.fd].directory == fs_root->subdirs["dir1"]);
        TASSERT(fd_table[cwd8.fd].directory == fs_root->subdirs["dir1"]);
        TASSERT(fd_table[cwd9.fd].directory == fs_root->subdirs["dir1"]);

        auto cwd10 = futil::directory(AT_FDCWD,"..");
        auto cwd11 = futil::directory(AT_FDCWD,"../");
        auto cwd12 = futil::directory(AT_FDCWD,"dir1/..");
        auto cwd13 = futil::directory(AT_FDCWD,"dir1/../");
        TASSERT(fd_table[cwd10.fd].directory == fs_root);
        TASSERT(fd_table[cwd11.fd].directory == fs_root);
        TASSERT(fd_table[cwd12.fd].directory == fs_root);
        TASSERT(fd_table[cwd13.fd].directory == fs_root);
    }

    TMOCK_TEST(test_unlink)
    {
        auto cwd = futil::directory(AT_FDCWD,"./");
        futil::mkdir(cwd,"dir1",0777);

        auto fd0 = futil::file(cwd,"fd0",O_CREAT | O_EXCL,0777);
        auto fd1 = futil::file(cwd,"dir1/fd1",O_CREAT | O_EXCL,0777);

        try
        {
            futil::unlink(cwd,"dir1");
            TABORT("Expected exception trying to unlink a directory without "
                   "specifying AT_REMOVEDIR.");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT(e.errnov == ENOENT);
        }
        TASSERT(fs_root->files.count("fd0"));
        TASSERT(fs_root->subdirs.count("dir1"));
        TASSERT(fs_root->subdirs["dir1"]->files.count("fd1"));

        try
        {
            futil::unlinkat(cwd.fd,"dir1",AT_REMOVEDIR);
            TABORT("Expected exception trying to unlink non-empty directory!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT(e.errnov == ENOTEMPTY);
        }
        TASSERT(fs_root->files.count("fd0"));
        TASSERT(fs_root->subdirs.count("dir1"));
        TASSERT(fs_root->subdirs["dir1"]->files.count("fd1"));

        try
        {
            futil::unlinkat(cwd.fd,"fd0",AT_REMOVEDIR);
            TABORT("Expected exception trying to unlink a file while "
                   "specifying AT_REMOVEDIR.");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT(e.errnov == ENOTDIR);
        }
        TASSERT(fs_root->files.count("fd0"));
        TASSERT(fs_root->subdirs.count("dir1"));
        TASSERT(fs_root->subdirs["dir1"]->files.count("fd1"));

        try
        {
            futil::unlink(cwd,"fd1");
            TABORT("Expected exception trying to delete non-existent file!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT(e.errnov == ENOENT);
        }
        TASSERT(fs_root->files.count("fd0"));
        TASSERT(fs_root->subdirs.count("dir1"));
        TASSERT(fs_root->subdirs["dir1"]->files.count("fd1"));
        
        futil::unlink(cwd,"fd0");
        TASSERT(!fs_root->files.count("fd0"));
        TASSERT(fs_root->subdirs.count("dir1"));
        TASSERT(fs_root->subdirs["dir1"]->files.count("fd1"));

        futil::unlink(cwd,"dir1/fd1");
        TASSERT(fs_root->subdirs.count("dir1"));
        TASSERT(!fs_root->subdirs["dir1"]->files.count("fd1"));

        futil::unlinkat(cwd.fd,"dir1",AT_REMOVEDIR);
        TASSERT(!fs_root->subdirs.count("dir1"));

        TASSERT_EQUIV(live_files.size(),2UL);
        TASSERT_EQUIV(live_dirs.size(),1UL);

        fd1.close();
        fd0.close();

        TASSERT_EQUIV(live_files.size(),0UL);
        TASSERT_EQUIV(live_dirs.size(),1UL);
    }

    TMOCK_TEST(test_readdir_empty)
    {
        auto cwd = futil::directory(AT_FDCWD,"./");
        int at_fd = futil::openat(cwd,".",O_DIRECTORY);
        auto* dirp = futil::fdopendir(at_fd);

        auto* dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,".");
        TASSERT_EQUIV(dp->d_type,DT_DIR);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"..");
        TASSERT_EQUIV(dp->d_type,DT_DIR);
        dp = futil::readdir(dirp);
        TASSERT(dp == NULL);

        futil::closedir(dirp);
    }

    TMOCK_TEST(test_readdir)
    {
        auto cwd = futil::directory(AT_FDCWD,"./");
        futil::mkdir(cwd,"0009",0777);
        futil::file(cwd, "0010",O_CREAT | O_EXCL,0777);
        futil::mkdir(cwd,"0007",0777);
        futil::file(cwd, "0006",O_CREAT | O_EXCL,0777);
        futil::mkdir(cwd,"0003",0777);
        futil::file(cwd, "0004",O_CREAT | O_EXCL,0777);
        futil::mkdir(cwd,"0005",0777);
        futil::file(cwd, "0008",O_CREAT | O_EXCL,0777);
        futil::mkdir(cwd,"0001",0777);

        int at_fd = futil::openat(cwd,".",O_DIRECTORY);
        auto* dirp = futil::fdopendir(at_fd);

        // Test ordering.  We get files first, then directories.  They are in
        // sorted order according to std::less<std::string>.
        auto* dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"0004");
        TASSERT_EQUIV(dp->d_type,DT_REG);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"0006");
        TASSERT_EQUIV(dp->d_type,DT_REG);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"0008");
        TASSERT_EQUIV(dp->d_type,DT_REG);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"0010");
        TASSERT_EQUIV(dp->d_type,DT_REG);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,".");
        TASSERT_EQUIV(dp->d_type,DT_DIR);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"..");
        TASSERT_EQUIV(dp->d_type,DT_DIR);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"0001");
        TASSERT_EQUIV(dp->d_type,DT_DIR);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"0003");
        TASSERT_EQUIV(dp->d_type,DT_DIR);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"0005");
        TASSERT_EQUIV(dp->d_type,DT_DIR);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"0007");
        TASSERT_EQUIV(dp->d_type,DT_DIR);
        dp = futil::readdir(dirp);
        TASSERT(dp != NULL);
        TASSERT_EQUIV(dp->d_name,"0009");
        TASSERT_EQUIV(dp->d_type,DT_DIR);
        dp = futil::readdir(dirp);
        TASSERT(dp == NULL);

        futil::closedir(dirp);
    }

    TMOCK_TEST(test_lseek)
    {
        auto fd   = futil::openat(AT_FDCWD,"fd",O_CREAT | O_EXCL,0777);
        auto& fde = fd_table[fd];

        TASSERT_EQUIV(fde.pos,(off_t)0);
        futil::write(fd,"1234567890",10);
        TASSERT_EQUIV(fde.pos,(off_t)10);
        try
        {
            futil::lseek(fd,-1,SEEK_SET);
            TABORT("Expected invalid negative offset exception!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,EINVAL);
        }
        TASSERT_EQUIV(fde.pos,(off_t)10);
        futil::lseek(fd,0,SEEK_SET);
        TASSERT_EQUIV(fde.pos,(off_t)0);
        futil::lseek(fd,15,SEEK_SET);
        TASSERT_EQUIV(fde.pos,(off_t)15);

        futil::lseek(fd,8,SEEK_CUR);
        TASSERT_EQUIV(fde.pos,(off_t)23);
        futil::lseek(fd,-3,SEEK_CUR);
        TASSERT_EQUIV(fde.pos,(off_t)20);
        futil::lseek(fd,-20,SEEK_CUR);
        TASSERT_EQUIV(fde.pos,(off_t)0);
        try
        {
            futil::lseek(fd,-1,SEEK_CUR);
            TABORT("Expected invalid negative offset exception!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,EINVAL);
        }
        TASSERT_EQUIV(fde.pos,(off_t)0);
        futil::lseek(fd,8,SEEK_CUR);
        TASSERT_EQUIV(fde.pos,(off_t)8);

        futil::lseek(fd,0,SEEK_END);
        TASSERT_EQUIV(fde.pos,(off_t)10);
        futil::lseek(fd,10,SEEK_END);
        TASSERT_EQUIV(fde.pos,(off_t)20);
        futil::lseek(fd,-10,SEEK_END);
        TASSERT_EQUIV(fde.pos,(off_t)0);
        try
        {
            futil::lseek(fd,-11,SEEK_END);
            TABORT("Expected invalid negative offset exception!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,EINVAL);
        }
    }

    TMOCK_TEST(test_mkdirat_if_not_exists)
    {
        TASSERT_EQUIV(
            futil::mkdirat_if_not_exists(AT_FDCWD,"test_dir",0777),
            true);
        TASSERT(fs_root->subdirs.count("test_dir"));

        TASSERT_EQUIV(
            futil::mkdirat_if_not_exists(AT_FDCWD,"test_dir2",0777),
            true);
        TASSERT(fs_root->subdirs.count("test_dir2"));

        TASSERT_EQUIV(
            futil::mkdirat_if_not_exists(AT_FDCWD,"test_dir2/test_dir3",0777),
            true);
        TASSERT(fs_root->subdirs["test_dir2"]->subdirs.count("test_dir3"));

        TASSERT_EQUIV(
            futil::mkdirat_if_not_exists(AT_FDCWD,"test_dir",0777),
            false);
        TASSERT_EQUIV(
            futil::mkdirat_if_not_exists(AT_FDCWD,"test_dir2",0777),
            false);
        TASSERT_EQUIV(
            futil::mkdirat_if_not_exists(AT_FDCWD,"test_dir2/test_dir3",0777),
            false);

        futil::unlinkat(AT_FDCWD,"test_dir",AT_REMOVEDIR);
        TASSERT(!fs_root->subdirs.count("test_dir"));
        TASSERT_EQUIV(
            futil::mkdirat_if_not_exists(AT_FDCWD,"test_dir",0777),
            true);
        TASSERT(fs_root->subdirs.count("test_dir"));
    }

    TMOCK_TEST(test_renameat_file)
    {
        futil::mkdirat(AT_FDCWD,"dir1",0777);
        futil::mkdirat(AT_FDCWD,"dir1/dir2",0777);
        futil::mkdirat(AT_FDCWD,"dir3",0777);
        auto fd = futil::openat(AT_FDCWD,"dir1/dir2/fd",O_CREAT | O_EXCL,0777);
        auto fn = fd_table[fd].file;
        futil::write(fd,"1234567890",10);
        futil::close(fd);

        auto fromfd = futil::openat(AT_FDCWD,"dir1/dir2",O_DIRECTORY);
        try
        {
            futil::renameat(fromfd,"blah",AT_FDCWD,"dir3");
            TABORT("Expected exception renaming nonexistent file!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,ENOENT);
        }

        try
        {
            futil::renameat(fromfd,"blah",AT_FDCWD,"fd");
            TABORT("Expected exception renaming nonexistent file!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,ENOENT);
        }

        try
        {
            futil::renameat(fromfd,"fd",AT_FDCWD,"dir3");
            TABORT("Expected exception renaming file over directory!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,EISDIR);
        }

        auto tofd = futil::openat(AT_FDCWD,"dir3",O_DIRECTORY);
        futil::renameat(fromfd,"fd",tofd,"fd_renamed");
        TASSERT(!fs_root->subdirs["dir1"]->subdirs["dir2"]->
                    files.count("fd"));
        TASSERT(fs_root->subdirs["dir3"]->files["fd_renamed"] == fn);

        auto fd2 = futil::openat(AT_FDCWD,"dir1/fd2",O_CREAT | O_EXCL,0777);
        auto fn2 = fd_table[fd2].file;
        TASSERT(live_files.count(fn));
        TASSERT(live_files.count(fn2));
        futil::write(fd2,"abcdefg",7);

        futil::close(fromfd);
        fromfd = tofd;
        tofd = futil::openat(AT_FDCWD,"dir1",O_DIRECTORY);
        TASSERT(fs_root->subdirs["dir1"]->files["fd2"] == fn2);
        futil::renameat(fromfd,"fd_renamed",tofd,"fd2");
        TASSERT(fs_root->subdirs["dir1"]->files["fd2"] == fn);

        TASSERT(live_files.count(fn2));
        futil::close(fd2);
        TASSERT(!live_files.count(fn2));

        char buf[11] = {};
        fd = futil::openat(AT_FDCWD,"dir1/fd2",O_RDONLY);
        TASSERT_EQUIV(futil::read(fd,buf,10),(ssize_t)10);
        TASSERT_EQUIV(buf,"1234567890");
    }

    TMOCK_TEST(test_renameat_dir)
    {
        // Hierarchy:
        //  /
        //  dir1/
        //  dir1/fd2
        //  dir1/dir2/
        //  dir1/dir2/fd1
        //  dir3/
        futil::mkdirat(AT_FDCWD,"dir1",0777);
        futil::mkdirat(AT_FDCWD,"dir1/dir2",0777);
        futil::mkdirat(AT_FDCWD,"dir3",0777);
        futil::close(futil::openat(AT_FDCWD,"dir1/dir2/fd1",O_CREAT | O_EXCL,
                                   0777));
        futil::close(futil::openat(AT_FDCWD,"dir1/fd2",O_CREAT | O_EXCL,0777));
        TASSERT_EQUIV(live_dirs.size(),4UL);

        auto fromfd = futil::openat(AT_FDCWD,"dir1/",O_DIRECTORY);
        try
        {
            futil::renameat(fromfd,"blah",AT_FDCWD,"dir1/dir2/dirX");
            TABORT("Expected exception trying to rename nonexistent "
                   "directory.");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,ENOENT);
        }

        try
        {
            futil::renameat(fromfd,"blah",AT_FDCWD,"dir1/dir2");
            TABORT("Expected exception trying to rename nonexistent "
                   "directory.");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,ENOENT);
        }

        try
        {
            futil::renameat(fromfd,"dir2",AT_FDCWD,"dir1/dir2/dirX");
            TABORT("Expected exception trying to rename into a child.");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,EINVAL);
        }

        // Rename: dir1/dir2 -> dir3/dirX
        auto dn1 = fs_root->subdirs["dir1"];
        auto dn2 = dn1->subdirs["dir2"];
        auto dn3 = fs_root->subdirs["dir3"];
        futil::renameat(fromfd,"dir2",AT_FDCWD,"dir3/dirX");

        // Hierarchy:
        //  /
        //  dir1/
        //  dir1/fd2
        //  dir3/dirX/
        //  dir3/dirX/fd1
        TASSERT(!fs_root->subdirs["dir1"]->subdirs.count("dir2"));
        TASSERT(fs_root->subdirs["dir3"]->subdirs["dirX"] == dn2);
        TASSERT(live_dirs.count(dn1));
        TASSERT(live_dirs.count(dn2));
        TASSERT(live_dirs.count(dn3));
        futil::close(fromfd);
        TASSERT_EQUIV(live_dirs.size(),4UL);

        // Try to rename: dir3/dirX -> dir1
        // Should fail because dir1 is not empty.
        fromfd = futil::openat(AT_FDCWD,"dir3",O_DIRECTORY);
        TASSERT(fs_root->subdirs["dir3"]->subdirs.count("dirX"));
        try
        {
            futil::renameat(fromfd,"dirX",AT_FDCWD,"dir1");
            TABORT("Expected directory-not-empty exception!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,ENOTEMPTY);
        }

        // Delete fd2 and try again.
        futil::unlinkat(AT_FDCWD,"dir1/fd2",0);
        futil::renameat(fromfd,"dirX",AT_FDCWD,"dir1");

        // Hierarchy:
        //  /
        //  dir1/
        //  dir1/fd1
        //  dir3/
        TASSERT_EQUIV(live_dirs.size(),3UL);
        TASSERT(fs_root->subdirs.count("dir1"));
        TASSERT(fs_root->subdirs.count("dir3"));
        TASSERT(!fs_root->subdirs["dir3"]->subdirs.count("dirX"));
        TASSERT(fs_root->subdirs["dir1"] == dn2);
        TASSERT(fs_root->subdirs["dir3"] == dn3);
        TASSERT(!live_dirs.count(dn1));
        TASSERT(live_dirs.count(dn2));
        TASSERT(live_dirs.count(dn3));

        // Rename: dir1 -> dir3
        // We still have dir3 open in fromfd.
        futil::renameat(AT_FDCWD,"dir1",AT_FDCWD,"dir3");

        // Hierarchy:
        //  /
        //  dir3/
        //  dir3/fd1
        //  [dir3/]
        TASSERT_EQUIV(live_dirs.size(),3UL);
        TASSERT(!fs_root->subdirs.count("dir1"));
        TASSERT(fs_root->subdirs.count("dir3"));
        TASSERT(fs_root->subdirs["dir3"] == dn2);
        TASSERT(!live_dirs.count(dn1));
        TASSERT(live_dirs.count(dn2));
        TASSERT(live_dirs.count(dn3));

        futil::close(fromfd);

        // Hierarchy:
        //  /
        //  dir3/
        //  dir3/fd1
        TASSERT_EQUIV(live_dirs.size(),2UL);
        TASSERT(!live_dirs.count(dn1));
        TASSERT(live_dirs.count(dn2));
        TASSERT(!live_dirs.count(dn3));

        TASSERT_EQUIV(fs_root->refcount,2UL);
        TASSERT_EQUIV(fs_root->subdirs["dir3"]->refcount,2UL);
    }

    TMOCK_TEST(test_renameat_if_not_exists)
    {
        // Hierarchy:
        //  /
        //  dir1/
        //  dir1/fd2
        //  dir1/dir2/
        //  dir1/dir2/fd1
        //  dir3/
        futil::mkdirat(AT_FDCWD,"dir1",0777);
        futil::mkdirat(AT_FDCWD,"dir1/dir2",0777);
        futil::mkdirat(AT_FDCWD,"dir3",0777);
        futil::close(futil::openat(AT_FDCWD,"dir1/dir2/fd1",O_CREAT | O_EXCL,
                                   0777));
        futil::close(futil::openat(AT_FDCWD,"dir1/fd2",O_CREAT | O_EXCL,0777));
        TASSERT_EQUIV(live_dirs.size(),4UL);
        TASSERT_EQUIV(live_files.size(),2UL);

        TASSERT_EQUIV(
            futil::renameat_if_not_exists(AT_FDCWD,"dir1",AT_FDCWD,"dir1"),
            false);
        TASSERT_EQUIV(live_dirs.size(),4UL);

        TASSERT_EQUIV(
            futil::renameat_if_not_exists(AT_FDCWD,"dir1",AT_FDCWD,"dir3"),
            false);
        TASSERT_EQUIV(live_dirs.size(),4UL);

        TASSERT_EQUIV(
            futil::renameat_if_not_exists(AT_FDCWD,"dir1",AT_FDCWD,"dir4"),
            true);
        TASSERT_EQUIV(live_dirs.size(),4UL);
        TASSERT(!fs_root->subdirs.count("dir1"));
        TASSERT(fs_root->subdirs.count("dir4"));

        // Hierarchy:
        //  /
        //  dir3/
        //  dir4/
        //  dir4/fd2
        //  dir4/dir2/
        //  dir4/dir2/fd1
        TASSERT_EQUIV(
            futil::renameat_if_not_exists(AT_FDCWD,"dir4/fd2",
                                          AT_FDCWD,"dir4/fd2"),
            false);
        TASSERT_EQUIV(live_files.size(),2UL);

        TASSERT_EQUIV(
            futil::renameat_if_not_exists(AT_FDCWD,"dir4/fd2",
                                          AT_FDCWD,"dir4/dir2/fd1"),
            false);
        TASSERT_EQUIV(live_files.size(),2UL);

        TASSERT_EQUIV(
            futil::renameat_if_not_exists(AT_FDCWD,"dir4/fd2",
                                          AT_FDCWD,"dir4/dir2/fdX"),
            true);
        TASSERT_EQUIV(live_files.size(),2UL);
        TASSERT(!fs_root->subdirs["dir4"]->files.count("fd2"));
        TASSERT(fs_root->subdirs["dir4"]->subdirs["dir2"]->files.
                    count("fd1"));
        TASSERT(fs_root->subdirs["dir4"]->subdirs["dir2"]->files.
                    count("fdX"));
    }
};

TMOCK_MAIN();
