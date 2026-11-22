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

    TMOCK_TEST(test_case_sensitive_names)
    {
        // By default, names that differ only in case are different entries,
        // as on Linux.
        auto cwd = futil::directory(AT_FDCWD,".");
        futil::mkdir(cwd,"Foo",0777);
        futil::mkdir(cwd,"foo",0777);
        auto foo1 = futil::directory(cwd,"Foo");
        auto foo2 = futil::directory(cwd,"foo");
        TASSERT(fd_table[foo1.fd].directory != fd_table[foo2.fd].directory);
        TASSERT_EQUIV(cwd.listdirs().size(),2UL);

        try
        {
            futil::directory(cwd,"FOO");
            TABORT("Expected ENOENT opening FOO!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,ENOENT);
        }
    }

    TMOCK_TEST(test_case_insensitive_names)
    {
        // In case-insensitive mode, any spelling finds the entry, which keeps
        // the name it was created with, and creating another spelling fails.
        fakefs_case_insensitive = true;
        auto cwd = futil::directory(AT_FDCWD,".");
        futil::mkdir(cwd,"Foo",0777);
        futil::mkdir(cwd,"tmp",0777);
        futil::close(futil::openat(AT_FDCWD,"Bar",O_CREAT | O_EXCL,0777));

        dir_node* foo_dn = fs_root->subdirs.find("Foo")->second;
        for (const char* name : {"Foo","foo","FOO","fOo"})
        {
            auto d = futil::directory(cwd,name);
            TASSERT(fd_table[d.fd].directory == foo_dn);
        }
        TASSERT_EQUIV(foo_dn->name,"Foo");
        TASSERT_EQUIV(fs_root->subdirs.find("foo")->first,"Foo");
        futil::close(futil::openat(AT_FDCWD,"bar",O_RDONLY));

        try
        {
            futil::mkdir(cwd,"foo",0777);
            TABORT("Expected EEXIST creating foo!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,EEXIST);
        }
        TASSERT(!futil::mkdirat_if_not_exists(AT_FDCWD,"FOO",0777));
        try
        {
            futil::openat(AT_FDCWD,"BAR",O_CREAT | O_EXCL,0777);
            TABORT("Expected EEXIST creating BAR!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,EEXIST);
        }
        TASSERT(!futil::renameat_if_not_exists(AT_FDCWD,"tmp",AT_FDCWD,
                                               "fOO"));

        auto dirs = cwd.listdirs();
        std::sort(dirs.begin(),dirs.end());
        TASSERT_EQUIV(dirs.size(),2UL);
        TASSERT_EQUIV(dirs[0],"Foo");
        TASSERT_EQUIV(dirs[1],"tmp");
    }

    TMOCK_TEST(test_fd_open_was_exact)
    {
        // On a case-sensitive file system, opening a name can only find the
        // entry with exactly that name.
        auto cwd = futil::directory(AT_FDCWD,".");
        futil::mkdir(cwd,"Foo",0777);
        auto foo = futil::directory(cwd,"Foo");
        auto bar = futil::file(cwd,"Bar",O_CREAT | O_EXCL | O_RDWR,0777);
        TASSERT(futil::fd_open_was_exact(foo.fd,"Foo"));
        TASSERT(futil::fd_open_was_exact(bar.fd,"Bar"));
    }

    TMOCK_TEST(test_fd_open_was_exact_case_insensitive)
    {
        // On a case-insensitive file system, opening another spelling finds
        // the entry, and fd_open_was_exact() says that open wasn't exact.
        fakefs_case_insensitive = true;
        auto cwd = futil::directory(AT_FDCWD,".");
        futil::mkdir(cwd,"Foo",0777);
        futil::close(futil::openat(AT_FDCWD,"Bar",O_CREAT | O_EXCL,0777));
        auto foo = futil::directory(cwd,"Foo");
        auto bar = futil::file(cwd,"Bar",O_RDONLY);
        auto foo2 = futil::directory(cwd,"foo");
        auto bar2 = futil::file(cwd,"BAR",O_RDONLY);
        TASSERT(futil::fd_open_was_exact(foo.fd,"Foo"));
        TASSERT(futil::fd_open_was_exact(bar.fd,"Bar"));
        TASSERT(!futil::fd_open_was_exact(foo2.fd,"foo"));
        TASSERT(!futil::fd_open_was_exact(bar2.fd,"BAR"));
    }

    TMOCK_TEST(test_open_exact_directory)
    {
        // open_exact_directory() opens a subdirectory only by its exact name,
        // even when a case-insensitive file system matches another spelling.
        fakefs_case_insensitive = true;
        auto cwd = futil::directory(AT_FDCWD,".");
        futil::mkdir(cwd,"Foo",0777);
        dir_node* foo_dn = fs_root->subdirs.find("Foo")->second;
        auto foo = futil::open_exact_directory(cwd,"Foo");
        TASSERT(fd_table[foo.fd].directory == foo_dn);

        for (const char* name : {"foo","FOO","bar"})
        {
            try
            {
                futil::open_exact_directory(cwd,name);
                TABORT("Expected ENOENT!");
            }
            catch (const futil::errno_exception& e)
            {
                TASSERT_EQUIV(e.errnov,ENOENT);
            }
        }
    }

    TMOCK_TEST(test_dir_opens_as_other_case)
    {
        // dir_opens_as_other_case() is true only when the name opens a
        // directory whose real name is spelled differently.
        fakefs_case_insensitive = true;
        auto cwd = futil::directory(AT_FDCWD,".");
        futil::mkdir(cwd,"Foo",0777);
        futil::close(futil::openat(AT_FDCWD,"Baz",O_CREAT | O_EXCL,0777));
        TASSERT(futil::dir_opens_as_other_case(cwd,"foo"));
        TASSERT(futil::dir_opens_as_other_case(cwd,"FOO"));
        TASSERT(!futil::dir_opens_as_other_case(cwd,"Foo"));
        TASSERT(!futil::dir_opens_as_other_case(cwd,"bar"));
        TASSERT(!futil::dir_opens_as_other_case(cwd,"baz"));
    }

    TMOCK_TEST(test_exact_case_helpers_case_sensitive)
    {
        // On a case-sensitive file system, other spellings simply don't
        // exist.
        auto cwd = futil::directory(AT_FDCWD,".");
        futil::mkdir(cwd,"Foo",0777);
        futil::open_exact_directory(cwd,"Foo");
        try
        {
            futil::open_exact_directory(cwd,"foo");
            TABORT("Expected ENOENT!");
        }
        catch (const futil::errno_exception& e)
        {
            TASSERT_EQUIV(e.errnov,ENOENT);
        }
        TASSERT(!futil::dir_opens_as_other_case(cwd,"foo"));
        TASSERT(!futil::dir_opens_as_other_case(cwd,"Foo"));
    }
};

TMOCK_MAIN();
