#!/usr/bin/env bash
mkdir -p $1
git log -n 1 --format=format:"#define GIT_COMMIT \"%H\"%n" HEAD > $1/tmp_version.h
git log -n 1 --format=format:"#define GIT_COMMIT16 u\"%H\"%n" HEAD >> $1/tmp_version.h

if test -n "$(git status --porcelain)"; then
        echo '#define GIT_DIRTY 1' >> $1/tmp_version.h
        echo '#define GIT_VERSION GIT_COMMIT ".dirty"' >> $1/tmp_version.h
        echo '#define GIT_VERSION16 GIT_COMMIT16 u".dirty"' >> $1/tmp_version.h
else
        echo '#define GIT_DIRTY 0' >> $1/tmp_version.h
        echo '#define GIT_VERSION GIT_COMMIT' >> $1/tmp_version.h
        echo '#define GIT_VERSION16 GIT_COMMIT16' >> $1/tmp_version.h
fi
if ( ! diff $1/tmp_version.h $1/version.h >/dev/null 2>/dev/null ); then
        mv $1/tmp_version.h $1/version.h
fi
