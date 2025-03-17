#!/bin/bash
# Copyright (c) 2018-2019 Terry Greeniaus.
# All rights reserved.

# Helper script to iterate through all "edit" commits in
# an interactive rebase and build them testing for build
# and unittest successes.  It will stop on the first commit
# that fails or go until the rebase is complete.
make clean
while true; do
    make -j || break

    # Add a sleep between building and checking out the next
    # set of source files.  It seems that file modification
    # dates are stored in seconds; so a build finishing and
    # outputting a binary file immediately followed by a git
    # checkout of new source files could result in the new
    # source file having the same timestamp as the old
    # binary, tricking 'make' into thinking everything is up-
    # to-date.
    sleep 1.1

    git rebase --continue || break
done
