You will need to check out all of the submodules:

git submodule update --init --recursive

Then, build them all.

zlib-ng
=======
./configure
make
make test

editline
========
This requires autoconf, automake and libtool, which you can install with brew.
./autogen.sh
./configure
make
