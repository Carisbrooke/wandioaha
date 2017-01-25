#!/bin/bash

make clean
make distclean
aclocal
automake
./configure
make -j7 && sudo make install
sudo ldconfig
