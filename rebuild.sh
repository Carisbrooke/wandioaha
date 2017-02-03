#!/bin/bash

make clean
make distclean
aclocal
automake --add-missing
./configure
make -j7 && sudo make install
sudo ldconfig
