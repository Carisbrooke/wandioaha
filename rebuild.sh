#!/bin/bash

make clean
make distclean
./configure
make -j7 && sudo make install
sudo ldconfig
