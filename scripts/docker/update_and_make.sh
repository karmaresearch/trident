#!/bin/sh
# Make sure kognac is updated
/app/kognac/scripts/docker/update_and_make.sh
# Update trident
cd /app/trident
git pull
cd build
cmake ..
make
cd ../build_debug
cmake ..
make
cd ..
