#!/usr/bin/env bash
#
# Install both static and shared library by cmake
#

set -e

if ! command -v cmake > /dev/null; then
    echo "error: cmake not found ..."
    exit 1
fi

curr_dir=$(cd "$(dirname "$0")"; pwd)
temp_dir=build

_cleanup() {
    rm -rf $curr_dir/$temp_dir $curr_dir/util/build_version.cc
}

_install() {
    echo "CMake Flags: $CMAKE_FLAGS"
    mkdir -p $temp_dir
    cd $temp_dir
    cmake $CMAKE_FLAGS ..
    make -j2
    make install
}

trap _cleanup exit

# install library
_cleanup
_install
