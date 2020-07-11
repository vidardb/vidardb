#!/usr/bin/env bash
#
# Install both static and shared library by cmake
#

set -e

if ! command -v cmake > /dev/null; then
    echo "error: cmake not found ..."
    exit 1
fi

temp_dir=build

_cleanup() {
    rm -rf $temp_dir util/build_version.cc
}

_install() {
    mkdir -p $temp_dir
    cd $temp_dir
    cmake ..
    make
    make install
}

trap _cleanup exit

# install library
_cleanup
_install
