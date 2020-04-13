#!/bin/sh
#

set -e

if [ -n "$DEBUG" ]; then
    set -x
fi

tests=$(grep -w 'all:' Makefile)
for test in $(echo "$tests")
do
    if [ "$test" = "all:" ]; then
        continue
    fi

    echo ""
    echo "=== run $test ==="

    ./${test}
done