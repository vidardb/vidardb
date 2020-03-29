#!/usr/bin/env bash
# Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

set -Eeo pipefail

CURDIR=$(cd "$(dirname "$0")"; pwd);
MYNAME="${0##*/}"

MADPACK=/usr/local/madlib/bin/madpack
PGHOST="127.0.0.1"
PGPORT="5432"
PGUSER="postgres"
PGPASSWORD=""
PGDATABASE="postgres"

_usage() {
    cat << USAGE
Usage: ${MYNAME} [OPTION]
Options:
    -h|--help               print usage

    -U|--username           database user name (default: "postgres")
    -P|--password           database user password (default: "")
    -D|--database           database name (default: "postgres")
USAGE
    exit 1
}

_parse_args() {
    while [[ $# -gt 0 ]]
    do
        key="$1"

        case $key in
        -U|--username)
            PGUSER=$2
            shift
            ;;
        -P|--password)
            PGPASSWORD=$2
            shift
            ;;
        -D|--database)
            PGDATABASE=$2
            shift
            ;;
        -h|--help)
            _usage
            ;;
        *)
            echo "error: unknown option [$key]"
            echo
            _usage
            ;;
        esac
        shift
    done
}

_install_madlib() {
    local conn="$PGUSER@$PGHOST:$PGPORT/$PGDATABASE"
    if [ -n "$PGPASSWORD" ]; then
        conn="$PGUSER/$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE"
    fi

    echo "****************************************************"
    $MADPACK -c $conn -p postgres install
    $MADPACK -c $conn -p postgres install-check
    echo "****************************************************"
}

main() {
    _parse_args "$@"
    _install_madlib
}

main "$@"