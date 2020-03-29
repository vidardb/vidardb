#!/usr/bin/env bash
# Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

_health_check() {
    PSQL="psql -U ${POSTGRES_USER:-postgres}"
    if [ -n "${POSTGRES_PASSWORD}" ]; then
        PSQL="PGPASSWORD=${POSTGRES_PASSWORD} $PSQL"
    fi

    local result=$(echo $($PSQL -q -t -c "select 1;"))
    if [ "$result" = "1" ]; then
        exit 0
    else
        exit 1
    fi
}

_health_check