#!/usr/bin/env bash

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