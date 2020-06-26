#!/bin/sh
if [ "$#" = "0" ]; then
  echo "Usage: $0 major|minor|patch|full"
  exit 1
fi

if [ "$1" = "major" ]; then
  cat include/vidardb/version.h  | grep MAJOR | head -n1 | awk '{print $3}'
fi
if [ "$1" = "minor" ]; then
  cat include/vidardb/version.h  | grep MINOR | head -n1 | awk '{print $3}'
fi
if [ "$1" = "patch" ]; then
  cat include/vidardb/version.h  | grep PATCH | head -n1 | awk '{print $3}'
fi
if [ "$1" = "full" ]; then
  awk '/#define VIDARDB/ { env[$2] = $3 }
       END { printf "%s.%s.%s\n", env["VIDARDB_MAJOR"],
                                  env["VIDARDB_MINOR"],
                                  env["VIDARDB_PATCH"] }'  \
      include/vidardb/version.h
fi
