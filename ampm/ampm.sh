#!/usr/bin/env bash

# This is the fastpath launcher for ampm
# It handles only `get` and `env` commands, with exact TYPE:HASH queries

if [ "$#" -eq 2 ] && [ "$1" = "get" ] && echo "$2" | grep -P '^.*:[a-z0-9]{32}$' > /dev/null; then
  readlink "$(echo "$2" | sed -E 's|^(.*):([a-z0-9]{32})$|/var/ampm/metadata/\1/\2.target|')" 2>/dev/null || "/opt/ampm/ampm" "$@"
elif [ "$#" -eq 2 ] && [ "$1" = "env" ] && echo "$2" | grep -P '^.*:[a-z0-9]{32}$' > /dev/null; then
  cat "$(echo "$2" | sed -E 's|^(.*):([a-z0-9]{32})$|/var/ampm/metadata/\1/\2.env|')" 2>/dev/null || "/opt/ampm/ampm" "$@"
else
  "/opt/ampm/ampm" "$@"
fi