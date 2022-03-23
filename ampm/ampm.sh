#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
if [ "$#" -eq 2 ] && [ "$1" = "get" ] && echo "$2" | grep -P '^.*:[a-z0-9]{32}$' > /dev/null; then
  readlink "$(echo "$2" | sed -E 's|^(.*):([a-z0-9]{32})$|/var/ampm/metadata/\1/\2.target|')" 2>/dev/null || "$SCRIPT_DIR/ampm_py" "$@"
elif [ "$#" -eq 2 ] && [ "$1" = "env" ] && echo "$2" | grep -P '^.*:[a-z0-9]{32}$' > /dev/null; then
  cat "$(echo "$2" | sed -E 's|^(.*):([a-z0-9]{32})$|/var/ampm/metadata/\1/\2.env|')" 2>/dev/null || "$SCRIPT_DIR/ampm_py" "$@"
else
  "$SCRIPT_DIR/ampm_py" "$@"
fi