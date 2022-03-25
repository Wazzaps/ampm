#!/usr/bin/env bash

set -e

cd "$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

rm -rf ./dist ./build
docker run --rm -it --env=PYTHONPATH='/code/vendor' --env=GITHUB_WORKSPACE='/code' -v "$PWD":/code wazzaps/pyinstaller-manylinux-py38 --clean --noconfirm --name=ampm --add-binary /usr/local/lib/libcrypt.so.2:. /code/ampm/cli.py
strip ./dist/ampm/*so* ./dist/ampm/ampm ./dist/ampm/lib-dynload/*
cp ./ampm/ampm.sh ./dist/ampm/ampm.sh
cp ./ampm/get_ampm.sh ./dist/get_ampm.sh
cd ./dist/ampm && tar -czvf ../ampm.tar.gz .
