#!/usr/bin/env bash

set -ex

cd "$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

rm -rf ./dist ./build
# 1. Wrap each {{ }} in <ignoreme> </ignoreme> to prevent minify from removing them
# 2. Minify
# 3. Remove <ignoreme> </ignoreme>
sed -E 's:(\{\{[^}]+ [^}]+}}):<ignoreme>\1</ignoreme>:g' < ./ampm/index_ui.html | \
  minify --type html | \
  sed -E 's:<ignoreme>([^<]+)</ignoreme>:\1:g' > ./ampm/index_ui.min.html
docker run --rm --env=PYTHONPATH='/code/vendor' --env=GITHUB_WORKSPACE='/code' -v "$PWD":/code wazzaps/pyinstaller-manylinux-py38 --clean --noconfirm --name=ampm --add-binary /usr/local/lib/libcrypt.so.2:. --add-data /code/ampm/index_ui.html:. --add-data /code/ampm/index_ui.min.html:. /code/ampm/cli.py
strip ./dist/ampm/*so* ./dist/ampm/ampm ./dist/ampm/lib-dynload/*
cp ./ampm/ampm.sh ./dist/ampm/ampm.sh
cp ./ampm/uninstall.sh ./dist/ampm/uninstall.sh
cp ./ampm/get_ampm.sh ./dist/get_ampm.sh
cd ./dist/ampm && tar -czvf ../ampm.tar.gz .
