#!/usr/bin/env bash
set -e

if [ "$(id -u)" -ne 0 ]; then
    echo 'This script must be run as root'
    exit 1
fi

echo 'Downloading ampm...'
mkdir -p /opt/ampm
cd /opt/ampm
curl --progress-bar https://github.com/Wazzaps/ampm/releases/latest/download/ampm.tar.gz | tar xz

echo ''
echo 'Adding ampm launcher to /usr/local/bin/...'
install /opt/ampm/ampm.sh /usr/local/bin/ampm

echo ''
echo 'Done! Try running `ampm --help` to see what you can do with ampm.'
