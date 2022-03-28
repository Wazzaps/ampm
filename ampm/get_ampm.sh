#!/usr/bin/env bash
set -e

REMOTE_REPO="nfs://127.0.0.1/mnt/myshareddir#ampm_repo"

if [ "$(id -u)" -ne 0 ]; then
    echo 'This script must be run as root'
    exit 1
fi

echo 'Downloading ampm...'
mkdir -p /opt/ampm.tmp
cd /opt/ampm.tmp
curl --progress-bar -fsSL https://github.com/Wazzaps/ampm/releases/latest/download/ampm.tar.gz | tar xz

echo ''
echo "Configuring remote repo to be '$REMOTE_REPO'..."
echo "$REMOTE_REPO" > /opt/ampm.tmp/repo_uri

echo ''
echo 'Adding ampm launcher to /usr/local/bin/...'
install /opt/ampm.tmp/ampm.sh /usr/local/bin/ampm

# Commit changes
rm -rf /opt/ampm
mv /opt/ampm.tmp /etc/ampm

echo ''
echo 'Done! Try running `ampm --help` to see what you can do with ampm.'
echo 'To uninstall, run `sudo /opt/ampm/uninstall.sh`.'
