import hashlib
import os
from pathlib import Path


def _calc_dir_size(path: Path) -> int:
    """
    Calculate the size of a directory.

    :param path: Path to the directory.
    :return: Sum of all file sizes inside the directory.
    """
    total_size = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def _hash_local_file(local_path: Path) -> str:
    hasher = hashlib.sha256(b'')
    fd = local_path.open('rb')

    while True:
        chunk = fd.read(1024 * 1024)
        if len(chunk) == 0:
            break
        hasher.update(chunk)

    return hasher.hexdigest()