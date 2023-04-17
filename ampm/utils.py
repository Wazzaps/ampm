import atexit
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
            try:
                total_size += os.path.getsize(fp)
            except FileNotFoundError:
                pass
    return total_size


def hash_local_file(local_path: Path) -> str:
    hasher = hashlib.sha256(b'')
    fd = local_path.open('rb')

    while True:
        chunk = fd.read(1024 * 1024)
        if len(chunk) == 0:
            break
        hasher.update(chunk)

    return hasher.hexdigest()


def randbytes(length: int) -> bytes:
    """
    Generate a random bytes object.

    :param length: Length of the random bytes object.
    :return: Random bytes object.
    """
    import random
    return bytes(random.getrandbits(8) for _ in range(length))


def remove_atexit(path: Path):
    atexit.register(lambda f: f.unlink(missing_ok=True), path)
