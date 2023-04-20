import atexit
import hashlib
import os
import sys
import time
from io import TextIOWrapper
from pathlib import Path
from typing import Optional


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


class LockFile:
    def __init__(self, path: Path, description: str):
        self.path = path
        self.description = description
        self.lockfile: Optional[TextIOWrapper] = None

    def take(self):
        waited_for = 0
        while True:
            try:
                self.lockfile = self.path.open('x')
                self.refresh()
                break
            except FileExistsError:
                last_locktime = 0
                while True:
                    try:
                        new_locktime = float(self.path.read_text() or '0')
                        if new_locktime != last_locktime:
                            last_locktime = new_locktime
                            print(f'INFO: [{waited_for}s] Waiting for lockfile on {self.description}', file=sys.stderr)
                            time.sleep(2)
                            waited_for += 2
                        else:
                            print(f'INFO: ampm that locked the download of {self.description} seems to be dead, force unlocking', file=sys.stderr)
                            self.path.unlink(missing_ok=True)
                            break
                    except FileNotFoundError:
                        break  # lockfile was deleted, we can take it

    def refresh(self):
        assert self.lockfile, 'Lockfile not taken'
        self.lockfile.seek(0)
        self.lockfile.truncate(0)
        self.lockfile.write(f'{time.time():0.2f}')
        self.lockfile.flush()

    def __enter__(self):
        self.take()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lockfile.close()
        self.lockfile = None
        self.path.unlink()
