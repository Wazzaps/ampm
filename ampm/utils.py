import random
import threading
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
    with local_path.open('rb') as fd:
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
    def handler():
        path.unlink(missing_ok=True)
    atexit.register(handler)


class LockFile:
    def __init__(self, path: Path, description: str):
        self.path = path
        self.description = description
        self.lockfile: Optional[TextIOWrapper] = None

        self._updater_thread_stop = threading.Event()
        self._updater_thread = None

    def take(self):
        waited_for = 0.0
        while True:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self.lockfile = self.path.open('x')
                self.refresh()
                break
            except FileExistsError:
                strikes = 0
                last_locktime = ''
                while True:
                    try:
                        new_locktime = self.path.read_text() or '0'
                        if new_locktime != last_locktime:
                            last_locktime = new_locktime
                            strikes = 0
                        else:
                            strikes += 1
                            # After about 10 seconds, we assume the process that locked the file died abruptly
                            if strikes > 20:
                                print(f'INFO: ampm that locked the download of {self.description} seems to be dead, force unlocking', file=sys.stderr)
                                self.path.unlink(missing_ok=True)
                                break

                        print(f'INFO: [{waited_for:0.1f}s] Waiting for lockfile on {self.description}', file=sys.stderr)
                        wait_for = 0.5 + random.random() / 4
                        time.sleep(wait_for)
                        waited_for += wait_for
                    except FileNotFoundError:
                        break  # lockfile was deleted, we can take it

    def refresh(self):
        assert self.lockfile, 'Lockfile not taken'
        self.lockfile.seek(0)
        self.lockfile.truncate(0)
        self.lockfile.write(f'{time.time():0.2f}')
        self.lockfile.flush()

    def take_and_spawn_refresher(self):
        self.take()

        def lockfile_updater():
            while True:
                self.refresh()
                if self._updater_thread_stop.wait(1):
                    break

        self._updater_thread_stop.clear()
        self._updater_thread = threading.Thread(target=lockfile_updater, daemon=True)
        self._updater_thread.start()

    def release_and_kill_refresher(self):
        self._updater_thread_stop.set()
        self._updater_thread.join()

        self.lockfile.close()
        self.lockfile = None
        self.path.unlink()

    def __enter__(self):
        self.take_and_spawn_refresher()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_and_kill_refresher()
