import shutil
import subprocess
import time

import pytest
from pathlib import Path
from ampm.repo.local import LOCAL_REPO
from ampm.repo.nfs import NfsRepo
from ampm.utils import randbytes

BIG_FILE_SIZE_MIB = 100


@pytest.fixture()
def nfs_repo_path(nfs_repo) -> Path:
    return Path(nfs_repo.mount_path) / nfs_repo.repo_path


@pytest.fixture()
def nfs_mount_path(nfs_repo) -> Path:
    return Path(nfs_repo.mount_path)


@pytest.fixture()
def nfs_repo_uri(nfs_repo) -> str:
    return nfs_repo.into_uri()


@pytest.fixture()
def local_repo_path() -> Path:
    return LOCAL_REPO.path


@pytest.fixture()
def clean_repos(local_repo_path, nfs_repo_path):
    shutil.rmtree(local_repo_path / 'metadata', ignore_errors=True)
    shutil.rmtree(local_repo_path / 'artifacts', ignore_errors=True)


@pytest.fixture(scope='session')
def big_file(tmp_path_factory: "TempPathFactory"):
    tmp_path: Path = tmp_path_factory.mktemp('big_file_')
    tmp_path.mkdir(parents=True, exist_ok=True)
    tmpfile_path = tmp_path / 'tmpfile'
    if tmpfile_path.exists():
        if tmpfile_path.stat().st_size == BIG_FILE_SIZE_MIB * 1024 * 1024:
            # Already exists, reuse it
            yield tmpfile_path
            return
        else:
            # Wrong size, recreate
            tmpfile_path.unlink()

    with Path('/dev/urandom').open('rb') as rng:
        with tmpfile_path.open('wb') as fd:
            for _ in range(BIG_FILE_SIZE_MIB):
                fd.write(rng.read(1024*1024))  # 1 MiB per chunk

    yield tmpfile_path


@pytest.fixture(scope='session')
def nfs_server(tmp_path_factory: "TempPathFactory"):
    print("Starting NFS server")
    tmp_path = tmp_path_factory.mktemp('nfs_tests_')

    nfs_root = tmp_path / f'nfs_server'
    nfs_root.mkdir(parents=True)

    exports = tmp_path / f'exports'
    exports.write_text(f'{str(nfs_root)} 127.0.0.1(rw,insecure)')

    server = subprocess.Popen([
        Path(__file__).parent / 'unfsd',
        '-d',  # Dont daemonize
        '-s',  # Single-user mode
        '-e', str(exports)  # Export file
    ])
    time.sleep(0.5)

    try:
        yield {'root': nfs_root, 'host': '127.0.0.1'}
    finally:
        server.kill()


@pytest.fixture()
def nfs_repo(nfs_server) -> NfsRepo:
    random_id = randbytes(8).hex()
    nfs_root = nfs_server["root"] / random_id
    nfs_root.mkdir()
    return NfsRepo.from_uri_part(f'{nfs_server["host"]}{nfs_root}#repo')

