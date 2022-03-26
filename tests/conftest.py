import random
import shutil
import subprocess
import time

import pytest
from pathlib import Path
from ampm.repo.local import LOCAL_REPO
from ampm.repo.nfs import NfsRepo


@pytest.fixture()
def nfs_repo_path(nfs_repo) -> Path:
    return Path(nfs_repo.remote_path)


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
    random_id = random.randbytes(8).hex()
    nfs_root = nfs_server["root"] / random_id
    nfs_root.mkdir()
    return NfsRepo.from_uri_part(f'{nfs_server["host"]}{nfs_root}')

