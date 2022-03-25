import shutil
import pytest
from pathlib import Path
from ampm.repo.base import REMOTE_REPO_URI, ArtifactRepo
from ampm.repo.local import LOCAL_REPO
from ampm.repo.nfs import NfsRepo


@pytest.fixture()
def nfs_repo() -> NfsRepo:
    return NfsRepo.from_uri_part(REMOTE_REPO_URI.split('://', 1)[1])


@pytest.fixture()
def nfs_repo_path(nfs_repo) -> Path:
    return Path(nfs_repo.remote_path)


@pytest.fixture()
def local_repo_path() -> Path:
    return LOCAL_REPO.path


@pytest.fixture()
def clean_repos(local_repo_path, nfs_repo_path):
    shutil.rmtree(local_repo_path / 'metadata', ignore_errors=True)
    shutil.rmtree(local_repo_path / 'artifacts', ignore_errors=True)
    shutil.rmtree(nfs_repo_path / 'metadata', ignore_errors=True)
    shutil.rmtree(nfs_repo_path / 'artifacts', ignore_errors=True)
    shutil.rmtree(nfs_repo_path / 'custom_dir', ignore_errors=True)
    shutil.rmtree(nfs_repo_path / 'nfs_tests', ignore_errors=True)