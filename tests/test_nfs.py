import random
# noinspection PyUnresolvedReferences
from utils import *
from pathlib import Path
from ampm.repo.nfs import NfsConnection, NfsRepo


def test_operations(clean_repos, nfs_repo: NfsRepo, nfs_repo_path: Path, local_repo_path):
    _ = clean_repos

    with NfsConnection.connect(nfs_repo.host, nfs_repo.remote_path) as nfs:
        ident = f'ampm_tests_{random.randbytes(8).hex()}'
        local_path = Path(f'/tmp/{ident}')
        remote_path = Path(f'nfs_tests')
        try:
            # Upload
            local_path.mkdir(parents=True, exist_ok=True)
            (local_path / 'foo.txt').write_text('foo bar')
            nfs.upload(local_path / 'foo.txt', str(remote_path / 'foo.txt'))
            assert (nfs_repo_path / remote_path / 'foo.txt').is_file(), 'Uploaded file missing'
            assert (nfs_repo_path / remote_path / 'foo.txt').read_text() == 'foo bar', 'Uploaded file content mismatch'

            # List
            assert sorted(list(nfs.list_dir(str(remote_path)))) == [b'.', b'..', b'foo.txt'], \
                'List dir mismatch after initial upload'

            # Download
            nfs.download(local_path / 'foo2.txt', str(remote_path / 'foo.txt'))
            assert (local_path / 'foo2.txt').is_file(), 'Downloaded file missing'
            assert (local_path / 'foo2.txt').read_text() == 'foo bar', 'Downloaded file content mismatch'

            # Rename
            nfs.rename(str(remote_path / 'foo.txt'), str(remote_path / 'foo2.txt'))
            assert sorted(list(nfs.list_dir(str(remote_path)))) == [b'.', b'..', b'foo2.txt'], \
                'List dir mismatch after rename'

            # Symlink
            nfs.symlink(str(nfs_repo_path / remote_path / 'foo2.txt'), str(remote_path / 'foo3.txt'))
            assert sorted(list(nfs.list_dir(str(remote_path)))) == [b'.', b'..', b'foo2.txt', b'foo3.txt'], \
                'List dir mismatch after symlink'

            # Readlink
            assert nfs.readlink(str(remote_path / 'foo3.txt')) \
                   == str(nfs_repo_path / remote_path / 'foo2.txt').encode(), 'Readlink mismatch'
        finally:
            shutil.rmtree(local_path, ignore_errors=True)
