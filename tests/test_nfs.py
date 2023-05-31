import random
import shutil
import pytest
from pathlib import Path
from ampm.repo.base import NiceTrySagi
from ampm.repo.nfs import NfsConnection, NfsRepo
from ampm.utils import randbytes


def test_operations(clean_repos, nfs_repo: NfsRepo, nfs_mount_path: Path):
    _ = clean_repos
    nfs = NfsConnection(nfs_repo.host, nfs_repo.mount_path)

    with nfs.connected():
        ident = f'ampm_tests_{randbytes(8).hex()}'
        local_path = Path(f'/tmp/{ident}')
        remote_path = Path(f'nfs_tests')
        try:
            # Upload
            local_path.mkdir(parents=True, exist_ok=True)
            (local_path / 'foo.txt').write_text('foo bar')
            nfs.upload(local_path / 'foo.txt', str(remote_path / 'foo.txt'))
            assert (nfs_mount_path / remote_path / 'foo.txt').is_file(), 'Uploaded file missing'
            assert (nfs_mount_path / remote_path / 'foo.txt').read_text() == 'foo bar', 'Uploaded file content mismatch'

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
            nfs.symlink(str(nfs_mount_path / remote_path / 'foo2.txt'), str(remote_path / 'foo3.txt'))
            assert sorted(list(nfs.list_dir(str(remote_path)))) == [b'.', b'..', b'foo2.txt', b'foo3.txt'], \
                'List dir mismatch after file symlink'
            nfs.symlink(str(nfs_mount_path / remote_path / '..'), str(remote_path / 'foo4.txt'))
            assert sorted(list(nfs.list_dir(str(remote_path)))) == [
                b'.', b'..', b'foo2.txt', b'foo3.txt', b'foo4.txt'
            ], 'List dir mismatch after relative symlink'
            nfs.symlink('/a/b', str(remote_path / 'foo5.txt'))
            assert sorted(list(nfs.list_dir(str(remote_path)))) == [
                b'.', b'..', b'foo2.txt', b'foo3.txt', b'foo4.txt', b'foo5.txt'
            ], 'List dir mismatch after absolute symlink'

            # Readlink
            assert nfs.readlink(str(remote_path / 'foo3.txt')) \
                   == str(nfs_mount_path / remote_path / 'foo2.txt').encode(), 'Readlink mismatch'
        finally:
            shutil.rmtree(local_path, ignore_errors=True)


def test_path_traversal(clean_repos, nfs_repo: NfsRepo, nfs_mount_path: Path):
    _ = clean_repos
    nfs = NfsConnection(nfs_repo.host, nfs_repo.mount_path)

    with nfs.connected():
        local_path = Path('/non_existent_path')
        remote_path = Path(f'nfs_tests')

        # Upload
        with pytest.raises(NiceTrySagi):
            nfs.upload(local_path / 'foo.txt', f'{str(remote_path)}/../foo.txt')

        # List
        with pytest.raises(NiceTrySagi):
            list(nfs.list_dir(f'{str(remote_path)}/..'))

        # Download
        with pytest.raises(NiceTrySagi):
            nfs.download(local_path / 'foo2.txt', f'{str(remote_path)}/../foo.txt')
        with pytest.raises(NiceTrySagi):
            nfs.download(local_path / 'foo2.txt', str(remote_path / '.foo.txt'))

        # Rename
        with pytest.raises(NiceTrySagi):
            nfs.rename(f'{str(remote_path)}/../foo.txt', str(remote_path / 'foo2.txt'))
        with pytest.raises(NiceTrySagi):
            nfs.rename(str(remote_path / 'foo.txt'), f'{str(remote_path)}/../foo2.txt')
        with pytest.raises(NiceTrySagi):
            nfs.rename(str(remote_path / 'foo.txt'), '../foo2.txt')

        # Symlink
        with pytest.raises(NiceTrySagi):
            nfs.symlink(str(nfs_mount_path / remote_path / 'foo2.txt'), f'{str(remote_path)}/../foo3.txt')
        with pytest.raises(NiceTrySagi):
            nfs.symlink(str(nfs_mount_path / remote_path / 'foo2.txt'), str('.foo3.txt'))

        # Readlink
        with pytest.raises(NiceTrySagi):
            nfs.readlink(f'{str(remote_path)}/../foo3.txt')


def test_big_readdir(clean_repos, nfs_repo: NfsRepo, nfs_mount_path: Path):
    _ = clean_repos
    nfs = NfsConnection(nfs_repo.host, nfs_repo.mount_path)

    with nfs.connected():
        expected_files1 = [b'.', b'..', b'_dir']
        expected_files2 = ['/_dir/inner']

        (nfs_mount_path / '_dir').mkdir()
        (nfs_mount_path / '_dir' / 'inner').write_text('inner')

        for i in range(1024):
            (nfs_mount_path / f'{i}.txt').write_text(f'{i}')
            expected_files1.append(f'{i}.txt'.encode())
            expected_files2.append(f'/{i}.txt')

        expected_files1.sort()
        expected_files2.sort()

        assert list(sorted(list(nfs.list_dir('')))) == expected_files1

        assert list(sorted(list(nfs.walk_files('')))) == expected_files2
