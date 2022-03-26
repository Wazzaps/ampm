import json
import gzip
import re
import time
import ampm.cli
# noinspection PyUnresolvedReferences
from utils import *
from pathlib import Path
from typing import Dict, Optional
from click.testing import CliRunner


def upload(local_path: str, artifact_type: str, compressed: bool, remote_path: Optional[str] = None) -> str:
    runner = CliRunner(mix_stderr=False)

    args = ['upload', local_path, '--type', artifact_type, '--compressed' if compressed else '--uncompressed']
    if remote_path:
        args += ['--remote-path', remote_path]

    result = runner.invoke(ampm.cli.cli, args)
    formatted_output = f'== STDERR ==\n{result.stderr}\n\n== STDOUT ==\n{result.stdout}'
    assert result.exit_code == 0, formatted_output

    artifact_hash = re.match(f'^{artifact_type}:([a-z0-9]{{32}})$', result.stdout.strip())
    assert artifact_hash is not None, f'Unexpected output:\n{formatted_output}'
    return artifact_hash.group(1)


def download(identifier: str, attributes: Dict[str, str]) -> Path:
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(ampm.cli.cli, ['get', identifier] + [f'--attr={k}={v}' for k, v in attributes.items()])
    formatted_output = f'== STDERR ==\n{result.stderr}\n\n== STDOUT ==\n{result.stdout}'
    assert result.exit_code == 0, formatted_output

    artifact_path = Path(result.stdout.strip())
    assert artifact_path.exists(), f'Downloaded artifact doesn\'t exist:\n{formatted_output}'
    return artifact_path


def list_(identifier: str, attributes: Dict[str, str]) -> dict:
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(ampm.cli.cli, ['list', identifier, '--format=json'] + [f'--attr={k}={v}' for k, v in attributes.items()])
    formatted_output = f'== STDERR ==\n{result.stderr}\n\n== STDOUT ==\n{result.stdout}'
    assert result.exit_code == 0, formatted_output

    return json.loads(result.stdout.strip())


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_upload_single_file(nfs_repo_path, clean_repos, is_compressed):
    _ = clean_repos
    artifact_hash = upload('tests/dummy_data/foobar.txt', artifact_type='foo', compressed=is_compressed == 'compressed')
    assert (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').is_file(), "Metadata file wasn't created"

    if is_compressed == 'compressed':
        assert (nfs_repo_path / 'artifacts' / 'foo' / artifact_hash / 'foobar.txt.gz').is_file(), \
            "Data file wasn't created"
        decompressed = gzip.decompress(
            (nfs_repo_path / 'artifacts' / 'foo' / artifact_hash / 'foobar.txt.gz').read_bytes()
        )
        assert decompressed == b'foo bar\n', "Data file has wrong contents"
    else:
        assert (nfs_repo_path / 'artifacts' / 'foo' / artifact_hash / 'foobar.txt').is_file(), \
            "Data file wasn't created"
        assert (nfs_repo_path / 'artifacts' / 'foo' / artifact_hash / 'foobar.txt').read_bytes() == b'foo bar\n', \
            "Data file has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_upload_single_file_location(nfs_repo_path, clean_repos, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foobar.txt',
        artifact_type='foo',
        remote_path='/custom_dir/foobar.txt' + ('.gz' if is_compressed == 'compressed' else ''),
        compressed=is_compressed == 'compressed'
    )
    assert (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').is_file(), "Metadata file wasn't created"
    if is_compressed == 'compressed':
        assert (nfs_repo_path / 'custom_dir' / 'foobar.txt.gz').is_file(), \
            "Data file wasn't created"
        decompressed = gzip.decompress(
            (nfs_repo_path / 'custom_dir' / 'foobar.txt.gz').read_bytes()
        )
        assert decompressed == b'foo bar\n', "Data file has wrong contents"
    else:
        assert (nfs_repo_path / 'custom_dir' / 'foobar.txt').is_file(), \
            "Data file wasn't created"
        assert (nfs_repo_path / 'custom_dir' / 'foobar.txt').read_bytes() == b'foo bar\n', \
            "Data file has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_upload_dir(nfs_repo_path, clean_repos, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foo_dir',
        artifact_type='foo',
        compressed=is_compressed == 'compressed'
    )
    assert (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').is_file(), \
        "Metadata file wasn't created"
    if is_compressed == 'compressed':
        assert (nfs_repo_path / 'artifacts' / 'foo' / artifact_hash / 'foo_dir.tar.gz').is_file(), \
            "Archive wasn't created"
    else:
        assert (nfs_repo_path / 'artifacts' / 'foo' / artifact_hash).is_dir(), \
            "Dir wasn't created"
        assert (nfs_repo_path / 'artifacts' / 'foo' / artifact_hash / 'foo_dir' / 'hello.txt').is_file(), \
            "File inside wasn't created"
        assert (nfs_repo_path / 'artifacts' / 'foo' / artifact_hash / 'foo_dir' / 'nested' / 'boo.txt').is_file(), \
            "File nested inside wasn't created"
        assert (nfs_repo_path / 'artifacts' / 'foo' / artifact_hash / 'foo_dir' / 'nested' / 'boo.txt').read_bytes() \
               == b"boo\n", "File nested has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_upload_dir_location(nfs_repo_path, clean_repos, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foo_dir',
        artifact_type='foo',
        remote_path='/custom_dir/foo_dir' + ('.tar.gz' if is_compressed == 'compressed' else ''),
        compressed=is_compressed == 'compressed'
    )
    assert (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').is_file(), \
        "Metadata file wasn't created"
    if is_compressed == 'compressed':
        assert (nfs_repo_path / 'custom_dir' / 'foo_dir.tar.gz').is_file(), \
            "Archive wasn't created"
    else:
        assert (nfs_repo_path / 'custom_dir' / 'foo_dir').is_dir(), \
            "Dir wasn't created"
        assert (nfs_repo_path / 'custom_dir' / 'foo_dir' / 'hello.txt').is_file(), \
            "File inside wasn't created"
        assert (nfs_repo_path / 'custom_dir' / 'foo_dir' / 'nested' / 'boo.txt').is_file(), \
            "File nested inside wasn't created"
        assert (nfs_repo_path / 'custom_dir' / 'foo_dir' / 'nested' / 'boo.txt').read_bytes() \
               == b"boo\n", "File nested has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_download_single_file_uncompressed(clean_repos, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foobar.txt',
        artifact_type='foo',
        compressed=is_compressed == 'compressed'
    )
    artifact_path = download(f'foo:{artifact_hash}', {})
    assert artifact_path.read_bytes() == b"foo bar\n", "Downloaded file has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_download_single_file_uncompressed_location(clean_repos, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foobar.txt',
        artifact_type='foo',
        remote_path='/custom_dir/foobar.txt',
        compressed=is_compressed == 'compressed'
    )
    artifact_path = download(f'foo:{artifact_hash}', {})
    assert artifact_path.read_bytes() == b"foo bar\n", "Downloaded file has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_download_dir_uncompressed(clean_repos, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foo_dir',
        artifact_type='foo',
        compressed=is_compressed == 'compressed'
    )
    artifact_path = download(f'foo:{artifact_hash}', {})
    assert (artifact_path / 'hello.txt').is_file(), "File inside wasn't created"
    assert (artifact_path / 'nested' / 'boo.txt').is_file(), "File nested inside wasn't created"
    assert (artifact_path / 'nested' / 'boo.txt').read_bytes() == b"boo\n", "File nested has wrong contents"


def test_stress(clean_repos):
    _ = clean_repos
    artifact_hashes = []

    COUNT = 200

    t = time.time()
    for i in range(COUNT):
        artifact_hashes.append(upload(
            'tests/dummy_data/foobar.txt',
            artifact_type='foo',
            compressed=False
        ))
    upload_duration = time.time() - t
    print(f'Uploaded {COUNT} artifacts in {upload_duration} seconds')
    assert upload_duration < 10, f"Uploading {COUNT} artifacts took too long"

    t = time.time()
    for artifact_hash in artifact_hashes:
        download(f'foo:{artifact_hash}', {})
    download_duration = time.time() - t
    print(f'Downloaded {COUNT} artifacts in {download_duration} seconds')
    assert download_duration < 5, f"Downloading {COUNT} artifacts took too long"

    t = time.time()
    artifacts = list_('foo', {})
    assert len(artifacts) == COUNT, f"Listing {COUNT} artifacts returned {len(artifacts)} instead"
    list_duration = time.time() - t
    print(f'Listed {COUNT} artifacts in {list_duration} seconds')
    assert list_duration < 1, f"Listing {COUNT} artifacts took too long"
