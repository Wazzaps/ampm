import json
import gzip
import multiprocessing
import re
import subprocess
import tarfile
import time
import pytest
import ampm.cli
from pathlib import Path
from typing import Dict, Optional
from click.testing import CliRunner


def assert_files_identical(path_a: Path, path_b: Path):
    sum_a = subprocess.check_output(['sha256sum', str(path_a)]).partition(b' ')[0]
    sum_b = subprocess.check_output(['sha256sum', str(path_b)]).partition(b' ')[0]
    assert sum_a == sum_b, f'Files were not identical:\n    {sum_a.decode()}  {str(path_a)}\n != {sum_b.decode()}  {str(path_b)}\n'


@pytest.fixture()
def upload(nfs_repo_uri):
    def _upload(
            local_path: Optional[str],
            artifact_type: str,
            compressed: bool,
            attributes=None,
            remote_path: Optional[str] = None
    ) -> str:
        if attributes is None:
            attributes = {}
        runner = CliRunner(mix_stderr=False, env={'AMPM_SERVER': nfs_repo_uri})

        args = ['upload', '--type', artifact_type, '--compressed' if compressed else '--uncompressed']
        if local_path:
            args += [local_path]
        if remote_path:
            args += ['--remote-path', remote_path]
        for k, v in attributes.items():
            args += ['--attr', f'{k}={v}']

        result = runner.invoke(ampm.cli.cli, args, catch_exceptions=False)
        formatted_output = f'== STDERR ==\n{result.stderr}\n\n== STDOUT ==\n{result.stdout}'
        assert result.exit_code == 0, formatted_output

        artifact_hash = re.match(f'^{artifact_type}:([a-z0-9]{{32}})$', result.stdout.strip())
        assert artifact_hash is not None, f'Unexpected output:\n{formatted_output}'
        return artifact_hash.group(1)
    return _upload


@pytest.fixture()
def download(nfs_repo_uri):
    def _download(identifier: str, attributes: Dict[str, str]) -> Path:
        runner = CliRunner(mix_stderr=False, env={'AMPM_SERVER': nfs_repo_uri})
        result = runner.invoke(
            ampm.cli.cli,
            ['get', identifier] + [f'--attr={k}={v}' for k, v in attributes.items()],
            catch_exceptions=False
        )
        formatted_output = f'== STDERR ==\n{result.stderr}\n\n== STDOUT ==\n{result.stdout}'
        assert result.exit_code == 0, formatted_output

        artifact_path = Path(result.stdout.strip())
        assert artifact_path.exists(), f'Downloaded artifact doesn\'t exist:\n{formatted_output}'
        return artifact_path
    return _download


@pytest.fixture()
def list_(nfs_repo_uri):
    def _list_(identifier: str, attributes: Dict[str, str], offline=False, override_server=None) -> dict:
        runner = CliRunner(mix_stderr=False, env={'AMPM_SERVER': override_server or nfs_repo_uri})
        result = runner.invoke(
            ampm.cli.cli,
            (['--offline'] if offline else []) + ['list', identifier, '--format=json'] + [f'--attr={k}={v}' for k, v in attributes.items()],
            catch_exceptions=False
        )
        formatted_output = f'== STDERR ==\n{result.stderr}\n\n== STDOUT ==\n{result.stdout}'
        assert result.exit_code == 0, formatted_output

        return json.loads(result.stdout.strip())
    return _list_


@pytest.fixture()
def remote_rm(nfs_repo_uri):
    def _remote_rm(identifier: str, accept: bool) -> Path:
        runner = CliRunner(mix_stderr=False, env={'AMPM_SERVER': nfs_repo_uri})
        result = runner.invoke(
            ampm.cli.cli,
            ['remote-rm', identifier]
            + (['--i-realise-this-may-break-other-peoples-builds-in-the-future'] if accept else []),
            catch_exceptions=False
        )
        formatted_output = f'== STDERR ==\n{result.stderr}\n\n== STDOUT ==\n{result.stdout}'
        assert result.exit_code == 0, formatted_output
    return _remote_rm


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_upload_single_file(nfs_repo_path, clean_repos, upload, is_compressed):
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
def test_upload_single_file_location(nfs_repo_path, nfs_mount_path, clean_repos, upload, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foobar.txt',
        artifact_type='foo',
        remote_path=str(
            nfs_mount_path / 'custom_dir' / ('foobar.txt' + ('.gz' if is_compressed == 'compressed' else ''))
        ),
        compressed=is_compressed == 'compressed'
    )
    assert (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').is_file(), "Metadata file wasn't created"
    if is_compressed == 'compressed':
        assert (nfs_mount_path / 'custom_dir' / 'foobar.txt.gz').is_file(), \
            "Data file wasn't created"
        decompressed = gzip.decompress(
            (nfs_mount_path / 'custom_dir' / 'foobar.txt.gz').read_bytes()
        )
        assert decompressed == b'foo bar\n', "Data file has wrong contents"
    else:
        assert (nfs_mount_path / 'custom_dir' / 'foobar.txt').is_file(), \
            "Data file wasn't created"
        assert (nfs_mount_path / 'custom_dir' / 'foobar.txt').read_bytes() == b'foo bar\n', \
            "Data file has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_upload_dir(nfs_repo_path, clean_repos, upload, is_compressed):
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
def test_upload_dir_location(nfs_repo_path, nfs_mount_path, clean_repos, upload, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foo_dir',
        artifact_type='foo',
        remote_path=str(
            nfs_mount_path / 'custom_dir' / ('foo_dir' + ('.tar.gz' if is_compressed == 'compressed' else ''))
        ),
        compressed=is_compressed == 'compressed'
    )
    assert (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').is_file(), \
        "Metadata file wasn't created"
    if is_compressed == 'compressed':
        assert (nfs_mount_path / 'custom_dir' / 'foo_dir.tar.gz').is_file(), \
            "Archive wasn't created"
    else:
        assert (nfs_mount_path / 'custom_dir' / 'foo_dir').is_dir(), \
            "Dir wasn't created"
        assert (nfs_mount_path / 'custom_dir' / 'foo_dir' / 'hello.txt').is_file(), \
            "File inside wasn't created"
        assert (nfs_mount_path / 'custom_dir' / 'foo_dir' / 'nested' / 'boo.txt').is_file(), \
            "File nested inside wasn't created"
        assert (nfs_mount_path / 'custom_dir' / 'foo_dir' / 'nested' / 'boo.txt').read_bytes() \
               == b"boo\n", "File nested has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_upload_external_single_file(nfs_repo_path, nfs_mount_path: Path, clean_repos, upload, is_compressed):
    _ = clean_repos
    remote_path = nfs_mount_path / 'custom_dir' / ('foobar.txt' + ('.gz' if is_compressed == 'compressed' else ''))

    data = b'hello\n'
    if is_compressed == 'compressed':
        file_contents = gzip.compress(data)
    else:
        file_contents = data

    remote_path.parent.mkdir(parents=True, exist_ok=True)
    remote_path.write_bytes(file_contents)

    artifact_hash = upload(
        local_path=None,
        artifact_type='foo',
        remote_path=str(remote_path),
        compressed=is_compressed == 'compressed'
    )
    assert (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').is_file(), "Metadata file wasn't created"
    assert remote_path.read_bytes() == file_contents, "Data file was modified/deleted!!!"


def test_upload_external_dir_ok(nfs_repo_path, nfs_mount_path: Path, clean_repos, upload):
    _ = clean_repos
    remote_path = nfs_mount_path / 'custom_dir' / 'foo_dir'

    data = b'hello\n'
    remote_path.mkdir(parents=True, exist_ok=True)
    (remote_path / 'a.txt').write_bytes(data)

    artifact_hash = upload(
        local_path=None,
        artifact_type='foo',
        remote_path=str(remote_path),
        compressed=False
    )
    assert (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').is_file(), "Metadata file wasn't created"
    assert remote_path.is_dir(), "Data dir was modified/deleted!!!"


def test_upload_external_dir_err(nfs_repo_path, nfs_mount_path: Path, clean_repos, upload):
    _ = clean_repos
    remote_path = nfs_mount_path / 'custom_dir' / 'foo_dir'

    data = b'hello\n'
    remote_path.mkdir(parents=True, exist_ok=True)
    (remote_path / 'a.txt').write_bytes(data)

    with pytest.raises(AssertionError):
        _artifact_hash = upload(
            local_path=None,
            artifact_type='foo',
            remote_path=str(remote_path),
            compressed=True
        )
    assert not (nfs_repo_path / 'metadata' / 'foo').is_dir(), "Metadata file was created"


def test_upload_external_archive(nfs_repo_path, nfs_mount_path: Path, clean_repos, upload):
    _ = clean_repos
    tmp_remote_path = nfs_mount_path / 'custom_dir' / 'a.txt'
    remote_path = nfs_mount_path / 'custom_dir' / 'foo_dir.tar.gz'

    data = b'hello\n'
    tmp_remote_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_remote_path.write_bytes(data)

    with tarfile.open(str(remote_path), 'w:gz') as tar:
        tar.add(str(tmp_remote_path), arcname='a.txt')

    file_contents = remote_path.read_bytes()

    artifact_hash = upload(
        local_path=None,
        artifact_type='foo',
        remote_path=str(remote_path),
        compressed=True
    )
    assert (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').is_file(), "Metadata file wasn't created"
    assert remote_path.read_bytes() == file_contents, "Data archive was modified/deleted!!!"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_download_single_file(clean_repos, upload, download, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foobar.txt',
        artifact_type='foo',
        compressed=is_compressed == 'compressed'
    )
    artifact_path = download(f'foo:{artifact_hash}', {})
    assert artifact_path.read_bytes() == b"foo bar\n", "Downloaded file has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_download_single_file_location(nfs_mount_path, clean_repos, upload, download, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        'tests/dummy_data/foobar.txt',
        artifact_type='foo',
        remote_path=str(nfs_mount_path / 'custom_dir' / 'foobar.txt'),
        compressed=is_compressed == 'compressed'
    )
    artifact_path = download(f'foo:{artifact_hash}', {})
    assert artifact_path.read_bytes() == b"foo bar\n", "Downloaded file has wrong contents"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_download_dir(clean_repos, upload, download, is_compressed):
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


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_download_external_single_file(nfs_mount_path: Path, clean_repos, upload, download, is_compressed):
    _ = clean_repos
    remote_path = nfs_mount_path / 'custom_dir' / ('foobar.txt' + ('.gz' if is_compressed == 'compressed' else ''))

    data = b'hello\n'
    if is_compressed == 'compressed':
        file_contents = gzip.compress(data)
    else:
        file_contents = data

    remote_path.parent.mkdir(parents=True, exist_ok=True)
    remote_path.write_bytes(file_contents)

    artifact_hash = upload(
        local_path=None,
        artifact_type='foo',
        remote_path=str(remote_path),
        compressed=is_compressed == 'compressed'
    )
    artifact_path = download(f'foo:{artifact_hash}', {})
    assert artifact_path.read_bytes() == data, "Downloaded file has wrong contents"


def test_download_external_dir_ok(nfs_mount_path: Path, clean_repos, upload, download):
    _ = clean_repos
    remote_path = nfs_mount_path / 'custom_dir' / 'foo_dir'

    file_contents = b'hello\n'
    remote_path.mkdir(parents=True, exist_ok=True)
    (remote_path / 'a.txt').write_bytes(file_contents)

    artifact_hash = upload(
        local_path=None,
        artifact_type='foo',
        remote_path=str(remote_path),
        compressed=False
    )
    artifact_path = download(f'foo:{artifact_hash}', {})
    assert (artifact_path / 'a.txt').read_bytes() == file_contents, "Downloaded file has wrong contents"


def test_download_external_archive(nfs_mount_path: Path, clean_repos, upload, download):
    _ = clean_repos
    tmp_remote_path = nfs_mount_path / 'custom_dir' / 'a.txt'
    remote_path = nfs_mount_path / 'custom_dir' / 'foo_dir.tar.gz'

    data = b'hello\n'
    tmp_remote_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_remote_path.write_bytes(data)

    with tarfile.open(str(remote_path), 'w:gz') as tar:
        tar.add(str(tmp_remote_path), arcname='a.txt')

    artifact_hash = upload(
        local_path=None,
        artifact_type='foo',
        remote_path=str(remote_path),
        compressed=True
    )
    artifact_path = download(f'foo:{artifact_hash}', {})
    assert (artifact_path / 'a.txt').read_bytes() == data, "Downloaded file has wrong contents"


@pytest.mark.parametrize('filter_type', ['num', 'date', 'semver'])
def test_attr_filters(clean_repos, upload, list_, filter_type):
    _ = clean_repos

    sample_data = {
        'num': ['1', '2', '3', '4', '5'],
        'date': ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05'],
        'semver': ['1.0.0', '1.0.1', '1.0.1-alpha', '1.1.0', '1.2.0', '1.3.0-alpha', '2.0.0'],
    }

    sample_queries = {
        'num': {
            '@num:biggest': '5',
            '@num:smallest': '1',
        },
        'date': {
            '@date:latest': '2020-01-05',
            '@date:earliest': '2020-01-01',
        },
        'semver': {
            '@semver:newest': '2.0.0',
            '@semver:oldest': '1.0.0',
            '@semver:^1.0.0': '1.2.0',
            '@semver:~1.0.0': '1.0.1',
            '@semver:^1.2.0,prerelease': '1.3.0-alpha',
            '@semver:<1.0.1,prerelease': '1.0.1-alpha',
            '@semver:^2.0.0': '2.0.0',
            '@semver:>1.0.0': '2.0.0',
        },
    }

    artifact_hashes = []
    for data in sample_data[filter_type]:
        artifact_hashes.append(upload(
            'tests/dummy_data/foobar.txt',
            artifact_type='foo',
            compressed=False,
            attributes={'attr': data}
        ))

    def do_test(query):
        return sorted(a['attributes']['attr'] for a in list_('foo', {'attr': query} if query else {}))

    artifacts = do_test(None)
    assert artifacts == sample_data[filter_type], "Wrong artifacts with no filter"

    artifacts = do_test(sample_data[filter_type][0])
    assert artifacts == [sample_data[filter_type][0]], "Wrong artifacts for exact match"

    for query, expected in sample_queries[filter_type].items():
        artifacts = do_test(query)
        assert artifacts == [expected], f"Wrong artifacts for {query}"


def test_attr_filters_ambiguous(clean_repos, upload, list_):
    _ = clean_repos

    artifact_hashes = []
    for i in range(5):
        artifact_hashes.append(upload(
            'tests/dummy_data/foobar.txt',
            artifact_type='foo',
            compressed=False,
            attributes={'a': f'{i}', 'b': f'{i % 2}'}
        ))

    assert len(list_('foo', {'a': '0'})) == 1, "Wrong number of artifacts with exact match of a == 0"
    assert len(list_('foo', {'a': '1'})) == 1, "Wrong number of artifacts with exact match of a == 1"

    assert len(list_('foo', {'b': '0'})) == 3, "Wrong number of artifacts with exact match of b == 0"
    assert len(list_('foo', {'b': '1'})) == 2, "Wrong number of artifacts with exact match of b == 1"

    with pytest.raises(AssertionError):
        list_('foo', {'a': '@num:biggest'})

    with pytest.raises(ValueError):
        list_('foo', {'a': '@ignore'})

    with pytest.raises(ValueError):
        list_('foo', {'@any': '@ignore'})

    assert len(list_('foo', {'a': '@num:biggest', 'b': '1'})) == 1, "Wrong number of artifacts with `biggest` on a"
    assert len(list_('foo', {'a': '@num:biggest', 'b': '1'})) == 1, "Wrong number of artifacts with `biggest` on a"
    assert len(list_('foo', {'a': '@num:smallest', 'b': '1'})) == 1, "Wrong number of artifacts with `smallest` on a"
    assert len(list_('foo', {'a': '1', 'b': '@num:biggest'})) == 1, "Wrong number of artifacts with `biggest` on b"
    assert len(list_('foo', {'a': '1', 'b': '@num:smallest'})) == 1, "Wrong number of artifacts with `smallest` on b"
    assert len(list_('foo', {'a': '@num:biggest', 'b': '@ignore'})) == 1, \
        "Wrong number of artifacts with `biggest` on a and `ignore` on b"
    assert len(list_('foo', {'a': '@num:biggest', '@any': '@ignore'})) == 1, \
        "Wrong number of artifacts with `biggest` on a and `ignore` on `any`"


def test_stress(clean_repos, upload, list_, download):
    _ = clean_repos
    artifact_hashes = []

    COUNT = 1000

    t = time.time()
    for i in range(COUNT):
        if i % 100 == 0:
            print(f'{i}/{COUNT}')
        artifact_hashes.append(upload(
            'tests/dummy_data/foobar.txt',
            artifact_type='foo',
            compressed=False
        ))
    upload_duration = time.time() - t
    print(f'Uploaded {COUNT} artifacts in {upload_duration} seconds')
    assert upload_duration < 120, f"Uploading {COUNT} artifacts took too long"

    t = time.time()
    for i, artifact_hash in enumerate(artifact_hashes):
        if i % 100 == 0:
            print(f'{i}/{COUNT}')
        download(f'foo:{artifact_hash}', {})
    download_duration = time.time() - t
    print(f'Downloaded {COUNT} artifacts in {download_duration} seconds')
    assert download_duration < 100, f"Downloading {COUNT} artifacts took too long"

    t = time.time()
    artifacts = list_('foo', {})
    assert len(artifacts) == COUNT, f"Listing {COUNT} artifacts returned {len(artifacts)} instead"
    list_duration = time.time() - t
    print(f'Listed {COUNT} artifacts in {list_duration} seconds')
    assert list_duration < 5, f"Listing {COUNT} artifacts took too long"


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_download_big_file(clean_repos, upload, download, big_file, is_compressed):
    _ = clean_repos
    artifact_hash = upload(
        str(big_file),
        artifact_type='foo',
        compressed=is_compressed == 'compressed'
    )
    artifact_path = download(f'foo:{artifact_hash}', {})
    assert_files_identical(artifact_path, big_file)


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_parallel_download_multiple_single_file(clean_repos, upload, download, big_file, is_compressed):
    _ = clean_repos

    artifact1_hash = upload(
        str(big_file),
        artifact_type='foo',
        compressed=is_compressed == 'compressed'
    )
    artifact2_hash = upload(
        str(big_file),
        artifact_type='foo',
        compressed=is_compressed == 'compressed'
    )

    threads = []
    for artifact_hash in (artifact1_hash, artifact2_hash):
        def inner():
            artifact_path = download(f'foo:{artifact_hash}', {})
            assert_files_identical(artifact_path, big_file)

        t = multiprocessing.Process(target=inner)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=60)


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_parallel_download_single_single_file(clean_repos, upload, download, big_file, is_compressed):
    _ = clean_repos

    artifact_hash = upload(
        str(big_file),
        artifact_type='foo',
        compressed=is_compressed == 'compressed'
    )

    threads = []
    for _ in range(5):
        def inner():
            artifact_path = download(f'foo:{artifact_hash}', {})
            assert_files_identical(artifact_path, big_file)

        t = multiprocessing.Process(target=inner)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=60)


def test_offline(clean_repos, upload, list_):
    _ = clean_repos

    upload(
        'tests/dummy_data/foobar.txt',
        artifact_type='foo',
        compressed=False,
    )

    assert list_('foo', {}, offline=True, override_server='SHOULDNT_BE_USED') == [], \
        'Local index shouldn\'t have been updated yet, offline mode broken'

    online_list = list_('foo', {})
    assert len(online_list), 'No artifacts found in online listing'

    offline_list = list_('foo', {}, offline=True, override_server='SHOULDNT_BE_USED')
    assert online_list == offline_list, 'Offline listing was different than cached listing'


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_remote_rm_file(clean_repos_now, upload, list_, remote_rm, nfs_repo_path: Path, is_compressed):
    clean_repos_now()

    artifact_hash = upload(
        'tests/dummy_data/foobar.txt',
        artifact_type='foo',
        compressed=is_compressed,
    )

    assert len(list_('foo', {})) != 0, 'Uploaded artifact not found'

    remote_rm(f'foo:{artifact_hash}', accept=True)
    clean_repos_now()

    assert len(list_('foo', {})) == 0, 'Uploaded artifact not removed'

    assert not (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').exists(), 'Metadata file not removed'
    assert not (nfs_repo_path / 'artifacts' / 'foo' / f'{artifact_hash}').exists(), 'Artifact file not removed'


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_remote_rm_dir(clean_repos_now, upload, list_, remote_rm, nfs_repo_path: Path, is_compressed):
    clean_repos_now()

    artifact_hash = upload(
        'tests/dummy_data/foo_dir',
        artifact_type='foo',
        compressed=is_compressed,
    )

    assert len(list_('foo', {})) != 0, 'Uploaded artifact not found'

    remote_rm(f'foo:{artifact_hash}', accept=True)
    clean_repos_now()

    assert len(list_('foo', {})) == 0, 'Uploaded artifact not removed'

    assert not (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').exists(), 'Metadata file not removed'
    assert not (nfs_repo_path / 'artifacts' / 'foo' / f'{artifact_hash}').exists(), 'Artifact file not removed'


@pytest.mark.parametrize('is_compressed', ['compressed', 'uncompressed'])
def test_remote_rm_location(clean_repos_now, upload, list_, remote_rm, nfs_mount_path: Path, nfs_repo_path: Path, is_compressed):
    clean_repos_now()

    remote_path = nfs_mount_path / 'custom_dir' / ('foobar.txt' + ('.gz' if is_compressed == 'compressed' else ''))
    artifact_hash = upload(
        'tests/dummy_data/foo_dir',
        artifact_type='foo',
        remote_path=str(remote_path),
        compressed=is_compressed,
    )

    assert len(list_('foo', {})) != 0, 'Uploaded artifact not found'

    remote_rm(f'foo:{artifact_hash}', accept=True)
    clean_repos_now()

    assert len(list_('foo', {})) == 0, 'Uploaded artifact not removed'

    assert not (nfs_repo_path / 'metadata' / 'foo' / f'{artifact_hash}.toml').exists(), 'Metadata file not removed'
    assert remote_path.exists(), 'Artifact file removed'


def test_remote_rm_required_arg(clean_repos, upload, list_, remote_rm):
    _ = clean_repos

    artifact_hash = upload(
        'tests/dummy_data/foobar.txt',
        artifact_type='foo',
        compressed=False,
    )

    assert len(list_('foo', {})) != 0, 'Uploaded artifact not found'

    with pytest.raises(AssertionError):
        remote_rm(f'foo:{artifact_hash}', accept=False)
