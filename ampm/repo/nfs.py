import contextlib
import hashlib
import os
import subprocess
import sys
import re
import shutil
import threading
from math import ceil
from pathlib import Path
from typing import List, Iterable, ContextManager, Optional, Dict

import toml
import tqdm
from pyNfsClient import (Portmap, Mount, NFSv3, MNT3_OK, NFS_PROGRAM,
                         NFS_V3, NFS3_OK, UNCHECKED, NFS3ERR_EXIST, UNSTABLE, NFS3ERR_NOTDIR, NFS3ERR_ISDIR, NFSSTAT3, NF3DIR, NFS3ERR_NOTSUPP)

from ampm.repo.base import ArtifactRepo, ArtifactMetadata, ArtifactQuery, QueryNotFoundError, ARTIFACT_TYPES, \
    ArtifactCorruptedError, NiceTrySagi
from ampm.repo.local import LOCAL_REPO
from ampm.utils import _calc_dir_size, remove_atexit, LockFile

DEFAULT_CHUNK_SIZE = int(os.environ.get("AMPM_CHUNK_SIZE", str(1024 * 32)))
NFS_OP_TIMEOUT_SEC = 16


def _common_prefix(iter1, iter2):
    for (a, b) in zip(iter1, iter2):
        if a != b:
            break
        yield a


def _validate_path(remote_path: str):
    if remote_path.startswith('.') or '/.' in remote_path:
        raise NiceTrySagi(f'Cannot access hidden directories: {remote_path}')


def _retry_reconnect_and_reduce_chunk_size(fn):
    def inner(self, *args, chunk_size, **kwargs):
        chunk_size = min(self.chunk_size_limit, chunk_size)
        is_first = True
        while self.chunk_size_limit > 1024 or is_first:
            try:
                return fn(self, *args, chunk_size=chunk_size, **kwargs)
            except Exception as e:
                if chunk_size <= 1024:
                    raise
                self.chunk_size_limit = chunk_size = int(ceil(chunk_size / 2 / 1024) * 1024)
                is_first = False
                print(f'WARN: Lowering chunk size to {chunk_size} due to IO related error: {e}', file=sys.stderr)
                self._reconnect()

    return inner


class NfsConnection:
    def __init__(self, host: str, remote_path: str):
        self.host = host
        self.remote_path = remote_path
        self.mount: Mount = None
        self.portmap: Portmap = None
        self.nfs3: NFSv3 = None
        self.root_fh: bytes = None
        self.chunk_size_limit = 1024 * 1024 * 1024  # 1 GiB
        self.supports_readdirplus = True

        self.auth = {
            "flavor": 1,
            "machine_name": "localhost",
            "uid": 0,
            "gid": 0,
            "aux_gid": list(),
        }

    def _reconnect(self):
        if self.nfs3:
            self._disconnect()
        self._connect()

    def _connect(self):
        _validate_path(self.remote_path)

        # portmap initialization
        self.portmap = Portmap(self.host, timeout=3600)
        self.portmap.connect()

        # mount initialization
        mnt_port = self.portmap.getport(Mount.program, Mount.program_version)
        self.mount = Mount(host=self.host, port=mnt_port, timeout=3600, auth=self.auth)
        self.mount.connect()

        # do mount
        mnt_res = self.mount.mnt(self.remote_path, self.auth)
        if mnt_res["status"] == MNT3_OK:
            self.nfs3 = None
            try:
                nfs_port = self.portmap.getport(NFS_PROGRAM, NFS_V3)
                self.nfs3 = NFSv3(self.host, nfs_port, NFS_OP_TIMEOUT_SEC, self.auth)
                self.nfs3.connect()
                self.root_fh = mnt_res["mountinfo"]["fhandle"]
            except Exception:
                if self.nfs3:
                    self.nfs3.disconnect()
                    self.nfs3 = None
                self.mount.umnt(self.auth)
                self.mount.disconnect()
                self.mount = None
                self.portmap.disconnect()
                self.portmap = None
                raise
        else:
            self.mount.disconnect()
            self.mount = None
            self.portmap.disconnect()
            self.portmap = None
            raise ConnectionError(f"NFS mount failed: code={mnt_res['status']} ({NFSSTAT3[mnt_res['status']]})")

    def _disconnect(self):
        if self.nfs3:
            self.nfs3.disconnect()
            self.nfs3 = None
        if self.mount:
            self.mount.umnt(self.auth)
            self.mount.disconnect()
            self.mount = None
        if self.portmap:
            self.portmap.disconnect()
            self.portmap = None

    @contextlib.contextmanager
    def connected(self) -> ContextManager["NfsConnection"]:
        if not self.nfs3:
            self._connect()
        yield self

    @staticmethod
    def _splitpath(remote_path: str) -> List[str]:
        remote_path = list(remote_path.strip('/').split('/'))
        while '' in remote_path:
            remote_path.remove('')
        while '.' in remote_path:
            remote_path.remove('.')
        return remote_path

    # Returns (file handle, file attributes)
    def _open(self, remote_path: List[str]) -> (bytes, dict):
        fh = self.root_fh
        attrs = {}
        for path_part in remote_path:
            lookup_res = self.nfs3.lookup(fh, path_part)
            if lookup_res["status"] == NFS3_OK:
                fh = lookup_res["resok"]["object"]["data"]
                attrs = lookup_res["resok"]["obj_attributes"]["attributes"]
            else:
                raise IOError(f"NFS lookup failed: code={lookup_res['status']} ({NFSSTAT3[lookup_res['status']]})")

        return fh, attrs

    def _remove(self, remote_path: List[str]):
        fh = self._open(remote_path[:-1])[0]
        remove_res = self.nfs3.remove(fh, remote_path[-1])
        if remove_res["status"] != NFS3_OK:
            raise IOError(f"NFS remove failed: code={remove_res['status']} ({NFSSTAT3[remove_res['status']]})")

    def remove(self, remote_path: str):
        _validate_path(remote_path)
        self._remove(self._splitpath(remote_path))

    def rmtree(self, remote_path: str):
        for path in self.walk_files_dirs_at_end(remote_path):
            print('Removing:', path, file=sys.stderr)
            self.remove(path)

    def _mkdir_recursive(self, remote_path: List[str]):
        dir_fh = self.root_fh
        for path_part in remote_path:
            mkdir_res = self.nfs3.mkdir(dir_fh, path_part, mode=0o777)
            # print('--- mkdir_res ---')
            # print(mkdir_res)
            if mkdir_res["status"] == NFS3_OK:
                dir_fh = mkdir_res["resok"]["obj"]["handle"]["data"]
            else:
                # Maybe it already exists?
                if mkdir_res["status"] == NFS3ERR_EXIST:
                    # Make sure it's a directory
                    if mkdir_res["resfail"]["after"]["attributes"]["type"] != 2:
                        raise IOError("Tried to create directory but file exists with same name")

                lookup_res = self.nfs3.lookup(dir_fh, path_part)
                # print('--- lookup_res ---')
                # print(lookup_res)
                if lookup_res["status"] == NFS3_OK:
                    dir_fh = lookup_res["resok"]["object"]["data"]
                else:
                    raise IOError(f"NFS mkdir.lookup failed: code={lookup_res['status']} ({NFSSTAT3[lookup_res['status']]})")

        return dir_fh

    def _create_with_dirs(self, remote_path: List[str]):
        dir_fh = self._mkdir_recursive(remote_path[:-1])

        create_res = self.nfs3.create(dir_fh, remote_path[-1], UNCHECKED, mode=0o777, size=0)
        # print('--- create_res ---')
        # print(create_res)
        if create_res["status"] == NFS3_OK:
            return create_res["resok"]["obj"]["handle"]["data"]
        else:
            raise IOError(f"NFS create failed: code={create_res['status']} ({NFSSTAT3[create_res['status']]})")

    def list_dir(self, remote_path: str):
        _validate_path(remote_path)
        fh, _attrs = self._open(self._splitpath(remote_path))
        cookie = 0
        cookie_verf = '0'
        while True:
            readdir_res = self.nfs3.readdir(fh, cookie=cookie, cookie_verf=cookie_verf)
            if readdir_res["status"] == NFS3_OK:
                cookie_verf = readdir_res["resok"]["cookieverf"]
                entry = readdir_res["resok"]["reply"]["entries"]
                while entry:
                    yield entry[0]['name']
                    cookie = entry[0]['cookie']
                    entry = entry[0]['nextentry']
                if readdir_res["resok"]["reply"]["eof"]:
                    break
            elif readdir_res["status"] == NFS3ERR_NOTDIR:
                raise NotADirectoryError()
            else:
                raise IOError(f"NFS readdir failed: code={readdir_res['status']} ({NFSSTAT3[readdir_res['status']]})")

    def walk_files(self, remote_path: str, include_dirs: bool = False):
        _validate_path(remote_path)
        fh, _attrs = self._open(self._splitpath(remote_path))
        cookie = 0
        cookie_verf = '0'
        while True:
            readdir_res = None
            if self.supports_readdirplus:
                readdir_res = self.nfs3.readdirplus(fh, cookie=cookie, cookie_verf=cookie_verf)
                if readdir_res["status"] == NFS3ERR_NOTSUPP:
                    self.supports_readdirplus = False

            if not self.supports_readdirplus:
                readdir_res = self.nfs3.readdir(fh, cookie=cookie, cookie_verf=cookie_verf)

            if readdir_res["status"] == NFS3_OK:
                if include_dirs:
                    yield remote_path
                cookie_verf = readdir_res["resok"]["cookieverf"]
                entry = readdir_res["resok"]["reply"]["entries"]
                while entry:
                    if not entry[0]['name'].startswith(b'.'):
                        next_path = remote_path + '/' + entry[0]['name'].decode()
                        if ('name_attributes' in entry[0] and
                                entry[0]['name_attributes']['attributes']['type'] != NF3DIR):
                            yield next_path
                        else:
                            yield from self.walk_files(next_path, include_dirs)
                    cookie = entry[0]['cookie']
                    entry = entry[0]['nextentry']
                if readdir_res["resok"]["reply"]["eof"]:
                    break
            elif readdir_res["status"] == NFS3ERR_NOTDIR:
                yield remote_path
                return
            else:
                raise IOError(f"NFS readdirplus failed: code={readdir_res['status']} ({NFSSTAT3[readdir_res['status']]})")

    def walk_files_dirs_at_end(self, remote_path: str):
        """Transform the output of `walk_files` such that the dirs appear after their contents"""
        last = ''

        for path in self.walk_files(remote_path, include_dirs=True):
            if not path.startswith(last + '/') and last:
                current_parts = path.split('/')
                last_parts = last.split('/')
                common_parts = list(_common_prefix(current_parts, last_parts))
                for subdir in [last_parts[:i] for i in range(len(last_parts), len(common_parts), -1)]:
                    yield '/'.join(subdir)
            last = path

        if not last:
            # Empty directory
            return

        base_parts = remote_path.split('/')[:-1]
        last_parts = last.split('/')
        common_parts = list(_common_prefix(base_parts, last_parts))
        for subdir in [last_parts[:i] for i in range(len(last_parts), len(common_parts), -1)]:
            yield '/'.join(subdir)

    def rename(self, old_path: str, new_path: str):
        _validate_path(old_path)
        _validate_path(new_path)
        old_path_parts = self._splitpath(old_path)
        new_path_parts = self._splitpath(new_path)
        old_fh, _attrs = self._open(old_path_parts[:-1])
        new_fh = self._mkdir_recursive(new_path_parts[:-1])
        rename_res = self.nfs3.rename(old_fh, old_path_parts[-1], new_fh, new_path_parts[-1])

        return rename_res["status"] == NFS3_OK

    def symlink(self, dest_path: str, link_path: str):
        _validate_path(link_path)
        link_path = self._splitpath(link_path)
        fh, _attrs = self._open(link_path[:-1])
        symlink_res = self.nfs3.symlink(fh, link_path[-1], dest_path)
        if symlink_res["status"] != NFS3_OK:
            raise IOError(f"NFS symlink failed: code={symlink_res['status']} ({NFSSTAT3[symlink_res['status']]})")

    def readlink(self, remote_path: str) -> bytes:
        _validate_path(remote_path)
        link_path = self._splitpath(remote_path)
        fh, _attrs = self._open(link_path)
        readlink_res = self.nfs3.readlink(fh)

        if readlink_res["status"] != NFS3_OK:
            raise IOError(f"NFS readlink failed: code={readlink_res['status']} ({NFSSTAT3[readlink_res['status']]})")
        return readlink_res["resok"]["data"]

    @_retry_reconnect_and_reduce_chunk_size
    def _read(self, fh, offset, chunk_size):
        read_res = self.nfs3.read(fh, offset, chunk_size)
        if read_res["status"] == NFS3_OK:
            data = read_res["resok"]["data"]
            if len(data) == 0:
                raise IOError("NFS read returned 0 bytes")
            return data
        elif read_res["status"] == NFS3ERR_ISDIR:
            raise IsADirectoryError()
        else:
            raise IOError(f"NFS read failed: code={read_res['status']} ({NFSSTAT3[read_res['status']]})")

    @_retry_reconnect_and_reduce_chunk_size
    def _write(self, fh, offset, content, chunk_size):
        write_res = self.nfs3.write(
            fh,
            offset=offset,
            count=min(len(content), chunk_size),
            content=content[:chunk_size],
            stable_how=UNSTABLE,
        )
        # print('--- write_res ---')
        # print(write_res)
        if write_res["status"] == NFS3_OK:
            if write_res["resok"]["count"] == 0:
                raise IOError("NFS write returned 0 bytes")
            return write_res["resok"]["count"]
        else:
            raise IOError(f"NFS write failed: code={write_res['status']} ({NFSSTAT3[write_res['status']]})")

    def read_stream(self, remote_path: str, chunk_size: int = DEFAULT_CHUNK_SIZE, progress_bar=False):
        _validate_path(remote_path)
        remote_path = self._splitpath(remote_path)
        fh, attrs = self._open(remote_path)
        if attrs["type"] != 1:
            raise IOError("Tried to read a non-file")

        offset = 0
        left = attrs["size"]
        bar = None
        if progress_bar:
            bar = tqdm.tqdm(total=ceil(attrs["size"] / 1024), desc=f"Reading {remote_path[-1]}", unit='KiB')

        while left > 0:
            data = self._read(fh, offset, chunk_size=chunk_size)
            offset += len(data)
            left -= len(data)
            if bar:
                bar.update(len(data) // 1024)
            yield data

        if bar:
            bar.reset()
            bar.update(bar.total)
            bar.close()

    def read(self, remote_path: str, chunk_size: int = DEFAULT_CHUNK_SIZE, progress_bar=False):
        _validate_path(remote_path)
        return b''.join(list(self.read_stream(remote_path, chunk_size, progress_bar)))

    def download(
            self,
            local_path: Path,
            remote_path: str,
            chunk_size: int = DEFAULT_CHUNK_SIZE,
            progress_bar=False
    ) -> Optional[str]:
        _validate_path(remote_path)
        got_one_file = False
        hasher = hashlib.sha256(b'')

        for remote_file_path in self.walk_files(remote_path):
            if got_one_file:
                hasher = None  # Disable hashing if we're downloading multiple files
            local_file_path = local_path / remote_file_path[len(remote_path):].strip('/')
            local_file_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                local_file_path.symlink_to(self.readlink(remote_file_path).decode())
                hasher = None  # Don't hash symlinks
            except IOError:
                # Not a symlink, read as file

                def opener(path, flags):
                    return os.open(path, flags, 0o755)

                with open(local_file_path, 'wb', opener=opener) as f:
                    for chunk in self.read_stream(remote_file_path, chunk_size, progress_bar):
                        f.write(chunk)
                        if hasher:
                            hasher.update(chunk)
            got_one_file = True

        if hasher:
            return hasher.hexdigest()

    def write_stream(self, contents_gen: Iterable[bytes], remote_path: str, contents_len: int, progress_bar=False):
        _validate_path(remote_path)
        remote_path = self._splitpath(remote_path)
        fh = self._create_with_dirs(remote_path)

        offset = 0
        bar = None
        if progress_bar:
            bar = tqdm.tqdm(total=ceil(contents_len / 1024), desc=f"Writing {remote_path[-1]}", unit='KiB')

        for chunk in contents_gen:
            while chunk:
                wrote = self._write(fh, offset, content=chunk, chunk_size=DEFAULT_CHUNK_SIZE)
                chunk = chunk[wrote:]
                offset += wrote
            if bar:
                bar.update(len(chunk) // 1024)

        self.nfs3.commit(fh)

        if bar:
            bar.reset()
            bar.update(bar.total)
            bar.close()

    def write(self, contents: bytes, remote_path: str, chunk_size: int = DEFAULT_CHUNK_SIZE, progress_bar=False):
        _validate_path(remote_path)

        def chunked():
            for i in range(0, len(contents), chunk_size):
                yield contents[i:i + chunk_size]

        self.write_stream(chunked(), remote_path, len(contents), progress_bar)

    def _upload_dir(self, local_path: Path, remote_path: str, progress_bar: Optional[tqdm.tqdm] = None):
        _validate_path(remote_path)
        for entry in os.listdir(local_path):
            entry = Path(local_path / entry)
            if not entry.is_symlink() and entry.is_dir():
                self._upload_dir(entry, f"{remote_path}/{entry.name}", progress_bar)
            else:
                file_size = entry.stat().st_size

                self.upload(entry, f"{remote_path}/{entry.name}")

                if progress_bar:
                    progress_bar.update(file_size // 1024)

    def upload(
            self,
            local_path: Path,
            remote_path: str,
            chunk_size: int = DEFAULT_CHUNK_SIZE,
            allow_dir=False,
            progress_bar=False
    ):
        _validate_path(remote_path)
        if local_path.is_symlink():
            self.symlink(str(local_path.readlink()), remote_path)

        elif local_path.is_dir():
            if not allow_dir:
                raise IOError("Tried to upload a directory, but `allow_dir` was set to False")

            whole_dir_size = _calc_dir_size(local_path)
            if progress_bar:
                bar = tqdm.tqdm(total=ceil(whole_dir_size / 1024), unit='KB', desc=f"Uploading dir {local_path}")
            else:
                bar = None

            self._upload_dir(local_path, remote_path, bar)

            if bar:
                bar.reset()
                bar.update(bar.total)
                bar.close()

        elif local_path.is_file():
            with open(local_path, 'rb') as f:
                file_len = f.seek(0, 2)
                f.seek(0)

                def chunked():
                    for i in range(0, file_len, chunk_size):
                        yield f.read(chunk_size)

                self.write_stream(chunked(), remote_path, file_len, progress_bar)

        else:
            raise IOError("Tried to upload a path that is neither a file nor a directory")


class NfsRepo(ArtifactRepo):
    def __init__(self, host: str, mount_path: str, repo_path: str):
        self.host = host
        self.mount_path = mount_path
        self.repo_path = repo_path
        self.nfs = NfsConnection(host, mount_path)

    @staticmethod
    def from_uri_part(uri_part: str) -> "NfsRepo":
        uri_part, repo_path = uri_part.split("#", 1)
        host, mount_path = uri_part.split("/", 1)
        return NfsRepo(host, '/' + mount_path.strip('/'), repo_path.strip('/'))

    def into_uri(self) -> str:
        return f"nfs://{self.host}/{self.mount_path.lstrip('/')}#{self.repo_path}"

    def upload(self, metadata: ArtifactMetadata, local_path: Optional[Path]):
        assert metadata.path_type in ARTIFACT_TYPES, f'Invalid artifact path type: {metadata.path_type}'

        with self.nfs.connected():
            if local_path is not None:
                print('Uploading artifact...', file=sys.stderr)

                tmp_remote_base_path = self.artifact_base_path_of(metadata, '.tmp')
                remote_base_path = self.artifact_base_path_of(metadata)
                tmp_remote_path = self.artifact_path_of(metadata, '.tmp')

                self.nfs.upload(local_path, tmp_remote_path, allow_dir=True, progress_bar=True)
                self.nfs.rename(tmp_remote_base_path, remote_base_path)

            print('Uploading metadata...', file=sys.stderr)
            tmp_remote_metadata_path = self.metadata_path_of(metadata.type, metadata.hash, '.toml.tmp')
            remote_metadata_path = self.metadata_path_of(metadata.type, metadata.hash)
            self.nfs.write(toml.dumps(metadata.to_dict(with_mutable=True)).encode('utf-8'), tmp_remote_metadata_path)
            self.nfs.rename(tmp_remote_metadata_path, remote_metadata_path)

            print('Done!', file=sys.stderr)

    def lookup(self, query: ArtifactQuery) -> Iterable[ArtifactMetadata]:
        with self.nfs.connected():
            if query.is_exact:
                # print('Downloading metadata')
                with LOCAL_REPO.metadata_lockfile:
                    metadata_path = LOCAL_REPO.metadata_path_of(query.type, query.hash, '.toml')
                    tmp_metadata_path = Path(str(metadata_path) + '.tmp')
                    tmp_metadata_path.parent.mkdir(parents=True, exist_ok=True)
                    tmp_metadata_path.unlink(missing_ok=True)

                    try:
                        self.nfs.download(tmp_metadata_path, self.metadata_path_of(query.type, query.hash))
                    except ConnectionError:
                        raise
                    except IOError:
                        raise QueryNotFoundError(query)

                    metadata = ArtifactMetadata.from_dict(toml.load(tmp_metadata_path))
                    tmp_metadata_path.rename(metadata_path)
                    yield metadata

    def download(self, metadata: ArtifactMetadata) -> Path:
        tmp_local_base_path = LOCAL_REPO.artifact_base_path_of(metadata, '.tmp')
        tmp_local_path = LOCAL_REPO.artifact_path_of(metadata, '.tmp')
        local_base_path = LOCAL_REPO.artifact_base_path_of(metadata)
        remote_path = self.artifact_path_of(metadata)
        remote_base_path = self.artifact_base_path_of(metadata)

        tmp_local_base_path.parent.mkdir(parents=True, exist_ok=True)
        # In case a concurrent ampm already downloaded this
        if local_base_path.exists():
            return LOCAL_REPO.artifact_path_of(metadata)

        shutil.rmtree(tmp_local_base_path, ignore_errors=True)
        shutil.rmtree(local_base_path, ignore_errors=True)

        actual_hash = None

        with self.nfs.connected():
            tmp_local_base_path.mkdir(parents=True, exist_ok=True)

            if metadata.path_type == 'file' or metadata.path_type == 'dir':
                if metadata.path_location:
                    actual_hash = self.nfs.download(tmp_local_base_path / metadata.name, remote_base_path, progress_bar=True)
                else:
                    actual_hash = self.nfs.download(tmp_local_base_path, remote_base_path, progress_bar=True)
            elif metadata.path_type == 'gz':
                decompressor = subprocess.Popen(['gzip', '-d'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
                hasher = hashlib.sha256(b'')

                def out_reader(tmp_local_file_path):
                    with open(tmp_local_file_path, 'wb') as output_file:
                        while True:
                            chunk = decompressor.stdout.read(1024 * 1024)
                            if len(chunk):
                                output_file.write(chunk)
                            else:
                                break

                def in_writer():
                    for chunk in self.nfs.read_stream(remote_path, progress_bar=True):
                        decompressor.stdin.write(chunk)
                        hasher.update(chunk)
                    decompressor.stdin.close()

                out_reader_thread = threading.Thread(target=out_reader, args=((tmp_local_base_path / metadata.name),))
                in_writer_thread = threading.Thread(target=in_writer)
                out_reader_thread.start()
                in_writer_thread.start()

                out_reader_thread.join()
                in_writer_thread.join()

                actual_hash = hasher.hexdigest()
            elif metadata.path_type == 'tar.gz':
                tmp_local_path.mkdir(parents=True)
                decompressor = subprocess.Popen(
                    ['tar', '--delay-directory-restore', '-xz'],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    cwd=str(tmp_local_path),
                )
                hasher = hashlib.sha256(b'')

                for chunk in self.nfs.read_stream(remote_path, progress_bar=True):
                    decompressor.stdin.write(chunk)
                    hasher.update(chunk)
                decompressor.stdin.close()
                decompressor.wait()

                actual_hash = hasher.hexdigest()
            else:
                raise ValueError(f'Unknown artifact type: {metadata.path_type}')

        if metadata.path_hash and actual_hash and metadata.path_hash != actual_hash:
            raise ArtifactCorruptedError(f'Hash mismatch for {metadata.type}:{metadata.hash}: '
                                         f'{metadata.path_hash} != {actual_hash}, Did someone modify the artifact on the server by hand?')

        LOCAL_REPO.generate_caches_for_artifact(metadata)
        tmp_local_base_path.rename(local_base_path)
        return LOCAL_REPO.artifact_path_of(metadata)

    def download_metadata_for_type(self, artifact_type: str):
        base_path = self.metadata_path_of(artifact_type, '', '')
        try:
            with self.nfs.connected():
                with LOCAL_REPO.metadata_lockfile:
                    for metadata_path in self.nfs.walk_files(base_path):
                        matches = re.match(r'(.*)/([a-z0-9]{32})\.toml$', metadata_path[len(base_path):])
                        if matches:
                            type_extra = matches.group(1)
                            artifact_hash = matches.group(2)
                            local_path = LOCAL_REPO.metadata_path_of(artifact_type + type_extra, artifact_hash, '.toml')
                            if local_path.exists():
                                continue
                            tmp_local_path = LOCAL_REPO.metadata_path_of(
                                artifact_type + type_extra,
                                artifact_hash,
                                '.toml.tmp',
                            )
                            tmp_local_path.parent.mkdir(parents=True, exist_ok=True)
                            self.nfs.download(tmp_local_path, self.metadata_path_of(artifact_type + type_extra, artifact_hash))
                            tmp_local_path.rename(local_path)
        except (ConnectionError, PermissionError):
            raise
        except IOError:
            pass

    def hash_remote_file(self, remote_path: str, progress_bar=False) -> str:
        assert remote_path.startswith(self.mount_path)
        remote_path = remote_path[len(self.mount_path):].lstrip('/')
        _validate_path(remote_path)

        with self.nfs.connected():
            hasher = hashlib.sha256(b'')
            for chunk in self.nfs.read_stream(remote_path, progress_bar):
                hasher.update(chunk)
            return hasher.hexdigest()

    def metadata_path_of(self, artifact_type: str, artifact_hash: str, suffix='.toml') -> str:
        return f'{self.repo_path}/metadata/{artifact_type}/{artifact_hash or ""}{suffix}'

    def artifact_base_path_of(self, metadata: ArtifactMetadata, suffix='') -> str:
        if metadata.path_location:
            assert metadata.path_location.startswith(self.mount_path)
            return metadata.path_location[len(self.mount_path):].lstrip('/')
        else:
            return f'{self.repo_path}/artifacts/{metadata.type.lower()}/{metadata.hash.lower()}{suffix}'

    def artifact_path_of(self, metadata: ArtifactMetadata, suffix='') -> str:
        if metadata.path_location:
            assert metadata.path_location.startswith(self.mount_path)
            return metadata.path_location[len(self.mount_path):].lstrip('/')
        else:
            return f'{self.repo_path}/artifacts/{metadata.type.lower()}/{metadata.hash.lower()}{suffix}/' \
                   f'{metadata.name}{metadata.path_suffix}'

    def remove_artifact(self, identifier: str):
        try:
            metadata = next(self.lookup(ArtifactQuery(identifier, {})))
        except QueryNotFoundError:
            print(f'Artifact {identifier} not found', file=sys.stderr)
            return False

        metadata_path = self.metadata_path_of(metadata.type, metadata.hash)

        self.nfs.rmtree(metadata_path)

        if metadata.path_location:
            print(f'Artifact has custom path, not removing {metadata.path_location}', file=sys.stderr)
        else:
            artifact_path = self.artifact_base_path_of(metadata)
            self.nfs.rmtree(artifact_path)

        return True

    def edit_artifact(self, identifier: str, attr: Dict[str, str], env: Dict[str, str]):
        try:
            metadata: ArtifactMetadata = next(self.lookup(ArtifactQuery(identifier, {})))
        except QueryNotFoundError:
            print(f'Artifact {identifier} not found', file=sys.stderr)
            return False

        # Apply changes to mutable attrs
        mut_attrs = metadata.mutable.setdefault('attributes', {})
        attrs_to_change = {k: v for k, v in attr.items() if not k.startswith('-')}
        attrs_to_remove = [k[1:] for k, _ in attr.items() if k.startswith('-')]

        mut_attrs.update(attrs_to_change)
        for k in attrs_to_remove:
            mut_attrs.pop(k)

        changed_static_attrs = set(mut_attrs.keys()).intersection(set(metadata.attributes.keys()))
        if len(changed_static_attrs) > 0:
            print(
                'ERROR: The following attributes were specified during upload and cannot be changed:',
                ', '.join(changed_static_attrs),
                file=sys.stderr,
            )
            exit(1)

        # Apply changes to mutable env vars
        mut_env = metadata.mutable.setdefault('env', {})
        env_to_change = {k: v for k, v in env.items() if not k.startswith('-')}
        env_to_remove = [k[1:] for k, _ in env.items() if k.startswith('-')]

        mut_env.update(env_to_change)
        for k in env_to_remove:
            mut_env.pop(k)

        changed_static_env = set(mut_env.keys()).intersection(set(metadata.env.keys()))
        if len(changed_static_env) > 0:
            print(
                'ERROR: The following environment vars were specified during upload and cannot be changed:',
                ', '.join(changed_static_env),
                file=sys.stderr,
            )
            exit(1)

        with self.nfs.connected():
            print('Uploading metadata...', file=sys.stderr)
            tmp_remote_metadata_path = self.metadata_path_of(metadata.type, metadata.hash, '.toml.tmp')
            remote_metadata_bak_path = self.metadata_path_of(metadata.type, metadata.hash, '.toml.bak')
            remote_metadata_path = self.metadata_path_of(metadata.type, metadata.hash)

            try:
                self.nfs.remove(remote_metadata_bak_path)
            except IOError:
                pass  # No backup, moving on

            try:
                self.nfs.remove(tmp_remote_metadata_path)
            except IOError:
                pass  # No temp fille, moving on

            self.nfs.write(toml.dumps(metadata.to_dict(with_mutable=True)).encode('utf-8'), tmp_remote_metadata_path)
            self.nfs.rename(remote_metadata_path, remote_metadata_bak_path)
            self.nfs.rename(tmp_remote_metadata_path, remote_metadata_path)

            print('Done!', file=sys.stderr)
