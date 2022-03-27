import contextlib
import gzip
import hashlib
import io
import os
import sys
import re
import shutil
import tarfile
from math import ceil
from pathlib import Path
from typing import List, Iterable, ContextManager, Optional

import toml
import tqdm
from pyNfsClient import (Portmap, Mount, NFSv3, MNT3_OK, NFS_PROGRAM,
                         NFS_V3, NFS3_OK, UNCHECKED, NFS3ERR_EXIST, UNSTABLE, NFS3ERR_NOTDIR, NFS3ERR_ISDIR, NFSSTAT3)

from ampm.repo.base import ArtifactRepo, ArtifactMetadata, ArtifactQuery, QueryNotFoundError, ARTIFACT_TYPES, \
    ArtifactCorruptedError
from ampm.repo.local import LOCAL_REPO
from ampm.utils import _calc_dir_size


class NfsConnection:
    def __init__(self, nfs3: NFSv3, root_fh: bytes):
        self.nfs3 = nfs3
        self.root_fh = root_fh

    @staticmethod
    @contextlib.contextmanager
    def connect(host, remote_path) -> ContextManager["NfsConnection"]:
        auth = {
            "flavor": 1,
            "machine_name": "localhost",
            "uid": 0,
            "gid": 0,
            "aux_gid": list(),
        }

        # portmap initialization
        portmap = Portmap(host, timeout=3600)
        portmap.connect()

        # mount initialization
        mnt_port = portmap.getport(Mount.program, Mount.program_version)
        mount = Mount(host=host, port=mnt_port, timeout=3600, auth=auth)
        mount.connect()

        # do mount
        mnt_res = mount.mnt(remote_path, auth)
        if mnt_res["status"] == MNT3_OK:
            root_fh = mnt_res["mountinfo"]["fhandle"]
            nfs3 = None
            try:
                nfs_port = portmap.getport(NFS_PROGRAM, NFS_V3)
                nfs3 = NFSv3(host, nfs_port, 3600, auth)
                nfs3.connect()
                yield NfsConnection(nfs3, root_fh)
            finally:
                if nfs3:
                    nfs3.disconnect()
                mount.umnt(auth)
                mount.disconnect()
                portmap.disconnect()
        else:
            mount.disconnect()
            portmap.disconnect()
            raise ConnectionError(f"NFS mount failed: code={mnt_res['status']} ({NFSSTAT3[mnt_res['status']]})")

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

    def _mkdir_recursive(self, remote_path: List[str]):
        dir_fh = self.root_fh
        for path_part in remote_path:
            mkdir_res = self.nfs3.mkdir(dir_fh, path_part, mode=0o777)
            # print('--- mkdir_res ---')
            # print(mkdir_res)
            if mkdir_res["status"] == NFS3_OK:
                dir_fh = mkdir_res["resok"]["obj"]["handle"]["data"]
            elif mkdir_res["status"] == NFS3ERR_EXIST:
                # Make sure it's a directory
                if mkdir_res["resfail"]["after"]["attributes"]["type"] != 2:
                    raise IOError("Tried to create directory but file exists with same name")

                lookup_res = self.nfs3.lookup(dir_fh, path_part)
                # print('--- lookup_res ---')
                # print(lookup_res)
                if lookup_res["status"] == NFS3_OK:
                    dir_fh = lookup_res["resok"]["object"]["data"]
                else:
                    raise IOError(f"NFS lookup failed: code={lookup_res['status']} ({NFSSTAT3[lookup_res['status']]})")
            else:
                raise IOError(f"NFS mkdir failed: code={mkdir_res['status']} ({NFSSTAT3[mkdir_res['status']]})")

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
        fh, _attrs = self._open(self._splitpath(remote_path))
        readdir_res = self.nfs3.readdir(fh)
        if readdir_res["status"] == NFS3_OK:
            entry = readdir_res["resok"]["reply"]["entries"]
            while entry:
                yield entry[0]['name']
                entry = entry[0]['nextentry']
        elif readdir_res["status"] == NFS3ERR_NOTDIR:
            raise NotADirectoryError()
        else:
            raise IOError(f"NFS readdir failed: code={readdir_res['status']} ({NFSSTAT3[readdir_res['status']]})")

    def walk_files(self, remote_path: str):
        fh, _attrs = self._open(self._splitpath(remote_path))
        readdir_res = self.nfs3.readdir(fh)
        if readdir_res["status"] == NFS3_OK:
            entry = readdir_res["resok"]["reply"]["entries"]
            while entry:
                if not entry[0]['name'].startswith(b'.'):
                    yield from self.walk_files(remote_path + '/' + entry[0]['name'].decode())
                entry = entry[0]['nextentry']
        elif readdir_res["status"] == NFS3ERR_NOTDIR:
            yield remote_path
        else:
            raise IOError(f"NFS readdir failed: code={readdir_res['status']} ({NFSSTAT3[readdir_res['status']]})")

    def rename(self, old_path: str, new_path: str):
        old_path_parts = self._splitpath(old_path)
        new_path_parts = self._splitpath(new_path)
        old_fh, _attrs = self._open(old_path_parts[:-1])
        new_fh = self._mkdir_recursive(new_path_parts[:-1])
        rename_res = self.nfs3.rename(old_fh, old_path_parts[-1], new_fh, new_path_parts[-1])

        return rename_res["status"] == NFS3_OK

    def symlink(self, dest_path: str, link_path: str):
        link_path = self._splitpath(link_path)
        fh, _attrs = self._open(link_path[:-1])
        symlink_res = self.nfs3.symlink(fh, link_path[-1], dest_path)
        if symlink_res["status"] != NFS3_OK:
            raise IOError(f"NFS symlink failed: code={symlink_res['status']} ({NFSSTAT3[symlink_res['status']]})")

    def readlink(self, remote_path: str) -> bytes:
        link_path = self._splitpath(remote_path)
        fh, _attrs = self._open(link_path)
        readlink_res = self.nfs3.readlink(fh)

        if readlink_res["status"] != NFS3_OK:
            raise IOError(f"NFS readlink failed: code={readlink_res['status']} ({NFSSTAT3[readlink_res['status']]})")
        return readlink_res["resok"]["data"]

    def read_stream(self, remote_path: str, chunk_size: int = 1024 * 50, progress_bar=False):
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
            read_res = self.nfs3.read(fh, offset, chunk_size)
            if read_res["status"] == NFS3_OK:
                data = read_res["resok"]["data"]
                if len(data) == 0:
                    raise IOError("NFS read returned 0 bytes")
                offset += len(data)
                left -= len(data)
                if bar:
                    bar.update(len(data) // 1024)
                yield data
            elif read_res["status"] == NFS3ERR_ISDIR:
                raise IsADirectoryError()
            else:
                raise IOError(f"NFS read failed: code={read_res['status']} ({NFSSTAT3[read_res['status']]})")

        if bar:
            bar.reset()
            bar.update(bar.total)
            bar.close()

    def read(self, remote_path: str, chunk_size: int = 1024 * 50, progress_bar=False):
        return b''.join(list(self.read_stream(remote_path, chunk_size, progress_bar)))

    def download(
            self,
            local_path: Path,
            remote_path: str,
            chunk_size: int = 1024 * 50,
            progress_bar=False
    ) -> Optional[str]:
        got_one_file = False
        hasher = hashlib.sha256(b'')

        for remote_file_path in self.walk_files(remote_path):
            if got_one_file:
                hasher = None  # Only hash one file
            local_file_path = local_path / remote_file_path[len(remote_path):].strip('/')
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_file_path, 'wb') as f:
                for chunk in self.read_stream(remote_file_path, chunk_size, progress_bar):
                    f.write(chunk)
                    if hasher:
                        hasher.update(chunk)
            got_one_file = True

        if hasher:
            return hasher.hexdigest()

    def write_stream(self, contents_gen: Iterable[bytes], remote_path: str, contents_len: int, progress_bar=False):
        remote_path = self._splitpath(remote_path)
        fh = self._create_with_dirs(remote_path)

        offset = 0
        bar = None
        if progress_bar:
            bar = tqdm.tqdm(total=ceil(contents_len / 1024), desc=f"Writing {remote_path[-1]}", unit='KiB')

        for chunk in contents_gen:
            write_res = self.nfs3.write(fh, offset=offset, count=len(chunk), content=chunk, stable_how=UNSTABLE)
            # print('--- write_res ---')
            # print(write_res)
            if write_res["status"] == NFS3_OK:
                assert write_res["resok"]["count"] == len(chunk)
            else:
                raise IOError(f"NFS write failed: code={write_res['status']} ({NFSSTAT3[write_res['status']]})")

            offset += len(chunk)
            if bar:
                bar.update(len(chunk) // 1024)

        self.nfs3.commit(fh)

        if bar:
            bar.reset()
            bar.update(bar.total)
            bar.close()

    def write(self, contents: bytes, remote_path: str, chunk_size: int = 1024 * 50, progress_bar=False):
        def chunked():
            for i in range(0, len(contents), chunk_size):
                yield contents[i:i + chunk_size]

        self.write_stream(chunked(), remote_path, len(contents), progress_bar)

    def _upload_dir(self, local_path: Path, remote_path: str, progress_bar: Optional[tqdm.tqdm] = None):
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
            chunk_size: int = 1024 * 50,
            allow_dir=False,
            progress_bar=False
    ):
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
        self.connection: Optional[NfsConnection] = None

    @contextlib.contextmanager
    def _connected(self) -> ContextManager["NfsConnection"]:
        if self.connection is None:
            with NfsConnection.connect(self.host, self.mount_path) as nfs:
                self.connection = nfs
                try:
                    yield nfs
                finally:
                    self.connection = None
        else:
            yield self.connection

    @staticmethod
    def from_uri_part(uri_part: str) -> "NfsRepo":
        uri_part, repo_path = uri_part.split("#", 1)
        host, mount_path = uri_part.split("/", 1)
        return NfsRepo(host, '/' + mount_path.strip('/'), repo_path.strip('/'))

    def into_uri(self) -> str:
        return f"nfs://{self.host}/{self.mount_path.lstrip('/')}#{self.repo_path}"

    def upload(self, metadata: ArtifactMetadata, local_path: Optional[Path]):
        assert metadata.path_type in ARTIFACT_TYPES, f'Invalid artifact path type: {metadata.path_type}'

        with self._connected() as nfs:
            if local_path is not None:
                print('Uploading artifact...', file=sys.stderr)

                tmp_remote_base_path = self.artifact_base_path_of(metadata, '.tmp')
                remote_base_path = self.artifact_base_path_of(metadata)
                tmp_remote_path = self.artifact_path_of(metadata, '.tmp')

                nfs.upload(local_path, tmp_remote_path, allow_dir=True, progress_bar=True)
                nfs.rename(tmp_remote_base_path, remote_base_path)

            print('Uploading metadata...', file=sys.stderr)
            tmp_remote_metadata_path = self.metadata_path_of(metadata.type, metadata.hash, '.toml.tmp')
            remote_metadata_path = self.metadata_path_of(metadata.type, metadata.hash)
            nfs.write(toml.dumps(metadata.to_dict()).encode('utf-8'), tmp_remote_metadata_path)
            nfs.rename(tmp_remote_metadata_path, remote_metadata_path)

            print('Done!', file=sys.stderr)

    def lookup(self, query: ArtifactQuery) -> Iterable[ArtifactMetadata]:
        with self._connected() as nfs:
            if query.is_exact:
                # print('Downloading metadata')
                metadata_path = LOCAL_REPO.metadata_path_of(query.type, query.hash, '.toml')
                tmp_metadata_path = Path(str(metadata_path) + '.tmp')
                tmp_metadata_path.parent.mkdir(parents=True, exist_ok=True)
                tmp_metadata_path.unlink(missing_ok=True)

                try:
                    nfs.download(tmp_metadata_path, self.metadata_path_of(query.type, query.hash))
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

        shutil.rmtree(tmp_local_base_path, ignore_errors=True)
        shutil.rmtree(local_base_path, ignore_errors=True)

        actual_hash = None

        with self._connected() as nfs:
            tmp_local_base_path.mkdir(parents=True, exist_ok=True)
            if metadata.path_type == 'file' or metadata.path_type == 'dir':
                if metadata.path_location:
                    actual_hash = nfs.download(tmp_local_base_path / metadata.name, remote_base_path, progress_bar=True)
                else:
                    actual_hash = nfs.download(tmp_local_base_path, remote_base_path, progress_bar=True)
            elif metadata.path_type == 'gz':
                buffer = io.BytesIO()
                hasher = hashlib.sha256(b'')

                tmp_local_file_path = tmp_local_base_path / metadata.name
                with open(tmp_local_file_path, 'wb') as output_file:
                    with gzip.GzipFile(fileobj=buffer, mode='rb') as decompressed:
                        # TODO: Stream chunks
                        compressed_data = nfs.read(remote_path, progress_bar=True)
                        hasher.update(compressed_data)
                        buffer.write(compressed_data)
                        buffer.seek(0)
                        output_file.write(decompressed.read())

                actual_hash = hasher.hexdigest()
            elif metadata.path_type == 'tar.gz':
                buffer = io.BytesIO()
                hasher = hashlib.sha256(b'')

                compressed_data = nfs.read(remote_path, progress_bar=True)
                hasher.update(compressed_data)
                buffer.write(compressed_data)
                buffer.seek(0, io.SEEK_SET)

                print('Extracting...', file=sys.stderr)
                with tarfile.open(fileobj=buffer, mode='r:gz') as tar:
                    tar.extractall(tmp_local_path)

                actual_hash = hasher.hexdigest()
            else:
                raise ValueError(f'Unknown artifact type: {metadata.path_type}')

        if metadata.path_hash and actual_hash and metadata.path_hash != actual_hash:
            raise ArtifactCorruptedError(f'Hash mismatch for {metadata.type}:{metadata.hash}: '
                                         f'{metadata.path_hash} != {actual_hash}')

        LOCAL_REPO.generate_caches_for_artifact(metadata)
        tmp_local_base_path.rename(local_base_path)
        return LOCAL_REPO.artifact_path_of(metadata)

    def download_metadata_for_type(self, artifact_type: str):
        base_path = self.metadata_path_of(artifact_type, '', '')
        try:
            with self._connected() as nfs:
                for metadata_path in nfs.walk_files(base_path):
                    matches = re.match(r'(.*)/([a-z0-9]{32})\.toml$', metadata_path[len(base_path):])
                    if matches:
                        type_extra = matches.group(1)
                        artifact_hash = matches.group(2)
                        local_path = LOCAL_REPO.metadata_path_of(artifact_type + type_extra, artifact_hash, '.toml')
                        tmp_local_path = LOCAL_REPO.metadata_path_of(
                            artifact_type + type_extra,
                            artifact_hash,
                            '.toml.tmp',
                        )
                        tmp_local_path.parent.mkdir(parents=True, exist_ok=True)
                        nfs.download(tmp_local_path, self.metadata_path_of(artifact_type + type_extra, artifact_hash))
                        tmp_local_path.rename(local_path)
        except (ConnectionError, PermissionError):
            raise
        except IOError as e:
            pass

    def hash_remote_file(self, remote_path: str, progress_bar=False) -> str:
        with self._connected() as nfs:
            hasher = hashlib.sha256(b'')
            for chunk in nfs.read_stream(remote_path, progress_bar):
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

