import contextlib
import os
from math import ceil
from pathlib import Path
from typing import List, Iterable, ContextManager, Optional

import pyNfsClient.utils
import tqdm
from pyNfsClient import (Portmap, Mount, NFSv3, MNT3_OK, NFS_PROGRAM,
                         NFS_V3, NFS3_OK, UNCHECKED, NFS3ERR_EXIST, UNSTABLE)


# Hotpatch pyNfsClient's `str_to_bytes` to accept bytes
def str_to_bytes(str_v):
    if isinstance(str_v, str):
        return str_v.encode()
    elif isinstance(str_v, bytes):
        return str_v
    else:
        raise TypeError("str_to_bytes: str or bytes expected")


pyNfsClient.utils.str_to_bytes.__code__ = str_to_bytes.__code__


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
            total_size += os.path.getsize(fp)
    return total_size


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
            raise ConnectionError(f"NFS mount failed: code={mnt_res['status']}", mnt_res)

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
                raise IOError("NFS lookup failed")

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
                    raise IOError("NFS lookup failed")
            else:
                raise IOError("NFS mkdir failed")

        return dir_fh

    def _create_with_dirs(self, remote_path: List[str]):
        dir_fh = self._mkdir_recursive(remote_path[:-1])

        create_res = self.nfs3.create(dir_fh, remote_path[-1], UNCHECKED, mode=0o777, size=0)
        # print('--- create_res ---')
        # print(create_res)
        if create_res["status"] == NFS3_OK:
            return create_res["resok"]["obj"]["handle"]["data"]
        else:
            raise IOError(f"NFS create failed: code={create_res['status']}")

    def list_dir(self, remote_path: str):
        fh, _attrs = self._open(self._splitpath(remote_path))
        readdir_res = self.nfs3.readdir(fh)
        if readdir_res["status"] == NFS3_OK:
            entry = readdir_res["resok"]["reply"]["entries"]
            while entry:
                yield entry[0]['name']
                entry = entry[0]['nextentry']

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
            raise IOError("NFS symlink failed")

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
            else:
                raise IOError("NFS read failed")

        if bar:
            bar.reset()
            bar.update(bar.total)
            bar.close()

    def read(self, remote_path: str, chunk_size: int = 1024 * 50, progress_bar=False):
        return b''.join(list(self.read_stream(remote_path, chunk_size, progress_bar)))

    def download(self, local_path: Path, remote_path: str, chunk_size: int = 1024 * 50, progress_bar=False):
        with open(local_path, 'wb') as f:
            for chunk in self.read_stream(remote_path, chunk_size, progress_bar):
                f.write(chunk)

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
                raise IOError("NFS write failed")

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
