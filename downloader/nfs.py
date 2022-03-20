import contextlib
import os
from typing import List

import pyNfsClient.utils
import tqdm
from pyNfsClient import (Portmap, Mount, NFSv3, MNT3_OK, NFS_PROGRAM,
                         NFS_V3, NFS3_OK, DATA_SYNC, UNCHECKED, NFS3ERR_EXIST, UNSTABLE)


# Hotpatch pyNfsClient's `str_to_bytes` to accept bytes
def str_to_bytes(str_v):
    if isinstance(str_v, str):
        return str(str_v).encode()
    elif isinstance(str_v, bytes):
        return str_v
    else:
        raise TypeError("str_to_bytes: str or bytes expected")


pyNfsClient.utils.str_to_bytes.__code__ = str_to_bytes.__code__


class NfsConnection:
    def __init__(self, nfs3: NFSv3, root_fh: bytes):
        self.nfs3 = nfs3
        self.root_fh = root_fh

    def _splitpath(self, path: str) -> List[str]:
        path = list(path.strip('/').split('/'))
        while '' in path:
            path.remove('')
        while '.' in path:
            path.remove('.')
        return path

    # Returns (file handle, file attributes)
    def _open(self, path: List[str]) -> (bytes, dict):
        fh = self.root_fh
        attrs = {}
        for path_part in path:
            lookup_res = self.nfs3.lookup(fh, path_part)
            if lookup_res["status"] == NFS3_OK:
                fh = lookup_res["resok"]["object"]["data"]
                attrs = lookup_res["resok"]["obj_attributes"]["attributes"]
                print(lookup_res)
            else:
                raise IOError("NFS lookup failed")

        return fh, attrs

    def _create_with_dirs(self, path: List[str]):
        dir_fh = self.root_fh
        for path_part in path[:-1]:
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

        create_res = self.nfs3.create(dir_fh, path[-1], UNCHECKED, mode=0o777, size=0)
        # print('--- create_res ---')
        # print(create_res)
        if create_res["status"] == NFS3_OK:
            return create_res["resok"]["obj"]["handle"]["data"]
        else:
            raise IOError("NFS create failed")

    def list_dir(self, path):
        fh, _attrs = self._open(self._splitpath(path))
        readdir_res = self.nfs3.readdir(fh)
        if readdir_res["status"] == NFS3_OK:
            entry = readdir_res["resok"]["reply"]["entries"]
            while entry:
                yield entry[0]['name']
                entry = entry[0]['nextentry']

    def read(self, path: str, chunk_size: int = 1024 * 50, progress_bar=False):
        path = self._splitpath(path)
        fh, attrs = self._open(path)
        if attrs["type"] != 1:
            raise IOError("Tried to read a non-file")

        print(attrs)

    def write(self, path: str, contents: bytes, chunk_size: int = 1024 * 50, progress_bar=False):
        path = self._splitpath(path)
        fh = self._create_with_dirs(path)

        it = range(0, len(contents), chunk_size)
        if progress_bar:
            it = tqdm.tqdm(it, desc=f"Writing {path[-1]}", unit_scale=int(chunk_size / 1024), unit='KB')

        for i in it:
            chunk = contents[i:i + chunk_size]
            write_res = self.nfs3.write(fh, offset=i, count=len(chunk), content=chunk, stable_how=UNSTABLE)
            # print('--- write_res ---')
            # print(write_res)
            if write_res["status"] == NFS3_OK:
                assert write_res["resok"]["count"] == len(chunk)
            else:
                raise IOError("NFS write failed")


@contextlib.contextmanager
def nfs_connection(host, mount_path):
    auth = {"flavor": 1,
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
    mnt_res = mount.mnt(mount_path, auth)
    if mnt_res["status"] == MNT3_OK:
        root_fh = mnt_res["mountinfo"]["fhandle"]
        nfs3 = None
        try:
            nfs_port = portmap.getport(NFS_PROGRAM, NFS_V3)
            # nfs actions
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
        print("Mount failed")
        mount.disconnect()
        portmap.disconnect()
