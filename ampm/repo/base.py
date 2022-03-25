import base64
import dataclasses
import datetime
import hashlib
import itertools
from functools import cached_property
from pathlib import Path
from typing import List, Dict, Optional, Iterable, Tuple

import toml

ARTIFACT_TYPES = ['file', 'dir', 'tar.gz', 'gz']

LOCAL_REPO_URI = 'file:///var/ampm'
REMOTE_REPO_URI = 'nfs://127.0.0.1/mnt/myshareddir'


def hash_buffer(buffer) -> str:
    return base64.b32encode(hashlib.sha256(buffer).digest()).decode("utf-8")[:32]


@dataclasses.dataclass(frozen=True)
class ArtifactMetadata:
    name: str
    description: str
    pubdate: datetime.datetime
    type: str
    attributes: Dict[str, str]
    env: Dict[str, str]
    path_type: str
    path_hash: Optional[str]
    path_location: Optional[str]

    def to_dict(self) -> Dict:
        result = {
            "artifact": {
                "name": self.name,
                "description": self.description,
                "pubdate": self.pubdate.isoformat(),
                "type": self.type,
            },
            "attributes": self.attributes,
            "env": self.env,
            "path": {
                "type": self.path_type,
            },
        }
        if self.path_location:
            result["path"]["location"] = self.path_location
        if self.path_hash:
            result["path"]["hash"] = self.path_hash
        return result

    @staticmethod
    def from_dict(data: Dict[str, any]) -> "ArtifactMetadata":
        assert data["path"]["type"] in ARTIFACT_TYPES

        return ArtifactMetadata(
            name=data["artifact"]["name"],
            description=data["artifact"]["description"],
            pubdate=datetime.datetime.fromisoformat(data["artifact"]["pubdate"]),
            type=data["artifact"]["type"],
            attributes=data["attributes"],
            env=data["env"],
            path_type=data["path"]["type"],
            path_location=data["path"].get("location", None),
            path_hash=data["path"].get("hash", None),
        )

    @cached_property
    def hash(self):
        return hash_buffer(toml.dumps(self.to_dict()).encode("utf-8")).lower()

    @property
    def path_suffix(self):
        if self.path_type == "file":
            return ''
        elif self.path_type == "dir":
            return ''
        elif self.path_type == "gz":
            return '.gz'
        elif self.path_type == "tar.gz":
            return '.tar.gz'
        else:
            raise ValueError(f"Unknown path type: {self.path_type}")

    @cached_property
    def combined_attrs(self):
        combined_attrs = {
            'name': self.name,
            'description': self.description,
            'pubdate': self.pubdate.astimezone().isoformat(sep=' '),
        }
        if self.path_location:
            combined_attrs['location'] = self.path_location
        combined_attrs.update(self.attributes)
        return combined_attrs


class ArtifactQuery:
    def __init__(self, identifier: str, attr: Dict[str, str]):
        self.type, _, self.hash = identifier.partition(':')
        self.attr = attr
        assert ':' not in self.hash, f'Invalid artifact hash: {identifier}'
        assert len(self.hash) == 32 or len(self.hash) == 0, f'Invalid hash length: {identifier}'

    @cached_property
    def is_exact(self):
        return bool(self.hash)

    def __str__(self):
        if self.hash:
            return f'{self.type}:{self.hash}'
        else:
            return f'{self.type}({", ".join(f"{k}={repr(v)}" for k, v in self.attr.items())})'


class ArtifactRepo:
    @staticmethod
    def by_uri(uri: str) -> "ArtifactRepo":
        assert '://' in uri, f'Server URI must be in the format `protocol://host/path`, ' \
                             f'e.g. `nfs://localhost/`, but got: {uri}'
        protocol, rest = uri.split("://", 1)

        if protocol == "file":
            from ampm.repo.local import LocalRepo
            return LocalRepo.from_uri_part(rest)
        if protocol == "nfs":
            from ampm.repo.nfs import NfsRepo
            return NfsRepo.from_uri_part(rest)
        else:
            raise ValueError(f"Unknown artifact repository protocol: {protocol}")

    @staticmethod
    def from_uri_part(uri_part: str) -> "ArtifactRepo":
        raise NotImplementedError()

    def upload(self, metadata: ArtifactMetadata, local_path: Optional[Path]):
        raise NotImplementedError()

    def lookup(self, query: ArtifactQuery) -> Iterable[ArtifactMetadata]:
        raise NotImplementedError()

    def download(self, artifact: ArtifactMetadata) -> Path:
        raise NotImplementedError()

    def download_metadata_for_type(self, artifact_type: str):
        raise NotImplementedError()

    def hash_remote_file(self, path: str, progress_bar=False) -> str:
        raise NotImplementedError()


class AmbiguousQueryError(Exception):
    def __init__(self, query: ArtifactQuery, options: List[ArtifactMetadata]):
        self.query = query
        self.options = options


class QueryNotFoundError(Exception):
    def __init__(self, query: Optional[ArtifactQuery]):
        self.query = query


class RepoGroup:
    def __init__(self, repos: Optional[List[ArtifactRepo]] = None, remote_uri: str = None):
        if repos is None:
            from ampm.repo.local import LOCAL_REPO

            repos = [LOCAL_REPO]
            if remote_uri:
                repos.append(ArtifactRepo.by_uri(remote_uri))

        self.repos = repos

    def lookup(self, query: ArtifactQuery) -> Iterable[ArtifactMetadata]:
        from ampm.repo.local import LOCAL_REPO

        if query.is_exact:
            for repo in self.repos:
                yield from repo.lookup(query)
        else:
            for repo in self.repos:
                repo.download_metadata_for_type(query.type)
            yield from LOCAL_REPO.lookup(query)

    def lookup_single(self, query: ArtifactQuery) -> ArtifactMetadata:
        if query.is_exact:
            results = list(itertools.islice(self.lookup(query), 1))
        else:
            results = list(self.lookup(query))

        if not results:
            raise QueryNotFoundError(query)

        if len(results) > 1:
            raise AmbiguousQueryError(query, results)

        return results[0]

    def get_single(self, query: ArtifactQuery) -> Tuple[Path, ArtifactMetadata]:
        metadata = self.lookup_single(query)

        for repo in self.repos:
            try:
                return repo.download(metadata), metadata
            except QueryNotFoundError:
                continue

        raise QueryNotFoundError(query)

    def download_metadata_for_type(self, artifact_type: str):
        for repo in self.repos:
            repo.download_metadata_for_type(artifact_type)
