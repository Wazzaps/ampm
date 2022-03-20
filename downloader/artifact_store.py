import base64
import dataclasses
import datetime
import hashlib
import toml
from pathlib import Path
from typing import Dict, Optional
from functools import cached_property

from nfs import NfsConnection


ARTIFACT_TYPES = ['file', 'dir', 'tar.gz', 'gz']


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
            pubdate=data["artifact"]["pubdate"],
            type=data["artifact"]["type"],
            attributes=data["attributes"],
            env=data["env"],
            path_type=data["path"]["type"],
            path_location=data["path"].get("location", None),
            path_hash=data["path"]["hash"],
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
        elif self.path_type == "tar.gz":
            return '.tar.gz'


class ArtifactStore:
    def __init__(self, local_store: Path, nfs: NfsConnection):
        self.local_store = local_store
        self.nfs = nfs

    def _artifact_path(self, artifact_type: str, artifact_hash: str, suffix: str = '') -> Path:
        assert len(artifact_hash) == 32, f'Invalid artifact hash: {artifact_hash}'
        return self.local_store / 'artifacts' / artifact_type.lower() / (artifact_hash.lower() + suffix)

    def _metadata_path(self, artifact_type: str, artifact_hash: str, suffix: str = '') -> Path:
        assert len(artifact_hash) == 32, f'Invalid artifact hash: {artifact_hash}'
        return self.local_store / 'metadata' / artifact_type.lower() / (artifact_hash.lower() + '.toml' + suffix)

    # TODO: Maybe don't add name for dirs?
    @staticmethod
    def _remote_artifact_path(artifact_type: str, artifact_hash: str, suffix: str = '') -> str:
        assert len(artifact_hash) == 32, f'Invalid artifact hash: {artifact_hash}'
        return str(Path('artifacts') / artifact_type.lower() / (artifact_hash.lower() + suffix))

    @staticmethod
    def _remote_metadata_path(artifact_type: str, artifact_hash: str, suffix: str = '') -> str:
        assert len(artifact_hash) == 32, f'Invalid artifact hash: {artifact_hash}'
        return str(Path('metadata') / artifact_type.lower() / (artifact_hash.lower() + '.toml' + suffix))

    def get_metadata_by_type_hash(self, artifact_type: str, artifact_hash: str) -> ArtifactMetadata:
        metadata_path = self._metadata_path(artifact_type, artifact_hash)

        if not metadata_path.exists():
            # print('Downloading metadata')
            tmp_metadata_path = self._metadata_path(artifact_type, artifact_hash, '.tmp')
            tmp_metadata_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                self.nfs.download(tmp_metadata_path, self._remote_metadata_path(artifact_type, artifact_hash))
            except IOError:
                raise FileNotFoundError(f'Artifact {artifact_type}:{artifact_hash} not found')
            tmp_metadata_path.rename(metadata_path)

        return ArtifactMetadata.from_dict(toml.load(metadata_path))

    def get_artifact_by_type_hash(self, artifact_type: str, artifact_hash: str) -> Path:
        artifact_metadata = self.get_metadata_by_type_hash(artifact_type, artifact_hash)
        assert artifact_metadata.path_type in ('file', 'dir'), 'Downloading compressed artifacts not supported yet'
        artifact_path = self._artifact_path(artifact_type, artifact_hash)

        if not artifact_path.exists():
            # print('Downloading artifact')

            artifact_suffix = artifact_metadata.path_suffix
            if artifact_metadata.path_location:
                remote_artifact_path = artifact_metadata.path_location
            else:
                remote_artifact_path = self._remote_artifact_path(
                    artifact_type, artifact_hash, artifact_suffix
                ) + '/' + artifact_metadata.name

            tmp_artifact_path = self._artifact_path(artifact_type, artifact_hash, suffix='.tmp')
            tmp_artifact_path.mkdir(parents=True, exist_ok=True)

            if artifact_metadata.path_type == 'file':
                self.nfs.download(tmp_artifact_path / artifact_metadata.name, remote_artifact_path, progress_bar=True)
            elif artifact_metadata.path_type == 'dir':
                tmp_artifact_path.mkdir(parents=True, exist_ok=True)

            tmp_artifact_path.rename(artifact_path)

        return artifact_path / artifact_metadata.name

    def upload_artifact(self, metadata: ArtifactMetadata, local_path: Optional[Path]):
        assert metadata.path_type in ARTIFACT_TYPES, f'Invalid artifact path type: {metadata.path_type}'

        if local_path is not None:
            if metadata.path_location:
                tmp_remote_path = metadata.path_location + '.tmp'
                remote_path = metadata.path_location
            else:
                tmp_remote_path = self._remote_artifact_path(metadata.type, metadata.hash, metadata.path_suffix + '.tmp')
                remote_path = self._remote_artifact_path(metadata.type, metadata.hash, metadata.path_suffix)
            self.nfs.upload(local_path, f'{tmp_remote_path}/{metadata.name}', allow_dir=True, progress_bar=True)
            self.nfs.rename(tmp_remote_path, remote_path)

        tmp_remote_metadata_path = self._remote_metadata_path(metadata.type, metadata.hash, '.tmp')
        remote_metadata_path = self._remote_metadata_path(metadata.type, metadata.hash)
        self.nfs.write(toml.dumps(metadata.to_dict()).encode('utf-8'), tmp_remote_metadata_path)
        self.nfs.rename(tmp_remote_metadata_path, remote_metadata_path)

        return f'{metadata.type}:{metadata.hash}'