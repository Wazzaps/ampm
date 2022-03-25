import shlex
from pathlib import Path
from typing import Iterable, Optional

import toml

from ampm.repo.base import ArtifactRepo, ArtifactQuery, ArtifactMetadata, QueryNotFoundError


class LocalRepo(ArtifactRepo):
    def __init__(self, path: Path):
        self.path = path

    @staticmethod
    def from_uri_part(uri_part: str) -> "LocalRepo":
        return LocalRepo(Path(uri_part))

    def upload(self, metadata: ArtifactMetadata, local_path: Optional[Path]):
        raise NotImplementedError('LocalRepo does not support upload')

    def _lookup_by_type(self, artifact_type: str) -> Iterable[ArtifactMetadata]:
        for metadata_path in self.metadata_path_of(artifact_type, None, '').glob('**/*.toml'):
            yield ArtifactMetadata.from_dict(toml.load(metadata_path))

    def lookup(self, query: ArtifactQuery) -> Iterable[ArtifactMetadata]:
        if query.is_exact:
            try:
                yield self.metadata_of(query.type, query.hash)
            except FileNotFoundError:
                pass
        else:
            for metadata in self._lookup_by_type(query.type):
                artifact_attrs = metadata.combined_attrs

                for attr in query.attr:
                    if query.attr[attr].startswith('@'):
                        # TODO: Attribute filters
                        raise NotImplementedError('Attribute filters not supported yet')

                    if attr not in artifact_attrs or query.attr[attr] != artifact_attrs[attr]:
                        break
                else:
                    yield metadata

    def download(self, metadata: ArtifactMetadata) -> Path:
        path = self.path / 'artifacts' / metadata.type.lower() / metadata.hash.lower() / metadata.name
        if path.exists():
            return path
        else:
            raise QueryNotFoundError(None)

    def download_metadata_for_type(self, artifact_type: str):
        # No need to download metadata for local repo
        pass

    def hash_remote_file(self, path: str, progress_bar=False) -> str:
        raise NotImplementedError()

    def metadata_of(self, artifact_type: str, artifact_hash: str) -> ArtifactMetadata:
        return ArtifactMetadata.from_dict(toml.load(self.metadata_path_of(artifact_type, artifact_hash)))

    def metadata_path_of(self, artifact_type: str, artifact_hash: Optional[str], suffix='.toml') -> Path:
        return self.path / 'metadata' / artifact_type / ((artifact_hash or '') + suffix)

    def artifact_base_path_of(self, metadata: ArtifactMetadata, suffix='') -> Path:
        return self.path / 'artifacts' / metadata.type.lower() / (metadata.hash.lower() + suffix)

    def artifact_path_of(self, metadata: ArtifactMetadata, suffix='') -> Path:
        return self.artifact_base_path_of(metadata, suffix) / metadata.name

    def generate_caches_for_artifact(self, metadata: ArtifactMetadata):
        # Env file
        self.metadata_path_of(metadata.type, metadata.hash, '.env').write_text(self.format_env_file(metadata))

        # Target symlink
        target_file = self.metadata_path_of(metadata.type, metadata.hash, '.target')
        target_file.unlink(missing_ok=True)
        target_file.symlink_to(self.artifact_path_of(metadata))

    def format_env_file(self, metadata: ArtifactMetadata) -> str:
        base_dir = self.artifact_path_of(metadata)
        return '\n'.join(
            f'export {shlex.quote(k)}={shlex.quote(v.replace("${BASE_DIR}", str(base_dir)))}'
            for k, v in metadata.env.items()
        )


LOCAL_REPO = LocalRepo(Path('/var/ampm'))
