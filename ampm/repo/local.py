import collections
import shlex
import toml
from functools import cmp_to_key
from pathlib import Path
from typing import Iterable, Optional
from ampm.attribute_comparators import COMPARATORS
from ampm.repo.base import ArtifactRepo, ArtifactQuery, ArtifactMetadata, QueryNotFoundError, AmbiguousComparisonError


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
        elif any(v.startswith('@') for v in query.attr.values()) or any(k.startswith('@') for k in query.attr.keys()):
            filtered_attrs = {}
            compared_attr = None
            ignored_attrs = {'name', 'description', 'pubdate'}
            any_attr_ignored = False

            # Find the compared attr, the filtered attrs and the ignored attrs
            for attr in query.attr:
                if attr == '@any':
                    any_attr_ignored = True
                elif attr.startswith('@'):
                    raise ValueError(f'Invalid attribute: {attr}')
                elif query.attr[attr].startswith('@'):
                    compare_type, _, compare_param = query.attr[attr].partition(':')
                    if compare_type == '@ignore':
                        if compare_param != '':
                            raise ValueError(f'@ignore takes no params: {attr}')
                        ignored_attrs.add(attr)
                    elif compare_type in COMPARATORS.keys():
                        if compared_attr:
                            raise ValueError(f'Only one attribute can be compared: {attr}, '
                                             f'already comparing using {compared_attr[0]}')
                        compared_attr = (attr, compare_type, compare_param)
                    else:
                        raise ValueError(f'Invalid attribute: {attr}')
                else:
                    filtered_attrs[attr] = query.attr[attr]

            if not compared_attr:
                raise ValueError(f"Couldn't find an attribute to compare artifacts with, "
                                 f"try using one of: {', '.join(COMPARATORS.keys())}")

            # Filter metadata by the filtered attrs and by comparator
            comparator = COMPARATORS[compared_attr[1]]
            matched_metadata = []
            all_seen_attrs = set()
            for metadata in self._lookup_by_type(query.type):
                artifact_attrs = metadata.combined_attrs
                for attr in filtered_attrs:
                    if attr not in artifact_attrs or filtered_attrs[attr] != artifact_attrs[attr]:
                        break
                else:
                    if comparator.filter(compared_attr[2], metadata.combined_attrs[compared_attr[0]]):
                        for attr in artifact_attrs:
                            all_seen_attrs.add(attr)
                        matched_metadata.append(metadata)

            # If no results, early exit
            if not matched_metadata:
                return

            # Collect all attributes which may be relevant
            groupby_attrs = all_seen_attrs.copy()
            groupby_attrs.difference_update(ignored_attrs)
            groupby_attrs.difference_update(filtered_attrs)
            if compared_attr[0] in groupby_attrs:
                groupby_attrs.remove(compared_attr[0])
            groupby_attrs = list(sorted(groupby_attrs))

            if any_attr_ignored:
                groupby_attrs = []

            # Group metadata by the attributes may be relevant
            groups = collections.defaultdict(list)
            for metadata in matched_metadata:
                groups[tuple(metadata.combined_attrs.get(attr, None) for attr in groupby_attrs)].append(
                    (metadata, metadata.combined_attrs[compared_attr[0]])
                )

            # Sort the groups by the compared attribute
            for group in groups:
                groups[group].sort(key=cmp_to_key(lambda x, y: comparator.compare(compared_attr[2], x[1], y[1])))

            # Warn if multiple groups with different values of the compared attribute
            first_group = next(iter(groups.values()))
            if len(groups) > 1:
                for group in groups.values():
                    if comparator.compare(compared_attr[2], group[0][1], first_group[0][1]) != 0:
                        raise AmbiguousComparisonError(
                            f'Error: Returning multiple values of `{compared_attr[0]}` because the attribute(s) '
                            f'{", ".join(f"`{a}`" for a in groupby_attrs)} are not unique.\n'
                            f'- Artifact 1 = {first_group[0][0].combined_attrs}\n'
                            f'- Artifact 2 = {group[0][0].combined_attrs}\n'
                            f'Try adding `{" ".join(f"-a {a}=@ignore" for a in groupby_attrs)}` '
                            f'(or just `-a @any=@ignore`) to the query to ignore these attributes when grouping.'
                        )

            # Select the most relevant artifact in each group
            results = []
            for key, group in groups.items():
                for res in group:
                    if comparator.compare(compared_attr[2], res[1], group[0][1]) == 0:
                        results.append(res[0])
                    else:
                        break

            # Return the results
            for v in results:
                yield v

        else:
            for metadata in self._lookup_by_type(query.type):
                artifact_attrs = metadata.combined_attrs

                for attr in query.attr:
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
        return self.path / 'metadata' / artifact_type.strip('/') / ((artifact_hash or '') + suffix)

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
