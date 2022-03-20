import datetime
import hashlib
from pathlib import Path
from typing import List, Tuple, Optional, Dict

import toml

from artifact_store import ArtifactStore, ArtifactMetadata, ARTIFACT_TYPES
from nfs import NfsConnection
import time
import click


SHAREDIR_MOUNT_PATH = '/mnt/sharedir'
SHAREDIR_IP = '127.0.0.1'

cli = click.Group()


def _parse_dict(_ctx, _param, value: Tuple[str]):
    result = {}
    for item in value:
        if '=' not in item:
            raise click.BadParameter(f'Must but in the form "key=value", but got: "{item}"')
        key, value = item.split('=', 1)
        result[key] = value
    return result


@cli.command()
@click.argument(
    'LOCAL_PATH',
    type=click.Path(exists=True, path_type=Path),
    required=False
)
@click.option('--type', help='Artifact type', required=True)
@click.option('--name', help='Override artifact name (default: same as filename)')
@click.option('--description', help='Set artifact description (default: empty string)')
@click.option(
    '--compressed',
    type=bool,
    default=True,
    help='Store artifact in compressed format (.tar.gz or .gz) (default: True)'
)
@click.option(
    '--remote-path',
    help='Store artifact in specified location (default: /<type>/<hash>)',
)
@click.option('-a', '--attr', help='Artifact attributes', multiple=True, callback=_parse_dict)
@click.option('-e', '--env', help='Artifact environment vars', multiple=True, callback=_parse_dict)
def upload(
        local_path: Optional[Path],
        type: str,
        name: Optional[str],
        description: Optional[str],
        compressed: bool,
        remote_path: Optional[str],
        attr: Dict[str, str],
        env: Dict[str, str]
):
    """
        Upload LOCAL_PATH to artifact storage.

        If LOCAL_PATH is unspecified, assume already it's uploaded to value of `--remote-path`
    """
    assert not compressed, 'Compressed uploads are not supported yet'

    if local_path is None and name is None:
        raise click.BadParameter('If LOCAL_PATH is missing then --name must be specified')
    if local_path is None and remote_path is None:
        raise click.BadParameter('Must specify either LOCAL_PATH or --remote-path')

    with NfsConnection.connect(SHAREDIR_IP, SHAREDIR_MOUNT_PATH) as nfs:
        if local_path is not None:
            if local_path.is_dir():
                artifact_hash = None
                if compressed:
                    artifact_type = 'tar.gz'
                else:
                    artifact_type = 'dir'
            elif local_path.is_file():
                artifact_hash = hashlib.sha256(local_path.read_bytes()).hexdigest()
                if compressed:
                    artifact_type = 'gz'
                else:
                    artifact_type = 'file'
            else:
                raise click.BadParameter(f'Unsupported file type: {local_path} ({local_path.stat().st_type})')
        else:
            # Get hash of remote file
            hasher = hashlib.sha256(b'')
            for chunk in nfs.read_stream(remote_path, progress_bar=True):
                hasher.update(chunk)
            artifact_hash = hasher.hexdigest()

        meta = ArtifactMetadata(
            name=name or local_path.name,
            description=description or '',
            pubdate=datetime.datetime.now(tz=datetime.timezone.utc),
            type=type,
            attributes=attr,
            env=env,
            path_type=artifact_type,
            path_hash=artifact_hash,
            path_location=remote_path,
        )
        store = ArtifactStore(local_store=Path('/tmp/downloader'), nfs=nfs)
        print(store.upload_artifact(meta, local_path))


@cli.command()
@click.argument(
    'IDENTIFIER',
    required=False
)
def get(identifier: Optional[str]):
    """
        Fetch artifact from artifact storage.

        IDENTIFIER is in the format <type>:<hash>, e.g.: foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2
    """

    identifier = identifier.split(':', 1)
    if len(identifier) != 2:
        raise click.BadParameter(f'IDENTIFIER must be in the format <type>:<hash>, but got: "{identifier[0]}"')

    # TODO: Don't connect to NFS every time
    with NfsConnection.connect(SHAREDIR_IP, SHAREDIR_MOUNT_PATH) as nfs:
        store = ArtifactStore(Path('/tmp/downloader'), nfs)
        print(store.get_artifact_by_type_hash(identifier[0], identifier[1]))


def main():
    cli()


if __name__ == '__main__':
    main()
