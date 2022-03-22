import datetime
import hashlib
import json
import sys
import click
import colorama
from pathlib import Path
from typing import Tuple, Optional, Dict
from ampm.artifact_store import ArtifactStore, ArtifactMetadata
from ampm.nfs import NfsConnection

LOCAL_STORE = Path('/var/ampm')

SHAREDIR_MOUNT_PATH = '/mnt/myshareddir'
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
    '--compressed/--uncompressed',
    default=True,
    help='Store artifact in compressed format (.tar.gz or .gz) (default: compressed)'
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
        Upload artifact to artifact storage.

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
        store = ArtifactStore(local_store=LOCAL_STORE, nfs=nfs)
        print(store.upload_artifact(meta, local_path))


@cli.command()
@click.argument(
    'IDENTIFIER',
    required=True
)
@click.option('-a', '--attr', help='Artifact attributes', multiple=True, callback=_parse_dict)
def get(identifier: str, attr: Dict[str, str]):
    """
        Fetch artifact from artifact storage.

        IDENTIFIER is in the format <type>:<hash>, e.g.: foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2
    """

    # TODO: Don't connect to NFS every time
    try:
        with NfsConnection.connect(SHAREDIR_IP, SHAREDIR_MOUNT_PATH) as nfs:
            store = ArtifactStore(local_store=LOCAL_STORE, nfs=nfs)
            artifacts = list(store.find_artifacts(identifier, attr))
            if len(artifacts) == 0:
                raise FileNotFoundError(f'Artifact not found: {identifier}')
            elif len(artifacts) > 1:
                raise LookupError(
                    f'Ambiguous artifact identifier: {identifier}, found multiple options:\n' +
                    '\n\n'.join(_format_artifact_metadata(artifact_metadata) for artifact_metadata in artifacts)
                )
            print(store.get_artifact_by_metadata(artifacts[0]))
    except (LookupError, FileNotFoundError) as e:
        print(' '.join(e.args), file=sys.stderr)
        sys.exit(1)
    except PermissionError:
        print(f'The local artifact store ({str(LOCAL_STORE)}) doesn\'t exist and you\'re not root. '
              f'Please run `sudo mkdir /var/ampm && sudo chmod 777 /var/ampm`.', file=sys.stderr)
        sys.exit(1)


def _format_artifact_metadata(artifact_metadata: ArtifactMetadata) -> str:
    combined_attrs = {
        'name': artifact_metadata.name,
        'description': artifact_metadata.description,
        'pubdate': artifact_metadata.pubdate.isoformat(sep=' '),
    }
    if artifact_metadata.path_location:
        combined_attrs['location'] = artifact_metadata.path_location
    combined_attrs.update(artifact_metadata.attributes)

    INDENT = 4
    SPACER = ', '
    MAX_LINE_LENGTH = 120
    parts = [
        f'{colorama.Fore.LIGHTGREEN_EX}{k}{colorama.Fore.RESET}='
        f'{colorama.Fore.LIGHTYELLOW_EX}{repr(v)}{colorama.Fore.RESET}'
        for k, v in combined_attrs.items()
    ]
    combined_attrs = ' ' * INDENT
    curr_line_len = 0

    for part in parts:
        if (curr_line_len + len(part) + len(SPACER) > MAX_LINE_LENGTH - INDENT) and curr_line_len > 0:
            combined_attrs += '\n' + ' ' * INDENT
            curr_line_len = 0

        if curr_line_len > 0:
            combined_attrs += SPACER

        combined_attrs += part
        curr_line_len += len(part) + len(SPACER)

    return f'{colorama.Style.BRIGHT}{artifact_metadata.type}{colorama.Style.RESET_ALL}' \
           f'{colorama.Fore.LIGHTBLACK_EX}:{artifact_metadata.hash}{colorama.Fore.RESET}' \
           f'\n{combined_attrs}'


@cli.command(name='list')
@click.argument(
    'IDENTIFIER',
    required=True
)
@click.option('-a', '--attr', help='Artifact attributes', multiple=True, callback=_parse_dict)
@click.option(
    '-f', '--format', 'output_format',
    type=click.Choice(['pretty', 'json']),
    help='Output format',
    default='pretty',
)
def list_(identifier: Optional[str], attr: Dict[str, str], output_format: str):
    """
        Get info about artifacts

        IDENTIFIER is in the format <type>:<hash>, e.g.: foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2.
        or <type> e.g.: foobar

        You may specify attributes to filter down the results
    """

    # TODO: Don't connect to NFS every time
    try:
        with NfsConnection.connect(SHAREDIR_IP, SHAREDIR_MOUNT_PATH) as nfs:
            store = ArtifactStore(local_store=LOCAL_STORE, nfs=nfs)
            artifacts = store.find_artifacts(identifier, attr)
            if output_format == 'pretty':
                print('\n\n'.join(_format_artifact_metadata(artifact_metadata) for artifact_metadata in artifacts))
            elif output_format == 'json':
                print(json.dumps([artifact_metadata.to_dict() for artifact_metadata in artifacts], indent=4))
            else:
                raise ValueError(f'Unknown output format: {output_format}')
    except (LookupError, FileNotFoundError) as e:
        print(' '.join(e.args), file=sys.stderr)
        sys.exit(1)
    except PermissionError:
        print(f'The local artifact store ({str(LOCAL_STORE)}) doesn\'t exist and you\'re not root. '
              f'Please run `sudo mkdir /var/ampm && sudo chmod 777 /var/ampm`.', file=sys.stderr)
        sys.exit(1)


@cli.command()
def env():
    raise NotImplementedError()


@cli.command()
@click.option('--remote', is_flag=True, default=False, help='Garbage collect on remote storage instead')
def gc(remote: bool):
    raise NotImplementedError()


def main():
    cli()


if __name__ == '__main__':
    main()
