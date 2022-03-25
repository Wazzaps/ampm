import contextlib
import datetime
import hashlib
import json
import sys
from pathlib import Path

import click
import colorama
from typing import Tuple, Optional, Dict

from ampm.repo.base import ArtifactQuery, AmbiguousQueryError, RepoGroup, QueryNotFoundError, \
    ArtifactMetadata, ArtifactRepo, REMOTE_REPO_URI
from ampm.repo.local import LOCAL_REPO

cli = click.Group()


def _parse_dict(_ctx, _param, value: Tuple[str]):
    result = {}
    for item in value:
        if '=' not in item:
            raise click.BadParameter(f'Must but in the form "key=value", but got: "{item}"')
        key, value = item.split('=', 1)
        result[key] = value
    return result


@contextlib.contextmanager
def handle_common_errors():
    try:
        yield
    except AmbiguousQueryError as e:
        print(
            f'Ambiguous artifact query: {e.query}, found multiple options:\n' +
            '\n\n'.join(_format_artifact_metadata(artifact_metadata) for artifact_metadata in e.options),
            file=sys.stderr
        )
        sys.exit(1)
    except QueryNotFoundError as e:
        print(f'Artifact not found matching query: {e.query}', file=sys.stderr)
        sys.exit(1)
    except PermissionError:
        local_repo_path = str(LOCAL_REPO.path)
        print(f'The local artifact store ({local_repo_path}) doesn\'t exist and you\'re not root. '
              f'Please run `sudo mkdir {local_repo_path} && sudo chmod 777 {local_repo_path}`.', file=sys.stderr)
        sys.exit(1)
    except ConnectionError as e:
        print(f'Remote repo cannot be contacted: {" ".join(str(a) for a in e.args)}')
        sys.exit(1)


def _format_artifact_metadata(artifact_metadata: ArtifactMetadata) -> str:
    combined_attrs = artifact_metadata.combined_attrs

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


@cli.command()
@click.argument(
    'IDENTIFIER',
    required=True
)
@click.option('-a', '--attr', help='Artifact attributes', multiple=True, callback=_parse_dict)
def get(identifier: str, attr: Dict[str, str]):
    """
        Fetch artifact from artifact storage, then print its local path

        IDENTIFIER is in the format <type>:<hash>, e.g.: foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2
    """

    with handle_common_errors():
        repos = RepoGroup()
        query = ArtifactQuery(identifier, attr)

        local_path, _metadata = repos.get_single(query)

        print(local_path)


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

    with handle_common_errors():
        repos = RepoGroup()
        query = ArtifactQuery(identifier, attr)

        if query.is_exact:
            artifacts = [repos.lookup_single(query)]
        else:
            repos.download_metadata_for_type(query.type)
            artifacts = list(LOCAL_REPO.lookup(query))

        if output_format == 'pretty':
            print('\n\n'.join(_format_artifact_metadata(artifact_metadata) for artifact_metadata in artifacts))
        elif output_format == 'json':
            print(json.dumps([artifact_metadata.to_dict() for artifact_metadata in artifacts], indent=4))
        else:
            raise ValueError(f'Unknown output format: {output_format}')


@cli.command()
@click.argument(
    'IDENTIFIER',
    required=True
)
@click.option('-a', '--attr', help='Artifact attributes', multiple=True, callback=_parse_dict)
def env(identifier: str, attr: Dict[str, str]):
    """
        Fetch artifact from artifact storage, then print its environment variables

        IDENTIFIER is in the format <type>:<hash>, e.g.: foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2
    """

    with handle_common_errors():
        repos = RepoGroup()
        query = ArtifactQuery(identifier, attr)

        _local_path, metadata = repos.get_single(query)

        print(LOCAL_REPO.format_env_file(metadata))


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
@click.option(
    '--remote-repo',
    help='Repository to store artifact in',
    default=REMOTE_REPO_URI,
    show_default=True
)
def upload(
        local_path: Optional[Path],
        type: str,
        name: Optional[str],
        description: Optional[str],
        compressed: bool,
        remote_path: Optional[str],
        attr: Dict[str, str],
        env: Dict[str, str],
        remote_repo: str,
):
    """
        Upload artifact to artifact storage.

        If LOCAL_PATH is unspecified, assume already it's uploaded to value of `--remote-path`
    """
    assert not compressed, 'Compressed uploads are not supported yet'

    if local_path is None and remote_path is None:
        raise click.BadParameter('Must specify either LOCAL_PATH or --remote-path')

    remote_repo = ArtifactRepo.by_uri(remote_repo)

    if local_path is not None:
        name = name or local_path.name
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
        name = name or remote_path.strip('/').split('/')[-1]
        try:
            artifact_type = 'file'
            artifact_hash = remote_repo.hash_remote_file(remote_path, progress_bar=True)
        except IsADirectoryError:
            artifact_type = 'dir'
            artifact_hash = None

    meta = ArtifactMetadata(
        name=name,
        description=description or '',
        pubdate=datetime.datetime.now(tz=datetime.timezone.utc),
        type=type,
        attributes=attr,
        env=env,
        path_type=artifact_type,
        path_hash=artifact_hash,
        path_location=remote_path,
    )

    remote_repo.upload(meta, local_path)

    print(f'{meta.type}:{meta.hash}')


@cli.command()
@click.option('--remote', is_flag=True, default=False, help='Garbage collect on remote storage instead')
def gc(remote: bool):
    raise NotImplementedError()


def main():
    cli()


if __name__ == '__main__':
    main()
