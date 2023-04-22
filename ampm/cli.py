import collections
import contextlib
import datetime
import gzip
import json
import os
import subprocess
import tempfile
import sys
import tarfile
import tqdm
import click
import colorama
from typing import Tuple, Optional, Dict, Mapping, List
from math import ceil
from pathlib import Path
from ampm.repo.base import ArtifactQuery, AmbiguousQueryError, RepoGroup, QueryNotFoundError, \
    ArtifactMetadata, ArtifactRepo, REMOTE_REPO_URI, AmbiguousComparisonError, NiceTrySagi
from ampm.repo.local import LOCAL_REPO
from ampm import __version__
from ampm.repo.nfs import NfsRepo
from ampm.utils import _calc_dir_size, randbytes, hash_local_file, remove_atexit


class OrderedGroup(click.Group):
    def __init__(self, name: Optional[str] = None, commands: Optional[Mapping[str, click.Command]] = None, **kwargs):
        super(OrderedGroup, self).__init__(name, commands, **kwargs)
        # the registered subcommands by their exported names.
        self.commands = commands or collections.OrderedDict()

    def list_commands(self, ctx: click.Context) -> Mapping[str, click.Command]:
        return self.commands


@click.group(cls=OrderedGroup)
@click.version_option(__version__)
@click.option('-s', '--server', help=f'Remote repository server (default: {REMOTE_REPO_URI})')
@click.option('--offline', is_flag=True, help=f'Don\'t try to contact the remote repository server')
@click.pass_context
def cli(ctx: click.Context, server: Optional[str], offline: bool):
    ctx.ensure_object(dict)

    ctx.obj['server'] = None
    if not offline:
        if server is not None:
            ctx.obj['server'] = server
        elif 'AMPM_SERVER' in os.environ:
            ctx.obj['server'] = os.environ['AMPM_SERVER']
        else:
            ctx.obj['server'] = REMOTE_REPO_URI


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
        local_user = os.environ.get('USER', 'YOUR_USER')
        print(f'The local artifact store ({local_repo_path}) doesn\'t exist and you\'re not root. '
              f'Please run `sudo mkdir -p {local_repo_path} && sudo chown -R {local_user}:{local_user} {local_repo_path}`.', file=sys.stderr)
        sys.exit(1)
    except ConnectionError as e:
        print(f'Remote repo cannot be contacted: {" ".join(str(a) for a in e.args)}', file=sys.stderr)
        sys.exit(1)
    except AmbiguousComparisonError as e:
        print(f'{" ".join(str(a) for a in e.args)}', file=sys.stderr)
        sys.exit(1)
    except NiceTrySagi as e:
        print(f'NiceTrySagi: {" ".join(str(a) for a in e.args)}', file=sys.stderr)
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


def _short_format_artifact_metadata(artifact_metadata: ArtifactMetadata) -> str:
    combined_attrs = artifact_metadata.combined_attrs

    parts = [
        f'{k}={repr(v)}'
        for k, v in combined_attrs.items()
    ]

    return f'{artifact_metadata.type}:{artifact_metadata.hash}\t{" ".join(sorted(parts))}'


def _make_link(artifact_metadata: ArtifactMetadata, prefix: str) -> str:
    return f'{prefix}/{artifact_metadata.type.lower()}/{artifact_metadata.hash.lower()}/{artifact_metadata.name}'


def _index_file_format_artifact_metadata(artifact_metadata: ArtifactMetadata, prefix: str) -> str:
    combined_attrs = artifact_metadata.combined_attrs

    parts = [
        f'{k}={repr(v)}'
        for k, v in combined_attrs.items()
    ]
    return f'{artifact_metadata.type}:{artifact_metadata.hash}\t' \
           f'{" ".join(sorted(parts))}\t' \
           f'{_make_link(artifact_metadata, prefix)}'


def _index_web_format_artifact_metadata(artifacts: List[ArtifactMetadata], index_webpage_template: str, prefix: str) -> str:
    preamble, _, rest = index_webpage_template.partition('{{foreach_artifact}}')
    foreach_artifact, _, postamble = rest.partition('{{end_foreach_artifact}}')
    artifacts = list(artifacts)

    def process_global_changes(inp, artifact_count):
        return inp \
            .replace('{{pretty_count}}', f'{artifact_count} Result' + ('s' if artifact_count != 1 else '')) \
            .replace('{{build_date}}', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    def process_artifact_changes(inp, artifact_count, artifact):
        return process_global_changes(inp, artifact_count) \
            .replace('{{quoted_type}}', f'"{artifact.type}"') \
            .replace('{{ident}}', f'{artifact.type}:{artifact.hash}') \
            .replace('{{name}}', artifact.name)\
            .replace('{{quoted_link}}', f'"{_make_link(artifact, prefix)}"')

    def process_attr_changes(inp, artifact_count, artifact, k, v):
        return process_artifact_changes(inp, artifact_count, artifact) \
            .replace('{{attr_name}}', k) \
            .replace('{{attr_val}}', v)

    result = process_global_changes(preamble, len(artifacts))
    for artifact in artifacts:
        artifact_preamble, _, rest = foreach_artifact.partition('{{foreach_attr}}')
        foreach_attr, _, artifact_postamble = rest.partition('{{end_foreach_attr}}')
        artifact_result = process_artifact_changes(artifact_preamble, len(artifacts), artifact)

        attrs = artifact.combined_attrs.copy()
        # Name is shown elsewhere
        del attrs['name']
        # Show description first, if not empty, and pubdate last
        desc = attrs.pop('description', None)
        pubdate = attrs.pop('pubdate')
        attrs = list(attrs.items())
        if desc:
            attrs.insert(0, ('description', desc))
        attrs.append(('pubdate', pubdate))

        for k, v in attrs:
            artifact_result += process_attr_changes(foreach_attr, len(artifacts), artifact, k, v)

        artifact_result += process_artifact_changes(artifact_postamble, len(artifacts), artifact)
        result += artifact_result

    result += process_global_changes(postamble, len(artifacts))
    return result


def verify_type(artifact_type: str):
    if ':' in artifact_type:
        raise click.BadParameter(f'Artifact type cannot contain ":": {artifact_type}')
    if '/.' in artifact_type:
        raise click.BadParameter(f'Artifact type cannot contain "/.": {artifact_type}')
    if artifact_type.startswith('.'):
        raise click.BadParameter(f'Artifact type cannot start with ".": {artifact_type}')


def verify_attributes(attrs: Dict[str, str]):
    for attr in list(attrs.keys()) + list(attrs.values()):
        if attr.startswith('@'):
            raise click.BadParameter(f'Attribute cannot start with "@": {attr}')


@cli.command()
@click.argument(
    'IDENTIFIER',
    required=True
)
@click.option('-a', '--attr', help='Artifact attributes', multiple=True, callback=_parse_dict)
@click.pass_context
def get(ctx: click.Context, identifier: str, attr: Dict[str, str]):
    """
        Fetch artifact, then print its local path

        IDENTIFIER is in the format <type>:<hash>, e.g.: foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2
    """

    with handle_common_errors():
        repos = RepoGroup(remote_uri=ctx.obj['server'])
        query = ArtifactQuery(identifier, attr)

        local_path, _metadata = repos.get_single(query)

        print(local_path)


@cli.command(name='list')
@click.argument('IDENTIFIER', required=False)
@click.option('-a', '--attr', help='Artifact attributes', multiple=True, callback=_parse_dict)
@click.option('--index-file-prefix', help='When using --format=index-file or --format=index-webpage, add this prefix to the artifact file path')
@click.option('--index-webpage-template', help='When using --format=index-webpage, use this as a template', type=click.Path(exists=True, path_type=Path))
@click.option(
    '-f', '--format', 'output_format',
    type=click.Choice(['pretty', 'json', 'short', 'index-file', 'index-webpage']),
    help='Output format',
    default='pretty',
)
@click.pass_context
def list_(
    ctx: click.Context,
    identifier: Optional[str],
    attr: Dict[str, str],
    index_file_prefix: Optional[str],
    index_webpage_template: Optional[Path],
    output_format: str
):
    """
        Get info about artifacts

        IDENTIFIER is in the format <type>:<hash>, e.g.: foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2.
        or <type> e.g.: foobar

        You may specify attributes to filter down the results
    """

    with handle_common_errors():
        repos = RepoGroup(remote_uri=ctx.obj['server'])
        query = ArtifactQuery(identifier or '', attr)

        if query.is_exact:
            artifacts = [repos.lookup_single(query)]
        else:
            repos.download_metadata_for_type(query.type)
            artifacts = list(LOCAL_REPO.lookup(query))

        if output_format == 'pretty':
            print('\n\n'.join(_format_artifact_metadata(artifact_metadata) for artifact_metadata in artifacts))
        elif output_format == 'json':
            print(json.dumps([artifact_metadata.to_dict() for artifact_metadata in artifacts], indent=4))
        elif output_format == 'short':
            print('\n'.join(_short_format_artifact_metadata(artifact_metadata) for artifact_metadata in artifacts))
        elif output_format == 'index-file':
            print('\n'.join(_index_file_format_artifact_metadata(artifact_metadata, index_file_prefix or '') for artifact_metadata in artifacts))
        elif output_format == 'index-webpage':
            if index_webpage_template is None:
                index_webpage_template = Path(__file__).parent / 'index_ui.min.html'
            print(_index_web_format_artifact_metadata(artifacts, index_webpage_template.read_text(), index_file_prefix))
        else:
            raise ValueError(f'Unknown output format: {output_format}')


@cli.command()
@click.argument(
    'IDENTIFIER',
    required=True
)
@click.option('-a', '--attr', help='Artifact attributes', multiple=True, callback=_parse_dict)
@click.pass_context
def env(ctx: click.Context, identifier: str, attr: Dict[str, str]):
    """
        Fetch artifact, then print its environment variables

        IDENTIFIER is in the format <type>:<hash>, e.g.: foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2
    """

    with handle_common_errors():
        repos = RepoGroup(remote_uri=ctx.obj['server'])
        query = ArtifactQuery(identifier, attr)

        _local_path, metadata = repos.get_single(query)

        print(LOCAL_REPO.format_env_file(metadata), end='')


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
@click.pass_context
def upload(
        ctx: click.Context,
        local_path: Optional[Path],
        type: str,
        name: Optional[str],
        description: Optional[str],
        compressed: bool,
        remote_path: Optional[str],
        attr: Dict[str, str],
        env: Dict[str, str],
):
    """
        Upload artifact to artifact storage.

        If LOCAL_PATH is unspecified, assume already it's uploaded to value of `--remote-path`
    """

    verify_type(type)
    verify_attributes(attr)

    if local_path is None and remote_path is None:
        raise click.BadParameter('Must specify either LOCAL_PATH or --remote-path')

    remote_repo = ArtifactRepo.by_uri(ctx.obj['server'])

    tmp_file_to_remove = None

    if local_path is not None:
        name = name or local_path.name
        if local_path.is_dir():
            artifact_hash = None
            if compressed:
                artifact_type = 'tar.gz'

                # Compress it
                tmp_file = Path(f'/tmp/ampm_tmp_{randbytes(8).hex()}')
                remove_atexit(tmp_file)
                total_size = ceil(_calc_dir_size(local_path) / 1024)
                bar = tqdm.tqdm(
                    total=total_size,
                    unit='KB',
                    desc=f"Compressing {local_path.name}"
                )
                size_left = total_size
                with tmp_file.open('wb') as f:
                    with tarfile.open(fileobj=f, mode='w:gz', compresslevel=6) as tar:
                        for path in local_path.rglob('*'):
                            if path.is_file():
                                tar.add(path, arcname=path.relative_to(local_path))
                                bar.update(min(ceil(path.stat().st_size / 1024), size_left))
                                size_left -= ceil(path.stat().st_size / 1024)
                            elif path.is_dir():
                                tar.add(path, arcname=path.relative_to(local_path), recursive=False)

                artifact_hash = hash_local_file(tmp_file)
                bar.close()
                local_path = tmp_file
            else:
                artifact_type = 'dir'
        elif local_path.is_file():
            if compressed:
                artifact_type = 'gz'

                # Compress it
                tmp_file = Path(f'/tmp/ampm_tmp_{randbytes(8).hex()}')
                remove_atexit(tmp_file)
                total_size = ceil(local_path.stat().st_size / 1024)
                bar = tqdm.tqdm(
                    total=total_size,
                    unit='KB',
                    desc=f"Compressing {local_path.name}"
                )
                size_left = total_size
                with gzip.GzipFile(tmp_file, mode='wb') as zbuffer:
                    with local_path.open('rb') as f:
                        while True:
                            data = f.read(1024*1024)
                            if len(data) == 0:
                                break
                            zbuffer.write(data)
                            bar.update(min(1024, size_left))
                            size_left -= 1024

                artifact_hash = hash_local_file(tmp_file)
                bar.close()
                local_path = tmp_file
            else:
                artifact_type = 'file'
                artifact_hash = hash_local_file(local_path)
        else:
            raise click.BadParameter(f'Unsupported file type: {local_path} ({local_path.stat().st_type})')
    else:
        if compressed:
            if remote_path.endswith('.tar.gz'):
                artifact_type = 'tar.gz'
                name = name or remote_path.strip('/').split('/')[-1][:-len('.tar.gz')]
            elif remote_path.endswith('.gz'):
                artifact_type = 'gz'
                name = name or remote_path.strip('/').split('/')[-1][:-len('.gz')]
            else:
                raise click.BadParameter(f'Remote artifact is not compressed using a known compression method '
                                         f'(.tar.gz or .gz): {remote_path}\n'
                                         f'Try adding `--uncompressed` to create an uncompressed artifact.')
        else:
            artifact_type = 'file'
            name = name or remote_path.strip('/').split('/')[-1]

        try:
            artifact_hash = remote_repo.hash_remote_file(remote_path, progress_bar=True)
        except OSError:
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

    if tmp_file_to_remove is not None:
        tmp_file_to_remove.unlink(missing_ok=True)


@cli.command()
@click.argument('artifact', type=str)
@click.option(
    '--i-realise-this-may-break-other-peoples-builds-in-the-future',
    is_flag=True,
    default=False,
    help='Make sure nobody will ever use this artifact ever again!!!',
)
@click.pass_context
def remote_rm(ctx: click.Context, artifact: str, i_realise_this_may_break_other_peoples_builds_in_the_future: bool):
    if not i_realise_this_may_break_other_peoples_builds_in_the_future:
        raise click.BadParameter('You must specify --i-realise-this-may-break-other-peoples-builds-in-the-future')

    remote_repo: NfsRepo = ArtifactRepo.by_uri(ctx.obj['server'])
    remote_repo.remove_artifact(artifact)


@cli.command()
@click.pass_context
def search(ctx: click.Context):
    """
        Open the web interface to search for artifacts
    """

    with handle_common_errors():
        repos = RepoGroup(remote_uri=ctx.obj['server'])
        query = ArtifactQuery('', {})

        if query.is_exact:
            artifacts = [repos.lookup_single(query)]
        else:
            repos.download_metadata_for_type(query.type)
            artifacts = list(LOCAL_REPO.lookup(query))

        index_webpage_template = Path(__file__).parent / 'index_ui.min.html'
        fd, filename = tempfile.mkstemp('.html', text=True)
        with open(fd, 'w') as f:
            f.write(_index_web_format_artifact_metadata(artifacts, index_webpage_template.read_text(), ''))

        subprocess.call(['xdg-open', filename])


def main():
    cli()


if __name__ == '__main__':
    main()
