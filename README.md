# ampm: Artifact Manager, (not a) Package Manager

## Building:

This will use a `pyinstaller-manylinux` docker image to build a portable binary

```shell
sudo ./build.sh
```

## Implemented:

```shell
# Upload file to default location
ampm upload foobar.txt --type='foobar' --uncompressed -a some_attr=some_value -e SOME_ENV=some_value

# Upload directory to default location
ampm upload foobar/ --type='foobar' --uncompressed

# Upload to custom location
ampm upload foobar.txt --type='foobar' --remote-path='/foobar.txt' --uncompressed

# Upload file to default location, with gz compression
ampm upload foobar.txt --type='foobar'

# Upload file to default location, in tar.gz archive
ampm upload foobar/ --type='foobar'

# Upload to custom location manually then register artifact
cp foobar.txt /mnt/myshareddir/foobar.txt && ampm upload --type='foobar' --name='foobar.txt' --remote-path='/foobar.txt'

# Download artifact by type and hash
ampm get foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2

# Types can contain slashes, they make dirs in artifact folders
ampm get foo/bar:7tew33maphcutgbfqui3cxnddus56isk

# Selects artifact by attribute (will fail if not unique)
ampm get foobar -a arch=x86_64

# List all artifacts with type
ampm list foobar

# List all subtypes (assuming foo/bar exists, etc.)
ampm list foo

# List all artifacts with type that match attributes
ampm list foobar -a some_attr=some_value

# List all artifacts
ampm list

# List all artifacts with json output
ampm list foobar --format=json

# Prints a shell script that can be sourced to export the artifact's env vars
ampm env foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2

# Custom NFS server
ampm --server='nfs://1.2.3.4/some/repo' get foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2
```

## TODO:

```shell
# Gets newest version if it's the only non-unique attribute
ampm get foobar -a arch=x86_64 -a version=@semver:^1.0

# Gets latest release if it's the only non-unique attribute
ampm get foobar -a arch=x86_64 -a pubdate=@date:latest

# Gets latest release if it's the only non-unique attribute, except for `irrelevant_attr`, which is ignored
ampm get foobar -a arch=x86_64 -a pubdate=@date:latest -a irrelevant_attr=@ignore

# Gets latest release, ignoring attributes other than `arch`
ampm get foobar -a arch=x86_64 -a pubdate=@date:latest -a @any=@ignore
```

## Hash calculation algorithm

`cat metadata.toml | sha256sum | xxd -ps -r | base32 | cut -c-32 | tr 'A-Z' 'a-z'`