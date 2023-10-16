# ampm: Artifact Manager, (not a) Package Manager

## How to install

The easiest way is by running:
```shell
curl -fsSL https://github.com/Wazzaps/ampm/releases/latest/download/get_ampm.sh | sudo sh
```
But you can also download `ampm.tar.gz`, extract it to `/opt/ampm/`,
then run `install /opt/ampm/ampm.sh /usr/local/bin/ampm`, and then put your repo uri in `/opt/ampm/repo_uri`.

## How to build manually:

This will use a `pyinstaller-manylinux` docker image to build a portable binary

```shell
sudo ./build.sh
```

## All features:

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

# Gets latest release if it's the only non-unique attribute
ampm get foobar -a arch=x86_64 -a pubdate=@date:latest

# Gets latest release if it's the only non-unique attribute, except for `irrelevant_attr`, which is ignored
ampm get foobar -a arch=x86_64 -a pubdate=@date:latest -a irrelevant_attr=@ignore

# Gets latest release, ignoring attributes other than `arch`
ampm get foobar -a arch=x86_64 -a pubdate=@date:latest -a @any=@ignore

# Gets any release with the current biggest major version
ampm get foobar -a arch=x86_64 -a major_ver=@num:biggest -a @any=@ignore

# Gets latest release that's newer-or-equal to 1.0.0 but older than 2.0.0 (not including prereleases)
ampm get foobar -a arch=x86_64 -a version='@semver:^1.0.0'

# Gets any release that matches a regex on the `arch` attribute
ampm get foobar -a arch='@regex:x86_64|i386' -a @any=@ignore

# Gets any release that matches a glob on the `arch` attribute
ampm get foobar -a arch='@glob:arm*' -a @any=@ignore

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

# List all artifacts with short output (type:hash  <attr>=<value>...)
ampm list foobar --format=json

# List all artifacts with index-file output (type:hash  <attr>=<value>...  <prefix>/<path>)
ampm list foobar --format=index-file --index-file-prefix='http://other/way/to/access/the/repo'

# List all artifacts present in local cache (don't contact server)
ampm --offline list

# Prints a shell script that can be sourced to export the artifact's env vars
ampm env foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2

# Custom NFS server
ampm --server='nfs://1.2.3.4/some/repo' get foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2

# Remove artifact from server (must be precise hash), Make sure no one is using the artifact!
ampm remote-rm --i-realise-this-may-break-other-peoples-builds-in-the-future foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2

# Update ampm
ampm update
```

## Hash calculation algorithm

`cat metadata.toml | sha256sum | xxd -ps -r | base32 | cut -c-32 | tr 'A-Z' 'a-z'`
