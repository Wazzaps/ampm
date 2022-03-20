# downloader

## Implemented:

```shell
# Upload file to default location
downloader upload foobar.txt --type='foobar' --compressed=false

# Upload directory to default location
downloader upload foobar/ --type='foobar' --compressed=false

# Upload to custom location
downloader upload foobar.txt --type='foobar' --remote-path='/foobar.txt' --compressed=false

# Upload to custom location manually then register artifact
cp foobar.txt /mnt/myshareddir/foobar.txt && downloader upload --type='foobar' --name='foobar.txt' --remote-path='/foobar.txt'

# Download artifact by type and hash
downloader get foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2

# Types can contain slashes, they make dirs in artifact folders
downloader get foo/bar:7tew33maphcutgbfqui3cxnddus56isk
```

## TODO:

```shell
# Upload file to default location, gz compression
downloader upload foobar.txt --type='foobar'

# Upload file to default location, in tar.gz archive
downloader upload foobar/ --type='foobar'

# Custom NFS server
downloader --server='1.2.3.4:/foo/bar' get foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2

# List all artifacts with type
downloader list foobar

# List all subtypes (assuming foo/bar exists, etc.)
downloader list foo

# Selects artifact by attribute (will fail if not unique)
downloader get foobar -- --arch=x86_64

# Same but gets newest version if it's the only non-unique attribute
downloader get foobar -- --arch=x86_64 --version=@semver:^1.0

# Prints a bash script that can be sourced to export the artifact's env vars
downloader env foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2
```

- Make `get` not connect to NFS if it exists locally
- Maybe not add `name` field as directory name if `path_type` is `dir`

## Hash calculation algorithm

`cat metadata.toml | sha256sum | xxd -ps -r | base32 | cut -c-32 | tr 'A-Z' 'a-z'`