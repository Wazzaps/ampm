# ampm: Artifact Manager, (not a) Package Manager

## Building:

### With docker:

```shell
sudo docker build -t ampm_build $PWD
sudo docker run --rm -it -v $PWD:/code ampm_build
ls -l ./dist/ampm
```

### Without docker:

Install dependencies as needed:

```shell
python3 ./vendor/PyInstaller/__main__.py --onefile --clean --noconfirm --name=ampm ampm/cli.py
mv ./dist/ampm/ampm ./dist/ampm/ampm_py
cp ./ampm/ampm.sh ./dist/ampm/ampm
ls -l ./dist/ampm
```

## Implemented:

```shell
# Upload file to default location
ampm upload foobar.txt --type='foobar' --uncompressed -a some_attr=some_value -e SOME_ENV=some_value

# Upload directory to default location
ampm upload foobar/ --type='foobar' --uncompressed

# Upload to custom location
ampm upload foobar.txt --type='foobar' --remote-path='/foobar.txt' --uncompressed

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
```

## TODO:

```shell
# Upload file to default location, gz compression
ampm upload foobar.txt --type='foobar'

# Upload file to default location, in tar.gz archive
ampm upload foobar/ --type='foobar'

# Custom NFS server
ampm --server='1.2.3.4:/foo/bar' get foobar:mbf5qxqli76zx7btc5n7fkq47tjs6cl2

# Same but gets newest version if it's the only non-unique attribute
ampm get foobar -a arch=x86_64 -a version=@semver:^1.0
ampm get foobar -a arch=x86_64 -a pubdate=@date:latest
```

- Make `get` not connect to NFS if it exists locally
- Maybe not add `name` field as directory name if `path_type` is `dir`

## Hash calculation algorithm

`cat metadata.toml | sha256sum | xxd -ps -r | base32 | cut -c-32 | tr 'A-Z' 'a-z'`