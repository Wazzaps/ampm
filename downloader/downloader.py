from click import Path

from nfs import nfs_connection, NfsConnection
import time
import click


SHAREDIR_MOUNT_PATH = '/mnt/sharedir'
SHAREDIR_IP = '127.0.0.1'


def f():
    with nfs_connection(SHAREDIR_IP, SHAREDIR_MOUNT_PATH) as nfs:
        # # List dir test
        # dir_listing = list(nfs.list_dir('.'))
        # print(dir_listing)

        # # Write test
        # t = time.time()
        # nfs.write('a/foo.txt', b'A'*(1024*1024*1024 + 100), progress_bar=True)
        # print('Upload took', time.time() - t, 'seconds')

        # Read test
        t = time.time()
        buf = nfs.read('a/foo.txt', progress_bar=True)
        print('Begin:', buf[:10])
        print('End:', buf[-10:])
        print('Len:', len(buf))
        print('Download took', time.time() - t, 'seconds')


@click.command()
@click.option('--local-path', help='Local Path', prompt="Local Path")
@click.option('--remote-path', prompt='Remote Path', help='Remote Path')
def upload(local_path: Path, remote_path: str):
    nfs: NfsConnection
    with nfs_connection(SHAREDIR_IP, SHAREDIR_MOUNT_PATH) as nfs:
        dir_listing = list(nfs.list_dir('.'))
        print(dir_listing)

        nfs.upload(local_path, remote_path, progress_bar=True)

        dir_listing = list(nfs.list_dir('.'))
        print(dir_listing)


def main():
    upload()


if __name__ == '__main__':
    main()
