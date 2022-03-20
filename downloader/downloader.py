from nfs import nfs_connection
import time

with nfs_connection('127.0.0.1', '/mnt/myshareddir') as nfs:
    # # List dir test
    # dir_listing = list(nfs.list_dir('.'))
    # print(dir_listing)

    # # Upload test
    # t = time.time()
    # nfs.write('a/foo.txt', b'A'*1024*1024*1024, progress_bar=True)
    # print('Upload took', time.time() - t, 'seconds')

    # Download test
    t = time.time()
    buf = nfs.read('a/foo.txt', progress_bar=True)
    print('Begin:', buf[:10])
    print('End:', buf[-10:])
    print('Len:', len(buf))
    print('Download took', time.time() - t, 'seconds')






