from hdfs_native.fsspec import HdfsFileSystem
import pytest

def test_dirs(fs: HdfsFileSystem):
    fs.mkdir('/testdir')
    assert fs.info('/testdir')['type'] == 'directory'

    try:
        fs.makedirs('/testdir', exist_ok=False)
        assert False, '"/testdir" already exists, should fail'
    except:
        pass

    fs.makedirs('/testdir', exist_ok=True)

    fs.mkdir('/testdir/nested/dir')
    assert fs.info('/testdir/nested/dir')['type'] == 'directory'

    try:
        fs.mkdir('/testdir/nested2/dir', create_parents=False)
        assert False, 'Should fail to make dir because parent doesn\'t exist'
    except:
        pass

    with pytest.raises(RuntimeError):
        fs.rm('/testdir', recursive=False)

    fs.rm('/testdir', recursive=True)

    assert not fs.exists('/testdir')

def test_io(fs: HdfsFileSystem):
    with fs.open('/test', mode='wb') as file:
        file.write(b'hello there')
    
    with fs.open('/test', mode='rb') as file:
        data = file.read()
        assert data == b'hello there'

    fs.write_bytes('/test2', b'hello again')
    assert fs.read_bytes('/test2') == b'hello again'
    assert fs.read_bytes('/test2', start=1) == b'ello again'
    assert fs.read_bytes('/test2', end=-1) == b'hello agai'

    fs.mv('/test2', '/test3')
    assert fs.read_text('/test3') == 'hello again'
    assert not fs.exists('/test2')

    fs.rm('/test')
    fs.rm('/test3')

def test_listing(fs: HdfsFileSystem):
    fs.mkdir('/testdir')

    fs.touch('/testdir/test1')
    fs.touch('/testdir/test2')

    assert fs.ls('/', detail=False) == ['/testdir']
    assert fs.ls('/testdir', detail=False) == ['/testdir/test1', '/testdir/test2']

    listing = fs.ls('/', detail=True)
    assert len(listing) == 1
    assert listing[0]['size'] == 0
    assert listing[0]['name'] == '/testdir'
    assert listing[0]['type'] == 'directory'
