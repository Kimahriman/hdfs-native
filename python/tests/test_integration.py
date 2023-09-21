import io
from hdfs_native import Client, WriteOptions
from .minidfs import MiniDfs

def test_integration():
    dfs = MiniDfs()

    client = Client(dfs.get_url())
    client.create("/testfile", WriteOptions()).close()
    # raise Exception("blashsdf")
    file_info = client.get_file_info("/testfile")
    
    assert file_info.path == "/testfile"

    file_list = list(client.list_status("/", False))
    assert len(file_list) == 1
    assert file_list[0].path == "/testfile"

    client.rename("/testfile", "/testfile2", False)

    file_list = list(client.list_status("/", False))
    assert len(file_list) == 1
    assert file_list[0].path == "/testfile2"

    client.delete("/testfile2", False)

    file_list = list(client.list_status("/", False))
    assert len(file_list) == 0

    file = client.create("/testfile", WriteOptions())
    data = io.BytesIO()

    for i in range(0, 32 * 1024 * 1024):
        data.write(i.to_bytes(4, 'big'))

    file.write(data.getbuffer())
    file.close()

    file = client.read("/testfile")
    data = io.BytesIO(file.read_range(0, file.file_length()))

    for i in range(0, 32 * 1024 * 1024):
        assert data.read(4) == i.to_bytes(4, 'big')

    client.delete("/testfile", False)