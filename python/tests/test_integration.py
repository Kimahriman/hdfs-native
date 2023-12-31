import io
from hdfs_native import Client, WriteOptions

def test_integration(minidfs: str):
    client = Client(minidfs)
    client.create("/testfile", WriteOptions()).close()
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

    with client.create("/testfile", WriteOptions()) as file:
        data = io.BytesIO()

        for i in range(0, 32 * 1024 * 1024):
            data.write(i.to_bytes(4, 'big'))

        file.write(data.getbuffer())

    with client.read("/testfile") as file:
        data = io.BytesIO(file.read())

    for i in range(0, 32 * 1024 * 1024):
        assert data.read(4) == i.to_bytes(4, 'big')

    with client.append("/testfile") as file:
        data = io.BytesIO()

        for i in range(32 * 1024 * 1024, 33 * 1024 * 1024):
            data.write(i.to_bytes(4, 'big'))

        file.write(data.getbuffer())

    with client.read("/testfile") as file:
        data = io.BytesIO(file.read())

    for i in range(0, 33 * 1024 * 1024):
        assert data.read(4) == i.to_bytes(4, 'big')

    client.delete("/testfile", False)