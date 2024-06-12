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
            data.write(i.to_bytes(4, "big"))

        file.write(data.getbuffer())

    with client.read("/testfile") as file:
        data = io.BytesIO(file.read())

    for i in range(0, 32 * 1024 * 1024):
        assert data.read(4) == i.to_bytes(4, "big")

    with client.append("/testfile") as file:
        data = io.BytesIO()

        for i in range(32 * 1024 * 1024, 33 * 1024 * 1024):
            data.write(i.to_bytes(4, "big"))

        file.write(data.getbuffer())

    with client.read("/testfile") as file:
        data = io.BytesIO(file.read())

    for i in range(0, 33 * 1024 * 1024):
        assert data.read(4) == i.to_bytes(4, "big")

    with client.read("/testfile") as file:
        # Skip first two ints
        file.seek(8)
        expected = 2
        assert file.read(4) == expected.to_bytes(4, "big")
        assert file.tell() == 12

    mtime = 1717641455
    atime = 1717641456
    client.set_times("/testfile", mtime, atime)
    file_info = client.get_file_info("/testfile")
    assert file_info.modification_time == mtime
    assert file_info.access_time == atime

    client.set_owner("/testfile", "testuser", "testgroup")
    file_info = client.get_file_info("/testfile")
    assert file_info.owner == "testuser"
    assert file_info.group == "testgroup"

    client.set_owner("/testfile", owner="testuser2")
    file_info = client.get_file_info("/testfile")
    assert file_info.owner == "testuser2"
    assert file_info.group == "testgroup"

    client.set_owner("/testfile", group="testgroup2")
    file_info = client.get_file_info("/testfile")
    assert file_info.owner == "testuser2"
    assert file_info.group == "testgroup2"

    assert file_info.permission == 0o644
    client.set_permission("/testfile", 0o600)
    file_info = client.get_file_info("/testfile")
    assert file_info.permission == 0o600

    client.set_replication("/testfile", 1)
    file_info = client.get_file_info("/testfile")
    assert file_info.replication == 1

    client.set_replication("/testfile", 2)
    file_info = client.get_file_info("/testfile")
    assert file_info.replication == 2

    content_summary = client.get_content_summary("/")
    assert content_summary.file_count == 1
    assert content_summary.directory_count == 1
    assert content_summary.length == 33 * 1024 * 1024 * 4

    client.delete("/testfile", False)
