import contextlib
import io

import pytest

from hdfs_native import Client
from hdfs_native.cli import main as cli_main


def test_cat(client: Client):
    with client.create("/testfile") as file:
        file.write(b"1234")

    buf = io.BytesIO()
    with contextlib.redirect_stdout(io.TextIOWrapper(buf)):
        cli_main(["cat", "/testfile"])
        assert buf.getvalue() == b"1234"

    with client.create("/testfile2") as file:
        file.write(b"5678")

    buf = io.BytesIO()
    with contextlib.redirect_stdout(io.TextIOWrapper(buf)):
        cli_main(["cat", "/testfile", "/testfile2"])
        assert buf.getvalue() == b"12345678"

    with pytest.raises(FileNotFoundError):
        cli_main(["cat", "/nonexistent"])

    client.delete("/testfile")
    client.delete("/testfile2")


def test_chown(client: Client):
    with pytest.raises(FileNotFoundError):
        cli_main(["chown", "testuser", "/testfile"])

    client.create("/testfile").close()
    status = client.get_file_info("/testfile")
    group = status.group

    cli_main(["chown", "testuser", "/testfile"])
    status = client.get_file_info("/testfile")
    assert status.owner == "testuser"
    assert status.group == group

    cli_main(["chown", ":testgroup", "/testfile"])
    status = client.get_file_info("/testfile")
    assert status.owner == "testuser"
    assert status.group == "testgroup"

    cli_main(["chown", "newuser:newgroup", "/testfile"])
    status = client.get_file_info("/testfile")
    assert status.owner == "newuser"
    assert status.group == "newgroup"

    client.delete("/testfile")

    client.mkdirs("/testdir")
    client.create("/testdir/testfile").close()
    file_status = client.get_file_info("/testdir/testfile")

    cli_main(["chown", "testuser:testgroup", "/testdir"])
    status = client.get_file_info("/testdir")
    assert status.owner == "testuser"
    assert status.group == "testgroup"
    status = client.get_file_info("/testdir/testfile")
    assert status.owner == file_status.owner
    assert status.group == file_status.group

    cli_main(["chown", "-R", "testuser:testgroup", "/testdir"])
    status = client.get_file_info("/testdir/testfile")
    assert status.owner == "testuser"
    assert status.group == "testgroup"

    client.delete("/testdir", True)


def test_mkdir(client: Client):
    cli_main(["mkdir", "/testdir"])
    assert client.get_file_info("/testdir").isdir

    with pytest.raises(FileNotFoundError):
        cli_main(["mkdir", "/testdir/nested/dir"])

    cli_main(["mkdir", "-p", "/testdir/nested/dir"])
    assert client.get_file_info("/testdir/nested/dir").isdir

    client.delete("/testdir", True)


def test_mv(client: Client):
    client.create("/testfile").close()
    client.mkdirs("/testdir")

    cli_main(["mv", "/testfile", "/testfile2"])

    client.get_file_info("/testfile2")

    with pytest.raises(ValueError):
        cli_main(["mv", "/testfile2", "hdfs://badnameservice/testfile"])

    with pytest.raises(FileNotFoundError):
        cli_main(["mv", "/testfile2", "/nonexistent/testfile"])

    cli_main(["mv", "/testfile2", "/testdir"])

    client.get_file_info("/testdir/testfile2")

    client.rename("/testdir/testfile2", "/testfile1")
    client.create("/testfile2").close()

    with pytest.raises(ValueError):
        cli_main(["mv", "/testfile1", "/testfile2", "/testfile3"])

    cli_main(["mv", "/testfile1", "/testfile2", "/testdir/"])

    client.get_file_info("/testdir/testfile1")
    client.get_file_info("/testdir/testfile2")
