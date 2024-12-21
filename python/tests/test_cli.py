import contextlib
import io

import pytest

from hdfs_native import Client
from hdfs_native.cli import main as cli_main


def test_cli(minidfs: str):
    client = Client(minidfs)

    # cat
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

    # mkdir
    cli_main(["mkdir", "/testdir"])
    assert client.get_file_info("/testdir").isdir

    with pytest.raises(RuntimeError):
        cli_main(["mkdir", "/testdir/nested/dir"])

    cli_main(["mkdir", "-p", "/testdir/nested/dir"])
    assert client.get_file_info("/testdir/nested/dir").isdir

    client.delete("/testdir", True)

    # mv
    client.create("/testfile").close()
    client.mkdirs("/testdir")

    cli_main(["mv", "/testfile", "/testfile2"])

    client.get_file_info("/testfile2")

    with pytest.raises(ValueError):
        cli_main(["mv", "/testfile2", "hdfs://badnameservice/testfile"])

    with pytest.raises(RuntimeError):
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
