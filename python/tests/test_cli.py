import contextlib
import dataclasses
import io
import os
import re
import stat
import time
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Callable, Iterator, List, Literal, Optional, Tuple, overload

import pytest

from hdfs_native import Client
from hdfs_native.cli import main as cli_main


def assert_not_exists(client: Client, path: str):
    try:
        client.get_file_info(path)
        pytest.fail(f"Expected file not to exist: {path}")
    except FileNotFoundError:
        pass


@overload
def capture_stdout(func: Callable[[], None], text: Literal[False]) -> bytes: ...


@overload
def capture_stdout(func: Callable[[], None], text: Literal[True] = True) -> str: ...


def capture_stdout(func: Callable[[], None], text: bool = True):
    buf = io.BytesIO()
    with contextlib.redirect_stdout(io.TextIOWrapper(buf)) as wrapper:
        func()
        if text:
            wrapper.seek(0)
            return wrapper.read()
        else:
            return buf.getvalue()


def test_cat(client: Client):
    with client.create("/testfile") as file:
        file.write(b"1234")

    output = capture_stdout(lambda: cli_main(["cat", "/testfile"]), False)
    assert output == b"1234"

    with client.create("/testfile2") as file:
        file.write(b"5678")

    output = capture_stdout(lambda: cli_main(["cat", "/testfile", "/testfile2"]), False)
    assert output == b"12345678"

    with pytest.raises(FileNotFoundError):
        cli_main(["cat", "/nonexistent"])


def test_chmod(client: Client):
    with pytest.raises(FileNotFoundError):
        cli_main(["chmod", "755", "/testfile"])

    client.create("/testfile").close()

    cli_main(["chmod", "700", "/testfile"])
    assert client.get_file_info("/testfile").permission == 0o700

    cli_main(["chmod", "007", "/testfile"])
    assert client.get_file_info("/testfile").permission == 0o007

    cli_main(["chmod", "1777", "/testfile"])
    assert client.get_file_info("/testfile").permission == 0o1777

    with pytest.raises(ValueError):
        cli_main(["chmod", "2777", "/testfile"])

    with pytest.raises(ValueError):
        cli_main(["chmod", "2778", "/testfile"])

    client.mkdirs("/testdir")
    client.create("/testdir/testfile").close()
    original_permission = client.get_file_info("/testdir/testfile").permission

    cli_main(["chmod", "700", "/testdir"])
    assert client.get_file_info("/testdir").permission == 0o700
    assert client.get_file_info("/testdir/testfile").permission == original_permission

    cli_main(["chmod", "-R", "700", "/testdir"])
    assert client.get_file_info("/testdir").permission == 0o700
    assert client.get_file_info("/testdir/testfile").permission == 0o700


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


def test_du(client: Client):
    with client.create("/testfile") as file:
        file.write(b"1234")

    client.mkdirs("/testdir")

    with client.create("/testdir/testfile") as file:
        for i in range(1024):
            file.write(i.to_bytes(4, "big"))

    assert capture_stdout(lambda: cli_main(["du", "/"])).strip().split("\n") == [
        "4096  12288  /testdir ",
        "   4     12  /testfile",
    ]

    assert capture_stdout(lambda: cli_main(["du", "-h", "/"])).strip().split("\n") == [
        "4.0K  12.0K  /testdir ",
        "   4     12  /testfile",
    ]

    assert capture_stdout(lambda: cli_main(["du", "-s", "/"])).strip().split("\n") == [
        "4100  12300  /",
    ]

    assert capture_stdout(lambda: cli_main(["du", "-fh", "/"])).strip().split("\n") == [
        "4.0K  12.0K  /testdir   1  1",
        "   4     12  /testfile  1  0",
    ]

    assert capture_stdout(lambda: cli_main(["du", "-vfh", "/"])).strip().split(
        "\n"
    ) == [
        "File Size  Disk Size  Path       File Count  Directory Count",
        "     4.0K      12.0K  /testdir            1                1",
        "        4         12  /testfile           1                0",
    ]

    assert capture_stdout(
        lambda: cli_main(["du", "-vsfh", "/testdir", "/testfile"])
    ).strip().split("\n") == [
        "File Size  Disk Size  Path       File Count  Directory Count",
        "     4.0K      12.0K  /testdir            1                1",
        "        4         12  /testfile           1                0",
    ]


def test_get(client: Client, monkeypatch: pytest.MonkeyPatch):
    data = b"0123456789"

    with pytest.raises(FileNotFoundError):
        cli_main(["get", "/testfile", "testfile"])

    with client.create("/testfile") as file:
        file.write(data)

    status = client.get_file_info("/testfile")

    with TemporaryDirectory() as tmp_dir:
        cli_main(["get", "/testfile", os.path.join(tmp_dir, "localfile")])
        with open(os.path.join(tmp_dir, "localfile"), "rb") as file:
            assert file.read() == data

        cli_main(["get", "/testfile", tmp_dir])
        with open(os.path.join(tmp_dir, "testfile"), "rb") as file:
            assert file.read() == data

        os.remove(os.path.join(tmp_dir, "testfile"))

        with monkeypatch.context() as m:
            m.chdir(tmp_dir)
            cli_main(["get", "/testfile"])

        with open(os.path.join(tmp_dir, "testfile"), "rb") as file:
            assert file.read() == data

        with pytest.raises(FileExistsError):
            cli_main(["get", "/testfile", tmp_dir])

        cli_main(["get", "-f", "-p", "/testfile", tmp_dir])
        st = os.stat(os.path.join(tmp_dir, "testfile"))
        assert stat.S_IMODE(st.st_mode) == status.permission
        assert int(st.st_atime * 1000) == status.access_time
        assert int(st.st_mtime * 1000) == status.modification_time

    with client.create("/testfile2") as file:
        file.write(data)

    with pytest.raises(ValueError):
        cli_main(["get", "/testfile", "/testfile2", "notadir"])

    with TemporaryDirectory() as tmp_dir:
        cli_main(["get", "/testfile", "/testfile2", tmp_dir])

        with open(os.path.join(tmp_dir, "testfile"), "rb") as file:
            assert file.read() == data

        with open(os.path.join(tmp_dir, "testfile2"), "rb") as file:
            assert file.read() == data


def test_ls(client: Client):
    @dataclasses.dataclass
    class FileOutput:
        permission: str
        replication: str
        size: str
        path: str

    def parse_output(output: str) -> Iterator[Tuple[int, List[FileOutput]]]:
        current_items: Optional[int] = None
        current_batch: List[FileOutput] = []

        for line in output.split("\n"):
            if match := re.match(r"Found (\d)+ items", line):
                if current_items is not None:
                    yield (current_items, current_batch)

                current_items = int(match.group(1))
                current_batch = []

            elif line.strip():
                match = re.match(
                    r"(\S+)\s+(\S+)\s+\S+\s+\S+\s+([0-9.]+\w?)\s+\S+\s+\S+\s+(\S+)",
                    line,
                )
                assert match is not None
                current_batch.append(
                    FileOutput(
                        permission=match.group(1),
                        replication=match.group(2),
                        size=match.group(3),
                        path=match.group(4),
                    )
                )

        if current_items is not None and len(current_batch) > 0:
            yield (current_items, current_batch)

    with pytest.raises(FileNotFoundError):
        cli_main(["ls", "/fake"])

    with client.create("/testfile1") as f:
        f.write(bytes(range(10)))

    # Make sure we wait a few milliseconds so we don't get the exact same timestamp
    time.sleep(0.01)

    with client.create("/testfile2") as f:
        for i in range(1024):
            f.write(i.to_bytes(4, "big"))

    time.sleep(0.01)

    client.mkdirs("/testdir")

    directory = FileOutput("drwxr-xr-x", "-", "0", "/testdir")
    file1 = FileOutput("-rw-r--r--", "3", "10", "/testfile1")
    file2 = FileOutput("-rw-r--r--", "3", "4096", "/testfile2")

    def check_output(command: List[str], expected: List[FileOutput]):
        groups = list(parse_output(capture_stdout(lambda: cli_main(command))))
        assert len(groups) == 1
        assert groups[0][0] == 3
        assert len(groups[0][1]) == 3
        assert groups[0][1] == expected

    check_output(["ls", "/"], [directory, file1, file2])
    check_output(["ls", "-t", "/"], [directory, file2, file1])
    check_output(["ls", "-r", "-t", "/"], [file1, file2, directory])
    check_output(["ls", "-S", "/"], [file2, file1, directory])
    check_output(["ls", "-r", "-S", "/"], [directory, file1, file2])

    check_output(
        ["ls", "-h", "/"], [directory, file1, dataclasses.replace(file2, size="4.0K")]
    )

    output = capture_stdout(lambda: cli_main(["ls", "-C", "/"])).strip().split("\n")
    assert output == [directory.path, file1.path, file2.path]


def test_mkdir(client: Client):
    cli_main(["mkdir", "/testdir"])
    assert client.get_file_info("/testdir").isdir

    with pytest.raises(FileNotFoundError):
        cli_main(["mkdir", "/testdir/nested/dir"])

    cli_main(["mkdir", "-p", "/testdir/nested/dir"])
    assert client.get_file_info("/testdir/nested/dir").isdir


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


def test_put(client: Client):
    data = b"0123456789"

    with pytest.raises(FileNotFoundError):
        cli_main(["put", "testfile", "/testfile"])

    with TemporaryDirectory() as tmp_dir:
        with open(os.path.join(tmp_dir, "testfile"), "wb") as file:
            file.write(data)

        cli_main(["put", os.path.join(tmp_dir, "testfile"), "/remotefile"])
        with client.read("/remotefile") as file:
            assert file.read() == data

        cli_main(["put", os.path.join(tmp_dir, "testfile"), "/"])
        with client.read("/testfile") as file:
            assert file.read() == data

        with pytest.raises(FileExistsError):
            cli_main(["put", os.path.join(tmp_dir, "testfile"), "/"])

        cli_main(["put", "-f", "-p", os.path.join(tmp_dir, "testfile"), "/"])
        st = os.stat(os.path.join(tmp_dir, "testfile"))
        status = client.get_file_info("/testfile")
        assert stat.S_IMODE(st.st_mode) == status.permission
        assert int(st.st_atime * 1000) == status.access_time
        assert int(st.st_mtime * 1000) == status.modification_time

        with open(os.path.join(tmp_dir, "testfile2"), "wb") as file:
            file.write(data)

        with pytest.raises(ValueError):
            cli_main(
                [
                    "put",
                    os.path.join(tmp_dir, "testfile"),
                    os.path.join(tmp_dir, "testfile2"),
                    "/notadir",
                ]
            )

        client.mkdirs("/testdir")
        cli_main(
            [
                "put",
                os.path.join(tmp_dir, "testfile"),
                os.path.join(tmp_dir, "testfile2"),
                "/testdir",
            ]
        )

        with client.read("/testdir/testfile") as file:
            assert file.read() == data
        with client.read("/testdir/testfile2") as file:
            assert file.read() == data


def test_rm(client: Client):
    with pytest.raises(ValueError):
        cli_main(["rm", "/testfile"])

    with pytest.raises(FileNotFoundError):
        cli_main(["rm", "-s", "/testfile"])

    cli_main(["rm", "-f", "-s", "/testfile"])

    client.create("/testfile").close()
    cli_main(["rm", "-s", "/testfile"])
    assert_not_exists(client, "/testfile")

    client.mkdirs("/testdir")
    client.create("/testdir/testfile").close()
    client.create("/testdir/testfile2").close()

    with pytest.raises(RuntimeError):
        cli_main(["rm", "-s", "/testdir"])

    cli_main(["rm", "-r", "-s", "/testdir"])
    assert_not_exists(client, "/testdir")


def test_rmdir(client: Client):
    with pytest.raises(FileNotFoundError):
        cli_main(["rmdir", "/testdir"])

    client.mkdirs("/testdir")
    client.create("/testdir/testfile").close()

    with pytest.raises(RuntimeError):
        cli_main(["rmdir", "/testdir"])

    client.delete("/testdir/testfile")

    cli_main(["rmdir", "/testdir"])

    try:
        client.get_file_info("/testdir")
        pytest.fail("Directory was not removed")
    except FileNotFoundError:
        pass


def test_touch(client: Client):
    cli_main(["touch", "/testfile"])
    client.get_file_info("/testfile")

    cli_main(["touch", "-c", "/testfile2"])
    try:
        client.get_file_info("/testfile2")
        pytest.fail("File should not have been created")
    except FileNotFoundError:
        pass

    cli_main(["touch", "-a", "/testfile"])
    status = client.get_file_info("/testfile")
    assert status.access_time > status.modification_time

    cli_main(["touch", "-m", "/testfile"])
    status = client.get_file_info("/testfile")
    assert status.modification_time > status.access_time

    cli_main(["touch", "-t", "20240101:000000", "/testfile"])
    timestamp = int(
        datetime.strptime("20240101:000000", r"%Y%m%d:%H%M%S").timestamp() * 1000
    )
    status = client.get_file_info("/testfile")
    assert status.modification_time == timestamp
    assert status.access_time == timestamp
