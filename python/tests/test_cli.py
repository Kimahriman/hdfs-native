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

from hdfs_native import AclEntry, Client
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


def test_getfacl(client: Client):
    """Test getfacl command with various scenarios"""
    # Test error handling for non-existent file
    with pytest.raises(FileNotFoundError):
        cli_main(["getfacl", "/nonexistent"])

    # Test basic functionality on a file
    client.create("/testfile").close()
    output = capture_stdout(lambda: cli_main(["getfacl", "/testfile"]))
    lines = output.strip().split("\n")
    assert any("# file:" in line for line in lines), "Output should contain file header"
    assert any("# owner:" in line for line in lines), "Output should contain owner"
    assert any("# group:" in line for line in lines), "Output should contain group"
    assert len(lines) >= 3, "Output should contain at least headers"

    # Test on a directory
    client.mkdirs("/testdir")
    output = capture_stdout(lambda: cli_main(["getfacl", "/testdir"]))
    lines = output.strip().split("\n")
    assert any("# file:" in line for line in lines), (
        "Directory output should contain file header"
    )
    assert any("# owner:" in line for line in lines), (
        "Directory output should contain owner"
    )
    assert any("# group:" in line for line in lines), (
        "Directory output should contain group"
    )

    # Test with multiple paths
    client.create("/testfile2").close()
    output = capture_stdout(lambda: cli_main(["getfacl", "/testfile", "/testfile2"]))
    file_headers = [line for line in output.split("\n") if "# file:" in line]
    assert len(file_headers) == 2, "Output should contain headers for both files"
    assert "/testfile" in output, "Output should mention testfile"
    assert "/testfile2" in output, "Output should mention testfile2"

    # Test recursive flag
    client.mkdirs("/testdir/subdir")
    client.create("/testdir/file1").close()
    client.create("/testdir/subdir/file2").close()
    output = capture_stdout(lambda: cli_main(["getfacl", "-R", "/testdir"]))
    file_headers = [line for line in output.split("\n") if "# file:" in line]
    assert len(file_headers) >= 3, (
        "Recursive output should contain multiple file headers"
    )
    assert "/testdir" in output, "Recursive output should mention testdir"
    assert "/file1" in output, "Recursive output should mention file1"
    assert "/file2" in output, "Recursive output should mention file2"

    # Test basic ACL entry format with base permissions
    output = capture_stdout(lambda: cli_main(["getfacl", "/testfile"]))
    lines = output.strip().split("\n")
    # Should have file, owner, group headers
    assert lines[0].startswith("# file:"), "First line should be file header"
    assert "owner:" in lines[1], "Second line should be owner header"
    assert "group:" in lines[2], "Third line should be group header"

    # Check for base permission entries (user::, group::, other::)
    assert any("user::" in line for line in lines), (
        "Output should contain base user permission entry"
    )
    assert any("group::" in line for line in lines), (
        "Output should contain base group permission entry"
    )
    assert any("other::" in line for line in lines), (
        "Output should contain base other permission entry"
    )

    # Test with named user and group ACL entries
    client.create("/testfile_with_named_acl").close()
    # Add named user and group ACL entries
    acl_entries = [
        AclEntry("user", "access", "rw-", "alice"),
        AclEntry("user", "access", "r--", "bob"),
        AclEntry("group", "access", "r--", "developers"),
        AclEntry("group", "access", "r--", "testers"),
    ]
    client.modify_acl_entries("/testfile_with_named_acl", acl_entries)

    output = capture_stdout(lambda: cli_main(["getfacl", "/testfile_with_named_acl"]))
    assert "# file:" in output, "Output should contain file header"
    assert "# owner:" in output, "Output should contain owner"
    assert "# group:" in output, "Output should contain group"
    assert "user::" in output, "Output should contain base user permission"
    assert "group::" in output, "Output should contain base group permission"
    assert "other::" in output, "Output should contain base other permission"

    # Verify named entries appear (they may or may not be set depending on HDFS config)
    acl_status = client.get_acl_status("/testfile_with_named_acl")
    if any(entry.name for entry in acl_status.entries if entry.type == "user"):
        assert "user:alice" in output or "user:bob" in output, (
            "Named user ACL entries should appear in output"
        )
    if any(entry.name for entry in acl_status.entries if entry.type == "group"):
        assert "group:developers" in output or "group:testers" in output, (
            "Named group ACL entries should appear in output"
        )

    # Test with default ACL entries
    client.mkdirs("/testdir_with_default_acl")
    default_acl_entries = [
        AclEntry("user", "default", "r--", None),
        AclEntry("group", "default", "r--", None),
    ]
    client.modify_acl_entries("/testdir_with_default_acl", default_acl_entries)

    output = capture_stdout(lambda: cli_main(["getfacl", "/testdir_with_default_acl"]))
    assert "# file:" in output, "Directory output should contain file header"

    # Check for default ACL entries if they were set
    acl_status = client.get_acl_status("/testdir_with_default_acl")
    has_default_entries = any(entry.scope == "default" for entry in acl_status.entries)
    if has_default_entries:
        # Default entries should be prefixed with "default:"
        assert any("default:" in line for line in output.split("\n")), (
            "Output should contain default ACL entries"
        )

    # Test output format consistency across multiple files
    output = capture_stdout(lambda: cli_main(["getfacl", "-R", "/testdir"]))
    output_lines = output.split("\n")

    # Count the number of file headers
    file_count = len([line for line in output_lines if "# file:" in line])
    # Each file should have corresponding owner and group lines
    owner_count = len([line for line in output_lines if "# owner:" in line])
    group_count = len([line for line in output_lines if "# group:" in line])
    assert file_count == owner_count == group_count, (
        "Each file should have owner and group headers"
    )

    # Verify base permissions appear for each file
    user_perm_count = len([line for line in output_lines if "user::" in line])
    group_perm_count = len([line for line in output_lines if "group::" in line])
    other_perm_count = len([line for line in output_lines if "other::" in line])
    assert file_count <= user_perm_count, "Each file should have user base permission"
    assert file_count <= group_perm_count, "Each file should have group base permission"
    assert file_count <= other_perm_count, "Each file should have other base permission"


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


def test_setfacl(client: Client):
    """Test setfacl command with various scenarios"""
    # Test error handling for non-existent file
    with pytest.raises(FileNotFoundError):
        cli_main(["setfacl", "-m", "user:testuser:rwx", "/nonexistent"])

    # Create a test file
    client.create("/testfile").close()

    # Test modify (-m) flag
    cli_main(["setfacl", "-m", "user:alice:rwx", "/testfile"])
    acl_status = client.get_acl_status("/testfile")
    assert any(
        entry.name == "alice" and entry.type == "user" and entry.permissions == "rwx"
        for entry in acl_status.entries
    ), "Named user ACL entry should be set"

    # Test remove (-x) flag
    cli_main(["setfacl", "-x", "user:alice:rwx", "/testfile"])
    acl_status = client.get_acl_status("/testfile")
    assert not any(
        entry.name == "alice" and entry.type == "user" for entry in acl_status.entries
    ), "Named user ACL entry should be removed"

    # Test with multiple ACL entries
    cli_main(
        [
            "setfacl",
            "-m",
            "user:alice:rwx,user:bob:r-x,group:developers:r--",
            "/testfile",
        ]
    )
    acl_status = client.get_acl_status("/testfile")
    assert any(
        entry.name == "alice" and entry.type == "user" and entry.permissions == "rwx"
        for entry in acl_status.entries
    ), "Alice's ACL entry should be set"
    assert any(
        entry.name == "bob" and entry.type == "user" and entry.permissions == "r-x"
        for entry in acl_status.entries
    ), "Bob's ACL entry should be set"
    assert any(
        entry.name == "developers"
        and entry.type == "group"
        and entry.permissions == "r--"
        for entry in acl_status.entries
    ), "Developers' ACL entry should be set"

    # Test set (--set) flag
    cli_main(
        [
            "setfacl",
            "--set",
            "user::rwx,group::r--,other::---,user:alice:r--,group:testers:rwx",
            "/testfile",
        ]
    )
    acl_status = client.get_acl_status("/testfile")
    # After --set, only the base entries and the new entries should exist
    assert any(
        entry.name == "alice" and entry.type == "user" and entry.permissions == "r--"
        for entry in acl_status.entries
    ), "Alice's ACL entry should be set to r--"
    assert any(
        entry.name == "testers" and entry.type == "group" and entry.permissions == "rwx"
        for entry in acl_status.entries
    ), "Testers' ACL entry should be set"
    # Bob and developers should be removed after --set
    assert not any(
        entry.name == "bob" and entry.type == "user" for entry in acl_status.entries
    ), "Bob's ACL entry should be removed after --set"
    assert not any(
        entry.name == "developers" and entry.type == "group"
        for entry in acl_status.entries
    ), "Developers' ACL entry should be removed after --set"

    # Test remove-all (-b) flag
    client.create("/testfile_remove_all").close()
    cli_main(["setfacl", "-m", "user:alice:rwx", "/testfile_remove_all"])
    cli_main(["setfacl", "-b", "/testfile_remove_all"])
    acl_status = client.get_acl_status("/testfile_remove_all")
    # Should only have base entries (user, group, other) after -b
    assert all(
        entry.name is None for entry in acl_status.entries if entry.scope == "access"
    ), "Only base ACL entries should remain after -b"

    # Test with directory and default ACLs
    client.mkdirs("/testdir")
    cli_main(
        [
            "setfacl",
            "-m",
            "default:user:alice:r--,default:group:testers:rwx",
            "/testdir",
        ]
    )
    acl_status = client.get_acl_status("/testdir")
    assert any(
        entry.name == "alice"
        and entry.type == "user"
        and entry.scope == "default"
        and entry.permissions == "r--"
        for entry in acl_status.entries
    ), "Default user ACL entry should be set"
    assert any(
        entry.name == "testers"
        and entry.type == "group"
        and entry.scope == "default"
        and entry.permissions == "rwx"
        for entry in acl_status.entries
    ), "Default group ACL entry should be set"

    # Test remove-default (-k) flag
    cli_main(["setfacl", "-k", "/testdir"])
    acl_status = client.get_acl_status("/testdir")
    assert not any(entry.scope == "default" for entry in acl_status.entries), (
        "All default ACL entries should be removed after -k"
    )

    # Test recursive flag
    client.mkdirs("/testdir_recursive")
    client.mkdirs("/testdir_recursive/subdir")
    client.create("/testdir_recursive/file1").close()
    client.create("/testdir_recursive/subdir/file2").close()

    cli_main(
        [
            "setfacl",
            "-R",
            "-m",
            "user:alice:rwx",
            "/testdir_recursive",
        ]
    )

    # For recursive, we need to check files that were updated
    acl_status = client.get_acl_status("/testdir_recursive/file1")
    assert any(
        entry.name == "alice" and entry.type == "user" and entry.permissions == "rwx"
        for entry in acl_status.entries
    ), "ACL should be set on file1 recursively"

    acl_status = client.get_acl_status("/testdir_recursive/subdir/file2")
    assert any(
        entry.name == "alice" and entry.type == "user" and entry.permissions == "rwx"
        for entry in acl_status.entries
    ), "ACL should be set on file2 recursively"

    # Test invalid ACL spec
    with pytest.raises(ValueError):
        cli_main(["setfacl", "-m", "invalid:alice:rwx", "/testfile"])

    with pytest.raises(ValueError):
        cli_main(["setfacl", "-m", "user:alice:xyz", "/testfile"])

    with pytest.raises(ValueError):
        cli_main(["setfacl", "-m", "user:alice", "/testfile"])


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
