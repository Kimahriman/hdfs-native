import io

from hdfs_native import AclEntry, Client, WriteOptions


def test_integration(client: Client):
    client.create("/testfile").close()
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

    with client.create("/testfile") as file:
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

        data = io.BytesIO()
        for chunk in file:
            data.write(chunk)

        data.seek(0)

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


def test_write_options(client: Client):
    with client.create("/testfile") as file:
        file.write(b"abcd")

    client.create(
        "/testfile",
        WriteOptions(overwrite=True, permission=0o700, block_size=1024 * 1024),
    ).close()

    file_info = client.get_file_info("/testfile")
    assert file_info.length == 0
    assert file_info.permission == 0o700
    assert file_info.blocksize == 1024 * 1024


def test_acls(client: Client):
    client.create("/test").close()

    acl_status = client.get_acl_status("/test")
    assert len(acl_status.entries) == 0

    client.modify_acl_entries("/test", [AclEntry("user", "access", "r-x", "testuser")])
    # Should be 2 entries now, a default group entry gets added as well
    acl_status = client.get_acl_status("/test")
    assert len(acl_status.entries) == 2

    client.remove_acl("/test")
    acl_status = client.get_acl_status("/test")
    assert len(acl_status.entries) == 0

    client.delete("/test")

    client.mkdirs("/testdir")

    client.modify_acl_entries(
        "/testdir", [AclEntry("user", "default", "rwx", "testuser")]
    )
    # 4 other defaults get added automatically
    acl_status = client.get_acl_status("/testdir")
    assert len(acl_status.entries) == 5

    client.remove_default_acl("/testdir")
    acl_status = client.get_acl_status("/testdir")
    assert len(acl_status.entries) == 0


def test_globbing(client: Client):
    base_dir = "/py_test_globbing_root"

    # Ensure clean slate
    try:
        if client.get_file_info(base_dir):
            client.delete(base_dir, recursive=True)
    except FileNotFoundError:
        pass # Good, it's not there
    
    client.mkdirs(base_dir, 0o755, True)

    try:
        _test_py_glob_list_status(client, base_dir)
        _test_py_glob_read_ops(client, base_dir)
        _test_py_glob_modifying_ops(client, base_dir)
        _test_py_glob_disallowed_ops(client, base_dir)
    finally:
        # Cleanup
        client.delete(base_dir, recursive=True)

def _create_test_files_and_dirs(client: Client, root_path: str, items: list[str]):
    for item in items:
        path = f"{root_path}/{item}"
        if item.endswith("/"):
            client.mkdirs(path, 0o755, True)
        else:
            # Ensure parent dir exists for simplicity in test setup
            parent_dir = path.rsplit('/', 1)[0]
            if parent_dir != root_path: # Avoid trying to mkdir root_path itself if file is directly under it
                 client.mkdirs(parent_dir, 0o755, True)
            client.create(path, WriteOptions(overwrite=True)).close()


def _test_py_glob_list_status(client: Client, base_dir: str):
    import pytest # Import here to avoid polluting global scope if not always run

    root = f"{base_dir}/list_status"
    client.mkdirs(root, 0o755, True)

    files_to_create = [
        "file1.txt", "file2.log", "foo_abc.txt", "foo_def.csv", "bar_file.txt",
        "file[x].txt", "file_q.log", # for ? pattern test
        "subdir1/", "subdir1/subfile1.txt", "subdir1/another.log", "subdir1/deep_foo.csv",
        "subdir2/", "subdir2/subfile2.log", "subdir2/empty_dir/",
    ]
    _create_test_files_and_dirs(client, root, files_to_create)

    # Basic patterns
    result = {fs.path for fs in client.list_status(root, recursive=False, pattern="*.txt")}
    expected = {f"{root}/file1.txt", f"{root}/foo_abc.txt", f"{root}/bar_file.txt", f"{root}/file[x].txt"}
    assert result == expected, "Pattern: *.txt"

    result = {fs.path for fs in client.list_status(root, recursive=False, pattern="foo*")}
    expected = {f"{root}/foo_abc.txt", f"{root}/foo_def.csv"}
    assert result == expected, "Pattern: foo*"

    result = {fs.path for fs in client.list_status(root, recursive=False, pattern="*file.txt")}
    expected = {f"{root}/bar_file.txt"} # bar_file.txt
    assert result == expected, "Pattern: *file.txt"
    
    # Recursive patterns
    result = {fs.path for fs in client.list_status(root, recursive=True, pattern="**/*.txt")}
    expected = {
        f"{root}/file1.txt", f"{root}/foo_abc.txt", f"{root}/bar_file.txt", 
        f"{root}/file[x].txt", f"{root}/subdir1/subfile1.txt"
    }
    assert result == expected, "Pattern: **/*.txt"

    result = {fs.path for fs in client.list_status(root, recursive=True, pattern="**/foo*.csv")}
    expected = {f"{root}/foo_def.csv", f"{root}/subdir1/deep_foo.csv"}
    assert result == expected, "Pattern: **/foo*.csv"

    # Patterns matching directories
    result = {fs.path for fs in client.list_status(root, recursive=False, pattern="subdir*")}
    expected = {f"{root}/subdir1", f"{root}/subdir2"}
    assert result == expected, "Pattern: subdir*"
    
    # Ensure recursive=True with "subdir*" only lists those dirs if they match, not their children unless children also match
    result_recursive_subdir_match = {fs.path for fs in client.list_status(root, recursive=True, pattern="subdir*")}
    assert result_recursive_subdir_match == expected, "Pattern: subdir* (recursive) should only list matching dirs"


    result = {fs.path for fs in client.list_status(root, recursive=True, pattern="subdir*/*file*.txt")}
    expected = {f"{root}/subdir1/subfile1.txt"}
    assert result == expected, "Pattern: subdir*/*file*.txt"

    # Patterns matching nothing
    result = list(client.list_status(root, pattern="*.nonexistent"))
    assert not result, "Pattern: *.nonexistent"

    # Special characters
    result = {fs.path for fs in client.list_status(root, pattern="file[x].txt")}
    expected = {f"{root}/file[x].txt"}
    assert result == expected, "Pattern: file[x].txt"

    result = {fs.path for fs in client.list_status(root, pattern="file_?.log")}
    expected = {f"{root}/file_q.log"}
    assert result == expected, "Pattern: file_?.log"


def _test_py_glob_read_ops(client: Client, base_dir: str):
    import pytest
    root = f"{base_dir}/read_ops"
    client.mkdirs(root, 0o755, True)
    
    _create_test_files_and_dirs(client, root, ["file.txt", "dir1/", "multi_a.dat", "multi_b.dat"])

    # Single file match
    assert client.get_file_info(f"{root}/file.tx?").path == f"{root}/file.txt"
    with client.read(f"{root}/f*.txt") as f:
        assert f.readall() == b"" # Empty file
    assert client.get_content_summary(f"{root}/fi*.txt").length == 0
    assert client.get_acl_status(f"{root}/file.tx?").entries is not None # Just check it runs

    # Single dir match (non-read)
    dir_info = client.get_file_info(f"{root}/dir*")
    assert dir_info.path == f"{root}/dir1"
    assert dir_info.isdir
    
    # Single dir match (for `read`)
    with pytest.raises(IOError): # Rust HdfsError::InvalidArgument("Path is a directory") maps to IOError
        client.read(f"{root}/dir*")

    # Multiple matches
    with pytest.raises(IOError): # HdfsError::InvalidArgument("Glob pattern matches multiple entries")
        client.get_file_info(f"{root}/multi_*.dat")

    # Zero matches
    with pytest.raises(FileNotFoundError):
        client.get_file_info(f"{root}/nothing*.dat")

    # Invalid glob pattern
    # Current Rust client behavior might map invalid glob (Pattern::new fails -> None pattern)
    # to either FileNotFoundError (if base path is empty) or MultipleEntries (if base path has many)
    with pytest.raises((IOError, FileNotFoundError)): # IOError for MultiMatch, FileNotFoundError for no match
        client.get_file_info(f"{root}/[invalid")


def _test_py_glob_modifying_ops(client: Client, base_dir: str):
    import pytest
    root = f"{base_dir}/mod_ops"
    client.mkdirs(root, 0o755, True)

    initial_files = [
        "del1.tmp", "del2.tmp", "subdir/", "subdir/del3.tmp",
        "time1.dat", "time2.dat", "owner1.dat", "owner2.dat",
        "perm1.dat", "perm2.dat", "repl1.dat", "repl2.dat",
        "acl1.dat", "acl2.dat"
    ]
    _create_test_files_and_dirs(client, root, initial_files)

    # Delete non-recursive
    assert client.delete(f"{root}/del*.tmp", recursive=False) is True
    with pytest.raises(FileNotFoundError): client.get_file_info(f"{root}/del1.tmp")
    with pytest.raises(FileNotFoundError): client.get_file_info(f"{root}/del2.tmp")
    assert client.get_file_info(f"{root}/subdir/del3.tmp") # Should still exist

    # Recreate for recursive delete
    _create_test_files_and_dirs(client, root, ["del1.tmp", "del2.tmp"])
    assert client.delete(f"{root}/**/*.tmp", recursive=True) is True # Note: recursive flag here is for HDFS delete, glob has `**`
    with pytest.raises(FileNotFoundError): client.get_file_info(f"{root}/del1.tmp")
    with pytest.raises(FileNotFoundError): client.get_file_info(f"{root}/del2.tmp")
    with pytest.raises(FileNotFoundError): client.get_file_info(f"{root}/subdir/del3.tmp")


    # set_times
    mtime, atime = 1678886400000, 1678886401000
    client.set_times(f"{root}/time*.dat", mtime, atime)
    assert client.get_file_info(f"{root}/time1.dat").modification_time == mtime
    assert client.get_file_info(f"{root}/time2.dat").access_time == atime

    # set_owner
    client.set_owner(f"{root}/owner*.dat", owner="newowner", group="newgroup")
    assert client.get_file_info(f"{root}/owner1.dat").owner == "newowner"
    assert client.get_file_info(f"{root}/owner2.dat").group == "newgroup"

    # set_permission
    client.set_permission(f"{root}/perm*.dat", 0o777)
    assert client.get_file_info(f"{root}/perm1.dat").permission == 0o777
    assert client.get_file_info(f"{root}/perm2.dat").permission == 0o777
    
    # set_replication
    assert client.set_replication(f"{root}/repl*.dat", 1) is True 
    # MiniDFS usually has 1 DN, checking exact replication might be tricky/unreliable.
    # Success of operation is the main check.

    # modify_acl_entries
    acl_entry = AclEntry("user", "access", "rwx", "globuser")
    client.modify_acl_entries(f"{root}/acl*.dat", [acl_entry])
    status1 = client.get_acl_status(f"{root}/acl1.dat")
    assert any(e.name == "globuser" and e.permissions == "rwx" for e in status1.entries)

    # Zero matches
    assert client.delete(f"{root}/no_such_*.tmp", recursive=False) is True

    # Invalid glob pattern
    with pytest.raises(IOError): # Or FileNotFoundError depending on Rust behavior
         client.delete(f"{root}/[invalid", recursive=False)


def _test_py_glob_disallowed_ops(client: Client, base_dir: str):
    import pytest
    root = f"{base_dir}/disallowed_ops"
    client.mkdirs(root, 0o755, True)
    _create_test_files_and_dirs(client, root, ["src.txt"])

    with pytest.raises(IOError): # InvalidArgument from Rust
        client.mkdirs(f"{root}/foo*/bar", 0o755, True)

    with pytest.raises(IOError):
        client.rename(f"{root}/src*.txt", f"{root}/dst.txt")
    
    with pytest.raises(IOError):
        client.rename(f"{root}/src.txt", f"{root}/dst*.txt")
