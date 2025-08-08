import urllib.parse
from abc import ABC, abstractmethod
from datetime import datetime

import fsspec
import pytest

from hdfs_native.fsspec import BaseFileSystem


def test_config(minidfs: str):
    url = urllib.parse.urlparse(minidfs)
    fs: BaseFileSystem = fsspec.filesystem(url.scheme, **{"fs.defaultFS": minidfs})
    assert len(fs.ls("/")) == 0


class TestFsspecBase(ABC):
    @abstractmethod
    def file_system(self, url: str) -> BaseFileSystem:
        pass

    @pytest.fixture(scope="class")
    def fs(self, minidfs: str) -> BaseFileSystem:
        return self.file_system(minidfs)

    def test_dirs(self, fs: BaseFileSystem):
        fs.mkdir("/testdir")
        assert fs.info("/testdir")["type"] == "directory"

        with pytest.raises(FileExistsError):
            fs.makedirs("/testdir", exist_ok=False)

        fs.makedirs("/testdir", exist_ok=True)

        fs.mkdir("/testdir/nested/dir")
        assert fs.info("/testdir/nested/dir")["type"] == "directory"

        with pytest.raises(FileNotFoundError):
            fs.mkdir("/testdir/nested2/dir", create_parents=False)

        with pytest.raises(RuntimeError):
            fs.rm("/testdir", recursive=False)

        fs.rm("/testdir", recursive=True)

        assert not fs.exists("/testdir")

    def test_io(self, fs: BaseFileSystem):
        with fs.open("/test", mode="wb") as file:
            file.write(b"hello there")

        with fs.open("/test", mode="rb") as file:
            data = file.read()
            assert data == b"hello there"

        with fs.open("/test", mode="rb", block_size=1024) as file:
            data = file.read()
            assert data == b"hello there"

        fs.write_bytes("/test2", b"hello again")
        assert fs.read_bytes("/test2") == b"hello again"
        assert fs.read_bytes("/test2", start=1) == b"ello again"
        assert fs.read_bytes("/test2", end=-1) == b"hello agai"

        fs.mv("/test2", "/test3")
        assert fs.read_text("/test3") == "hello again"
        assert not fs.exists("/test2")

        millis = fs.info("/test3")["modification_time"]
        expected_dt = datetime.fromtimestamp(millis / 1000)
        modified_dt = fs.modified("/test3")
        assert isinstance(modified_dt, datetime)
        assert modified_dt == expected_dt

        fs.rm("/test")
        fs.rm("/test3")

    def test_listing(self, fs: BaseFileSystem):
        fs.mkdir("/testdir")

        fs.touch("/testdir/test1")
        fs.touch("/testdir/test2")

        assert fs.ls("/", detail=False) == ["/testdir"]
        assert fs.ls("/testdir", detail=False) == ["/testdir/test1", "/testdir/test2"]

        listing = fs.ls("/", detail=True)
        assert len(listing) == 1
        assert isinstance(listing[0], dict)
        assert listing[0]["size"] == 0
        assert listing[0]["name"] == "/testdir"
        assert listing[0]["type"] == "directory"

        fs.rm("/testdir", True)

    def test_du(self, fs: BaseFileSystem):
        with fs.open("/test", mode="wb") as file:
            file.write(b"hello there")

        with fs.open("/test2", mode="wb") as file:
            file.write(b"hello again")

        assert fs.du("/test") == 11
        assert fs.du("/test2") == 11
        assert fs.du("/") == 22

        assert fs.du("/", total=False) == {"/test": 11, "/test2": 11}


class TestBaseFileSystem(TestFsspecBase):
    def file_system(self, minidfs: str):
        url = urllib.parse.urlparse(minidfs)

        return fsspec.filesystem("hdfs", host=url.hostname, port=url.port)


class TestViewfsFileSystem(TestFsspecBase):
    def file_system(self, minidfs: str):
        return fsspec.filesystem(
            "viewfs",
            host="test",
            **{"fs.viewfs.mounttable.test.linkFallback": minidfs},
        )


def test_parsing(minidfs: str):
    with fsspec.open(f"{minidfs}/test", "wb") as f:
        f.write(b"hey there")

    url = urllib.parse.urlparse(minidfs)
    fs: BaseFileSystem
    urlpath: str
    fs, urlpath = fsspec.url_to_fs(f"{minidfs}/path")
    assert fs.host == url.hostname
    assert fs.port == url.port
    assert urlpath == "/path"

    assert fs.unstrip_protocol("/path") == f"{minidfs}/path"
