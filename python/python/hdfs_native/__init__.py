import io
from typing import Iterator, Optional
from typing_extensions import Buffer

from ._internal import *

class FileReader(io.RawIOBase):
    
    def __init__(self, inner: "RawFileReader"):
        self.inner = inner

    def __len__(self) -> int:
        return self.inner.file_length()
    
    def __enter__(self):
        # Don't need to do anything special here
        return self
    
    def __exit__(self, *_args):
        # Future updates could close the file manually here if that would help clean things up
        pass

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        """Read up to `size` bytes from the file, or all content if -1"""
        return self.inner.read(size)
    
    def readall(self) -> bytes:
        return self.read()

    def read_range(self, offset: int, len: int) -> bytes:
        """Read `len` bytes from the file starting at `offset`. Doesn't affect the position in the file"""
        return self.inner.read_range(offset, len)
    
    def close(self) -> None:
        pass

class FileWriter(io.RawIOBase):

    def __init__(self, inner: "RawFileWriter"):
        self.inner = inner

    def writable(self) -> bool:
        return True

    def write(self, buf: Buffer) -> int:
        """Writes `buf` to the file. Always writes all bytes"""
        return self.inner.write(buf)

    def close(self) -> None:
        """Closes the file and saves the final metadata to the NameNode"""
        self.inner.close()

    def __enter__(self) -> "FileWriter":
        return self
    
    def __exit__(self, *_args):
        self.close()

class Client:

    def __init__(self, url: str):
        self.inner = RawClient(url)

    def get_file_info(self, path: str) -> "FileStatus":
        """Gets the file status for the file at `path`"""
        return self.inner.get_file_info(path)

    def list_status(self, path: str, recursive: bool) -> Iterator["FileStatus"]:
        """Gets the status of files rooted at `path`. If `recursive` is true, lists all files recursively."""
        return self.inner.list_status(path, recursive)

    def read(self, path: str) -> FileReader:
        """Opens a file for reading at `path`"""
        return FileReader(self.inner.read(path))

    def create(self, path: str, write_options: WriteOptions) -> FileWriter:
        """Creates a new file and opens it for writing at `path`"""
        return FileWriter(self.inner.create(path, write_options))
    
    def append(self, path: str) -> FileWriter:
        """Opens an existing file to append to at `path`"""
        return FileWriter(self.inner.append(path))

    def mkdirs(self, path: str, permission: int, create_parent: bool) -> None:
        """
        Creates a directory at `path` with unix permissions `permission`. If `create_parent` is true,
        any parent directories that don't exist will also be created. Otherwise this will fail if
        all parent directories don't already exist.
        """
        return self.inner.mkdirs(path, permission, create_parent)

    def rename(self, src: str, dst: str, overwrite: bool) -> None:
        """
        Moves a file or directory from `src` to `dst`. If `overwrite` is True, the destination will be
        overriden if it already exists, otherwise the operation will fail if the destination
        exists.
        """
        return self.inner.rename(src, dst, overwrite)

    def delete(self, path: str, recursive: bool) -> bool:
        """
        Deletes a file or directory at `path`. If `recursive` is True and the target is a directory,
        this will delete all contents underneath the directory. If `recursive` is False and the target
        is a non-empty directory, this will fail.
        """
        return self.inner.delete(path, recursive)
