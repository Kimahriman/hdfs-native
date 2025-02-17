import io
import os
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

# For some reason mypy doesn't think this exists
from typing_extensions import Buffer  # type: ignore

from ._internal import (
    AclEntry,
    AclStatus,
    ContentSummary,
    FileStatus,
    RawClient,
    WriteOptions,
)

if TYPE_CHECKING:
    from ._internal import (
        RawFileReader,
        RawFileWriter,
    )

__all__ = [
    "Client",
    "FileReader",
    "FileWriter",
    "FileStatus",
    "ContentSummary",
    "WriteOptions",
    "AclEntry",
    "AclStatus",
]


class FileReader(io.RawIOBase):
    def __init__(self, inner: "RawFileReader"):
        self.inner = inner

    def __len__(self) -> int:
        return self.inner.file_length()

    def __iter__(self) -> Iterator[bytes]:
        return self.read_range_stream(0, len(self))

    def __enter__(self):
        # Don't need to do anything special here
        return self

    def __exit__(self, *_args):
        # Future updates could close the file manually here if that would help clean things up
        pass

    @property
    def size(self):
        return len(self)

    def seek(self, offset: int, whence=os.SEEK_SET):
        """Seek to `offset` relative to `whence`"""
        if whence == os.SEEK_SET:
            self.inner.seek(offset)
        elif whence == os.SEEK_CUR:
            self.inner.seek(self.tell() + offset)
        elif whence == os.SEEK_END:
            self.inner.seek(self.inner.file_length() + offset)
        else:
            raise ValueError(f"Unsupported whence {whence}")

    def seekable(self):
        return True

    def tell(self) -> int:
        return self.inner.tell()

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

    def read_range_stream(self, offset: int, len: int) -> Iterator[bytes]:
        """
        Read `len` bytes from the file starting at `offset` as an iterator of bytes. Doesn't affect
        the position in the file.

        This is the most efficient way to iteratively read a file.
        """
        return self.inner.read_range_stream(offset, len)

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
    def __init__(
        self,
        url: Optional[str] = None,
        config: Optional[Dict[str, str]] = None,
    ):
        self.inner = RawClient(url, config)

    def get_file_info(self, path: str) -> FileStatus:
        """Gets the file status for the file at `path`"""
        return self.inner.get_file_info(path)

    def list_status(self, path: str, recursive: bool = False) -> Iterator[FileStatus]:
        """Gets the status of files rooted at `path`. If `recursive` is true, lists all files recursively."""
        return self.inner.list_status(path, recursive)

    def read(self, path: str) -> FileReader:
        """Opens a file for reading at `path`"""
        return FileReader(self.inner.read(path))

    def create(
        self,
        path: str,
        write_options: Optional[WriteOptions] = None,
    ) -> FileWriter:
        """Creates a new file and opens it for writing at `path`"""
        if not write_options:
            write_options = WriteOptions()

        return FileWriter(self.inner.create(path, write_options))

    def append(self, path: str) -> FileWriter:
        """Opens an existing file to append to at `path`"""
        return FileWriter(self.inner.append(path))

    def mkdirs(
        self,
        path: str,
        permission: int = 0o0755,
        create_parent: bool = False,
    ) -> None:
        """
        Creates a directory at `path` with unix permissions `permission`. If `create_parent` is true,
        any parent directories that don't exist will also be created. Otherwise this will fail if
        all parent directories don't already exist.
        """
        return self.inner.mkdirs(path, permission, create_parent)

    def rename(self, src: str, dst: str, overwrite: bool = False) -> None:
        """
        Moves a file or directory from `src` to `dst`. If `overwrite` is True, the destination will be
        overriden if it already exists, otherwise the operation will fail if the destination
        exists.
        """
        return self.inner.rename(src, dst, overwrite)

    def delete(self, path: str, recursive: bool = False) -> bool:
        """
        Deletes a file or directory at `path`. If `recursive` is True and the target is a directory,
        this will delete all contents underneath the directory. If `recursive` is False and the target
        is a non-empty directory, this will fail.
        """
        return self.inner.delete(path, recursive)

    def set_times(self, path: str, mtime: int, atime: int) -> None:
        """
        Changes the modification time and access time of the file at `path` to `mtime` and `atime`, respectively.
        """
        return self.inner.set_times(path, mtime, atime)

    def set_owner(
        self,
        path: str,
        owner: Optional[str] = None,
        group: Optional[str] = None,
    ) -> None:
        """
        Sets the owner and/or group for the file at `path`
        """
        return self.inner.set_owner(path, owner, group)

    def set_permission(self, path: str, permission: int) -> None:
        """
        Sets the permissions for file at `path` to the octal value `permission`.
        For example, to set "rw-r--r--" Unix style permissions, use permission=0o644.
        """
        return self.inner.set_permission(path, permission)

    def set_replication(self, path: str, replication: int) -> bool:
        """
        Sets the replication for file at `path` to `replication`
        """
        return self.inner.set_replication(path, replication)

    def get_content_summary(self, path: str) -> ContentSummary:
        """
        Gets a content summary for `path`
        """
        return self.inner.get_content_summary(path)

    def modify_acl_entries(self, path: str, entries: List[AclEntry]) -> None:
        """
        Update ACL entries for file or directory at `path`. Existing entries will remain.
        """
        return self.inner.modify_acl_entries(path, entries)

    def remove_acl_entries(self, path: str, entries: List[AclEntry]) -> None:
        """
        Remove specific ACL entries for file or directory at `path`.
        """
        return self.inner.remove_acl_entries(path, entries)

    def remove_default_acl(self, path: str) -> None:
        """
        Remove all default ACLs for file or directory at `path`.
        """
        return self.inner.remove_default_acl(path)

    def remove_acl(self, path: str) -> None:
        """
        Remove all ACL entries for file or directory at `path`.
        """
        return self.inner.remove_acl(path)

    def set_acl(self, path: str, entries: List[AclEntry]) -> None:
        """
        Override all ACL entries for file or directory at `path`. If only access ACLs are provided,
        default ACLs are maintained. Likewise if only default ACLs are provided, access ACLs are
        maintained.
        """
        return self.inner.set_acl(path, entries)

    def get_acl_status(self, path: str) -> AclStatus:
        """
        Get the ACL status for the file or directory at `path`.
        """
        return self.inner.get_acl_status(path)
