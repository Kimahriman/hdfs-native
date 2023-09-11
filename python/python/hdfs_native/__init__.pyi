from typing import Iterator, Optional


class FileStatus:
    path: str
    length: int
    isdir: bool
    permission: int
    owner: str
    group: str
    modification_time: int
    access_time: int


class FileReader:
    def file_length(self) -> int:
        """Returns the size of the file"""

    def read(self, len: int) -> bytes:
        """Reads `len` bytes from the file, advancing the position in the file"""

    def read_range(self, offset: int, len: int) -> bytes:
        """Read `len` bytes from the file starting at `offset`. Doesn't affect the position in the file"""

class WriteOptions:
    block_size: Optional[int]
    replication: Optional[int]
    permission: int
    overwrite: bool
    create_parent: bool

class FileWriter:
    def write(self, buf: bytes) -> None:
        """Writes `buf` to the file"""

    def close(self) -> None:
        """Closes the file and saves the final metadata to the NameNode"""

class Client:
    def __init__(self, url: str) -> None:
        """Creates a new Client for the NameNode or Name Service defined by `url`"""

    def get_file_info(self, path: str) -> FileStatus:
        """Gets the file status for the file at `path`"""

    def list_status(self, path: str, recursive: bool) -> Iterator[FileStatus]:
        """Gets the status of files rooted at `path`. If `recursive` is true, lists all files recursively."""

    def read(self, path: str) -> FileReader:
        """Opens a file for reading at `path`"""

    def create(self, path: str, write_options: WriteOptions) -> FileWriter:
        """Creates a new file and opens it for writing at `path`"""

    def mkdirs(self, path: str, permission: int, create_parent: bool) -> None:
        """
        Creates a directory at `path` with unix permissions `permission`. If `create_parent` is true,
        any parent directories that don't exist will also be created. Otherwise this will fail if
        all parent directories don't already exist.
        """

    def rename(self, src: str, dst: str, overwrite: bool) -> None:
        """
        Moves a file or directory from `src` to `dst`. If `overwrite` is True, the destination will be
        overriden if it already exists, otherwise the operation will fail if the destination
        exists.
        """

    def delete(self, path: str, recursive: bool) -> bool:
        """
        Deletes a file or directory at `path`. If `recursive` is True and the target is a directory,
        this will delete all contents underneath the directory. If `recursive` is False and the target
        is a non-empty directory, this will fail.
        """