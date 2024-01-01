from typing import Iterator, Optional
from typing_extensions import Buffer


class FileStatus:
    path: str
    length: int
    isdir: bool
    permission: int
    owner: str
    group: str
    modification_time: int
    access_time: int

class WriteOptions:
    block_size: Optional[int]
    replication: Optional[int]
    permission: int
    overwrite: bool
    create_parent: bool

class RawFileReader:
    def file_length(self) -> int:
        """Returns the size of the file"""

    def read(self, len: int) -> bytes:
        """Reads `len` bytes from the file, advancing the position in the file"""

    def read_range(self, offset: int, len: int) -> bytes:
        """Read `len` bytes from the file starting at `offset`. Doesn't affect the position in the file"""

class RawFileWriter:
    def write(self, buf: Buffer) -> int:
        """Writes `buf` to the file"""

    def close(self) -> None:
        """Closes the file and saves the final metadata to the NameNode"""

class RawClient:
    def __init__(self, url: str) -> None: ...

    def get_file_info(self, path: str) -> FileStatus: ...

    def list_status(self, path: str, recursive: bool) -> Iterator[FileStatus]: ...

    def read(self, path: str) -> RawFileReader: ...

    def create(self, path: str, write_options: WriteOptions) -> RawFileWriter: ...

    def append(self, path: str) -> RawFileWriter: ...

    def mkdirs(self, path: str, permission: int, create_parent: bool) -> None: ...

    def rename(self, src: str, dst: str, overwrite: bool) -> None: ...

    def delete(self, path: str, recursive: bool) -> bool: ...