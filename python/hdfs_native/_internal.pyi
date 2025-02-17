from typing import Dict, Iterator, List, Literal, Optional

# For some reason mypy doesn't think this exists
from typing_extensions import Buffer  # type: ignore

class FileStatus:
    path: str
    length: int
    isdir: bool
    permission: int
    owner: str
    group: str
    modification_time: int
    access_time: int
    replication: Optional[int]
    blocksize: Optional[int]

class ContentSummary:
    length: int
    file_count: int
    directory_count: int
    quota: int
    space_consumed: int
    space_quota: int

AclEntryType = Literal["user", "group", "mask", "other"]
AclEntryScope = Literal["access", "default"]
FsAction = Literal["---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx"]

class AclEntry:
    type: AclEntryType
    scope: AclEntryScope
    permissions: FsAction
    name: Optional[str]

    def __init__(
        self,
        type: AclEntryType,
        scope: AclEntryScope,
        permissions: FsAction,
        name: Optional[str] = None,
    ): ...

class AclStatus:
    owner: str
    group: str
    sticky: bool
    entries: List[AclEntry]
    permission: int

class WriteOptions:
    block_size: Optional[int]
    replication: Optional[int]
    permission: int
    overwrite: bool
    create_parent: bool

    def __init__(
        self,
        block_size: Optional[int] = None,
        replication: Optional[int] = None,
        permission: Optional[int] = None,
        overwrite: Optional[bool] = None,
        create_parent: Optional[bool] = None,
    ): ...

class RawFileReader:
    def file_length(self) -> int:
        """Returns the size of the file"""

    def seek(self, pos: int) -> None:
        """Sets the cursor to the given position"""

    def tell(self) -> int:
        """Returns the current cursor position in the file"""

    def read(self, len: int) -> bytes:
        """Reads `len` bytes from the file, advancing the position in the file"""

    def read_range(self, offset: int, len: int) -> bytes:
        """Read `len` bytes from the file starting at `offset`. Doesn't affect the position in the file"""

    def read_range_stream(self, offset: int, len: int) -> Iterator[bytes]:
        """
        Read `len` bytes from the file starting at `offset` as an iterator of bytes. Doesn't affect
        the position in the file.
        """

class RawFileWriter:
    def write(self, buf: Buffer) -> int:
        """Writes `buf` to the file"""

    def close(self) -> None:
        """Closes the file and saves the final metadata to the NameNode"""

class RawClient:
    def __init__(
        self,
        url: Optional[str],
        config: Optional[Dict[str, str]],
    ) -> None: ...
    def get_file_info(self, path: str) -> FileStatus: ...
    def list_status(self, path: str, recursive: bool) -> Iterator[FileStatus]: ...
    def read(self, path: str) -> RawFileReader: ...
    def create(self, path: str, write_options: WriteOptions) -> RawFileWriter: ...
    def append(self, path: str) -> RawFileWriter: ...
    def mkdirs(self, path: str, permission: int, create_parent: bool) -> None: ...
    def rename(self, src: str, dst: str, overwrite: bool) -> None: ...
    def delete(self, path: str, recursive: bool) -> bool: ...
    def set_times(self, path: str, mtime: int, atime: int) -> None: ...
    def set_owner(
        self,
        path: str,
        owner: Optional[str],
        group: Optional[str],
    ) -> None: ...
    def set_permission(self, path: str, permission: int) -> None: ...
    def set_replication(self, path: str, replication: int) -> bool: ...
    def get_content_summary(self, path: str) -> ContentSummary: ...
    def modify_acl_entries(self, path: str, entries: List[AclEntry]) -> None: ...
    def remove_acl_entries(self, path: str, entries: List[AclEntry]) -> None: ...
    def remove_default_acl(self, path: str) -> None: ...
    def remove_acl(self, path: str) -> None: ...
    def set_acl(self, path: str, entries: List[AclEntry]) -> None: ...
    def get_acl_status(self, path: str) -> AclStatus: ...
