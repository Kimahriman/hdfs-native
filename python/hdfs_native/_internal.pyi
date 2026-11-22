from collections.abc import AsyncIterator, Iterator
from typing import Literal, TypeAlias

from typing_extensions import Buffer

class HdfsError(Exception):
    """Base class for all errors raised by the native HDFS client."""

class DataTransferError(HdfsError):
    """An error occurred while transferring file data to or from a DataNode."""

class ChecksumError(HdfsError):
    """A file block failed checksum verification."""

class InvalidPath(HdfsError):
    """The supplied HDFS path is invalid."""

class InvalidArgument(HdfsError):
    """An argument to an HDFS operation is invalid."""

class UrlParseError(HdfsError):
    """The HDFS URL could not be parsed."""

class OperationFailed(HdfsError):
    """An HDFS operation failed."""

class BlocksNotFound(HdfsError):
    """Block metadata required for a file operation could not be found."""

class TrashNotEnabled(HdfsError):
    """The HDFS trash feature is not enabled."""

class UnsupportedErasureCodingPolicy(HdfsError):
    """The requested erasure-coding policy is unsupported."""

class ErasureCodingError(HdfsError):
    """An erasure-coding operation failed."""

class InternalError(HdfsError):
    """An unexpected internal client error occurred."""

class InvalidRPCResponse(HdfsError):
    """The NameNode returned an invalid RPC response."""

class RPCError(HdfsError):
    """A non-fatal error was returned by a Hadoop RPC call."""

class FatalRPCError(HdfsError):
    """A fatal error was returned by a Hadoop RPC call."""

class SASLError(HdfsError):
    """SASL authentication or negotiation failed."""

class GSSAPIError(HdfsError):
    """GSSAPI or Kerberos authentication failed."""

class NoSASLMechanism(HdfsError):
    """No compatible SASL authentication mechanism was available."""

class XmlParseError(HdfsError):
    """A Hadoop XML configuration file could not be parsed."""

class FileStatus:
    """Metadata describing an HDFS file or directory.

    Attributes:
        path: HDFS path.
        length: File length in bytes.
        isdir: Whether the path is a directory.
        permission: Unix permission bits.
        owner: File or directory owner.
        group: File or directory group.
        modification_time: Modification time in milliseconds since the Unix epoch.
        access_time: Access time in milliseconds since the Unix epoch.
        replication: Replication factor, or ``None`` when not applicable.
        blocksize: Block size in bytes, or ``None`` when not applicable.
    """

    path: str
    length: int
    isdir: bool
    permission: int
    owner: str
    group: str
    modification_time: int
    access_time: int
    replication: int | None
    blocksize: int | None

class ContentSummary:
    """Aggregate counts, lengths, and quotas for an HDFS path.

    Attributes:
        length: Total file length in bytes.
        file_count: Number of files.
        directory_count: Number of directories.
        quota: Namespace quota, or ``-1`` when unset.
        space_consumed: Total space consumed in bytes.
        space_quota: Space quota in bytes, or ``-1`` when unset.
    """

    length: int
    file_count: int
    directory_count: int
    quota: int
    space_consumed: int
    space_quota: int

AclEntryType: TypeAlias = Literal["user", "group", "mask", "other"]
AclEntryScope: TypeAlias = Literal["access", "default"]
FsAction: TypeAlias = Literal["---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx"]

class AclEntry:
    """One access-control-list entry for a file or directory.

    Attributes:
        type: Entry type, such as ``user`` or ``group``.
        scope: Whether the entry applies to the current path or as a default.
        permissions: Permissions expressed as a three-character mode such as ``rwx``.
        name: User or group name for named entries, or ``None`` for unnamed entries.
    """

    type: AclEntryType
    scope: AclEntryScope
    permissions: FsAction
    name: str | None

    def __init__(
        self,
        type: AclEntryType,
        scope: AclEntryScope,
        permissions: FsAction,
        name: str | None = None,
    ):
        """Create an ACL entry.

        Args:
            type: ACL entry type.
            scope: ACL scope, either access or default.
            permissions: Permissions for this entry.
            name: Optional user or group name.
        """

class AclStatus:
    """ACL owner, permissions, and entries for an HDFS path.

    Attributes:
        owner: Path owner.
        group: Path group.
        sticky: Whether the sticky bit is set.
        entries: ACL entries attached to the path.
        permission: Unix permission bits.
    """

    owner: str
    group: str
    sticky: bool
    entries: list[AclEntry]
    permission: int

class WriteOptions:
    """Options controlling how a new HDFS file is created.

    Attributes:
        block_size: Requested block size in bytes, or ``None`` for the configured default.
        replication: Requested replication factor, or ``None`` for the configured default.
        permission: Unix permission bits for the new file.
        overwrite: Whether to replace an existing file.
        create_parent: Whether to create missing parent directories.
    """

    block_size: int | None
    replication: int | None
    permission: int
    overwrite: bool
    create_parent: bool

    def __init__(
        self,
        block_size: int | None = None,
        replication: int | None = None,
        permission: int | None = None,
        overwrite: bool | None = None,
        create_parent: bool | None = None,
    ):
        """Create file-creation options.

        Args:
            block_size: Requested block size in bytes.
            replication: Requested replication factor.
            permission: Unix permission bits for the file.
            overwrite: Whether to replace an existing file.
            create_parent: Whether to create missing parent directories.
        """

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

    def hsync(self) -> None: ...
    def close(self) -> None:
        """Closes the file and saves the final metadata to the NameNode"""

class RawClient:
    def __init__(
        self,
        url: str | None,
        config: dict[str, str] | None,
        config_dir: str | None,
        kerberos_principal: str | None = None,
        kerberos_keytab: str | None = None,
        kerberos_cache: str | None = None,
        user: str | None = None,
    ) -> None: ...
    def get_file_info(self, path: str) -> FileStatus: ...
    def list_status(self, path: str, recursive: bool) -> Iterator[FileStatus]: ...
    def glob_status(self, pattern: str) -> list[FileStatus]: ...
    def read(self, path: str) -> RawFileReader: ...
    def create(self, path: str, write_options: WriteOptions) -> RawFileWriter: ...
    def append(self, path: str) -> RawFileWriter: ...
    def recover_lease(self, path: str) -> bool: ...
    def mkdirs(self, path: str, permission: int, create_parent: bool) -> None: ...
    def rename(self, src: str, dst: str, overwrite: bool) -> None: ...
    def delete(self, path: str, recursive: bool) -> bool: ...
    def trash(self, path: str) -> str | None: ...
    def set_times(self, path: str, mtime: int, atime: int) -> None: ...
    def set_owner(
        self,
        path: str,
        owner: str | None,
        group: str | None,
    ) -> None: ...
    def set_permission(self, path: str, permission: int) -> None: ...
    def set_replication(self, path: str, replication: int) -> bool: ...
    def get_content_summary(self, path: str) -> ContentSummary: ...
    def modify_acl_entries(self, path: str, entries: list[AclEntry]) -> None: ...
    def remove_acl_entries(self, path: str, entries: list[AclEntry]) -> None: ...
    def remove_default_acl(self, path: str) -> None: ...
    def remove_acl(self, path: str) -> None: ...
    def set_acl(self, path: str, entries: list[AclEntry]) -> None: ...
    def get_acl_status(self, path: str) -> AclStatus: ...
    def check_access(self, path: str, mode: FsAction) -> None: ...

class AsyncRawFileReader:
    def file_length(self) -> int:
        """Returns the size of the file"""

    def seek(self, pos: int) -> None:
        """Sets the cursor to the given position"""

    def tell(self) -> int:
        """Returns the current cursor position in the file"""

    async def read(self, len: int) -> bytes:
        """Reads `len` bytes from the file, advancing the position in the file"""

    async def read_range(self, offset: int, len: int) -> bytes:
        """Read `len` bytes from the file starting at `offset`. Doesn't affect the position in the file"""

    def read_range_stream(self, offset: int, len: int) -> AsyncIterator[bytes]:
        """
        Read `len` bytes from the file starting at `offset` as an iterator of bytes. Doesn't affect
        the position in the file.
        """

class AsyncRawFileWriter:
    async def write(self, buf: Buffer) -> int:
        """Writes `buf` to the file"""

    async def hsync(self) -> None: ...
    async def close(self) -> None:
        """Closes the file and saves the final metadata to the NameNode"""

class AsyncRawClient:
    def __init__(
        self,
        url: str | None,
        config: dict[str, str] | None,
        config_dir: str | None,
        kerberos_principal: str | None = None,
        kerberos_keytab: str | None = None,
        kerberos_cache: str | None = None,
        user: str | None = None,
    ) -> None: ...
    async def get_file_info(self, path: str) -> FileStatus: ...
    def list_status(self, path: str, recursive: bool) -> AsyncIterator[FileStatus]: ...
    async def glob_status(self, pattern: str) -> list[FileStatus]: ...
    async def read(self, path: str) -> AsyncRawFileReader: ...
    async def create(
        self,
        path: str,
        write_options: WriteOptions,
    ) -> AsyncRawFileWriter: ...
    async def append(self, path: str) -> AsyncRawFileWriter: ...
    async def recover_lease(self, path: str) -> bool: ...
    async def mkdirs(self, path: str, permission: int, create_parent: bool) -> None: ...
    async def rename(self, src: str, dst: str, overwrite: bool) -> None: ...
    async def delete(self, path: str, recursive: bool) -> bool: ...
    async def trash(self, path: str) -> str | None: ...
    async def set_times(self, path: str, mtime: int, atime: int) -> None: ...
    async def set_owner(
        self,
        path: str,
        owner: str | None,
        group: str | None,
    ) -> None: ...
    async def set_permission(self, path: str, permission: int) -> None: ...
    async def set_replication(self, path: str, replication: int) -> bool: ...
    async def get_content_summary(self, path: str) -> ContentSummary: ...
    async def modify_acl_entries(self, path: str, entries: list[AclEntry]) -> None: ...
    async def remove_acl_entries(self, path: str, entries: list[AclEntry]) -> None: ...
    async def remove_default_acl(self, path: str) -> None: ...
    async def remove_acl(self, path: str) -> None: ...
    async def set_acl(self, path: str, entries: list[AclEntry]) -> None: ...
    async def get_acl_status(self, path: str) -> AclStatus: ...
    async def check_access(self, path: str, mode: FsAction) -> None: ...
