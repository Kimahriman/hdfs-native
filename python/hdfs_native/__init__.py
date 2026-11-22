import io
import os
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Self

from typing_extensions import Buffer

from ._internal import (
    AclEntry,
    AclStatus,
    AsyncRawClient,
    ContentSummary,
    FileStatus,
    RawClient,
    WriteOptions,
)

if TYPE_CHECKING:
    from ._internal import (
        AsyncRawFileReader,
        AsyncRawFileWriter,
        FsAction,
        RawFileReader,
        RawFileWriter,
    )

__all__ = [
    "AclEntry",
    "AclStatus",
    "AsyncClient",
    "AsyncFileReader",
    "AsyncFileWriter",
    "Client",
    "ContentSummary",
    "FileReader",
    "FileStatus",
    "FileWriter",
    "WriteOptions",
]


class FileReader(io.RawIOBase):
    def __init__(self, inner: "RawFileReader"):
        """Wrap a native file reader.

        Args:
            inner: Native reader returned by ``Client.read``.
        """
        self.inner = inner

    def __len__(self) -> int:
        return self.inner.file_length()

    def __iter__(self) -> Iterator[bytes]:
        return self.read_range_stream(0, len(self))

    def __enter__(self):
        # Don't need to do anything special here
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Future updates could close the file manually here if that would help clean things up
        pass

    @property
    def size(self):
        """Return the file size.

        Returns:
            File size in bytes.
        """
        return len(self)

    def seek(self, offset: int, whence=os.SEEK_SET):
        """Move the read position.

        Args:
            offset: Byte offset relative to ``whence``.
            whence: Reference point: ``os.SEEK_SET``, ``os.SEEK_CUR``, or ``os.SEEK_END``.
        """
        if whence == os.SEEK_SET:
            self.inner.seek(offset)
        elif whence == os.SEEK_CUR:
            self.inner.seek(self.tell() + offset)
        elif whence == os.SEEK_END:
            self.inner.seek(self.inner.file_length() + offset)
        else:
            raise ValueError(f"Unsupported whence {whence}")

    def seekable(self):
        """Return whether the reader supports seeking.

        Returns:
            Always ``True``.
        """
        return True

    def tell(self) -> int:
        """Return the current read position.

        Returns:
            Current byte offset.
        """
        return self.inner.tell()

    def readable(self) -> bool:
        """Return whether the reader supports reading.

        Returns:
            Always ``True``.
        """
        return True

    def read(self, size: int = -1) -> bytes:
        """Read bytes from the current position.

        Args:
            size: Maximum number of bytes to read. A negative value reads to the end.

        Returns:
            The bytes read.
        """
        return self.inner.read(size)

    def readall(self) -> bytes:
        """Read all remaining bytes from the current position.

        Returns:
            The remaining file contents.
        """
        return self.read()

    def read_range(self, offset: int, len: int) -> bytes:
        """Read a byte range without changing the current read position.

        Args:
            offset: Starting byte offset.
            len: Number of bytes to read.

        Returns:
            The bytes read from the requested range.
        """
        return self.inner.read_range(offset, len)

    def read_range_stream(self, offset: int, len: int) -> Iterator[bytes]:
        """
        Read `len` bytes from the file starting at `offset` as an iterator of bytes. Doesn't affect
        the position in the file.

        This is the most efficient way to iteratively read a file.

        Args:
            offset: Starting byte offset.
            len: Number of bytes to read.

        Returns:
            An iterator yielding chunks of the requested range.
        """
        return self.inner.read_range_stream(offset, len)

    def close(self) -> None:
        """Provide a no-op close method for file-like compatibility."""
        pass


class FileWriter(io.RawIOBase):
    def __init__(self, inner: "RawFileWriter"):
        """Wrap a native file writer.

        Args:
            inner: Native writer returned by ``Client.create`` or ``Client.append``.
        """
        self.inner = inner

    def writable(self) -> bool:
        """Return whether the writer supports writing.

        Returns:
            Always ``True``.
        """
        return True

    def write(self, buf: Buffer) -> int:
        """Write bytes to the file.

        Args:
            buf: Bytes-like data to write.

        Returns:
            The number of bytes written. This writer writes all bytes in ``buf``.
        """
        return self.inner.write(buf)

    def hsync(self) -> None:
        """Persist written data and update its visible length without closing.

        Supported for replicated files; erasure-coded files are unsupported.

        Raises:
            NotImplementedError: If the file is erasure-coded.
        """
        self.inner.hsync()

    def close(self) -> None:
        """Close the writer and save the final metadata to the NameNode."""
        self.inner.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Client:
    def __init__(
        self,
        url: str | None = None,
        config: dict[str, str] | None = None,
        config_dir: str | None = None,
        *,
        user: str | None = None,
        kerberos_principal: str | None = None,
        kerberos_keytab: str | None = None,
        kerberos_cache: str | None = None,
    ):
        """Create a synchronous HDFS client.

        Args:
            url: HDFS URL. If omitted, use the default filesystem from Hadoop config.
            config: Hadoop settings that override values loaded from configuration files.
            config_dir: Hadoop config directory; overrides ``HADOOP_CONF_DIR``.
            user: Effective HDFS user. If omitted, use ``HADOOP_USER_NAME`` or
                ``HADOOP_PROXY_USER`` when set.
            kerberos_principal: Kerberos principal used to authenticate the client.
            kerberos_keytab: Keytab file used for Kerberos authentication.
            kerberos_cache: Kerberos credential cache file used for authentication.
        """
        self.inner = RawClient(
            url,
            config,
            config_dir,
            kerberos_principal=kerberos_principal,
            kerberos_keytab=kerberos_keytab,
            kerberos_cache=kerberos_cache,
            user=user,
        )

    def get_file_info(self, path: str) -> FileStatus:
        """Get metadata for a file or directory.

        Args:
            path: HDFS path to inspect.

        Returns:
            The file status.
        """
        return self.inner.get_file_info(path)

    def list_status(self, path: str, recursive: bool = False) -> Iterator[FileStatus]:
        """List files and directories beneath a path.

        Args:
            path: HDFS path to list.
            recursive: Whether to include descendants recursively.

        Returns:
            An iterator of file statuses.
        """
        return self.inner.list_status(path, recursive)

    def glob_status(self, pattern: str) -> list[FileStatus]:
        """List file statuses matching an HDFS glob pattern.

        Args:
            pattern: HDFS path pattern to match.

        Returns:
            The matching file statuses.
        """
        return self.inner.glob_status(pattern)

    def read(self, path: str) -> FileReader:
        """Open a file for reading.

        Args:
            path: HDFS path to open.

        Returns:
            A reader for the file.
        """
        return FileReader(self.inner.read(path))

    def create(
        self,
        path: str,
        write_options: WriteOptions | None = None,
    ) -> FileWriter:
        """Create a file and open it for writing.

        Args:
            path: HDFS path to create.
            write_options: Optional file creation settings. Uses defaults when omitted.

        Returns:
            A writer for the new file.
        """
        if not write_options:
            write_options = WriteOptions()

        return FileWriter(self.inner.create(path, write_options))

    def append(self, path: str) -> FileWriter:
        """Open an existing file for appending.

        Args:
            path: HDFS path to open.

        Returns:
            A writer positioned at the end of the file.
        """
        return FileWriter(self.inner.append(path))

    def recover_lease(self, path: str) -> bool:
        """Force lease recovery for a file owned exclusively by this recovery attempt.

        Returns True once the file is closed, or False while recovery proceeds.
        This fences the previous writer. The caller owns polling, timeout, and
        cancellation, and must obtain exclusive recovery authority first.

        Args:
            path: HDFS path of the file whose lease should be recovered.

        Returns:
            ``True`` when the file is closed, or ``False`` while recovery continues.
        """
        return self.inner.recover_lease(path)

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

        Args:
            path: HDFS directory path to create.
            permission: Unix permission bits to apply to the directory.
            create_parent: Whether to create missing parent directories.
        """
        return self.inner.mkdirs(path, permission, create_parent)

    def rename(self, src: str, dst: str, overwrite: bool = False) -> None:
        """
        Moves a file or directory from `src` to `dst`. If `overwrite` is True, the destination will be
        overriden if it already exists, otherwise the operation will fail if the destination
        exists.

        Args:
            src: Existing HDFS path to move.
            dst: Destination HDFS path.
            overwrite: Whether to replace an existing destination.
        """
        return self.inner.rename(src, dst, overwrite)

    def delete(self, path: str, recursive: bool = False) -> bool:
        """
        Deletes a file or directory at `path`. If `recursive` is True and the target is a directory,
        this will delete all contents underneath the directory. If `recursive` is False and the target
        is a non-empty directory, this will fail.

        Args:
            path: HDFS path to delete.
            recursive: Whether to recursively delete a directory's contents.

        Returns:
            Whether an entry was deleted.
        """
        return self.inner.delete(path, recursive)

    def trash(self, path: str) -> str | None:
        """
        Moves a file or directory at `path` into the user's trash. Returns the new location in the
        trash, or None if `path` is already in the trash.

        Args:
            path: HDFS path to move to the user's trash.

        Returns:
            The new trash path, or ``None`` if the path is already in the trash.
        """
        return self.inner.trash(path)

    def set_times(self, path: str, mtime: int, atime: int) -> None:
        """
        Changes the modification time and access time of the file at `path` to `mtime` and `atime`, respectively.

        Args:
            path: HDFS path to update.
            mtime: Modification time in milliseconds since the Unix epoch.
            atime: Access time in milliseconds since the Unix epoch.
        """
        return self.inner.set_times(path, mtime, atime)

    def set_owner(
        self,
        path: str,
        owner: str | None = None,
        group: str | None = None,
    ) -> None:
        """
        Sets the owner and/or group for the file at `path`

        Args:
            path: HDFS path to update.
            owner: New owner name, or ``None`` to leave it unchanged.
            group: New group name, or ``None`` to leave it unchanged.
        """
        return self.inner.set_owner(path, owner, group)

    def set_permission(self, path: str, permission: int) -> None:
        """
        Sets the permissions for file at `path` to the octal value `permission`.
        For example, to set "rw-r--r--" Unix style permissions, use permission=0o644.

        Args:
            path: HDFS path to update.
            permission: Unix permission bits, such as ``0o644``.
        """
        return self.inner.set_permission(path, permission)

    def set_replication(self, path: str, replication: int) -> bool:
        """
        Sets the replication for file at `path` to `replication`

        Args:
            path: HDFS file path to update.
            replication: Desired replication factor.

        Returns:
            Whether the replication factor was updated.
        """
        return self.inner.set_replication(path, replication)

    def get_content_summary(self, path: str) -> ContentSummary:
        """
        Gets a content summary for `path`

        Args:
            path: HDFS path to summarize.

        Returns:
            The summary of file and directory counts and space usage.
        """
        return self.inner.get_content_summary(path)

    def modify_acl_entries(self, path: str, entries: list[AclEntry]) -> None:
        """
        Update ACL entries for file or directory at `path`. Existing entries will remain.

        Args:
            path: HDFS path to update.
            entries: ACL entries to add or modify.
        """
        return self.inner.modify_acl_entries(path, entries)

    def remove_acl_entries(self, path: str, entries: list[AclEntry]) -> None:
        """
        Remove specific ACL entries for file or directory at `path`.

        Args:
            path: HDFS path to update.
            entries: ACL entries to remove.
        """
        return self.inner.remove_acl_entries(path, entries)

    def remove_default_acl(self, path: str) -> None:
        """
        Remove all default ACLs for file or directory at `path`.

        Args:
            path: HDFS path whose default ACL entries should be removed.
        """
        return self.inner.remove_default_acl(path)

    def remove_acl(self, path: str) -> None:
        """
        Remove all ACL entries for file or directory at `path`.

        Args:
            path: HDFS path whose ACL entries should be removed.
        """
        return self.inner.remove_acl(path)

    def set_acl(self, path: str, entries: list[AclEntry]) -> None:
        """
        Override all ACL entries for file or directory at `path`. If only access ACLs are provided,
        default ACLs are maintained. Likewise if only default ACLs are provided, access ACLs are
        maintained.

        Args:
            path: HDFS path to update.
            entries: ACL entries to set.
        """
        return self.inner.set_acl(path, entries)

    def get_acl_status(self, path: str) -> AclStatus:
        """
        Get the ACL status for the file or directory at `path`.

        Args:
            path: HDFS path to inspect.

        Returns:
            The ACL status.
        """
        return self.inner.get_acl_status(path)

    def check_access(self, path: str, mode: "FsAction") -> None:
        """
        Check that the client's user is allowed to access `path` with every permission in
        `mode`, such as "r--" or "rw-". Raises `PermissionError` if access is denied and
        `FileNotFoundError` if `path` doesn't exist.

        Args:
            path: HDFS path to check.
            mode: Required permissions, expressed as a three-character mode such as ``r--`` or ``rwx``.

        Raises:
            PermissionError: If the user lacks any requested permission.
            FileNotFoundError: If the path does not exist.
        """
        return self.inner.check_access(path, mode)


class AsyncFileReader:
    def __init__(self, inner: "AsyncRawFileReader"):
        """Wrap a native asynchronous file reader.

        Args:
            inner: Native reader returned by ``AsyncClient.read``.
        """
        self.inner = inner

    def __len__(self) -> int:
        return self.inner.file_length()

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self.read_range_stream(0, len(self))

    async def __aenter__(self):
        # Don't need to do anything special here
        return self

    async def __aexit__(self, *_args):
        # Future updates could close the file manually here if that would help clean things up
        pass

    @property
    def size(self):
        """Return the file size.

        Returns:
            File size in bytes.
        """
        return len(self)

    def seek(self, offset: int, whence=os.SEEK_SET):
        """Move the read position.

        Args:
            offset: Byte offset relative to ``whence``.
            whence: Reference point: ``os.SEEK_SET``, ``os.SEEK_CUR``, or ``os.SEEK_END``.
        """
        if whence == os.SEEK_SET:
            self.inner.seek(offset)
        elif whence == os.SEEK_CUR:
            self.inner.seek(self.tell() + offset)
        elif whence == os.SEEK_END:
            self.inner.seek(self.inner.file_length() + offset)
        else:
            raise ValueError(f"Unsupported whence {whence}")

    def seekable(self):
        """Return whether the reader supports seeking.

        Returns:
            Always ``True``.
        """
        return True

    def tell(self) -> int:
        """Return the current read position.

        Returns:
            Current byte offset.
        """
        return self.inner.tell()

    def readable(self) -> bool:
        """Return whether the reader supports reading.

        Returns:
            Always ``True``.
        """
        return True

    async def read(self, size: int = -1) -> bytes:
        """Read bytes from the current position.

        Args:
            size: Maximum number of bytes to read. A negative value reads to the end.

        Returns:
            The bytes read.
        """
        return await self.inner.read(size)

    async def readall(self) -> bytes:
        """Read all remaining bytes from the current position.

        Returns:
            The remaining file contents.
        """
        return await self.read()

    async def read_range(self, offset: int, len: int) -> bytes:
        """Read a byte range without changing the current read position.

        Args:
            offset: Starting byte offset.
            len: Number of bytes to read.

        Returns:
            The bytes read from the requested range.
        """
        return await self.inner.read_range(offset, len)

    def read_range_stream(self, offset: int, len: int) -> AsyncIterator[bytes]:
        """
        Read `len` bytes from the file starting at `offset` as an iterator of bytes. Doesn't affect
        the position in the file.

        This is the most efficient way to iteratively read a file.

        Args:
            offset: Starting byte offset.
            len: Number of bytes to read.

        Returns:
            An asynchronous iterator yielding chunks of the requested range.
        """
        return self.inner.read_range_stream(offset, len)

    async def close(self) -> None:
        """Provide a no-op close method for file-like compatibility."""
        pass


class AsyncFileWriter:
    def __init__(self, inner: "AsyncRawFileWriter"):
        """Wrap a native asynchronous file writer.

        Args:
            inner: Native writer returned by ``AsyncClient.create`` or ``AsyncClient.append``.
        """
        self.inner = inner

    def writable(self) -> bool:
        """Return whether the writer supports writing.

        Returns:
            Always ``True``.
        """
        return True

    async def write(self, buf: Buffer) -> int:
        """Write bytes to the file.

        Args:
            buf: Bytes-like data to write.

        Returns:
            The number of bytes written. This writer writes all bytes in ``buf``.
        """
        return await self.inner.write(buf)

    async def hsync(self) -> None:
        """Persist written data and update its visible length without closing.

        Supported for replicated files; erasure-coded files are unsupported.

        Raises:
            NotImplementedError: If the file is erasure-coded.
        """
        await self.inner.hsync()

    async def close(self) -> None:
        """Close the writer and save the final metadata to the NameNode."""
        await self.inner.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args):
        await self.close()


class AsyncClient:
    def __init__(
        self,
        url: str | None = None,
        config: dict[str, str] | None = None,
        config_dir: str | None = None,
        *,
        user: str | None = None,
        kerberos_principal: str | None = None,
        kerberos_keytab: str | None = None,
        kerberos_cache: str | None = None,
    ):
        """Create an asynchronous HDFS client.

        Args:
            url: HDFS URL. If omitted, use the default filesystem from Hadoop config.
            config: Hadoop settings that override values loaded from configuration files.
            config_dir: Hadoop config directory; overrides ``HADOOP_CONF_DIR``.
            user: Effective HDFS user. If omitted, use ``HADOOP_USER_NAME`` or
                ``HADOOP_PROXY_USER`` when set.
            kerberos_principal: Kerberos principal used to authenticate the client.
            kerberos_keytab: Keytab file used for Kerberos authentication.
            kerberos_cache: Kerberos credential cache file used for authentication.
        """
        self.inner = AsyncRawClient(
            url,
            config,
            config_dir,
            kerberos_principal=kerberos_principal,
            kerberos_keytab=kerberos_keytab,
            kerberos_cache=kerberos_cache,
            user=user,
        )

    async def get_file_info(self, path: str) -> FileStatus:
        """Get metadata for a file or directory.

        Args:
            path: HDFS path to inspect.

        Returns:
            The file status.
        """
        return await self.inner.get_file_info(path)

    def list_status(
        self,
        path: str,
        recursive: bool = False,
    ) -> AsyncIterator[FileStatus]:
        """List files and directories beneath a path.

        Args:
            path: HDFS path to list.
            recursive: Whether to include descendants recursively.

        Returns:
            An asynchronous iterator of file statuses.
        """
        return self.inner.list_status(path, recursive)

    async def glob_status(self, pattern: str) -> list[FileStatus]:
        """List file statuses matching an HDFS glob pattern.

        Args:
            pattern: HDFS path pattern to match.

        Returns:
            The matching file statuses.
        """
        return await self.inner.glob_status(pattern)

    async def read(self, path: str) -> AsyncFileReader:
        """Open a file for reading.

        Args:
            path: HDFS path to open.

        Returns:
            An asynchronous reader for the file.
        """
        return AsyncFileReader(await self.inner.read(path))

    async def create(
        self,
        path: str,
        write_options: WriteOptions | None = None,
    ) -> AsyncFileWriter:
        """Create a file and open it for writing.

        Args:
            path: HDFS path to create.
            write_options: Optional file creation settings. Uses defaults when omitted.

        Returns:
            An asynchronous writer for the new file.
        """
        if not write_options:
            write_options = WriteOptions()

        return AsyncFileWriter(await self.inner.create(path, write_options))

    async def append(self, path: str) -> AsyncFileWriter:
        """Open an existing file for appending.

        Args:
            path: HDFS path to open.

        Returns:
            An asynchronous writer positioned at the end of the file.
        """
        return AsyncFileWriter(await self.inner.append(path))

    async def recover_lease(self, path: str) -> bool:
        """Force lease recovery for a file owned exclusively by this recovery attempt.

        Returns True once the file is closed, or False while recovery proceeds.
        This fences the previous writer. The caller owns polling, timeout, and
        cancellation, and must obtain exclusive recovery authority first.

        Args:
            path: HDFS path of the file whose lease should be recovered.

        Returns:
            ``True`` when the file is closed, or ``False`` while recovery continues.
        """
        return await self.inner.recover_lease(path)

    async def mkdirs(
        self,
        path: str,
        permission: int = 0o0755,
        create_parent: bool = False,
    ) -> None:
        """
        Creates a directory at `path` with unix permissions `permission`. If `create_parent` is true,
        any parent directories that don't exist will also be created. Otherwise this will fail if
        all parent directories don't already exist.

        Args:
            path: HDFS directory path to create.
            permission: Unix permission bits to apply to the directory.
            create_parent: Whether to create missing parent directories.
        """
        return await self.inner.mkdirs(path, permission, create_parent)

    async def rename(self, src: str, dst: str, overwrite: bool = False) -> None:
        """
        Moves a file or directory from `src` to `dst`. If `overwrite` is True, the destination will be
        overriden if it already exists, otherwise the operation will fail if the destination
        exists.

        Args:
            src: Existing HDFS path to move.
            dst: Destination HDFS path.
            overwrite: Whether to replace an existing destination.
        """
        return await self.inner.rename(src, dst, overwrite)

    async def delete(self, path: str, recursive: bool = False) -> bool:
        """
        Deletes a file or directory at `path`. If `recursive` is True and the target is a directory,
        this will delete all contents underneath the directory. If `recursive` is False and the target
        is a non-empty directory, this will fail.

        Args:
            path: HDFS path to delete.
            recursive: Whether to recursively delete a directory's contents.

        Returns:
            Whether an entry was deleted.
        """
        return await self.inner.delete(path, recursive)

    async def trash(self, path: str) -> str | None:
        """
        Moves a file or directory at `path` into the user's trash. Returns the new location in the
        trash, or None if `path` is already in the trash.

        Args:
            path: HDFS path to move to the user's trash.

        Returns:
            The new trash path, or ``None`` if the path is already in the trash.
        """
        return await self.inner.trash(path)

    async def set_times(self, path: str, mtime: int, atime: int) -> None:
        """
        Changes the modification time and access time of the file at `path` to `mtime` and `atime`, respectively.

        Args:
            path: HDFS path to update.
            mtime: Modification time in milliseconds since the Unix epoch.
            atime: Access time in milliseconds since the Unix epoch.
        """
        return await self.inner.set_times(path, mtime, atime)

    async def set_owner(
        self,
        path: str,
        owner: str | None = None,
        group: str | None = None,
    ) -> None:
        """
        Sets the owner and/or group for the file at `path`

        Args:
            path: HDFS path to update.
            owner: New owner name, or ``None`` to leave it unchanged.
            group: New group name, or ``None`` to leave it unchanged.
        """
        return await self.inner.set_owner(path, owner, group)

    async def set_permission(self, path: str, permission: int) -> None:
        """
        Sets the permissions for file at `path` to the octal value `permission`.
        For example, to set "rw-r--r--" Unix style permissions, use permission=0o644.

        Args:
            path: HDFS path to update.
            permission: Unix permission bits, such as ``0o644``.
        """
        return await self.inner.set_permission(path, permission)

    async def set_replication(self, path: str, replication: int) -> bool:
        """
        Sets the replication for file at `path` to `replication`

        Args:
            path: HDFS file path to update.
            replication: Desired replication factor.

        Returns:
            Whether the replication factor was updated.
        """
        return await self.inner.set_replication(path, replication)

    async def get_content_summary(self, path: str) -> ContentSummary:
        """
        Gets a content summary for `path`

        Args:
            path: HDFS path to summarize.

        Returns:
            The summary of file and directory counts and space usage.
        """
        return await self.inner.get_content_summary(path)

    async def modify_acl_entries(self, path: str, entries: list[AclEntry]) -> None:
        """
        Update ACL entries for file or directory at `path`. Existing entries will remain.

        Args:
            path: HDFS path to update.
            entries: ACL entries to add or modify.
        """
        return await self.inner.modify_acl_entries(path, entries)

    async def remove_acl_entries(self, path: str, entries: list[AclEntry]) -> None:
        """
        Remove specific ACL entries for file or directory at `path`.

        Args:
            path: HDFS path to update.
            entries: ACL entries to remove.
        """
        return await self.inner.remove_acl_entries(path, entries)

    async def remove_default_acl(self, path: str) -> None:
        """
        Remove all default ACLs for file or directory at `path`.

        Args:
            path: HDFS path whose default ACL entries should be removed.
        """
        return await self.inner.remove_default_acl(path)

    async def remove_acl(self, path: str) -> None:
        """
        Remove all ACL entries for file or directory at `path`.

        Args:
            path: HDFS path whose ACL entries should be removed.
        """
        return await self.inner.remove_acl(path)

    async def set_acl(self, path: str, entries: list[AclEntry]) -> None:
        """
        Override all ACL entries for file or directory at `path`. If only access ACLs are provided,
        default ACLs are maintained. Likewise if only default ACLs are provided, access ACLs are
        maintained.

        Args:
            path: HDFS path to update.
            entries: ACL entries to set.
        """
        return await self.inner.set_acl(path, entries)

    async def get_acl_status(self, path: str) -> AclStatus:
        """
        Get the ACL status for the file or directory at `path`.

        Args:
            path: HDFS path to inspect.

        Returns:
            The ACL status.
        """
        return await self.inner.get_acl_status(path)

    async def check_access(self, path: str, mode: "FsAction") -> None:
        """
        Check that the client's user is allowed to access `path` with every permission in
        `mode`, such as "r--" or "rw-". Raises `PermissionError` if access is denied and
        `FileNotFoundError` if `path` doesn't exist.

        Args:
            path: HDFS path to check.
            mode: Required permissions, expressed as a three-character mode such as ``r--`` or ``rwx``.

        Raises:
            PermissionError: If the user lacks any requested permission.
            FileNotFoundError: If the path does not exist.
        """
        return await self.inner.check_access(path, mode)
