import io
import posixpath
import secrets
import shutil
import time
import urllib.parse
from contextlib import suppress
from datetime import datetime
from typing import TYPE_CHECKING

from fsspec import AbstractFileSystem
from fsspec.utils import tokenize

from . import Client, WriteOptions

if TYPE_CHECKING:
    from . import FileStatus

__all__ = ["HdfsFileSystem", "ViewfsFileSystem"]


class BaseFileSystem(AbstractFileSystem):
    root_marker = "/"

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        *args,
        **storage_options,
    ):
        """Create an fsspec adapter for an HDFS filesystem.

        Args:
            host: NameNode host, if specifying a host directly.
            port: NameNode port, if specifying a host directly.
            *args: Positional options forwarded to ``AbstractFileSystem``.
            **storage_options: Hadoop configuration values passed to the HDFS client.
        """
        super().__init__(host, port, *args, **storage_options)
        self.host = host
        self.port = port
        url = f"{self.protocol}://"
        if host:
            url += host
            if port:
                url += f":{port}"
        self.client = Client(url, storage_options)

    @property
    def fsid(self):
        """Return an identifier for this filesystem configuration."""
        return f"hdfs_native_{tokenize(self.protocol, self.host, self.port)}"

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        url = urllib.parse.urlparse(path)
        return url.path or cls.root_marker

    def unstrip_protocol(self, name: str) -> str:
        """Add this filesystem's protocol and authority to a path.

        Args:
            name: HDFS path to qualify.

        Returns:
            The fully qualified path.
        """
        path = self._strip_protocol(name)

        url = f"{self.protocol}://"
        if self.host:
            url += self.host
            if self.port:
                url += f":{self.port}"

        return f"{url}{path}"

    @staticmethod
    def _get_kwargs_from_urls(path):
        url = urllib.parse.urlparse(path)
        return {"host": url.hostname, "port": url.port}

    def _convert_file_status(self, file_status: "FileStatus") -> dict:
        return {
            "name": file_status.path,
            "size": file_status.length,
            "type": "directory" if file_status.isdir else "file",
            "permission": file_status.permission,
            "owner": file_status.owner,
            "group": file_status.group,
            "modification_time": file_status.modification_time,
            "access_time": file_status.access_time,
        }

    def info(self, path, **_kwargs) -> dict:
        """Return metadata for a file or directory.

        Args:
            path: HDFS path to inspect.
            **_kwargs: Additional fsspec options, accepted for interface compatibility.

        Returns:
            A dictionary containing file status fields.
        """
        file_status = self.client.get_file_info(self._strip_protocol(path))
        return self._convert_file_status(file_status)

    def exists(self, path, **_kwargs):
        """Check whether a path exists.

        Args:
            path: HDFS path to check.
            **_kwargs: Additional fsspec options, accepted for interface compatibility.

        Returns:
            ``True`` if the path exists, otherwise ``False``.
        """
        try:
            self.info(path)
            return True
        except FileNotFoundError:
            return False

    def ls(self, path: str, detail=True, **kwargs) -> list[str | dict]:
        """List entries in a directory.

        Args:
            path: HDFS directory path to list.
            detail: Whether to return file status dictionaries instead of paths.
            **kwargs: Additional fsspec options, accepted for interface compatibility.

        Returns:
            File paths or detailed file status dictionaries.
        """
        listing = self.client.list_status(self._strip_protocol(path), False)
        if detail:
            return [self._convert_file_status(status) for status in listing]
        else:
            return [status.path for status in listing]

    def touch(self, path: str, truncate=True, **kwargs):
        """Create a file or update its timestamps.

        Args:
            path: HDFS path to create or update.
            truncate: Whether to truncate an existing file. If false, preserve an existing file's contents and update its timestamps.
            **kwargs: Options forwarded when opening a new file.
        """
        path = self._strip_protocol(path)
        if truncate or not self.exists(path):
            with self.open(path, "wb", **kwargs):
                pass
        else:
            now = int(time.time() * 1000)
            self.client.set_times(path, now, now)

    def du(
        self,
        path: str,
        total=True,
        maxdepth: int | None = None,
        withdirs=False,
        **kwargs,
    ) -> int | dict[str, int]:
        """Return file or directory disk usage.

        Args:
            path: HDFS path to measure.
            total: Whether to return a single total or per-path usage details.
            maxdepth: Maximum directory depth for detailed usage. Unsupported when ``total`` is true.
            withdirs: Whether detailed usage includes directories.
            **kwargs: Additional options forwarded to the base fsspec implementation.

        Returns:
            Total bytes used, or a mapping of paths to byte counts.

        Raises:
            NotImplementedError: If ``total`` is true and ``maxdepth`` is specified.
        """
        if total:
            if maxdepth is not None:
                raise NotImplementedError("maxdepth is not supported with total")

            content_summary = self.client.get_content_summary(path)
            return content_summary.length
        else:
            return super().du(path, total, maxdepth, withdirs, **kwargs)

    def mkdir(self, path: str, create_parents=True, **kwargs):
        """Create a directory.

        Args:
            path: HDFS directory path to create.
            create_parents: Whether to create missing parent directories.
            **kwargs: Additional options; ``permission`` sets the directory's Unix permission bits.
        """
        self.client.mkdirs(
            self._strip_protocol(path),
            kwargs.get("permission", 0o755),
            create_parents,
        )

    def makedirs(self, path: str, exist_ok=False):
        """Create a directory and any missing parents.

        Args:
            path: HDFS directory path to create.
            exist_ok: Whether to allow the directory to already exist.
        """
        path = self._strip_protocol(path)
        if not exist_ok and self.exists(path):
            raise FileExistsError("File or directory already exists")

        return self.mkdir(path, create_parents=True)

    def mv(self, path1: str, path2: str, **kwargs):  # ty:ignore[invalid-method-override]
        """Move a file or directory.

        Like ``shutil.move`` and the ``mv`` of the other fsspec filesystems, an
        existing destination is replaced. Pass ``overwrite=False`` to get the
        behavior of ``hdfs dfs -mv``, which fails with ``FileExistsError``.

        Args:
            path1: Source HDFS path.
            path2: Destination HDFS path.
            **kwargs: Additional options; ``overwrite`` controls replacement of an existing destination.
        """
        self.client.rename(
            self._strip_protocol(path1),
            self._strip_protocol(path2),
            kwargs.get("overwrite", True),
        )

    def cp_file(self, path1, path2, **kwargs):
        """Copy a file to another HDFS path.

        Args:
            path1: Source HDFS file path.
            path2: Destination HDFS file path.
            **kwargs: Additional fsspec options, accepted for interface compatibility.
        """
        with self._open(self._strip_protocol(path1), "rb") as lstream:
            dst = self._strip_protocol(path2)
            # Hidden temporary file next to the destination, so the final rename
            # stays within one directory.
            tmp_fname = posixpath.join(
                posixpath.dirname(dst),
                f".{posixpath.basename(dst)}.tmp.{secrets.token_hex(6)}",
            )
            try:
                with self.open(tmp_fname, "wb") as rstream:
                    shutil.copyfileobj(lstream, rstream)
                self.mv(tmp_fname, dst, overwrite=True)
            except BaseException:
                with suppress(FileNotFoundError):
                    self.rm(tmp_fname)
                raise

    def rmdir(self, path: str) -> None:
        """Remove an empty directory.

        Args:
            path: HDFS directory path to remove.
        """
        self.client.delete(self._strip_protocol(path), False)

    def rm(self, path: str, recursive=False, maxdepth: int | None = None) -> None:
        """Remove a file or directory.

        Args:
            path: HDFS path to remove.
            recursive: Whether to recursively remove a directory's contents.
            maxdepth: Unsupported; must be ``None``.

        Raises:
            NotImplementedError: If ``maxdepth`` is specified.
        """
        if maxdepth is not None:
            raise NotImplementedError("maxdepth is not supported")
        self.client.delete(self._strip_protocol(path), recursive)

    def rm_file(self, path: str):
        """Remove a file.

        Args:
            path: HDFS file path to remove.
        """
        self.rm(self._strip_protocol(path))

    def modified(self, path: str):
        """Get a file's modification time.

        Args:
            path: HDFS path to inspect.

        Returns:
            The modification time as a ``datetime``.
        """
        file_info = self.client.get_file_info(self._strip_protocol(path))
        return datetime.fromtimestamp(file_info.modification_time / 1000)

    def _open(
        self,
        path: str,
        mode="rb",
        overwrite=True,
        replication: int | None = None,
        block_size: int | None = None,
        **_kwargs,
    ):  # ty:ignore[invalid-method-override]
        path = self._strip_protocol(path)
        if mode == "rb":
            reader = self.client.read(path)
            if not block_size:
                return reader
            return io.BufferedReader(reader, buffer_size=block_size)
        elif mode == "wb":
            write_options = WriteOptions()
            write_options.overwrite = overwrite
            if replication:
                write_options.replication = replication
            if block_size:
                write_options.block_size = block_size
            return self.client.create(path, write_options=write_options)
        elif mode == "ab":
            return self.client.append(path)
        else:
            raise ValueError(f"Mode {mode} is not supported")


class HdfsFileSystem(BaseFileSystem):
    """fsspec filesystem adapter for the ``hdfs://`` protocol."""

    protocol = "hdfs"


class ViewfsFileSystem(BaseFileSystem):
    """fsspec filesystem adapter for the ``viewfs://`` protocol."""

    protocol = "viewfs"
