import io
import secrets
import shutil
import time
import urllib.parse
from contextlib import suppress
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from fsspec import AbstractFileSystem
from fsspec.utils import tokenize

from . import Client, WriteOptions

if TYPE_CHECKING:
    from . import FileStatus


class HdfsFileSystem(AbstractFileSystem):
    root_marker = "/"

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        *args,
        **storage_options,
    ):
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
        return f"hdfs_native_{tokenize(self.protocol, self.host, self.port)}"

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        url = urllib.parse.urlparse(path)
        return url.path or cls.root_marker

    def unstrip_protocol(self, name: str) -> str:
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

    def _convert_file_status(self, file_status: "FileStatus") -> Dict:
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

    def info(self, path, **_kwargs) -> Dict:
        file_status = self.client.get_file_info(self._strip_protocol(path))
        return self._convert_file_status(file_status)

    def exists(self, path, **_kwargs):
        try:
            self.info(path)
            return True
        except FileNotFoundError:
            return False

    def ls(self, path: str, detail=True, **kwargs) -> List[Union[str, Dict]]:
        listing = self.client.list_status(self._strip_protocol(path), False)
        if detail:
            return [self._convert_file_status(status) for status in listing]
        else:
            return [status.path for status in listing]

    def touch(self, path: str, truncate=True, **kwargs):
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
        maxdepth: Optional[int] = None,
        withdirs=False,
        **kwargs,
    ) -> Union[int, Dict[str, int]]:
        if total:
            if maxdepth is not None:
                raise NotImplementedError("maxdepth is not supported with total")

            content_summary = self.client.get_content_summary(path)
            return content_summary.length
        else:
            return super().du(path, total, maxdepth, withdirs, **kwargs)

    def mkdir(self, path: str, create_parents=True, **kwargs):
        self.client.mkdirs(
            self._strip_protocol(path),
            kwargs.get("permission", 0o755),
            create_parents,
        )

    def makedirs(self, path: str, exist_ok=False):
        path = self._strip_protocol(path)
        if not exist_ok and self.exists(path):
            raise FileExistsError("File or directory already exists")

        return self.mkdir(path, create_parents=True)

    def mv(self, path1: str, path2: str, **kwargs):
        self.client.rename(
            self._strip_protocol(path1),
            self._strip_protocol(path2),
            kwargs.get("overwrite", False),
        )

    def cp_file(self, path1, path2, **kwargs):
        with self._open(self._strip_protocol(path1), "rb") as lstream:
            tmp_fname = f".{self._strip_protocol(path2)}.tmp.{secrets.token_hex(6)}"
            try:
                with self.open(tmp_fname, "wb") as rstream:
                    shutil.copyfileobj(lstream, rstream)
                self.mv(tmp_fname, path2)
            except BaseException:  # noqa
                with suppress(FileNotFoundError):
                    self.fs.delete_file(tmp_fname)
                raise

    def rmdir(self, path: str) -> None:
        self.client.delete(self._strip_protocol(path), False)

    def rm(self, path: str, recursive=False, maxdepth: Optional[int] = None) -> None:
        if maxdepth is not None:
            raise NotImplementedError("maxdepth is not supported")
        self.client.delete(self._strip_protocol(path), recursive)

    def rm_file(self, path: str):
        self.rm(self._strip_protocol(path))

    def modified(self, path: str):
        file_info = self.client.get_file_info(self._strip_protocol(path))
        return datetime.fromtimestamp(file_info.modification_time)

    def _open(
        self,
        path: str,
        mode="rb",
        overwrite=True,
        replication: Optional[int] = None,
        block_size: Optional[int] = None,
        **_kwargs,
    ):
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
