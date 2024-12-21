import functools
import os
import sys
from argparse import ArgumentParser, Namespace
from typing import List, Optional, Sequence
from urllib.parse import urlparse

from hdfs_native import Client


@functools.cache
def _get_client(connection_url: Optional[str] = None):
    return Client(connection_url)


def _client_for_url(url: str) -> Client:
    parsed = urlparse(url)

    if parsed.scheme:
        connection_url = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            connection_url += f":{parsed.port}"
        return _get_client(connection_url)
    elif parsed.hostname or parsed.port:
        raise ValueError(
            f"Cannot provide host or port without scheme: {parsed.hostname}"
        )
    else:
        return _get_client()


def _verify_nameservices_match(url: str, *urls: str) -> None:
    first = urlparse(url)

    for url in urls:
        parsed = urlparse(url)
        if first.scheme != parsed.scheme or first.hostname != parsed.hostname:
            raise ValueError(
                f"Protocol and host must match: {first.scheme}://{first.hostname} != {parsed.scheme}://{parsed.hostname}"
            )


def _path_for_url(url: str) -> str:
    return urlparse(url).path


def _glob_path(client: Client, glob: str) -> List[str]:
    # TODO: Actually implement this, for now just pretend we have multiple results
    return [glob]


def cat(args: Namespace):
    for src in args.src:
        client = _client_for_url(src)
        for path in _glob_path(client, src):
            with client.read(path) as file:
                while chunk := file.read(1024 * 1024):
                    sys.stdout.buffer.write(chunk)

    sys.stdout.buffer.flush()


def mkdir(args: Namespace):
    create_parent = args.parent

    for path in args.path:
        client = _client_for_url(path)
        client.mkdirs(path, create_parent=create_parent)


def mv(args: Namespace):
    _verify_nameservices_match(args.dst, *args.src)

    client = _client_for_url(args.dst)
    dst_path = _path_for_url(args.dst)

    dst_isdir = False
    try:
        dst_isdir = client.get_file_info(dst_path).isdir
    except FileNotFoundError:
        pass

    resolved_src = [
        path for pattern in args.src for path in _glob_path(client, pattern)
    ]

    if len(resolved_src) > 1 and not dst_isdir:
        raise ValueError(
            "destination must be a directory if multiple sources are provided"
        )

    for src in resolved_src:
        src_path = _path_for_url(src)
        if dst_isdir:
            target_path = os.path.join(dst_path, os.path.basename(src_path))
        else:
            target_path = dst_path

        client.rename(src_path, target_path)


def main(in_args: Optional[Sequence[str]] = None):
    parser = ArgumentParser(
        description="""Command line utility for interacting with HDFS using hdfs-native.
        Globs are not currently supported, all file paths are treated as exact paths."""
    )

    subparsers = parser.add_subparsers(title="Subcommands", required=True)

    cat_parser = subparsers.add_parser(
        "cat",
        help="Print the contents of a file",
        description="Print the contents of a file to stdout",
    )
    cat_parser.add_argument("src", nargs="+", help="File pattern to print")
    cat_parser.set_defaults(func=cat)

    mkdir_parser = subparsers.add_parser(
        "mkdir",
        help="Create a directory",
        description="Create a directory in a specified path",
    )
    mkdir_parser.add_argument(
        "path",
        nargs="+",
        help="Path for the directory to create",
    )
    mkdir_parser.add_argument(
        "-p",
        "--parent",
        action="store_true",
        help="Create any missing parent directories",
    )
    mkdir_parser.set_defaults(func=mkdir)

    mv_parser = subparsers.add_parser(
        "mv",
        help="Move files or directories",
        description="""Move a file or directory from <src> to <dst>. Must be part of the same name service.
        If multiple src are provided, dst must be a directory""",
    )
    mv_parser.add_argument("src", nargs="+", help="Files or directories to move")
    mv_parser.add_argument("dst", help="Target destination of file or directory")
    mv_parser.set_defaults(func=mv)

    args = parser.parse_args(in_args)
    args.func(args)


if __name__ == "__main__":
    main()
