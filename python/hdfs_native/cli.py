import os
from argparse import ArgumentParser, Namespace
from typing import Optional, Sequence
from urllib.parse import urlparse

from hdfs_native import Client


def _client_for_url(url: str) -> Client:
    parsed = urlparse(url)

    if parsed.scheme:
        connection_url = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            connection_url += f":{parsed.port}"
        return Client(connection_url)
    elif parsed.hostname or parsed.port:
        raise ValueError(
            f"Cannot provide host or port without scheme: {parsed.hostname}"
        )
    else:
        return Client()


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


def mv(args: Namespace):
    _verify_nameservices_match(args.dst, *args.src)

    client = _client_for_url(args.dst)
    dst_path = _path_for_url(args.dst)

    dst_isdir = False
    try:
        dst_isdir = client.get_file_info(dst_path).isdir
    except FileNotFoundError:
        pass

    if len(args.src) > 1 and not dst_isdir:
        raise ValueError(
            "destination must be a directory if multiple sources are provided"
        )

    for src in args.src:
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
