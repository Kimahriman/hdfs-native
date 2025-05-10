import functools
import glob
import os
import re
import shutil
import stat
import sys
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

from hdfs_native import Client
from hdfs_native._internal import FileStatus, WriteOptions

__all__ = ["main"]


@functools.cache
def _get_client(connection_url: Optional[str] = None):
    return Client(connection_url)


def _prefix_for_url(url: str) -> str:
    parsed = urlparse(url)

    if parsed.scheme:
        prefix = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            prefix += f":{parsed.port}"
        return prefix

    return ""


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


def _glob_local_path(glob_pattern: str) -> List[str]:
    return glob.glob(glob_pattern)


def _download_file(
    client: Client,
    remote_src: str,
    local_dst: str,
    force: bool = False,
    preserve: bool = False,
) -> None:
    if not force and os.path.exists(local_dst):
        raise FileExistsError(f"{local_dst} already exists, use --force to overwrite")

    with client.read(remote_src) as remote_file:
        with open(local_dst, "wb") as local_file:
            for chunk in remote_file.read_range_stream(0, len(remote_file)):
                local_file.write(chunk)

    if preserve:
        status = client.get_file_info(remote_src)
        os.utime(
            local_dst,
            (status.access_time / 1000, status.modification_time / 1000),
        )
        os.chmod(local_dst, status.permission)


def _upload_file(
    client: Client,
    local_src: str,
    remote_dst: str,
    direct: bool = False,
    force: bool = False,
    preserve: bool = False,
) -> None:
    if not direct and not force:
        # Check if file already exists before we write it to a temporary file
        try:
            client.get_file_info(remote_dst)
            raise FileExistsError(
                f"{remote_dst} already exists, use --force to overwrite"
            )
        except FileNotFoundError:
            pass

    if direct:
        write_destination = remote_dst
    else:
        write_destination = f"{remote_dst}.__COPYING__"

    with open(local_src, "rb") as local_file:
        with client.create(
            write_destination,
            WriteOptions(overwrite=force),
        ) as remote_file:
            shutil.copyfileobj(local_file, remote_file)

    if preserve:
        st = os.stat(local_src)
        client.set_times(
            write_destination,
            int(st.st_mtime * 1000),
            int(st.st_atime * 1000),
        )
        client.set_permission(write_destination, stat.S_IMODE(st.st_mode))

    if not direct:
        client.rename(write_destination, remote_dst, overwrite=force)


def _get_widths(parsed: list[dict]) -> dict[str, int]:
    widths: dict[str, int] = defaultdict(lambda: 0)

    for file in parsed:
        for key, value in file.items():
            if isinstance(value, str):
                widths[key] = max(widths[key], len(value))

    return widths


def _human_size(num: int):
    if num < 1024:
        return str(num)

    adjusted = num / 1024.0
    for unit in ("K", "M", "G", "T", "P", "E", "Z"):
        if abs(adjusted) < 1024.0:
            return f"{adjusted:.1f}{unit}"
        adjusted /= 1024.0
    return f"{adjusted:.1f}Y"


def cat(args: Namespace):
    for src in args.src:
        client = _client_for_url(src)
        for path in _glob_path(client, _path_for_url(src)):
            with client.read(path) as file:
                while chunk := file.read(1024 * 1024):
                    sys.stdout.buffer.write(chunk)

    sys.stdout.buffer.flush()


def chmod(args: Namespace):
    if not re.fullmatch(r"1?[0-7]{3}", args.octalmode):
        raise ValueError(f"Invalid mode supplied: {args.octalmode}")

    permission = int(args.octalmode, base=8)

    for url in args.path:
        client = _client_for_url(url)
        for path in _glob_path(client, _path_for_url(url)):
            if args.recursive:
                for status in client.list_status(path, True):
                    client.set_permission(status.path, permission)
            else:
                client.set_permission(path, permission)


def chown(args: Namespace):
    split = args.owner.split(":")
    if len(split) > 2:
        raise ValueError(f"Invalid owner and group pattern: {args.owner}")

    if len(split) == 1:
        owner = split[0]
        group = None
    else:
        owner = split[0] if split[0] else None
        group = split[1]

    for url in args.path:
        client = _client_for_url(url)
        for path in _glob_path(client, _path_for_url(url)):
            if args.recursive:
                for status in client.list_status(path, True):
                    client.set_owner(status.path, owner, group)
            else:
                client.set_owner(path, owner, group)


def du(args: Namespace):
    parsed: List[Dict[str, str]] = []

    if args.verbose:
        header = {
            "file_size": "File Size",
            "disk_size": "Disk Size",
            "path": "Path",
        }
        if args.file_count:
            header["file_count"] = "File Count"
            header["directory_count"] = "Directory Count"

        parsed.append(header)

    for url in args.path:
        client = _client_for_url(url)
        for path in _glob_path(client, _path_for_url(url)):
            prefix = _prefix_for_url(url)

            if args.summary:
                summaries = [(prefix + path, client.get_content_summary(path))]
            else:
                summaries = []
                for status in client.list_status(path):
                    summaries.append(
                        (prefix + status.path, client.get_content_summary(status.path))
                    )

            for path, summary in summaries:
                if args.human_readable:
                    file_size = _human_size(summary.length)
                    disk_size = _human_size(summary.space_consumed)
                else:
                    file_size = str(summary.length)
                    disk_size = str(summary.space_consumed)

                parsed_file = {
                    "file_size": file_size,
                    "disk_size": disk_size,
                    "path": path,
                }

                if args.file_count:
                    parsed_file["file_count"] = str(summary.file_count)
                    parsed_file["directory_count"] = str(summary.directory_count)

                parsed.append(parsed_file)

    widths = _get_widths(parsed)

    def format(
        file: Dict[str, str],
        field: str,
        right_align: bool = False,
    ):
        value = str(file[field])

        width = len(value)
        if widths and field in widths:
            width = widths[field]

        if right_align:
            return f"{value:>{width}}"
        return f"{value:{width}}"

    for file in parsed:
        formatted_fields = [
            format(file, "file_size", True),
            format(file, "disk_size", True),
            format(file, "path"),
        ]
        if args.file_count:
            formatted_fields.append(format(file, "file_count", True))
            formatted_fields.append(format(file, "directory_count", True))

        print("  ".join(formatted_fields))


def get(args: Namespace):
    paths: List[Tuple[Client, str]] = []

    if len(args.src) > 1:
        srcs = args.src[:-1]
        dst = args.src[-1]
    else:
        srcs = args.src
        dst = os.getcwd()

    for url in srcs:
        client = _client_for_url(url)
        for path in _glob_path(client, _path_for_url(url)):
            paths.append((client, path))

    dst_is_dir = os.path.isdir(dst)

    if len(paths) > 1 and not dst_is_dir:
        raise ValueError("Destination must be directory when copying multiple files")
    elif not dst_is_dir:
        _download_file(
            paths[0][0],
            paths[0][1],
            dst,
            force=args.force,
            preserve=args.preserve,
        )
    else:
        with ThreadPoolExecutor(args.threads) as executor:
            futures = []
            for client, path in paths:
                filename = os.path.basename(path)

                futures.append(
                    executor.submit(
                        _download_file,
                        client,
                        path,
                        os.path.join(dst, filename),
                        force=args.force,
                        preserve=args.preserve,
                    )
                )

            # Iterate to raise any exceptions thrown
            for f in as_completed(futures):
                f.result()


def ls(args: Namespace):
    def parse_status(status: FileStatus, prefix: str) -> Dict[str, Union[int, str]]:
        file_time = status.modification_time
        if args.access_time:
            file_time = status.access_time

        file_time_string = datetime.fromtimestamp(file_time / 1000).strftime(
            r"%Y-%m-%d %H:%M"
        )

        permission = status.permission
        if status.isdir:
            permission |= stat.S_IFDIR
        else:
            permission |= stat.S_IFREG

        mode = stat.filemode(permission)

        if args.human_readable:
            length_string = _human_size(status.length)
        else:
            length_string = str(status.length)

        path = prefix + status.path

        return {
            "mode": mode,
            "replication": str(status.replication) if status.replication else "-",
            "owner": status.owner,
            "group": status.group,
            "length": status.length,
            "length_formatted": length_string,
            "time": file_time,
            "time_formatted": file_time_string,
            "path": path,
        }

    def print_files(
        parsed: List[Dict[str, Union[int, str]]],
        widths: Optional[Dict[str, int]] = None,
    ):
        if args.sort_time:
            parsed = sorted(parsed, key=lambda x: x["time"], reverse=not args.reverse)
        elif args.sort_size:
            parsed = sorted(parsed, key=lambda x: x["length"], reverse=not args.reverse)

        def format(
            file: Dict[str, Union[int, str]],
            field: str,
            right_align: bool = False,
        ):
            value = str(file[field])

            width = len(value)
            if widths and field in widths:
                width = widths[field]

            if right_align:
                return f"{value:>{width}}"
            return f"{value:{width}}"

        for file in parsed:
            if args.path_only:
                print(file["path"])
            else:
                formatted_fields = [
                    format(file, "mode"),
                    format(file, "replication"),
                    format(file, "owner"),
                    format(file, "group"),
                    format(file, "length_formatted", True),
                    format(file, "time_formatted"),
                    format(file, "path"),
                ]
                print(" ".join(formatted_fields))

    for url in args.path:
        client = _client_for_url(url)
        for path in _glob_path(client, _path_for_url(url)):
            status = client.get_file_info(path)

            prefix = _prefix_for_url(url)

            if status.isdir:
                parsed = [
                    parse_status(status, prefix)
                    for status in client.list_status(path, args.recursive)
                ]

                if not args.path_only:
                    print(f"Found {len(parsed)} items")

                widths = _get_widths(parsed)
                print_files(parsed, widths)
            else:
                print_files([parse_status(status, prefix)])


def mkdir(args: Namespace):
    create_parent = args.parent

    for url in args.path:
        client = _client_for_url(url)
        client.mkdirs(_path_for_url(url), create_parent=create_parent)


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
        path
        for pattern in args.src
        for path in _glob_path(client, _path_for_url(pattern))
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


def put(args: Namespace):
    paths: List[str] = []

    for pattern in args.localsrc:
        for path in _glob_local_path(pattern):
            paths.append(path)

    if len(paths) == 0:
        raise FileNotFoundError("No files matched patterns")

    client = _client_for_url(args.dst)
    dst_path = _path_for_url(args.dst)

    dst_is_dir = False
    try:
        dst_is_dir = client.get_file_info(dst_path).isdir
    except FileNotFoundError:
        pass

    if len(paths) > 1 and not dst_is_dir:
        raise ValueError("Destination must be directory when copying multiple files")
    elif not dst_is_dir:
        _upload_file(
            client,
            paths[0],
            dst_path,
            direct=args.direct,
            force=args.force,
            preserve=args.preserve,
        )
    else:
        with ThreadPoolExecutor(args.threads) as executor:
            futures = []
            for path in paths:
                filename = os.path.basename(path)

                futures.append(
                    executor.submit(
                        _upload_file,
                        client,
                        path,
                        os.path.join(dst_path, filename),
                        direct=args.direct,
                        force=args.force,
                        preserve=args.preserve,
                    )
                )

            # Iterate to raise any exceptions thrown
            for f in as_completed(futures):
                f.result()


def rm(args: Namespace):
    if not args.skip_trash:
        raise ValueError(
            "Moving files to the trash is not currently supported. Pass --skip-trash to permanently delete the files."
        )

    for url in args.src:
        client = _client_for_url(url)
        for path in _glob_path(client, _path_for_url(url)):
            if not client.delete(path, args.recursive) and not args.force:
                raise FileNotFoundError(f"Failed to delete {path}")


def rmdir(args: Namespace):
    for url in args.dir:
        client = _client_for_url(url)
        for path in _glob_path(client, _path_for_url(url)):
            status = client.get_file_info(path)
            if not status.isdir:
                raise ValueError(f"{path} is not a directory")

            client.delete(path)


def touch(args: Namespace):
    for url in args.path:
        client = _client_for_url(url)
        for path in _glob_path(client, _path_for_url(url)):
            timestamp = None
            if args.timestamp:
                timestamp = datetime.strptime(args.timestamp, r"%Y%m%d:%H%M%S")

            try:
                status = client.get_file_info(path)
                if timestamp is None:
                    timestamp = datetime.now(timezone.utc)
            except FileNotFoundError:
                if args.only_change:
                    return

                client.create(path).close()
                status = client.get_file_info(path)

            if timestamp:
                access_time = int(timestamp.timestamp() * 1000)
                modification_time = int(timestamp.timestamp() * 1000)
                if args.access_time:
                    modification_time = status.modification_time
                if args.modification_time:
                    access_time = status.access_time

                client.set_times(path, modification_time, access_time)


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
        add_help=False,
    )
    cat_parser.add_argument("src", nargs="+", help="File pattern to print")
    cat_parser.set_defaults(func=cat)

    chmod_parser = subparsers.add_parser(
        "chmod",
        help="Changes permissions of a file",
        description="Changes permissions of a file. Only octal permissions are supported.",
        add_help=False,
    )
    chmod_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        help="Modify files recursively",
    )
    chmod_parser.add_argument(
        "octalmode",
        help="The mode to set the permission to in octal format",
    )
    chmod_parser.add_argument("path", nargs="+", help="File pattern to update")
    chmod_parser.set_defaults(func=chmod)

    chown_parser = subparsers.add_parser(
        "chown",
        help="Changes owner and group of a file",
        description="Changes owner and group of a file",
        add_help=False,
    )
    chown_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        help="Modify files recursively",
    )
    chown_parser.add_argument(
        "owner",
        metavar="[owner[:group] | :group]",
        help="Owner and group to set for the file",
    )
    chown_parser.add_argument("path", nargs="+", help="File pattern to modify")
    chown_parser.set_defaults(func=chown)

    du_parser = subparsers.add_parser(
        "du",
        help="Show the amount of space used by the files that match the specified file pattern",
        description="Show the amount of space used by the files that match the specified file pattern",
        add_help=False,
    )
    du_parser.add_argument(
        "-s",
        "--summary",
        action="store_true",
        default=False,
        help="Show the total size of matching directories instead of traversing their children",
    )
    du_parser.add_argument(
        "-h",
        "--human-readable",
        action="store_true",
        default=False,
        help="Format the size in a human-readable fashion",
    )
    du_parser.add_argument(
        "-f",
        "--file-count",
        action="store_true",
        default=False,
        help="Include the file count",
    )
    du_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Include a header line",
    )
    du_parser.add_argument("path", nargs="+")
    du_parser.set_defaults(func=du)

    get_parser = subparsers.add_parser(
        "get",
        aliases=["copyToLocal"],
        help="Copy files to a local destination",
        description="""Copy files matching a pattern to a local destination.
            When copying multiple files, the destination must be a directory""",
        add_help=False,
    )
    get_parser.add_argument(
        "-p",
        "--preserve",
        action="store_true",
        default=False,
        help="Preserve timestamps and the mode",
    )
    get_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite the destination if it already exists",
    )
    get_parser.add_argument(
        "-t",
        "--threads",
        type=int,
        help="Number of threads to use",
        default=1,
    )
    get_parser.add_argument(
        "src",
        nargs="+",
        help="Source patterns to copy",
    )
    get_parser.add_argument(
        "localdst",
        nargs="?",
        help="Local destination to write to. Defaults to current directory.",
    )
    get_parser.set_defaults(func=get)

    ls_parser = subparsers.add_parser(
        "ls",
        help="List contents that match the specified patterns",
        description="""List contents that match the specified patterns. For a directory, list its
        direct children.""",
        add_help=False,
    )
    ls_parser.add_argument(
        "-C",
        "--path-only",
        action="store_true",
        default=False,
        help="Display the path of files and directories only.",
    )
    ls_parser.add_argument(
        "-h",
        "--human-readable",
        action="store_true",
        default=False,
        help="Formats the sizes of files in a human-readable fashion rather than a number of bytes",
    )
    ls_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Recursively list the contents of directories",
    )
    ls_parser.add_argument(
        "-t",
        "--sort-time",
        action="store_true",
        default=False,
        help="Sort files by modification time (most recent first)",
    )
    ls_parser.add_argument(
        "-S",
        "--sort-size",
        action="store_true",
        default=False,
        help="Sort files by size (largest first)",
    )
    ls_parser.add_argument(
        "-r",
        "--reverse",
        action="store_true",
        default=False,
        help="Reverse the order of the sort",
    )
    ls_parser.add_argument(
        "-u",
        "--access-time",
        action="store_true",
        default=False,
        help="Use the last access time instead of modification time for display and sorting",
    )
    ls_parser.add_argument("path", nargs="+", help="Path to display contents of")
    ls_parser.set_defaults(func=ls)

    mkdir_parser = subparsers.add_parser(
        "mkdir",
        help="Create a directory",
        description="Create a directory in a specified path",
        add_help=False,
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
        add_help=False,
    )
    mv_parser.add_argument("src", nargs="+", help="Files or directories to move")
    mv_parser.add_argument("dst", help="Target destination of file or directory")
    mv_parser.set_defaults(func=mv)

    put_parser = subparsers.add_parser(
        "put",
        aliases=["copyFromLocal"],
        help="Copy local files to a remote destination",
        description="""Copy files matching a pattern to a remote destination.
            When copying multiple files, the destination must be a directory""",
        add_help=False,
    )
    put_parser.add_argument(
        "-p",
        "--preserve",
        action="store_true",
        default=False,
        help="Preserve timestamps, ownership, and the mode",
    )
    put_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite the destination if it already exists",
    )
    put_parser.add_argument(
        "-d",
        "--direct",
        action="store_true",
        default=False,
        help="Skip creation of temporary file (<dst>._COPYING_) and write directly to file",
    )
    put_parser.add_argument(
        "-t",
        "--threads",
        type=int,
        help="Number of threads to use",
        default=1,
    )
    put_parser.add_argument(
        "localsrc",
        nargs="+",
        help="Source patterns to copy",
    )
    put_parser.add_argument(
        "dst",
        help="Local destination to write to",
    )
    put_parser.set_defaults(func=put)

    rm_parser = subparsers.add_parser(
        "rm",
        help="Delete files",
        description="Delete all files matching the specified file patterns",
        add_help=False,
    )
    rm_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Ignore if the file does not exist",
    )
    rm_parser.add_argument(
        "-r",
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Recursively delete directories",
    )
    rm_parser.add_argument(
        "-s",
        "--skip-trash",
        action="store_true",
        default=False,
        help="Permanently delete files instead of moving them to the trash",
    )
    rm_parser.add_argument(
        "src",
        nargs="+",
        help="File patterns to delete",
    )
    rm_parser.set_defaults(func=rm)

    rmdir_parser = subparsers.add_parser(
        "rmdir",
        help="Delete an empty directory",
        description="Delete an empty directory",
        add_help=False,
    )
    rmdir_parser.add_argument(
        "dir",
        nargs="+",
        help="Source patterns to copy",
    )
    rmdir_parser.set_defaults(func=rmdir)

    touch_parser = subparsers.add_parser(
        "touch",
        help="Updates the access and modification times of a file or creates it if it doesn't exist",
        description="""Updates the access and modification times of the file specified by the <path> to
                    the current time. If the file does not exist, then a zero length file is created
                    at <path> with current time as the timestamp of that <path>.""",
        add_help=False,
    )
    touch_parser_time_group = touch_parser.add_mutually_exclusive_group()
    touch_parser_time_group.add_argument(
        "-a",
        "--access-time",
        action="store_true",
        default=False,
        help="Only change the access time",
    )
    touch_parser_time_group.add_argument(
        "-m",
        "--modification-time",
        action="store_true",
        default=False,
        help="Only change the modification time",
    )
    touch_parser.add_argument(
        "-t",
        "--timestamp",
        help="Use specified timestamp instead of current time in the format yyyyMMdd:HHmmss",
    )
    touch_parser.add_argument(
        "-c",
        "--only-change",
        action="store_true",
        default=False,
        help="Don't create the file if it doesn't exist",
    )
    touch_parser.add_argument(
        "path",
        nargs="+",
    )
    touch_parser.set_defaults(func=touch)

    def show_help(args: Namespace):
        subparsers.choices[args.cmd].print_help()

    subparser_keys = list(subparsers.choices.keys())

    help_parser = subparsers.add_parser(
        "help",
        help="Display usage of a subcommand",
        description="Display usage of a subcommand",
    )
    help_parser.add_argument(
        "cmd",
        choices=subparser_keys,
        help="Command to show usage for",
    )
    help_parser.set_defaults(func=show_help)

    args = parser.parse_args(in_args)
    args.func(args)


if __name__ == "__main__":
    main()
