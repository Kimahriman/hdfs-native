import pytest

from hdfs_native import Client
from hdfs_native.cli import main as cli_main


def test_cli(minidfs: str):
    client = Client(minidfs)

    def qualify(path: str) -> str:
        return f"{minidfs}{path}"

    # mv
    client.create("/testfile").close()
    client.mkdirs("/testdir")

    cli_main(["mv", qualify("/testfile"), qualify("/testfile2")])

    client.get_file_info("/testfile2")

    with pytest.raises(ValueError):
        cli_main(["mv", qualify("/testfile2"), "hdfs://badnameservice/testfile"])

    with pytest.raises(RuntimeError):
        cli_main(["mv", qualify("/testfile2"), qualify("/nonexistent/testfile")])

    cli_main(["mv", qualify("/testfile2"), qualify("/testdir")])

    client.get_file_info("/testdir/testfile2")

    client.rename("/testdir/testfile2", "/testfile1")
    client.create("/testfile2").close()

    with pytest.raises(ValueError):
        cli_main(
            ["mv", qualify("/testfile1"), qualify("/testfile2"), qualify("/testfile3")]
        )

    cli_main(["mv", qualify("/testfile1"), qualify("/testfile2"), qualify("/testdir/")])

    client.get_file_info("/testdir/testfile1")
    client.get_file_info("/testdir/testfile2")
