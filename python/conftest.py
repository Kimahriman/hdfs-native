import os
import subprocess
import urllib
import urllib.parse

import fsspec
import pytest

from hdfs_native import Client
from hdfs_native.fsspec import HdfsFileSystem


@pytest.fixture(scope="module")
def minidfs():
    child = subprocess.Popen(
        [
            "mvn",
            "-f",
            "../rust/minidfs",
            "--quiet",
            "clean",
            "compile",
            "exec:java",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        universal_newlines=True,
        encoding="utf8",
        bufsize=0,
    )

    output = child.stdout.readline().strip()
    assert output == "Ready!", output

    os.environ["HADOOP_CONF_DIR"] = "target/test"

    yield "hdfs://127.0.0.1:9000"

    try:
        child.communicate(input="\n", timeout=30)
    except:  # noqa: E722
        child.kill()


@pytest.fixture
def client(minidfs: str):
    client = Client(minidfs)

    try:
        yield client
    finally:
        statuses = list(client.list_status("/"))
        for status in statuses:
            client.delete(status.path, True)


@pytest.fixture(scope="module")
def fs(minidfs: str) -> HdfsFileSystem:
    url = urllib.parse.urlparse(minidfs)
    return fsspec.filesystem(url.scheme, host=url.hostname, port=url.port)
