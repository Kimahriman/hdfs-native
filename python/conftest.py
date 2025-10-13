import asyncio
import os
import subprocess

import pytest
import pytest_asyncio
from pytest_benchmark.fixture import BenchmarkFixture

from hdfs_native import AsyncClient, Client


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


@pytest.fixture
def async_client(minidfs: str):
    client = AsyncClient(minidfs)

    yield client


@pytest_asyncio.fixture
async def aio_benchmark(benchmark: BenchmarkFixture):
    async def run_async_coroutine(func, *args, **kwargs):
        return await func(*args, **kwargs)

    def _wrapper(func, *args, **kwargs):
        assert asyncio.iscoroutinefunction(func)

        @benchmark
        def _():
            return asyncio.get_event_loop().run_until_complete(
                run_async_coroutine(func, *args, **kwargs)
            )

    return _wrapper
