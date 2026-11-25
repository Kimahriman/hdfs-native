import asyncio
import inspect
import os
import signal
import subprocess
import time

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
            "../rust/hdfs-native/minidfs",
            "--quiet",
            "clean",
            "compile",
            "exec:exec",
            "-Dexec.args=-cp %classpath main.Main trash",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=os.name == "posix",
        universal_newlines=True,
        encoding="utf8",
        bufsize=0,
    )

    assert child.stdout is not None

    output = child.stdout.readline().strip()
    assert output == "Ready!", output

    os.environ["HADOOP_CONF_DIR"] = "target/test"

    yield "hdfs://127.0.0.1:9000"

    if os.name == "posix":
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            child.poll()
            try:
                os.killpg(child.pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    else:
        child.send_signal(signal.SIGTERM)
        try:
            child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            child.kill()

    child.wait()


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
        assert inspect.iscoroutinefunction(func)

        @benchmark
        def _():
            return asyncio.get_event_loop().run_until_complete(
                run_async_coroutine(func, *args, **kwargs)
            )

    return _wrapper
