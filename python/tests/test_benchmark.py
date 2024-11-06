from concurrent.futures import ThreadPoolExecutor

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from hdfs_native import Client


def do_work(client: Client):
    def delete(path: str):
        client.delete(path)

    with ThreadPoolExecutor(100) as executor:
        for i in range(100):
            executor.submit(delete, f"/bench{i}")


@pytest.mark.benchmark
def test_threading(client: Client, benchmark: BenchmarkFixture):
    for i in range(100):
        client.create(f"/bench{i}").close()

    benchmark(do_work, client)
