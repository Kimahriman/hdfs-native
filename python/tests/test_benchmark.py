from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from hdfs_native import Client


def do_work(client: Client):
    def func(path: str):
        client.create(path).close()
        return client.delete(path)

    with ThreadPoolExecutor(100) as executor:
        futures = []
        for i in range(1000):
            futures.append(executor.submit(func, f"/bench{i}"))

        for future in as_completed(futures):
            assert future.result()


@pytest.mark.benchmark
def test_threading(client: Client, benchmark: BenchmarkFixture):
    benchmark(do_work, client)
