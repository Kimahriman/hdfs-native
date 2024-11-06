from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from hdfs_native import Client


@pytest.mark.benchmark
def test_benchmark_threading(client: Client, benchmark: BenchmarkFixture):
    def do_work():
        def func(path: str):
            client.create(path).close()
            return client.delete(path)

        with ThreadPoolExecutor(100) as executor:
            futures = []
            for i in range(1000):
                futures.append(executor.submit(func, f"/bench{i}"))

            for future in as_completed(futures):
                assert future.result()

    benchmark(do_work)


@pytest.mark.benchmark
def test_benchmark_listing(client: Client, benchmark: BenchmarkFixture):
    for i in range(1000):
        client.create(f"/bench{i}").close()

    def do_work():
        for _ in client.list_status("/"):
            pass

    benchmark(do_work)
