import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from hdfs_native import AsyncClient, Client


@pytest.mark.benchmark
def test_benchmark_threading(client: Client, benchmark: BenchmarkFixture):
    def do_work():
        def func(path: str):
            client.create(path).close()
            return client.delete(path)

        with ThreadPoolExecutor(10) as executor:
            futures = []
            for i in range(1000):
                futures.append(executor.submit(func, f"/bench{i}"))

            for future in as_completed(futures):
                assert future.result()

    benchmark(do_work)


@pytest.mark.benchmark
def test_benchmark_threading_async(
    async_client: AsyncClient,
    aio_benchmark,
):
    async def do_work():
        async def func(path: str):
            await (await async_client.create(path)).close()
            return await async_client.delete(path)

        async with asyncio.TaskGroup() as tg:
            for i in range(1000):
                tg.create_task(func(f"/bench{i}"))

    aio_benchmark(do_work)
