import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from hdfs_native import AsyncClient, Client


@pytest.mark.benchmark
def test_benchmark_listing(client: Client, benchmark: BenchmarkFixture):
    for i in range(1000):
        client.create(f"/bench{i}").close()

    def do_work():
        for _ in client.list_status("/"):
            pass

    benchmark(do_work)


@pytest.mark.benchmark
def test_benchmark_listing_async(
    client: Client,
    async_client: AsyncClient,
    aio_benchmark,
):
    for i in range(1000):
        client.create(f"/bench{i}").close()

    async def do_work():
        async for _ in async_client.list_status("/"):
            pass

    aio_benchmark(do_work)
