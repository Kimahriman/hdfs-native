use std::collections::HashSet;

use criterion::*;
use futures::future::join_all;
use hdfs_native::{minidfs::MiniDfs, Client, WriteOptions};

fn bench(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();

    let _dfs = MiniDfs::with_features(&HashSet::new());
    let client = Client::default();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        client
            .create("/bench", WriteOptions::default())
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
    });

    let mut group = c.benchmark_group("rpc");
    group.bench_function("getFileInfo-native", |b| {
        b.to_async(&rt)
            .iter(|| async { client.get_file_info("/bench").await.unwrap() })
    });
    #[cfg(unix)]
    group.bench_function("getFileInfo-libhdfs", |b| {
        let fs = hdfs::hdfs::get_hdfs().unwrap();
        b.iter(|| fs.get_file_status("/bench").unwrap())
    });

    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("getFileInfo-parallel", |b| {
        b.to_async(&rt).iter_batched(
            || {
                (0..100)
                    .map(|_| client.get_file_info("/bench"))
                    .collect::<Vec<_>>()
            },
            |futures| async {
                for result in join_all(futures).await {
                    result.unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
