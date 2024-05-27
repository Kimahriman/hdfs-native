use std::collections::HashSet;

use criterion::*;
use hdfs::hdfs::get_hdfs;
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

    let fs = get_hdfs().unwrap();

    let mut group = c.benchmark_group("rpc");
    group.bench_function("getFileInfo-native", |b| {
        b.to_async(&rt)
            .iter(|| async { client.get_file_info("/bench").await.unwrap() })
    });
    group.bench_function("getFileInfo-libhdfs", |b| {
        b.iter(|| fs.get_file_status("/bench").unwrap())
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
