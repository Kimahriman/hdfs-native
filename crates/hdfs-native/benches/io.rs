use std::collections::HashSet;

use bytes::{Buf, BufMut, BytesMut};
use criterion::*;
use hdfs::hdfs::get_hdfs;
use hdfs_native::{
    minidfs::{DfsFeatures, MiniDfs},
    Client, WriteOptions,
};

async fn write_file(client: &Client, path: &str, ints: usize) {
    let mut writer = client
        .create(path, WriteOptions::default().overwrite(true))
        .await
        .unwrap();

    let mut data = BytesMut::with_capacity(ints * 4);
    for i in 0..ints {
        data.put_u32(i as u32);
    }
    writer.write(data.freeze()).await.unwrap();
    writer.close().await.unwrap();
}

fn bench(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();

    let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::EC]));
    let client = Client::default();

    let ints_to_write: usize = 32 * 1024 * 1024; // 128 MiB file

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Bytes((ints_to_write * 4) as u64));
    group.sample_size(10);

    group.bench_function("read-native", |b| {
        rt.block_on(async { write_file(&client, "/bench", ints_to_write).await });
        let reader = rt.block_on(client.read("/bench")).unwrap();
        b.to_async(&rt).iter(|| async {
            // let reader = client.read("/bench").await.unwrap();

            reader.read_range(0, reader.file_length()).await.unwrap()
        })
    });
    group.sample_size(10);
    group.bench_function("read-libhdfs", |b| {
        rt.block_on(async { write_file(&client, "/bench", ints_to_write).await });
        let fs = get_hdfs().unwrap();
        b.iter(|| {
            let mut buf = BytesMut::zeroed(ints_to_write * 4);
            let mut bytes_read = 0;
            let reader = fs.open("/bench").unwrap();

            while bytes_read < ints_to_write * 4 {
                bytes_read += reader
                    .read(&mut buf[bytes_read..ints_to_write * 4])
                    .unwrap() as usize;
            }
            reader.close().unwrap();
            buf
        })
    });

    drop(group);

    let mut group = c.benchmark_group("read-ec");
    group.throughput(Throughput::Bytes((ints_to_write * 4) as u64));
    group.sample_size(10);
    group.bench_function("read-native", |b| {
        rt.block_on(async { write_file(&client, "/ec-3-2/bench", ints_to_write).await });
        let reader = rt.block_on(client.read("/ec-3-2/bench")).unwrap();
        b.to_async(&rt)
            .iter(|| async { reader.read_range(0, reader.file_length()).await.unwrap() })
    });
    group.sample_size(10);
    group.bench_function("read-libhdfs", |b| {
        rt.block_on(async { write_file(&client, "/ec-3-2/bench", ints_to_write).await });
        let fs = get_hdfs().unwrap();
        b.iter(|| {
            let mut buf = BytesMut::zeroed(ints_to_write * 4);
            let mut bytes_read = 0;
            let reader = fs.open("/ec-3-2/bench").unwrap();

            while bytes_read < ints_to_write * 4 {
                bytes_read += reader
                    .read(&mut buf[bytes_read..ints_to_write * 4])
                    .unwrap() as usize;
            }
            reader.close().unwrap();
            buf
        })
    });

    drop(group);

    let mut data_to_write = BytesMut::with_capacity(ints_to_write * 4);
    for i in 0..ints_to_write {
        data_to_write.put_i32(i as i32);
    }

    let buf = data_to_write.freeze();

    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Bytes((ints_to_write * 4) as u64));
    group.sample_size(10);
    group.bench_function("write-native", |b| {
        b.to_async(&rt).iter(|| async {
            let mut writer = client
                .create("/bench-write", WriteOptions::default().overwrite(true))
                .await
                .unwrap();

            writer.write(buf.clone()).await.unwrap();
            writer.close().await.unwrap();
        })
    });
    group.sample_size(10);
    group.bench_function("write-libhdfs", |b| {
        let fs = get_hdfs().unwrap();
        b.iter(|| {
            let mut buf = buf.clone();
            let writer = fs.create_with_overwrite("/bench-write", true).unwrap();

            while buf.remaining() > 0 {
                let written = writer.write(&buf[..]).unwrap();
                buf.advance(written as usize);
            }
            writer.close().unwrap();
        })
    });

    drop(group);

    let mut group = c.benchmark_group("write-ec");
    group.throughput(Throughput::Bytes((ints_to_write * 4) as u64));
    group.sample_size(10);
    group.bench_function("write-native", |b| {
        b.to_async(&rt).iter(|| async {
            let mut writer = client
                .create(
                    "/ec-3-2/bench-write",
                    WriteOptions::default().overwrite(true),
                )
                .await
                .unwrap();

            writer.write(buf.clone()).await.unwrap();
            writer.close().await.unwrap();
        })
    });
    group.sample_size(10);
    group.bench_function("write-libhdfs", |b| {
        let fs = get_hdfs().unwrap();
        b.iter(|| {
            let mut buf = buf.clone();
            let writer = fs
                .create_with_overwrite("/ec-3-2/bench-write", true)
                .unwrap();

            while buf.remaining() > 0 {
                let written = writer.write(&buf[..]).unwrap();
                buf.advance(written as usize);
            }
            writer.close().unwrap();
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
