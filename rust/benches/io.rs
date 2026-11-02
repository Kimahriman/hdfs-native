use std::{collections::HashSet, hint::black_box, time::Duration};

#[allow(unused_imports)]
use bytes::{Buf, BufMut, BytesMut};
use criterion::*;
use hdfs_native::{
    Client, WriteOptions,
    minidfs::{DfsFeatures, MiniDfs},
};
use rand::{RngExt, SeedableRng, rngs::StdRng};

async fn write_file(client: &Client, path: &str, ints: usize) {
    let mut writer = client
        .create(path, WriteOptions::default().overwrite(true))
        .await
        .unwrap();

    // Keep fixture creation bounded even for multi-GiB files.
    const CHUNK_INTS: usize = 4 * 1024 * 1024;
    for start in (0..ints).step_by(CHUNK_INTS) {
        let end = (start + CHUNK_INTS).min(ints);
        let mut data = BytesMut::with_capacity((end - start) * 4);
        for i in start..end {
            data.put_u32(i as u32);
        }
        writer.write_bytes(data.freeze()).await.unwrap();
    }
    writer.close().await.unwrap();
}

fn bench_random_reads(
    c: &mut Criterion,
    client: &Client,
    rt: &tokio::runtime::Runtime,
    dfs_url: &str,
) {
    // https://github.com/Kimahriman/hdfs-native/issues/340: 16 MiB reads at
    // random locations in a multi-GB file. Offsets are not block-aligned, so
    // requests can cross block and checksum boundaries.
    const FILE_SIZE: usize = 2 * 1024 * 1024 * 1024;
    const READ_SIZE: usize = 16 * 1024 * 1024;
    const READS_PER_ITER: usize = 64;
    const PATH: &str = "/bench-random";

    let mut rng = StdRng::seed_from_u64(340);
    let offsets: Vec<usize> = (0..READS_PER_ITER)
        .map(|_| rng.random_range(0..=FILE_SIZE - READ_SIZE))
        .collect();

    // Criterion only calls a benchmark's closure when its filter matches.
    // Create the large fixture once, outside timing, only if it is needed.
    let mut fixture_ready = false;
    let mut prepare_fixture = || {
        if !fixture_ready {
            rt.block_on(write_file(client, PATH, FILE_SIZE / 4));
            fixture_ready = true;
        }
    };

    let mut group = c.benchmark_group("read-random");
    group.throughput(Throughput::Bytes((READ_SIZE * READS_PER_ITER) as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("read-native", |b| {
        prepare_fixture();
        let reader = rt.block_on(client.read(PATH)).unwrap();
        b.to_async(rt).iter(|| async {
            let mut buf = vec![0; READ_SIZE];
            for &offset in &offsets {
                reader.read_range_buf(&mut buf, offset).await.unwrap();
                black_box(&buf);
            }
            buf
        });
    });

    #[cfg(unix)]
    group.bench_function("read-libhdfs", |b| {
        prepare_fixture();
        let fs = hdfs::hdfs::get_hdfs_by_full_path(dfs_url).unwrap();
        let reader = fs.open(PATH).unwrap();
        b.iter(|| {
            let mut buf = vec![0; READ_SIZE];
            for &offset in &offsets {
                let mut bytes_read = 0;
                while bytes_read < READ_SIZE {
                    let count = reader
                        .read_with_pos((offset + bytes_read) as i64, &mut buf[bytes_read..])
                        .unwrap();
                    assert!(count > 0, "Unexpected EOF during random read");
                    bytes_read += count as usize;
                }
                black_box(&buf);
            }
            buf
        });
        reader.close().unwrap();
    });
}

fn bench(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();

    let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::EC]));
    let client = Client::default();

    let ints_to_write: usize = 4 * 32 * 1024 * 1024; // 128 MiB file

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async { write_file(&client, "/bench", ints_to_write).await });

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Bytes((ints_to_write * 4) as u64));
    group.sample_size(50);

    group.bench_function("read-native", |b| {
        b.to_async(&rt).iter(|| async {
            let reader = client.read("/bench").await.unwrap();

            reader.read_range(0, reader.file_length()).await.unwrap()
        })
    });
    group.sample_size(50);
    #[cfg(unix)]
    group.bench_function("read-libhdfs", |b| {
        let fs = hdfs::hdfs::get_hdfs().unwrap();
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

    bench_random_reads(c, &client, &rt, &_dfs.url);

    rt.block_on(async { write_file(&client, "/ec-3-2/bench", ints_to_write).await });

    let mut group = c.benchmark_group("read-ec");
    group.throughput(Throughput::Bytes((ints_to_write * 4) as u64));
    group.sample_size(50);
    group.bench_function("read-native", |b| {
        let reader = rt.block_on(client.read("/ec-3-2/bench")).unwrap();
        b.to_async(&rt)
            .iter(|| async { reader.read_range(0, reader.file_length()).await.unwrap() })
    });
    group.sample_size(50);
    #[cfg(unix)]
    group.bench_function("read-libhdfs", |b| {
        let fs = hdfs::hdfs::get_hdfs().unwrap();
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

            writer.write_bytes(buf.clone()).await.unwrap();
            writer.close().await.unwrap();
        })
    });
    group.sample_size(10);
    #[cfg(unix)]
    group.bench_function("write-libhdfs", |b| {
        let fs = hdfs::hdfs::get_hdfs().unwrap();
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

            writer.write_bytes(buf.clone()).await.unwrap();
            writer.close().await.unwrap();
        })
    });
    group.sample_size(10);
    #[cfg(unix)]
    group.bench_function("write-libhdfs", |b| {
        let fs = hdfs::hdfs::get_hdfs().unwrap();
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
