use bytes::{BufMut, Bytes, BytesMut};
use criterion::*;
use hdfs_native::ec::gf256::Coder;

fn bench(c: &mut Criterion) {
    let mut matrix = Coder::gen_rs_matrix(6, 3);
    matrix.select_rows([3, 4, 5, 6, 7, 8].into_iter());

    let mut group = c.benchmark_group("matrix-inversion");
    group.bench_function("invert", |b| {
        let mut matrix = matrix.clone();
        b.iter(move || matrix.invert());
    });
    group.finish();

    let coder = Coder::new(6, 3);
    let slice_size: u64 = 16 * 1024 * 1024;

    let slices: Vec<_> = (0..6)
        .map(|i| {
            let mut buf = BytesMut::with_capacity(slice_size as usize);
            for v in 0..slice_size / 4 {
                buf.put_i32((v + i * slice_size) as i32);
            }
            buf.freeze()
        })
        .collect();

    let mut group = c.benchmark_group("rs-encode");
    group.throughput(Throughput::Bytes(slice_size * 6));
    group.sample_size(30);
    group.bench_function("encode", |b| b.iter(|| coder.encode(&slices[..])));
    group.finish();

    // Get the actual encoded slices
    let parity_shards = coder.encode(&slices[..]);
    let mut decode_slices: Vec<Option<Bytes>> = Vec::new();
    decode_slices.extend(slices.iter().cloned().map(Some));
    decode_slices.extend(parity_shards.iter().cloned().map(Some));

    let mut group = c.benchmark_group("rs-decode");
    group.throughput(Throughput::Bytes(slice_size * 6));
    group.bench_function("decode-1-slice", |b| {
        b.iter(|| {
            decode_slices[0] = None;
            coder.decode(&mut decode_slices[..])
        })
    });
    group.bench_function("decode-2-slice", |b| {
        b.iter(|| {
            decode_slices[0] = None;
            decode_slices[1] = None;
            coder.decode(&mut decode_slices[..])
        })
    });
    group.bench_function("decode-3-slice", |b| {
        b.iter(|| {
            decode_slices[0] = None;
            decode_slices[1] = None;
            decode_slices[2] = None;
            coder.decode(&mut decode_slices[..])
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
