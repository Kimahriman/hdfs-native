#![allow(dead_code)]
use bytes::Buf;

pub const TEST_FILE_INTS: usize = 4 * 1024 * 1024;

pub fn assert_bufs_equal(buf1: &impl Buf, buf2: &impl Buf, message: Option<String>) {
    assert_eq!(buf1.chunk().len(), buf2.chunk().len());

    let message = message.unwrap_or_default();

    buf1.chunk()
        .iter()
        .zip(buf2.chunk())
        .enumerate()
        .for_each(move |(i, (b1, b2))| {
            assert_eq!(b1, b2, "data is different as position {i} {message}")
        });
}
