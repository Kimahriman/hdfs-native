#![allow(dead_code)]
use bytes::Buf;
use std::ffi::OsString;

pub const TEST_FILE_INTS: usize = 4 * 1024 * 1024;

pub struct EnvVarGuard {
    name: &'static str,
    original: Option<OsString>,
}

impl EnvVarGuard {
    pub fn set(name: &'static str, value: &str) -> Self {
        let original = std::env::var_os(name);
        unsafe { std::env::set_var(name, value) };
        Self { name, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.original.take() {
            Some(value) => unsafe { std::env::set_var(self.name, value) },
            None => unsafe { std::env::remove_var(self.name) },
        }
    }
}

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
