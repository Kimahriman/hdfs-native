use std::collections::HashSet;
use std::io::{BufWriter, Write};
use std::process::Command;
use tempfile::NamedTempFile;
use which::which;

use hdfs_native::minidfs::{DfsFeatures, MiniDfs};

pub const TEST_FILE_INTS: usize = 64 * 1024 * 1024;

pub fn setup(features: &HashSet<DfsFeatures>) -> MiniDfs {
    let hadoop_exc = which("hadoop").expect("Failed to find hadoop executable");

    let dfs = MiniDfs::with_features(features);

    let mut file = NamedTempFile::new_in("target/test").unwrap();
    {
        let mut writer = BufWriter::new(file.as_file_mut());
        for i in 0..TEST_FILE_INTS as i32 {
            let bytes = i.to_be_bytes();
            writer.write_all(&bytes).unwrap();
        }
        writer.flush().unwrap();
    }

    let status = Command::new(hadoop_exc)
        .args([
            "fs",
            "-copyFromLocal",
            "-f",
            file.path().to_str().unwrap(),
            &format!("{}/testfile", dfs.url),
        ])
        .status()
        .unwrap();
    assert!(status.success());

    dfs
}
