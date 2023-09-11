use bytes::{Buf, Bytes};
use std::collections::HashSet;
use std::io::{self, BufWriter, Write};
use std::process::Command;
use tempfile::NamedTempFile;
use which::which;

use crate::common::minidfs::{DfsFeatures, MiniDfs};
use hdfs_native::{client::Client, Result};

fn create_file(url: &str, path: &str, size: usize) -> io::Result<()> {
    assert!(size % 4 == 0);
    let num_ints = size / 4;

    let hadoop_exc = which("hadoop").expect("Failed to find hadoop executable");
    let mut file = NamedTempFile::new_in("target/test").unwrap();
    {
        let mut writer = BufWriter::new(file.as_file_mut());
        for i in 0..num_ints as u32 {
            let bytes = i.to_be_bytes();
            writer.write(&bytes)?;
        }
        writer.flush()?;
    }

    let mut cmd = Command::new(hadoop_exc)
        .args([
            "fs",
            "-copyFromLocal",
            "-f",
            file.path().to_str().unwrap(),
            &format!("{}{}", url, path),
        ])
        .spawn()
        .unwrap();
    assert!(cmd.wait().unwrap().success());

    Ok(())
}

fn verify_read(mut data: Bytes, size: usize) {
    assert!(size % 4 == 0);
    let num_ints = size / 4;

    for i in 0..num_ints as u32 {
        assert_eq!(data.get_u32(), i);
    }
}

#[tokio::test]
#[cfg(feature = "token")]
async fn test_erasure_coded_read() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::EC]));
    let client = Client::new(&dfs.url)?;

    for file_size in [16usize, 1024 * 1024, 128 * 1024 * 1024] {
        create_file(&dfs.url, "/ec/small", file_size)?;

        let mut reader = client.read("/ec/small").await?;
        assert_eq!(reader.file_length(), file_size);

        let data = reader.read(reader.file_length()).await?;

        verify_read(data, file_size);
    }

    Ok(())
}
