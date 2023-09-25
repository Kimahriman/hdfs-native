use bytes::{Buf, Bytes};
use serial_test::serial;
use std::collections::HashSet;
use std::io::{self, BufWriter, Write};
use std::process::{Command, Stdio};
use tempfile::NamedTempFile;
use which::which;

use hdfs_native::minidfs::{DfsFeatures, MiniDfs};
use hdfs_native::test::{EcFaultInjection, EC_FAULT_INJECTOR};
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
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    assert!(cmd.wait().unwrap().success());

    Ok(())
}

fn verify_read(mut data: Bytes, size: usize) {
    assert!(size % 4 == 0);
    let num_ints = size / 4;

    for i in 0..num_ints as u32 {
        assert_eq!(data.get_u32(), i, "Different values at integer {}", i);
    }
}

#[tokio::test]
#[serial]
#[cfg(all(feature = "kerberos", feature = "rs"))]
async fn test_erasure_coded_read() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::EC, DfsFeatures::SECURITY]));
    let client = Client::new(&dfs.url)?;

    // Test each of Hadoop's built-in RS policies
    for (data, parity) in [(3usize, 2usize), (6, 3), (10, 4)] {
        let file = format!("/ec-{}-{}/testfile", data, parity);

        // Try a variety of sizes to catch any possible edge cases
        let sizes_to_test = [
            16usize,                // Small
            1024 * 1024,            // One cell
            1024 * 1024 - 4,        // Just smaller than one cell
            1024 * 1024 + 4,        // Just bigger than one cell
            1024 * 1024 * data * 5, // Five "rows" of cells
            1024 * 1024 * data * 5 - 4,
            1024 * 1024 * data * 5 + 4,
            128 * 1024 * 1024,
            128 * 1024 * 1024 - 4,
            128 * 1024 * 1024 + 4,
        ];

        for file_size in sizes_to_test {
            create_file(&dfs.url, &file, file_size)?;

            let reader = client.read(&file).await?;
            assert_eq!(reader.file_length(), file_size);

            for faults in 0..parity {
                let _ = EC_FAULT_INJECTOR.lock().unwrap().insert(EcFaultInjection {
                    fail_blocks: (0..faults).into_iter().collect(),
                });
                let data = reader.read_range(0, reader.file_length()).await?;
                verify_read(data, file_size);
            }

            // Fail more than the number of parity shards, read should fail
            let _ = EC_FAULT_INJECTOR.lock().unwrap().insert(EcFaultInjection {
                fail_blocks: (0..=parity).into_iter().collect(),
            });

            assert!(reader.read_range(0, reader.file_length()).await.is_err());
        }

        assert!(client.delete(&file, false).await?);
    }
    Ok(())
}
