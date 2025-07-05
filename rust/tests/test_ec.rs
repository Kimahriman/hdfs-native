#[cfg(feature = "integration-test")]
mod test {

    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use hdfs_native::file::FileReader;
    use hdfs_native::WriteOptions;
    use serial_test::serial;
    use std::collections::HashSet;
    use std::io::{self, BufWriter, Write};
    use std::process::{Command, Stdio};
    use tempfile::NamedTempFile;
    use which::which;

    use hdfs_native::minidfs::{DfsFeatures, MiniDfs};
    use hdfs_native::test::{EcFaultInjection, EC_FAULT_INJECTOR};
    use hdfs_native::{client::Client, Result};

    const CELL_SIZE: usize = 1024 * 1024;

    fn create_file(url: &str, path: &str, size: usize) -> io::Result<()> {
        assert!(size % 4 == 0);
        let num_ints = size / 4;

        let hadoop_exc = which("hadoop").expect("Failed to find hadoop executable");
        let mut file = NamedTempFile::new_in("target/test").unwrap();
        {
            let mut writer = BufWriter::new(file.as_file_mut());
            for i in 0..num_ints as u32 {
                let bytes = i.to_be_bytes();
                writer.write_all(&bytes)?;
            }
            writer.flush()?;
        }

        let mut cmd = Command::new(hadoop_exc)
            .args([
                "fs",
                "-copyFromLocal",
                "-f",
                file.path().to_str().unwrap(),
                &format!("{url}{path}"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        assert!(cmd.wait().unwrap().success());

        Ok(())
    }

    fn verify_read(mut data: Bytes, size: usize) {
        assert_eq!(size % 4, 0);
        assert_eq!(data.len(), size);
        let num_ints = size / 4;

        for i in 0..num_ints as u32 {
            assert_eq!(data.get_u32(), i, "Different values at integer {i}");
        }
    }

    async fn verify_positioned_read(reader: &FileReader, offset: usize, len: usize) -> Result<()> {
        assert!(offset % 4 == 0);
        assert!(len % 4 == 0);
        let first_int = offset / 4;
        let num_ints = len / 4;

        let mut data = reader.read_range(offset, len).await?;

        for i in first_int..(first_int + num_ints) {
            assert_eq!(data.get_u32(), i as u32, "Different values at integer {i}");
        }

        Ok(())
    }

    fn sizes_to_test(data_units: usize) -> Vec<usize> {
        vec![
            16usize,                    // Small
            CELL_SIZE,                  // One cell
            CELL_SIZE - 4,              // Just smaller than one cell
            CELL_SIZE + 4,              // Just bigger than one cell
            CELL_SIZE * data_units * 5, // Five "rows" of cells
            CELL_SIZE * data_units * 5 - 4,
            CELL_SIZE * data_units * 5 + 4,
        ]
    }
    #[tokio::test]
    #[serial]
    async fn test_erasure_coded_read() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let dfs_features = HashSet::from([DfsFeatures::EC]);

        let dfs = MiniDfs::with_features(&dfs_features);
        let client = Client::default();

        // Test each of Hadoop's built-in RS policies
        for (data, parity) in [(3usize, 2usize), (6, 3), (10, 4)] {
            let file = format!("/ec-{data}-{parity}/testfile");

            for file_size in sizes_to_test(data) {
                create_file(&dfs.url, &file, file_size)?;

                let reader = client.read(&file).await?;
                assert_eq!(reader.file_length(), file_size);

                for faults in 0..parity {
                    let _ = EC_FAULT_INJECTOR.lock().unwrap().insert(EcFaultInjection {
                        fail_blocks: (0..faults).collect(),
                    });
                    let data = reader.read_range(0, file_size).await?;
                    verify_read(data, file_size);
                }

                // Fail more than the number of parity shards, read should fail
                let _ = EC_FAULT_INJECTOR.lock().unwrap().insert(EcFaultInjection {
                    fail_blocks: (0..=parity).collect(),
                });

                assert!(reader.read_range(0, reader.file_length()).await.is_err());
            }

            // Reset fault injector
            // Fail more than the number of parity shards, read should fail
            let _ = EC_FAULT_INJECTOR.lock().unwrap().insert(EcFaultInjection {
                fail_blocks: vec![],
            });

            // Test positioned reads
            // Create 1 "row" of data
            create_file(&dfs.url, &file, data * CELL_SIZE)?;

            let reader = client.read(&file).await?;

            // Read the first cell completely
            verify_positioned_read(&reader, 0, CELL_SIZE).await?;

            // Read part of the first cell from the beginning
            verify_positioned_read(&reader, 0, 1024).await?;

            // Read part of the first cell in the middle
            verify_positioned_read(&reader, 1024, 2048).await?;

            // Read the second cell completely
            verify_positioned_read(&reader, CELL_SIZE, CELL_SIZE).await?;

            // Read part of the second cell from the beginning
            verify_positioned_read(&reader, CELL_SIZE, CELL_SIZE + 1024).await?;

            // Read part of the second cell in the middle
            verify_positioned_read(&reader, CELL_SIZE + 1024, CELL_SIZE + 2048).await?;

            assert!(client.delete(&file, false).await?);
        }
        let _ = EC_FAULT_INJECTOR.lock().unwrap().take();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_erasure_coded_write() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let dfs_features = HashSet::from([DfsFeatures::EC]);

        let _dfs = MiniDfs::with_features(&dfs_features);
        let client = Client::default();

        // Test each of Hadoop's built-in RS policies
        for (data, parity) in [(3usize, 2usize)] {
            let file = format!("/ec-{data}-{parity}/testfile");

            for file_size in sizes_to_test(data) {
                let num_ints = file_size / 4;
                let mut writer = client.create(&file, WriteOptions::default()).await?;
                let mut buf = BytesMut::with_capacity(file_size);
                for i in 0..num_ints as u32 {
                    buf.put_u32(i);
                }

                writer.write(buf.freeze()).await?;
                writer.close().await?;

                let reader = client.read(&file).await?;
                assert_eq!(reader.file_length(), file_size);

                for faults in 0..parity {
                    let _ = EC_FAULT_INJECTOR.lock().unwrap().insert(EcFaultInjection {
                        fail_blocks: (0..faults).collect(),
                    });
                    let data = reader.read_range(0, reader.file_length()).await?;
                    verify_read(data, file_size);
                }

                // Fail more than the number of parity shards, read should fail
                let _ = EC_FAULT_INJECTOR.lock().unwrap().insert(EcFaultInjection {
                    fail_blocks: (0..=parity).collect(),
                });

                assert!(reader.read_range(0, reader.file_length()).await.is_err());

                assert!(client.delete(&file, false).await?);
            }

            let mut writer = client.create(&file, WriteOptions::default()).await?;
            let num_ints = 4;
            let mut buf = BytesMut::with_capacity(16);
            for i in 0..num_ints as u32 {
                buf.put_u32(i);
            }

            writer.write(buf.freeze()).await?;
            writer.close().await?;

            let mut writer = client.append(&file).await?;
            let file_size = 16usize;
            let num_ints = file_size / 4;
            let mut buf = BytesMut::with_capacity(16);
            for i in num_ints..num_ints * 2 {
                buf.put_u32(i as u32);
            }

            writer.write(buf.freeze()).await?;
            writer.close().await?;

            let _ = EC_FAULT_INJECTOR.lock().unwrap().take();

            let reader = client.read(&file).await?;
            assert_eq!(reader.file_length(), file_size * 2);
            verify_read(
                reader.read_range(0, reader.file_length()).await?,
                file_size * 2,
            )
        }

        Ok(())
    }
}
