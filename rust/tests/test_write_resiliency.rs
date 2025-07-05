#[cfg(feature = "integration-test")]
mod test {

    use std::{
        collections::{HashMap, HashSet},
        sync::atomic::Ordering,
        time::Duration,
    };

    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use hdfs_native::{
        file::FileReader,
        minidfs::{DfsFeatures, MiniDfs},
        test::{WRITE_CONNECTION_FAULT_INJECTOR, WRITE_REPLY_FAULT_INJECTOR},
        Client, Result, WriteOptions,
    };
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_lease_renewal() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA]));
        let client = Client::default();

        // Second client for checking lease
        let client2 = Client::default();

        let file = "/testfile";

        let mut writer = client.create(file, WriteOptions::default()).await?;

        writer.write(Bytes::from(vec![0u8, 1, 2, 3])).await?;

        // First client owns the lease, so can't append to the file
        assert!(client2.append("/testfile").await.is_err());

        tokio::time::sleep(Duration::from_secs(70)).await;

        // First client should still own the lease from the lease renewal. If not this would
        // trigger lease recovery on the namenode and the first client wouldn't be able to finish
        // writing.
        assert!(client2.append("/testfile").await.is_err());

        writer.write(Bytes::from(vec![4u8, 5, 6, 7])).await?;

        // This succeeding means the lease was renewed
        writer.close().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_write_failures_basic() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA]));
        test_write_failures().await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_write_failures_security() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA, DfsFeatures::Security]));
        test_write_failures().await?;
        Ok(())
    }

    async fn test_write_failures() -> Result<()> {
        fn replace_dn_conf(should_replace: bool) -> HashMap<String, String> {
            HashMap::from([
                (
                    "dfs.client.block.write.replace-datanode-on-failure.enable".to_string(),
                    should_replace.to_string(),
                ),
                (
                    "dfs.client.block.write.replace-datanode-on-failure.policy".to_string(),
                    "ALWAYS".to_string(),
                ),
            ])
        }

        for (i, client) in [
            Client::default_with_config(replace_dn_conf(true)).unwrap(),
            Client::default_with_config(replace_dn_conf(false)).unwrap(),
        ]
        .iter()
        .enumerate()
        {
            let file = format!("/testfile{i}");
            let bytes_to_write = 2usize * 1024 * 1024;

            let mut data = BytesMut::with_capacity(bytes_to_write);
            for i in 0..(bytes_to_write / 4) {
                data.put_i32(i as i32);
            }

            // Test connection failure before writing data
            let mut writer = client
                .create(&file, WriteOptions::default().replication(3))
                .await?;

            WRITE_CONNECTION_FAULT_INJECTOR.store(true, Ordering::SeqCst);

            let data = data.freeze();
            writer.write(data.clone()).await?;
            writer.close().await?;

            let reader = client.read(&file).await?;
            check_file_content(&reader, data.clone()).await?;

            // Test connection failure after data has been written
            let mut writer = client
                .create(
                    &file,
                    WriteOptions::default().replication(3).overwrite(true),
                )
                .await?;

            writer.write(data.slice(..bytes_to_write / 2)).await?;

            // Give a little time for the packets to send
            tokio::time::sleep(Duration::from_millis(100)).await;

            WRITE_CONNECTION_FAULT_INJECTOR.store(true, Ordering::SeqCst);

            writer.write(data.slice(bytes_to_write / 2..)).await?;
            writer.close().await?;

            let reader = client.read(&file).await?;
            check_file_content(&reader, data.clone()).await?;

            // Test failure in from ack status before any data is written
            let mut writer = client
                .create(
                    &file,
                    WriteOptions::default().replication(3).overwrite(true),
                )
                .await?;

            *WRITE_REPLY_FAULT_INJECTOR.lock().unwrap() = Some(2);

            writer.write(data.clone()).await?;
            writer.close().await?;

            let reader = client.read(&file).await?;
            check_file_content(&reader, data.clone()).await?;

            // Test failure in from ack status after some data has been written
            let mut writer = client
                .create(
                    &file,
                    WriteOptions::default().replication(3).overwrite(true),
                )
                .await?;

            writer.write(data.slice(..bytes_to_write / 2)).await?;

            // Give a little time for the packets to send
            tokio::time::sleep(Duration::from_millis(100)).await;

            *WRITE_REPLY_FAULT_INJECTOR.lock().unwrap() = Some(2);

            writer.write(data.slice(bytes_to_write / 2..)).await?;
            writer.close().await?;

            let reader = client.read(&file).await?;
            check_file_content(&reader, data.clone()).await?;
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_replace_failed_datanode() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let mut replace_dn_on_failure_conf: HashMap<String, String> = HashMap::new();
        replace_dn_on_failure_conf.insert(
            "dfs.client.block.write.replace-datanode-on-failure.enable".to_string(),
            "true".to_string(),
        );
        replace_dn_on_failure_conf.insert(
            "dfs.client.block.write.replace-datanode-on-failure.policy".to_string(),
            "ALWAYS".to_string(),
        );

        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA]));
        let client = Client::default_with_config(replace_dn_on_failure_conf).unwrap();

        let file = "/testfile_replace_failed_datanode";
        let bytes_to_write = 2usize * 1024 * 1024;

        let mut data = BytesMut::with_capacity(bytes_to_write);
        for i in 0..(bytes_to_write / 4) {
            data.put_i32(i as i32);
        }
        let data = data.freeze();

        let mut writer = client
            .create(file, WriteOptions::default().replication(3))
            .await?;

        writer.write(data.slice(..bytes_to_write / 3)).await?;

        WRITE_CONNECTION_FAULT_INJECTOR.store(true, Ordering::SeqCst);
        writer
            .write(data.slice(bytes_to_write / 3..2 * bytes_to_write / 3))
            .await?;
        // Give a little time for the packets to send
        tokio::time::sleep(Duration::from_millis(100)).await;

        WRITE_CONNECTION_FAULT_INJECTOR.store(true, Ordering::SeqCst);
        writer.write(data.slice(2 * bytes_to_write / 3..)).await?;
        // Give a little time for the packets to send
        tokio::time::sleep(Duration::from_millis(100)).await;

        WRITE_CONNECTION_FAULT_INJECTOR.store(true, Ordering::SeqCst);
        writer.close().await?;
        // Give a little time for the packets to send
        tokio::time::sleep(Duration::from_millis(100)).await;

        let reader = client.read(file).await?;
        check_file_content(&reader, data).await?;

        Ok(())
    }

    async fn check_file_content(reader: &FileReader, mut expected: Bytes) -> Result<()> {
        assert_eq!(reader.file_length(), expected.len());

        let mut file_data = reader.read_range(0, reader.file_length()).await?;
        for _ in 0..expected.len() / 4 {
            assert_eq!(file_data.get_i32(), expected.get_i32());
        }
        Ok(())
    }
}
