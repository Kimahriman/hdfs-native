#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use crate::common::{setup, TEST_FILE_INTS};
    use bytes::{Buf, BufMut, BytesMut};
    use hdfs_native::{minidfs::DfsFeatures, Client, Result, WriteOptions};
    use serial_test::serial;
    use std::collections::HashSet;

    #[tokio::test]
    #[serial]
    async fn test_basic() {
        test_with_features(&HashSet::new()).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    #[cfg(feature = "kerberos")]
    async fn test_security_kerberos() {
        test_with_features(&HashSet::from([DfsFeatures::SECURITY]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    #[cfg(feature = "token")]
    async fn test_security_token() {
        test_with_features(&HashSet::from([DfsFeatures::SECURITY, DfsFeatures::TOKEN]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    #[cfg(feature = "token")]
    async fn test_privacy_token() {
        test_with_features(&HashSet::from([
            DfsFeatures::SECURITY,
            DfsFeatures::TOKEN,
            DfsFeatures::PRIVACY,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    #[cfg(feature = "kerberos")]
    async fn test_privacy_kerberos() {
        test_with_features(&HashSet::from([
            DfsFeatures::SECURITY,
            DfsFeatures::PRIVACY,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_basic_ha() {
        test_with_features(&HashSet::from([DfsFeatures::HA]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    #[cfg(feature = "kerberos")]
    async fn test_security_privacy_ha() {
        test_with_features(&HashSet::from([
            DfsFeatures::SECURITY,
            DfsFeatures::PRIVACY,
            DfsFeatures::HA,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    #[cfg(feature = "token")]
    async fn test_security_token_ha() {
        test_with_features(&HashSet::from([
            DfsFeatures::SECURITY,
            DfsFeatures::TOKEN,
            DfsFeatures::HA,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_rbf() {
        test_with_features(&HashSet::from([DfsFeatures::RBF]))
            .await
            .unwrap();
    }

    pub async fn test_with_features(features: &HashSet<DfsFeatures>) -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = setup(features);
        let client = Client::default();

        test_file_info(&client).await?;
        test_listing(&client).await?;
        test_read(&client).await?;
        test_rename(&client).await?;
        test_dirs(&client).await?;
        test_write(&client).await?;
        // We use writing to create files, so do this after
        test_recursive_listing(&client).await?;

        Ok(())
    }

    async fn test_file_info(client: &Client) -> Result<()> {
        let status = client.get_file_info("/testfile").await?;
        // Path is empty, I guess because we already know what file we just got the info for?
        assert_eq!(status.path, "/testfile");
        assert_eq!(status.length, TEST_FILE_INTS * 4);
        Ok(())
    }

    async fn test_listing(client: &Client) -> Result<()> {
        let statuses = client.list_status("/", false).await?;
        assert_eq!(statuses.len(), 1);
        let status = &statuses[0];
        assert_eq!(status.path, "/testfile");
        assert_eq!(status.length, TEST_FILE_INTS * 4);
        Ok(())
    }

    async fn test_read(client: &Client) -> Result<()> {
        // Read the whole file
        let reader = client.read("/testfile").await?;
        let mut buf = reader.read_range(0, TEST_FILE_INTS * 4).await?;
        for i in 0..TEST_FILE_INTS as i32 {
            assert_eq!(buf.get_i32(), i);
        }

        // Read a single integer from the file
        let mut buf = reader.read_range(TEST_FILE_INTS / 2 * 4, 4).await?;
        assert_eq!(buf.get_i32(), TEST_FILE_INTS as i32 / 2);

        // Read the whole file in 1 MiB chunks
        let mut offset = 0;
        let mut val = 0;
        while offset < TEST_FILE_INTS * 4 {
            let mut buf = reader.read_range(offset, 1024 * 1024).await?;
            while !buf.is_empty() {
                assert_eq!(buf.get_i32(), val);
                val += 1;
            }
            offset += 1024 * 1024;
        }

        Ok(())
    }

    async fn test_rename(client: &Client) -> Result<()> {
        client.rename("/testfile", "/testfile2", false).await?;

        assert!(client.list_status("/testfile", false).await.is_err());
        assert_eq!(client.list_status("/testfile2", false).await?.len(), 1);

        client.rename("/testfile2", "/testfile", false).await?;
        assert!(client.list_status("/testfile2", false).await.is_err());
        assert_eq!(client.list_status("/testfile", false).await?.len(), 1);

        Ok(())
    }

    async fn test_dirs(client: &Client) -> Result<()> {
        client.mkdirs("/testdir", 0o755, false).await?;
        assert!(client
            .list_status("/testdir", false)
            .await
            .is_ok_and(|s| s.is_empty()));

        client.delete("/testdir", false).await?;
        assert!(client.list_status("/testdir", false).await.is_err());

        client.mkdirs("/testdir1/testdir2", 0o755, true).await?;
        assert!(client
            .list_status("/testdir1", false)
            .await
            .is_ok_and(|s| s.len() == 1));

        // Deleting non-empty dir without recursive fails
        assert!(client.delete("/testdir1", false).await.is_err());
        assert!(client.delete("/testdir1", true).await.is_ok_and(|r| r));

        Ok(())
    }

    async fn test_write(client: &Client) -> Result<()> {
        let mut write_options = WriteOptions::default();

        // Create an empty file
        let mut writer = client.create("/newfile", write_options.clone()).await?;

        writer.close().await?;

        assert_eq!(client.get_file_info("/newfile").await?.length, 0);

        // Overwrite now
        write_options.overwrite = true;

        // Check a small files, a file that is exactly one block, and a file slightly bigger than a block
        for size_to_check in [16i32, 128 * 1024 * 1024, 130 * 1024 * 1024] {
            let ints_to_write = size_to_check / 4;

            let mut writer = client.create("/newfile", write_options.clone()).await?;

            let mut data = BytesMut::with_capacity(size_to_check as usize);
            for i in 0..ints_to_write {
                data.put_i32(i);
            }

            let buf = data.freeze();

            writer.write(buf.clone()).await?;
            writer.close().await?;

            assert_eq!(
                client.get_file_info("/newfile").await?.length,
                size_to_check as usize
            );

            let mut reader = client.read("/newfile").await?;
            let read_data = reader.read(reader.file_length()).await?;

            assert_eq!(buf.len(), read_data.len());

            for pos in 0..buf.len() {
                assert_eq!(
                    buf[pos], read_data[pos],
                    "data is different as position {} for size {}",
                    pos, size_to_check
                );
            }
        }

        assert!(client.delete("/newfile", false).await.is_ok_and(|r| r));

        Ok(())
    }

    async fn test_recursive_listing(client: &Client) -> Result<()> {
        let write_options = WriteOptions::default();
        client.mkdirs("/dir/nested", 0o755, true).await?;
        client
            .create("/dir/file1", write_options.clone())
            .await?
            .close()
            .await?;
        client
            .create("/dir/nested/file2", write_options.clone())
            .await?
            .close()
            .await?;
        client
            .create("/dir/nested/file3", write_options.clone())
            .await?
            .close()
            .await?;

        let statuses = client.list_status("/dir", true).await?;
        assert_eq!(statuses.len(), 4);

        client.delete("/dir", true).await?;

        Ok(())
    }
}
