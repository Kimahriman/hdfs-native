#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use crate::common::{assert_bufs_equal, setup, TEST_FILE_INTS};
    use bytes::{BufMut, BytesMut};
    use hdfs_native::{client::FileStatus, minidfs::DfsFeatures, Client, Result, WriteOptions};
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
        test_with_features(&HashSet::from([DfsFeatures::Security]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_security_token() {
        test_with_features(&HashSet::from([DfsFeatures::Security, DfsFeatures::Token]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_integrity_kerberos() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Integrity,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_integrity_token() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Token,
            DfsFeatures::Integrity,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    #[cfg(feature = "kerberos")]
    async fn test_privacy_kerberos() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_privacy_token() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Token,
            DfsFeatures::Privacy,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_aes() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::AES,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_forced_data_transfer_encryption() {
        // DataTransferEncryption enabled but privacy isn't, still force encryption
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::DataTransferEncryption,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_data_transfer_encryption() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::DataTransferEncryption,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_data_transfer_encryption_aes() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::DataTransferEncryption,
            DfsFeatures::AES,
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
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::HA,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_security_token_ha() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Token,
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
        test_rename(&client).await?;
        test_dirs(&client).await?;
        test_read_write(&client).await?;
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
        let statuses: Vec<FileStatus> = client
            .list_status("/", false)
            .await?
            .into_iter()
            // Only include files, since federation things could result in folders being created
            .filter(|s| !s.isdir)
            .collect();
        assert_eq!(statuses.len(), 1);
        let status = &statuses[0];
        assert_eq!(status.path, "/testfile");
        assert_eq!(status.length, TEST_FILE_INTS * 4);
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

    async fn test_read_write(client: &Client) -> Result<()> {
        let write_options = WriteOptions::default().overwrite(true);

        // Create an empty file
        let mut writer = client.create("/newfile", &write_options).await?;

        writer.close().await?;

        assert_eq!(client.get_file_info("/newfile").await?.length, 0);

        let mut writer = client.create("/newfile", &write_options).await?;

        let mut file_contents = BytesMut::new();
        let mut data = BytesMut::new();
        for i in 0..1024 {
            file_contents.put_i32(i);
            data.put_i32(i);
        }

        let buf = data.freeze();

        writer.write(buf).await?;
        writer.close().await?;

        assert_eq!(client.get_file_info("/newfile").await?.length, 4096);

        let mut reader = client.read("/newfile").await?;
        let read_data = reader.read(reader.file_length()).await?;

        assert_bufs_equal(&file_contents, &read_data, None);

        let mut data = BytesMut::new();
        for i in 0..1024 {
            file_contents.put_i32(i);
            data.put_i32(i);
        }

        let buf = data.freeze();

        let mut writer = client.append("/newfile").await?;
        writer.write(buf).await?;
        writer.close().await?;

        let mut reader = client.read("/newfile").await?;
        let read_data = reader.read(reader.file_length()).await?;

        assert_bufs_equal(&file_contents, &read_data, None);

        assert!(client.delete("/newfile", false).await.is_ok_and(|r| r));

        Ok(())
    }

    async fn test_recursive_listing(client: &Client) -> Result<()> {
        let write_options = WriteOptions::default();
        client.mkdirs("/dir/nested", 0o755, true).await?;
        client
            .create("/dir/file1", &write_options)
            .await?
            .close()
            .await?;
        client
            .create("/dir/nested/file2", &write_options)
            .await?
            .close()
            .await?;
        client
            .create("/dir/nested/file3", &write_options)
            .await?
            .close()
            .await?;

        let statuses = client.list_status("/dir", true).await?;
        assert_eq!(statuses.len(), 4);

        client.delete("/dir", true).await?;

        Ok(())
    }
}
