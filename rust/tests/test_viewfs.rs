#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use hdfs_native::{
        minidfs::{DfsFeatures, MiniDfs},
        Client, Result, WriteOptions,
    };
    use serial_test::serial;
    use std::collections::HashSet;

    async fn touch(client: &Client, path: &str) {
        client
            .create(path, WriteOptions::default())
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_viewfs() {
        let features = HashSet::from([DfsFeatures::VIEWFS]);
        let _ = env_logger::builder().is_test(true).try_init();

        // VIEWFS feature creates one mount with a fallback
        // /mount1 resolves to hdfs://ns0/nested
        // fallback resolves to hdfs://ns1/nested
        let _dfs = MiniDfs::with_features(&features);
        let viewfs = Client::default();
        let hdfs1 = Client::new("hdfs://ns0").unwrap();
        let hdfs2 = Client::new("hdfs://ns1").unwrap();

        touch(&viewfs, "/mount1/file1").await;
        touch(&viewfs, "/root/file2").await;

        test_file_info(&viewfs, &hdfs1, &hdfs2).await.unwrap();
        test_listing(&viewfs).await.unwrap();
        test_rename(&viewfs, &hdfs1, &hdfs2).await.unwrap();
    }

    async fn test_file_info(viewfs: &Client, hdfs1: &Client, hdfs2: &Client) -> Result<()> {
        assert_eq!(
            viewfs.get_file_info("/mount1/file1").await?.path,
            "/mount1/file1"
        );
        assert_eq!(
            viewfs.get_file_info("/root/file2").await?.path,
            "/root/file2"
        );
        assert_eq!(
            hdfs1.get_file_info("/nested/file1").await?.path,
            "/nested/file1"
        );
        assert_eq!(
            hdfs2.get_file_info("/nested/root/file2").await?.path,
            "/nested/root/file2"
        );

        Ok(())
    }

    async fn test_listing(viewfs: &Client) -> Result<()> {
        let mut statuses = viewfs.list_status("/", true).await?;
        assert_eq!(statuses.len(), 2);
        statuses.sort_by_key(|x| x.path.clone());

        // TODO: Should /mount1 show up here if it doesn't exist in the fallback system?
        assert_eq!(statuses[0].path, "/root");
        assert_eq!(statuses[1].path, "/root/file2");

        let statuses = viewfs.list_status("/mount1", true).await?;
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].path, "/mount1/file1");
        Ok(())
    }

    // async fn test_read(client: &Client) -> Result<()> {
    //     // Read the whole file
    //     let reader = client.read("/testfile").await?;
    //     let mut buf = reader.read_range(0, TEST_FILE_INTS * 4).await?;
    //     for i in 0..TEST_FILE_INTS as i32 {
    //         assert_eq!(buf.get_i32(), i);
    //     }

    //     // Read a single integer from the file
    //     let mut buf = reader
    //         .read_range(TEST_FILE_INTS as usize / 2 * 4, 4)
    //         .await?;
    //     assert_eq!(buf.get_i32(), TEST_FILE_INTS as i32 / 2);

    //     // Read the whole file in 1 MiB chunks
    //     let mut offset = 0;
    //     let mut val = 0;
    //     while offset < TEST_FILE_INTS * 4 {
    //         let mut buf = reader.read_range(offset, 1024 * 1024).await?;
    //         while !buf.is_empty() {
    //             assert_eq!(buf.get_i32(), val);
    //             val += 1;
    //         }
    //         offset += 1024 * 1024;
    //     }

    //     Ok(())
    // }

    async fn test_rename(viewfs: &Client, hdfs1: &Client, hdfs2: &Client) -> Result<()> {
        viewfs
            .rename("/mount1/file1", "/mount1/file3", false)
            .await
            .unwrap();
        assert!(hdfs1.get_file_info("/nested/file3").await.is_ok());
        assert!(hdfs1.get_file_info("/nested/file1").await.is_err());
        viewfs
            .rename("/mount1/file3", "/mount1/file1", false)
            .await
            .unwrap();

        viewfs
            .rename("/root/file2", "/root/file4", false)
            .await
            .unwrap();
        assert!(hdfs2.get_file_info("/nested/root/file4").await.is_ok());
        assert!(hdfs2.get_file_info("/nested/root/file2").await.is_err());
        viewfs
            .rename("/root/file4", "/root/file3", false)
            .await
            .unwrap();

        assert!(viewfs
            .rename("/mount1/file1", "/root/file1", false)
            .await
            .is_err());

        Ok(())
    }

    // async fn test_dirs(client: &Client) -> Result<()> {
    //     client.mkdirs("/testdir", 0o755, false).await?;
    //     assert!(client
    //         .list_status("/testdir", false)
    //         .await
    //         .is_ok_and(|s| s.len() == 0));

    //     client.delete("/testdir", false).await?;
    //     assert!(client.list_status("/testdir", false).await.is_err());

    //     client.mkdirs("/testdir1/testdir2", 0o755, true).await?;
    //     assert!(client
    //         .list_status("/testdir1", false)
    //         .await
    //         .is_ok_and(|s| s.len() == 1));

    //     // Deleting non-empty dir without recursive fails
    //     assert!(client.delete("/testdir1", false).await.is_err());
    //     assert!(client.delete("/testdir1", true).await.is_ok_and(|r| r));

    //     Ok(())
    // }

    // async fn test_write(client: &Client) -> Result<()> {
    //     let mut write_options = WriteOptions::default();

    //     // Create an empty file
    //     let mut writer = client.create("/newfile", write_options.clone()).await?;

    //     writer.close().await?;

    //     assert_eq!(client.get_file_info("/newfile").await?.length, 0);

    //     // Overwrite now
    //     write_options.overwrite = true;

    //     // Check a small files, a file that is exactly one block, and a file slightly bigger than a block
    //     for size_to_check in [16i32, 128 * 1024 * 1024, 130 * 1024 * 1024] {
    //         let ints_to_write = size_to_check / 4;

    //         let mut writer = client.create("/newfile", write_options.clone()).await?;

    //         let mut data = BytesMut::with_capacity(size_to_check as usize);
    //         for i in 0..ints_to_write {
    //             data.put_i32(i);
    //         }

    //         let buf = data.freeze();

    //         writer.write(buf.clone()).await?;
    //         writer.close().await?;

    //         assert_eq!(
    //             client.get_file_info("/newfile").await?.length,
    //             size_to_check as usize
    //         );

    //         let mut reader = client.read("/newfile").await?;
    //         let read_data = reader.read(reader.file_length()).await?;

    //         assert_eq!(buf.len(), read_data.len());

    //         for pos in 0..buf.len() {
    //             assert_eq!(
    //                 buf[pos], read_data[pos],
    //                 "data is different as position {} for size {}",
    //                 pos, size_to_check
    //             );
    //         }
    //     }

    //     assert!(client.delete("/newfile", false).await.is_ok_and(|r| r));

    //     Ok(())
    // }

    // async fn test_recursive_listing(client: &Client) -> Result<()> {
    //     let write_options = WriteOptions::default();
    //     client.mkdirs("/dir/nested", 0o755, true).await?;
    //     client
    //         .create("/dir/file1", write_options.clone())
    //         .await?
    //         .close()
    //         .await?;
    //     client
    //         .create("/dir/nested/file2", write_options.clone())
    //         .await?
    //         .close()
    //         .await?;
    //     client
    //         .create("/dir/nested/file3", write_options.clone())
    //         .await?
    //         .close()
    //         .await?;

    //     let statuses = client.list_status("/dir", true).await?;
    //     assert_eq!(statuses.len(), 4);

    //     client.delete("/dir", true).await?;

    //     Ok(())
    // }
}
