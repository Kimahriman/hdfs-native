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
        let features = HashSet::from([DfsFeatures::ViewFS]);
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
}
