#[cfg(feature = "integration-test")]
mod test {

    use std::{collections::HashSet, time::Duration};

    use bytes::Bytes;
    use hdfs_native::{
        minidfs::{DfsFeatures, MiniDfs},
        Client, Result, WriteOptions,
    };
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_write_resiliency() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        #[cfg(feature = "kerberos")]
        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA, DfsFeatures::Security]));
        #[cfg(not(feature = "kerberos"))]
        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA, DfsFeatures::RBF]));
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
}
