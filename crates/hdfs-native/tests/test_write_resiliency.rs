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
        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA, DfsFeatures::SECURITY]));
        #[cfg(not(feature = "kerberos"))]
        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA]));
        let client = Client::default();

        let file = "/testfile";

        let mut writer = client.create(file, WriteOptions::default()).await?;

        writer.write(Bytes::from(vec![0u8, 1, 2, 3])).await?;

        tokio::time::sleep(Duration::from_secs(70)).await;

        writer.write(Bytes::from(vec![4u8, 5, 6, 7])).await?;

        writer.close().await?;

        Ok(())
    }
}
