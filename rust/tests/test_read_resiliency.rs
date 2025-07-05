#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use std::sync::atomic::Ordering;

    use crate::common::TEST_FILE_INTS;
    use bytes::Buf;
    use futures::StreamExt;
    use hdfs_native::{
        minidfs::MiniDfs,
        test::{DATANODE_CONNECT_FAULT_INJECTOR, DATANODE_READ_FAULT_INJECTOR},
        Client, Result, WriteOptions,
    };
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_read_resiliency() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::default();
        let client = Client::default();

        let mut file = client.create("/testfile", WriteOptions::default()).await?;
        for i in 0..TEST_FILE_INTS as i32 {
            file.write(i.to_be_bytes().to_vec().into()).await?;
        }
        file.close().await?;

        // Failing to connect to the first datanode should failover to the next
        DATANODE_CONNECT_FAULT_INJECTOR.store(true, Ordering::SeqCst);

        let reader = client.read("/testfile").await?;
        let mut buf = reader.read_range(0, 4).await?;
        assert_eq!(buf.get_i32(), 0);

        // Create a single replicated file to test IO failures retrying on the same DataNode
        let mut file = client
            .create(
                "/testfile",
                WriteOptions::default().replication(1).overwrite(true),
            )
            .await?;
        for i in 0..TEST_FILE_INTS as i32 {
            file.write(i.to_be_bytes().to_vec().into()).await?;
        }
        file.close().await?;

        let reader = client.read("/testfile").await?;
        let mut stream = reader.read_range_stream(0, reader.file_length());

        // Start the read
        stream.next().await.unwrap()?;

        // Inject an IO error
        DATANODE_READ_FAULT_INJECTOR.store(true, Ordering::SeqCst);

        // Read another packet
        stream.next().await.unwrap()?;

        Ok(())
    }
}
