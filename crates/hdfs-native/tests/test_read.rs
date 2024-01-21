#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use crate::common::{setup, TEST_FILE_INTS};
    use bytes::Buf;
    use hdfs_native::{minidfs::DfsFeatures, Client, Result};
    use serial_test::serial;
    use std::collections::HashSet;

    #[tokio::test]
    #[serial]
    async fn test_read() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = setup(&HashSet::from([DfsFeatures::HA]));
        let client = Client::default();

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
}
