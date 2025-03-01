#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use crate::common::TEST_FILE_INTS;
    use bytes::{Buf, BytesMut};
    use hdfs_native::{
        minidfs::{DfsFeatures, MiniDfs},
        Client, Result, WriteOptions,
    };
    use serial_test::serial;
    use std::collections::HashSet;

    #[tokio::test]
    #[serial]
    async fn test_read_simple() {
        test_read(&HashSet::from([DfsFeatures::HA])).await.unwrap();
    }

    // These tests take a long time, so don't run by default
    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_read_sasl_encryption() {
        test_read(&HashSet::from([
            DfsFeatures::HA,
            DfsFeatures::Security,
            DfsFeatures::Privacy,
        ]))
        .await
        .unwrap();
    }

    // These tests take a long time, so don't run by default
    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_read_cipher_encryption() {
        test_read(&HashSet::from([
            DfsFeatures::HA,
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::AES,
        ]))
        .await
        .unwrap();
    }

    async fn test_read(features: &HashSet<DfsFeatures>) -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::with_features(features);
        let client = Client::default();

        let mut file = client.create("/testfile", WriteOptions::default()).await?;
        for i in 0..TEST_FILE_INTS as i32 {
            file.write(i.to_be_bytes().to_vec().into()).await?;
        }
        file.close().await?;

        // Read the whole file
        let mut reader = client.read("/testfile").await?;
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

        // Positioned reads
        let mut buf = reader.read(reader.file_length()).await?;
        for i in 0..TEST_FILE_INTS as i32 {
            assert_eq!(buf.get_i32(), i);
        }

        reader.seek(0);

        let mut buf = BytesMut::zeroed(reader.file_length());
        assert_eq!(reader.read_buf(&mut buf).await?, reader.file_length());
        for i in 0..TEST_FILE_INTS as i32 {
            assert_eq!(buf.get_i32(), i);
        }

        // Trying to read again should return nothing
        assert!(reader.read(1).await?.is_empty());

        // Same with reading into a provided buffer
        assert_eq!(reader.read_buf(&mut [0u8]).await?, 0);

        Ok(())
    }
}
