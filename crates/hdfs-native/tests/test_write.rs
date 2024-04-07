#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use crate::common::{assert_bufs_equal, setup};
    use bytes::{BufMut, BytesMut};
    use hdfs_native::{minidfs::DfsFeatures, Client, Result, WriteOptions};
    use serial_test::serial;
    use std::collections::HashSet;

    #[tokio::test]
    #[serial]
    async fn test_write_simple() {
        test_write(&HashSet::from([DfsFeatures::HA])).await.unwrap();
    }

    // These tests take a long time, so don't run by default
    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_write_sasl_encryption() {
        test_write(&HashSet::from([
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
    async fn test_write_cipher_encryption() {
        test_write(&HashSet::from([
            DfsFeatures::HA,
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::AES,
        ]))
        .await
        .unwrap();
    }

    async fn test_write(features: &HashSet<DfsFeatures>) -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = setup(features);
        let client = Client::default();

        test_create(&client).await?;
        test_append(&client).await?;
        Ok(())
    }

    async fn test_create(client: &Client) -> Result<()> {
        let write_options = WriteOptions::default().overwrite(true);

        // Create an empty file
        let mut writer = client.create("/newfile", &write_options).await?;

        writer.close().await?;

        assert_eq!(client.get_file_info("/newfile").await?.length, 0);

        // Check a small files, a file that is exactly one block, and a file slightly bigger than a block
        for size_to_check in [16i32, 128 * 1024 * 1024, 130 * 1024 * 1024] {
            let ints_to_write = size_to_check / 4;

            let mut writer = client.create("/newfile", &write_options).await?;

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

            assert_bufs_equal(
                &buf,
                &read_data,
                Some(format!("for size {}", size_to_check)),
            );
        }

        assert!(client.delete("/newfile", false).await.is_ok_and(|r| r));

        Ok(())
    }

    async fn test_append(client: &Client) -> Result<()> {
        // Create an empty file
        client
            .create("/newfile", WriteOptions::default())
            .await?
            .close()
            .await?;

        assert_eq!(client.get_file_info("/newfile").await?.length, 0);

        // Keep track of what should be in the file
        let mut file_contents = BytesMut::new();

        // Test a few different things with each range:

        for range in [
            // Append a few bytes to an empty file
            0u32..4,
            // Append a few bytes to a partial chunk
            4..8,
            // Append multiple chunks to a file
            8..2048,
            // Append to the file filling up the block
            2048..(128 * 1024 * 1024),
            // Append some bytes to a new block
            0..511,
            // Append to a chunk with only one byte missing
            512..1024,
        ] {
            let mut data = BytesMut::new();
            for i in range {
                file_contents.put_u8((i % 256) as u8);
                data.put_u8((i % 256) as u8);
            }

            let buf = data.freeze();

            let mut writer = client.append("/newfile").await?;
            writer.write(buf).await?;
            writer.close().await?;

            let mut reader = client.read("/newfile").await?;
            let read_data = reader.read(reader.file_length()).await?;

            assert_bufs_equal(&file_contents, &read_data, None);
        }

        Ok(())
    }
}
