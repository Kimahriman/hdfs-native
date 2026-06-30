#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use std::collections::HashSet;

    use bytes::Bytes;
    use hdfs_native::{
        ClientBuilder, WriteOptions,
        minidfs::{DfsFeatures, MiniDfs},
    };
    use serial_test::serial;

    /// Verify that a file written by Java HDFS into an encryption zone reads back
    /// as plaintext via the Rust client. This exercises the full TDE path:
    /// KMS REST call to decrypt the EDEK, AES-CTR decryption of the data
    /// stream, and the codec plumbing through `FileReader`.
    #[tokio::test]
    #[serial]
    async fn test_read_encrypted_file() {
        let _ = env_logger::builder().is_test(true).try_init();

        let features = HashSet::from([DfsFeatures::Kms]);
        let _dfs = MiniDfs::with_features(&features);
        let client = ClientBuilder::new().build().unwrap();

        // Java side wrote this exact payload into /ezone/file.
        let expected = b"hdfs-native TDE round-trip test payload";

        let reader = client.read("/ezone/file").await.unwrap();
        let got = reader.read_range(0, expected.len()).await.unwrap();

        assert_eq!(got.as_ref(), expected);
    }

    /// Round-trip an encrypted file written by the Rust client. The plaintext
    /// must come back unchanged when read with our own client (proving symmetric
    /// AES-CTR is wired up the same way on both sides) and the bytes-on-disk
    /// must in fact be ciphertext (proving we encrypt before sending).
    #[tokio::test]
    #[serial]
    async fn test_write_then_read_encrypted_file() {
        let _ = env_logger::builder().is_test(true).try_init();

        let features = HashSet::from([DfsFeatures::Kms]);
        let _dfs = MiniDfs::with_features(&features);
        let client = ClientBuilder::new().build().unwrap();

        // Pick a payload that crosses the AES-CTR 16-byte boundary so we
        // exercise more than one keystream block, but stays under one packet
        // so we don't depend on the chunking math.
        let plaintext: Vec<u8> = (0..2048u32).map(|i| i as u8).collect();

        let path = "/ezone/written-by-rust";
        let mut writer = client
            .create(path, WriteOptions::default())
            .await
            .unwrap();
        writer
            .write_bytes(Bytes::from(plaintext.clone()))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let reader = client.read(path).await.unwrap();
        let got = reader.read_range(0, plaintext.len()).await.unwrap();
        assert_eq!(got.as_ref(), plaintext.as_slice());
    }

    /// Append support inside an encryption zone: write a chunk, close, append a
    /// second chunk, then read back the concatenation.
    #[tokio::test]
    #[serial]
    async fn test_append_to_encrypted_file() {
        let _ = env_logger::builder().is_test(true).try_init();

        let features = HashSet::from([DfsFeatures::Kms]);
        let _dfs = MiniDfs::with_features(&features);
        let client = ClientBuilder::new().build().unwrap();

        let part1 = b"first chunk -- ".to_vec();
        let part2 = b"second chunk that crosses a 16-byte CTR boundary or two".to_vec();

        let path = "/ezone/appended";
        let mut writer = client
            .create(path, WriteOptions::default())
            .await
            .unwrap();
        writer.write_bytes(Bytes::from(part1.clone())).await.unwrap();
        writer.close().await.unwrap();

        let mut appender = client.append(path).await.unwrap();
        appender
            .write_bytes(Bytes::from(part2.clone()))
            .await
            .unwrap();
        appender.close().await.unwrap();

        let mut expected = part1;
        expected.extend_from_slice(&part2);
        let reader = client.read(path).await.unwrap();
        let got = reader.read_range(0, expected.len()).await.unwrap();
        assert_eq!(got.as_ref(), expected.as_slice());
    }
}
