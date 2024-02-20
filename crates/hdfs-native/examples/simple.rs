use std::collections::HashSet;

#[cfg(feature = "integration-test")]
use hdfs_native::minidfs::MiniDfs;
use hdfs_native::{Client, WriteOptions};

#[tokio::main]
async fn main() {
    let _ = env_logger::builder().format_timestamp_millis().try_init();

    // If using the integration-test feature, create a MiniDFS cluster. Otherwise
    // assume the environment has configs pointing to an existing HDFS cluster.
    #[cfg(feature = "integration-test")]
    let _dfs = MiniDfs::with_features(&HashSet::new());

    let client = Client::default();

    // Create an empty file
    client
        .create("/hdfs-native-test", WriteOptions::default())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();

    // List files
    let listing = client.list_status("/", false).await.unwrap();
    println!("{:?}", listing);

    // Get info on a specific file
    let status = client.get_file_info("/hdfs-native-test").await.unwrap();
    println!("{:?}", status);

    // Rename a file
    client
        .rename("/hdfs-native-test", "/hdfs-native-test2", false)
        .await
        .unwrap();

    // Delete a file
    client.delete("/hdfs-native-test2", false).await.unwrap();

    // Write to a new file
    let mut writer = client
        .create("/hdfs-native-write", WriteOptions::default())
        .await
        .unwrap();

    writer.write(vec![1, 2, 3, 4].into()).await.unwrap();
    writer.close().await.unwrap();

    // Append to an existing file
    let mut writer = client.append("/hdfs-native-write").await.unwrap();

    writer.write(vec![5, 6, 7, 8].into()).await.unwrap();
    writer.close().await.unwrap();

    // Read a file
    let reader = client.read("/hdfs-native-write").await.unwrap();
    let content = reader.read_range(0, reader.file_length()).await.unwrap();
    println!("{:?}", content);

    client.delete("/hdfs-native-write", false).await.unwrap();
}
