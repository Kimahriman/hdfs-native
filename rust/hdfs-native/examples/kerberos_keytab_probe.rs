use hdfs_native::{ClientBuilder, WriteOptions};

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let url = args.next().expect("HDFS URL argument");
    let config_dir = args.next().expect("Hadoop config directory argument");
    let keytab = args.next().expect("keytab path argument");
    let principal = args.next().expect("principal argument");
    let path = args
        .next()
        .unwrap_or_else(|| "/tmp/hdfs-native-keytab-probe".to_string());
    let payload = b"hdfs-native explicit keytab probe";

    // Prove that authentication uses the credentials configured on this client.
    unsafe { std::env::remove_var("KRB5CCNAME") };
    let client = ClientBuilder::new()
        .with_url(url)
        .with_config_dir(config_dir)
        .with_kerberos_principal(principal)
        .with_kerberos_keytab(keytab)
        .build()
        .expect("build keytab-backed client");

    let mut writer = client
        .create(&path, WriteOptions::default().overwrite(true))
        .await
        .expect("create file through Kerberos RPC");
    writer
        .write_bytes(payload.to_vec().into())
        .await
        .expect("write through authenticated DataTransfer");
    writer.close().await.expect("close file");

    let reader = client.read(&path).await.expect("open file");
    let bytes = reader
        .read_range(0, reader.file_length())
        .await
        .expect("read through authenticated DataTransfer");
    assert_eq!(bytes.as_ref(), payload);
    assert!(client.delete(&path, false).await.expect("delete fixture"));

    println!("explicit keytab integration passed");
}
