mod object_store;

use std::str;

use hdfs_native::client::Client;
use hdfs_native::error::Result;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let client = Client::new("hdfs://127.0.0.1:9000")?;

    let file = client.read("/Cargo.lock").await?;
    // println!("{:?}", file);
    let data = file.read(10, 2000).await?;
    println!("{}", data.len());
    println!("{}", str::from_utf8(&data).unwrap());
    Ok(())
}
