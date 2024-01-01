# Object store implementation for the Native Rust HDFS client

## Usage

```rust
use hdfs_native::Client;
use hdfs_native_object_store::HdfsObjectStore;
use hdfs_native::Result;
fn main() -> Result<()> {
    let client = Client::new("hdfs://localhost:9000")?;
    let store = HdfsObjectStore::new(client);
Ok(())
}
```