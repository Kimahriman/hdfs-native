Native HDFS Python bindings based on hdfs-native Rust package.

## Installation

```bash
pip install hdfs-native
```

## Example

```python
from hdfs_native import Client
client = Client("hdfs://localhost:9000")

status = client.get_file_info("/file.txt")
```