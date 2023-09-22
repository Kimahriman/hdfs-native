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

## Running tests
The same requirements apply as the Rust tests, requiring Java, Maven, Hadoop, and Kerberos tools to be on your path. Then you can:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install maturin
pip3 install -r requirements-dev.txt
maturin develop
pytest
```