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

## Kerberos support
Kerberos (SASL GSSAPI) is supported through a runtime dynamic link to `libgssapi_krb5`. This must be installed separately, but is likely already installed on your system. If not you can install it by:

#### Debian-based systems
```bash
apt-get install libgssapi-krb5-2
```

#### RHEL-based systems
```bash
yum install krb5-libs
```

#### MacOS
```bash
brew install krb5
```

## Running tests
The same requirements apply as the Rust tests, requiring Java, Maven, Hadoop, and Kerberos tools to be on your path. Then you can:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install maturin
maturin develop -E devel
pytest
```