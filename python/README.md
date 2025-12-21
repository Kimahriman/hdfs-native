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

## CLI
There is a built-in CLI `hdfsn` that implements most of the behavior of `hdfs dfs` but with a more bash-like syntax. The easiest way to use the CLI is with UV.

### Install CLI with UV
```bash
uv tool install hdfs-ntaive
```

### Auto-complete support
The CLI supports auto-complete for HDFS paths using `argcomplete`. There are two ways to enable this support

To permanently enable support for all Python modules using `argcomplete`:
```bash
uv tool install argcomplete
activate-global-python-argcomplete
```

To enable support just for `hdfsn` in your active shell:
```bash
uv tool install argcomplete
eval "$(register-python-argcomplete hdfsn)"
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