# Native Rust HDFS client
This is an experimental HDFS client written natively in Rust. Several basic features are working, however it is not nearly as robust and the real HDFS client.

## Supported HDFS features
Here is a list of currently supported and unsupported but possible future features.

### HDFS Operations
- [x] Listing
- [x] Reading
- [x] Writing
- [x] Rename
- [x] Delete
- [x] Basic Permissions and ownership
- [x] ACLs
- [x] Content summary
- [x] Set replication
- [x] Set timestamps

### HDFS Features
- [x] Name Services
- [x] Observer reads
- [x] ViewFS
- [x] Router based federation
- [x] Erasure coded reads and writes
    - RS schema only, no support for RS-Legacy or XOR

### Security Features
- [x] Kerberos authentication (GSSAPI SASL support) (requires libgssapi_krb5, see below)
- [x] Token authentication (DIGEST-MD5 SASL support)
- [x] NameNode SASL connection
- [x] DataNode SASL connection
- [x] DataNode data transfer encryption
- [ ] Encryption at rest (KMS support)

### Kerberos Support
Kerberos (SASL GSSAPI) mechanism is supported through a runtime dynamic link to `libgssapi_krb5`. This must be installed separately, but is likely already installed on your system. If not you can install it by:

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

#### Windows
Download and install the Microsoft Kerberos package from https://web.mit.edu/kerberos/dist/

Copy the `<INSTALL FOLDER>\MIT\Kerberos\bin\gssapi64.dll` file to a folder in %PATH% and change the name to `gssapi_krb5.dll`

## Supported HDFS Settings
The client will attempt to read Hadoop configs `core-site.xml` and `hdfs-site.xml` in the directories `$HADOOP_CONF_DIR` or if that doesn't exist, `$HADOOP_HOME/etc/hadoop`. Currently the supported configs that are used are:
- `fs.defaultFS` - Client::default() support
- `dfs.ha.namenodes` - name service support
- `dfs.namenode.rpc-address.*` - name service support
- `dfs.client.failover.resolve-needed.*` - DNS based NameNode discovery
- `dfs.client.failover.resolver.useFQDN.*` - DNS based NameNode discovery
- `dfs.client.failover.random.order.*` - Randomize order of NameNodes to try
- `dfs.client.block.write.replace-datanode-on-failure.enable`
- `dfs.client.block.write.replace-datanode-on-failure.policy`
- `dfs.client.block.write.replace-datanode-on-failure.best-effort`
- `fs.viewfs.mounttable.*.link.*` - ViewFS links
- `fs.viewfs.mounttable.*.linkFallback` - ViewFS link fallback

All other settings are generally assumed to be the defaults currently. For instance, security is assumed to be enabled and SASL negotiation is always done, but on insecure clusters this will just do SIMPLE authentication. Any setups that require other customized Hadoop client configs may not work correctly. 

## Building

```
cargo build
```

## Object store implementation
An object_store implementation for HDFS is provided in the [hdfs-native-object-store](https://github.com/datafusion-contrib/hdfs-native-object-store) crate.

## Running tests
The tests are mostly integration tests that utilize a small Java application in `rust/mindifs/` that runs a custom `MiniDFSCluster`. To run the tests, you need to have Java, Maven, Hadoop binaries, and Kerberos tools available and on your path. Any Java version between 8 and 17 should work.

```bash
cargo test -p hdfs-native --features intergation-test
```

### Python tests
See the [Python README](./python/README.md)

## Running benchmarks
Some of the benchmarks compare performance to the JVM based client through libhdfs via the fs-hdfs3 crate. Because of that, some extra setup is required to run the benchmarks:

```bash
export HADOOP_CONF_DIR=$(pwd)/rust/target/test
export CLASSPATH=$(hadoop classpath)
```

then you can run the benchmarks with
```bash
cargo bench -p hdfs-native --features benchmark
```

The `benchmark` feature is required to expose `minidfs` and the internal erasure coding functions to benchmark.

## Running examples
The examples make use of the `minidfs` module to create a simple HDFS cluster to run the example. This requires including the `integration-test` feature to enable the `minidfs` module. Alternatively, if you want to run the example against an existing HDFS cluster you can exclude the `integration-test` feature and make sure your `HADOOP_CONF_DIR` points to a directory with HDFS configs for talking to your cluster.

```bash
cargo run --example simple --features integration-test
```
