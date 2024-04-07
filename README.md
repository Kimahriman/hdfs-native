# Native Rust HDFS client
This is a proof-of-concept HDFS client written natively in Rust. All other clients I have found in any other language are simply wrappers around libhdfs and require all the same Java dependencies, so I wanted to see if I could write one from scratch given that HDFS isn't really changing very often anymore. Several basic features are working, however it is not nearly as robust and the real HDFS client.

What this is not trying to do is implement all HDFS client/FileSystem interfaces, just things involving reading and writing data.

## Supported HDFS features
Here is a list of currently supported and unsupported but possible future features.

### HDFS Operations
- [x] Listing
- [x] Reading
- [x] Writing
- [x] Rename
- [x] Delete

### HDFS Features
- [x] Name Services
- [ ] Observer reads (state ID tracking is supported, but needs improvements on tracking Observer/Active NameNode)
- [x] ViewFS
- [x] Router based federation
- [x] Erasure coded reads and writes
    - RS schema only, no support for RS-Legacy or XOR

### Security Features
- [x] Kerberos authentication (GSSAPI SASL support)
- [x] Token authentication (DIGEST-MD5 SASL support)
- [x] NameNode SASL connection
- [x] DataNode SASL connection
- [x] DataNode data transfer encryption
- [ ] Encryption at rest (KMS support)

## Supported HDFS Settings
The client will attempt to read Hadoop configs `core-site.xml` and `hdfs-site.xml` in the directories `$HADOOP_CONF_DIR` or if that doesn't exist, `$HADOOP_HOME/etc/hadoop`. Currently the supported configs that are used are:
- `fs.defaultFS` - Client::default() support
- `dfs.ha.namenodes` - name service support
- `dfs.namenode.rpc-address.*` - name service support
- `fs.viewfs.mounttable.*.link.*` - ViewFS links
- `fs.viewfs.mounttable.*.linkFallback` - ViewFS link fallback

All other settings are generally assumed to be the defaults currently. For instance, security is assumed to be enabled and SASL negotiation is always done, but on insecure clusters this will just do SIMPLE authentication. Any setups that require other customized Hadoop client configs may not work correctly. 

## Building

### Mac
```
brew install krb5
# You might need these env vars on newer Macs
export BINDGEN_EXTRA_CLANG_ARGS="-I/opt/homebrew/include"
export LIBRARY_PATH=/opt/homebrew/lib
cargo build --features kerberos
```

### Ubuntu
```
apt-get install clang libkrb5-dev
cargo build --features kerberos
```

## Crate features
- `kerberos` - enables kerberos GSSAPI authentication support. This uses the `libgssapi` crate and supports integrity as well as confidentiality

## Object store implementation
An object_store implementation for HDFS is provided in the [hdfs-native-object-store](./crates/hdfs-native-object-store/) crate.

## Running tests
The tests are mostly integration tests that utilize a small Java application in `rust/mindifs/` that runs a custom `MiniDFSCluster`. To run the tests, you need to have Java, Maven, Hadoop binaries, and Kerberos tools available and on your path. Any Java version between 8 and 17 should work.

```bash
cargo test -p hdfs-native --features kerberos,intergation-test
```

### Python tests
See the [Python README](./python/README.md)

## Running benchmarks
Some of the benchmarks compare performance to the JVM based client through libhdfs via the fs-hdfs3 crate. Because of that, some extra setup is required to run the benchmarks:

```bash
export HADOOP_CONF_DIR=$(pwd)/crates/hdfs-native/target/test
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
