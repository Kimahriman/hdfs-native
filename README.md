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
- [x] Erasure coded reads 
    - RS schema only, no support for RS-Legacy or XOR
- [ ] Erasure coded writes

### Security Features
- [x] Kerberos authentication (GSSAPI SASL support)
- [x] Token authentication (DIGEST-MD5 SASL support, no encryption support)
- [x] NameNode SASL connection
- [ ] DataNode SASL connection
- [ ] DataNode data transfer encryption
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
brew install gsasl krb5
# You might need these env vars on newer Macs
export BINDGEN_EXTRA_CLANG_ARGS="-I/opt/homebrew/include"
export LIBRARY_PATH=/opt/homebrew/lib
cargo build --features token,kerberos
```

### Ubuntu
```
apt-get install clang libkrb5-dev libgsasl-dev
cargo build --features token,kerberos
```

## Crate features
- `token` - enables token based DIGEST-MD5 authentication support. This uses the `gsasl` native library and only supports authentication, not integrity or confidentiality
- `kerberos` - enables kerberos GSSAPI authentication support. This uses the `libgssapi` crate and supports integrity as well as confidentiality
- `object_store` - provides an `object_store` wrapper around the HDFS client

## Running tests
The tests are mostly integration tests that utilize a small Java application in `rust/mindifs/` that runs a custom `MiniDFSCluster`. To run the tests, you need to have Java, Maven, Hadoop binaries, and Kerberos tools available and on your path. Any Java version between 8 and 17 should work.

```bash
cargo test -p hdfs-native --features token,kerberos,intergation-test
```

### Python tests
See the [Python README](./python/README.md)