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
- [ ] Federated router
- [x] Erasure coded reads 
    - only supported in Python release, relies on a fork of https://github.com/rust-rse/reed-solomon-erasure
    - RS schema only, no support for RS-Legacy or XOR
- [ ] Erasure coded writes

### Security Features
- [x] Kerberos authentication (GSSAPI SASL support)
- [x] Token authentication (DIGEST-MD5 SASL support, no encryption support)
- [x] NameNode SASL connection
- [ ] DataNode SASL connection
- [ ] DataNode data transfer encryption
- [ ] Encryption at rest (KMS support)

### Other improvements
- [ ] Better error handling
- [ ] RPC retries
- [x] Async support

## Supported HDFS Settings
The client will attempt to read Hadoop configs `core-site.xml` and `hdfs-site.xml` in the directories `$HADOOP_CONF_DIR` or if that doesn't exist, `$HADOOP_HOME/conf`. Currently the supported configs that are used are:
- `dfs.ha.namenodes` - name service support
- `dfs.namenode.rpc-address.*` - name service support

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
- `rs` - support Reed-Solomon codecs for erasure coded reads, currently only supported for the Python build, as it relies on a fork of https://github.com/rust-rse/reed-solomon-erasure
- `protobuf-src` - builds protobuf from source to avoid having to pre-install it