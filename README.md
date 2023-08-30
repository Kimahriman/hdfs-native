# Native Rust HDFS client
This is a proof-of-concept HDFS client written natively in Rust. All other clients I have found in any other language are simply wrappers around libhdfs and require all the same Java dependencies, so I wanted to see if I could write one from scratch given that HDFS isn't really changing very often anymore. While several of the features are currently working, the code still contains a lot of panics and needs a lot of cleanup before being actually viable.

What this is not trying to do is implement all HDFS client/FileSystem interfaces, just things involving reading and writing data.

## Supported HDFS features
Here is a list of currently supported and unsupported but possible future features.

### HDFS Operations
- [x] Listing
- [x] Reading
- [ ] Writing
- [x] Rename
- [x] Delete

### HDFS Features
- [x] Name Services
- [ ] Observer reads
- [ ] Federated router
- [ ] Erasure coding

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

## Building

### Mac
```
brew install protobuf gsasl krb5
# You might need these env vars on newer Macs
export BINDGEN_EXTRA_CLANG_ARGS="-I/opt/homebrew/include"
export LIBRARY_PATH=/opt/homebrew/lib
cargo build --all-features
```

### Ubuntu
```
apt-get install clang protobuf-compiler libkrb5-dev libgsasl-dev
cargo build --all-features
```

## Crate features
- `token` - enables token based DIGEST-MD5 authentication support. This uses the `gsasl` native library and only supports authentication, not integrity or confidentiality
- `kerberos` - enables kerberos GSSAPI authentication support. This uses the `libgssapi` crate and supports integrity as well as confidentiality
- `object_store` - provides an `object_store` wrapper around the HDFS client