# Native Rust HDFS client
This is a proof-of-concept HDFS client written natively in Rust. All other clients I have found in any other language are simply wrappers around libhdfs and require all the same Java dependencies, so I wanted to see if I could write one from scratch given that HDFS isn't really changing very often anymore. While several of the features are currently working, the code still contains a lot of panics and needs a lot of cleanup before being actually viable.

What this is not trying to do is implement all HDFS client/FileSystem interfaces, just things involving reading and writing data.

## Supported features
Here is a list of currently supported and unsupported but possible future features.

### HDFS Operations
- [x] Listing
- [x] Reading
- [ ] Writing
- [ ] Rename
- [ ] Delete

### HDFS Features
- [x] Name Services
- [x] Observer reads
- [ ] Federated router
- [ ] Erasure coding

### Security Features
- [x] Kerberos authentication (GSSAPI SASL support)
- [ ] Token authentication (DIGEST-MD5 SASL support)
- [x] NameNode RPC encryption
- [ ] DataNode RPC encryption
- [ ] DataNode data transfer encryption
- [ ] Encryption at rest (KMS support)

### Other improvements
- [ ] Better error handling
- [ ] RPC retries
- [ ] Async support