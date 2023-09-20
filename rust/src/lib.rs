//! Native HDFS client implementation in Rust
//!
//! # Usage
//!
//! Create a client to a single NameNode
//! ```rust
//! use hdfs_native::Client;
//! # use hdfs_native::Result;
//! # fn main() -> Result<()> {
//! let client = Client::new("hdfs://localhost:9000")?;
//! # Ok(())
//! # }
//! ```
//!
//! Create a client for a Name Service
//! ```rust
//! use hdfs_native::Client;
//! # use hdfs_native::Result;
//! # fn main() -> Result<()> {
//! let client = Client::new("hdfs://ns")?;
//! # Ok(())
//! # }
//! ```
//!
//! # Optional cargo package features
//! - `kerberos` - include support for Kerberos authentication. Uses the libgssapi package. Supports all RPC authentication and encryption methods.
//! - `token` - include support for Token authentication. Uses the gsasl native library. Only
//!   supports authentication, not integrity or privacy modes.
//! - `object_store` - an `object_store` implementation for HDFS.
//! - `rs` - support Reed-Solomon codecs for erasure coded reads. It relies on a fork of <https://github.com/rust-rse/reed-solomon-erasure> with the PR <https://github.com/rust-rse/reed-solomon-erasure/pull/108>, so you must include a `patch` for it to compile:
//! ```toml
//! [patch.crates-io]
//! reed-solomon-erasure = { git = "https://github.com/Kimahriman/reed-solomon-erasure.git", branch = "SNB/23C24_external_matrix" }
//! ```

pub mod client;
pub(crate) mod common;
pub(crate) mod error;
pub mod file;
pub(crate) mod hdfs;
#[cfg(feature = "integration-test")]
pub mod minidfs;
pub(crate) mod proto;
pub(crate) mod security;

#[cfg(feature = "object_store")]
pub mod object_store;

pub use client::Client;
pub use client::WriteOptions;
pub use error::HdfsError;
pub use error::Result;

// Module for testing hooks into non-test code
#[cfg(feature = "integration-test")]
pub mod test;
