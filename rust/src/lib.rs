// #![warn(missing_docs)]
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
//! use std::collections::HashMap;
//! use hdfs_native::Client;
//! # use hdfs_native::Result;
//! # fn main() -> Result<()> {
//! let config = HashMap::from([
//!     ("dfs.ha.namenodes.ns".to_string(), "nn-1,nn-2".to_string()),
//!     ("dfs.namenode.rpc-address.ns.nn-1".to_string(), "nn-1:9000".to_string()),
//!     ("dfs.namenode.rpc-address.ns.nn-2".to_string(), "nn-2:9000".to_string()),
//! ]);
//! let client = Client::new_with_config("hdfs://ns", config)?;
//! # Ok(())
//! # }
//! ```
pub mod acl;
pub mod client;
pub(crate) mod common;
#[cfg(feature = "benchmark")]
pub mod ec;
#[cfg(not(feature = "benchmark"))]
pub(crate) mod ec;
pub(crate) mod error;
pub mod file;
pub(crate) mod hdfs;
#[cfg(any(feature = "integration-test", feature = "benchmark"))]
pub mod minidfs;
pub(crate) mod proto;
pub(crate) mod security;

pub use client::Client;
pub use client::WriteOptions;
pub use error::HdfsError;
pub use error::Result;

// Module for testing hooks into non-test code
#[cfg(feature = "integration-test")]
pub mod test;
