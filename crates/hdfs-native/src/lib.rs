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
