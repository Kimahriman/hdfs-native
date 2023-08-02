//! Native HDFS client implementation in Rust
//!
//! # Usage
//!
//! Create a client to a single NameNode
//! ```rust
//! use hdfs_native::Client;
//! let client = Client::new("hdfs://localhost:9000").unwrap();
//! ```
//!
//! Create a client for a Name Service
//! ```rust
//! use hdfs_native::Client;
//! let client = Client::new("hdfs://ns").unwrap();
//! ```
//!
//! # Optional cargo package features
//! - `kerberos` - include support for Kerberos authentication. Uses the rsasl package which uses
//!   libgssapi under the hood. Supports all RPC authentication and encryption methods.
//! - `token` - include support for Token authentication. Uses the gsasl native library. Only
//!   supports authentication, not integrity or privacy modes.
//! - `object_store` - an `object_store` implementation for HDFS.

pub mod client;
pub(crate) mod common;
pub(crate) mod error;
pub mod hdfs;
pub(crate) mod proto;
pub(crate) mod security;

#[cfg(feature = "object_store")]
pub mod object_store;

pub use client::Client;
pub use error::HdfsError;
pub use error::Result;
