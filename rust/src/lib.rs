// #![warn(missing_docs)]
//! Native HDFS client implementation in Rust
//!
//! # Usage
//!
//! ## Async client
//!
//! Create an async client to a single NameNode
//! ```rust
//! use hdfs_native::ClientBuilder;
//! let client = ClientBuilder::new().with_url("hdfs://localhost:9000").build().unwrap();
//! ```
//!
//! Create an async client for a Name Service
//! ```rust
//! use hdfs_native::ClientBuilder;
//! let client = ClientBuilder::new()
//!     .with_url("hdfs://ns")
//!     .with_config(vec![
//!         ("dfs.ha.namenodes.ns", "nn-1,nn-2"),
//!         ("dfs.namenode.rpc-address.ns.nn-1", "nn-1:9000"),
//!         ("dfs.namenode.rpc-address.ns.nn-2", "nn-2:9000"),
//!     ])
//!     .build()
//!     .unwrap();
//! ```
//!
//! ## Sync client
//!
//! The [`sync`] module provides blocking wrappers around the async client. This is useful for
//! applications that do not use Tokio directly.
//!
//! Create a sync client to a single NameNode
//! ```rust
//! use hdfs_native::sync::ClientBuilder;
//! let client = ClientBuilder::new().with_url("hdfs://localhost:9000").build().unwrap();
//! ```
//!
//! Use the sync client with standard IO traits
//! ```rust,no_run
//! use std::io::{Read, Write};
//! use hdfs_native::{WriteOptions, sync::ClientBuilder};
//!
//! let client = ClientBuilder::new().with_url("hdfs://localhost:9000").build().unwrap();
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
pub(crate) mod glob;
pub(crate) mod hdfs;
#[cfg(any(feature = "integration-test", feature = "benchmark"))]
pub mod minidfs;
pub(crate) mod proto;
pub(crate) mod security;
pub mod sync;

pub use client::WriteOptions;
pub use client::{Client, ClientBuilder};
pub use error::HdfsError;
pub use error::Result;

// Module for testing hooks into non-test code
#[cfg(feature = "integration-test")]
pub mod test;
