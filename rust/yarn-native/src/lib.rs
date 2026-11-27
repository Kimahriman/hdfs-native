//! Native Apache Hadoop YARN client implementation in Rust
//!
//! The crate has a client for each YARN protocol:
//! - [`YarnClient`] talks to the ResourceManager to submit, monitor and kill applications.
//! - [`AmRmClient`] is used by an application's ApplicationMaster to register with the
//!   ResourceManager, request containers and unregister.
//! - [`NmClient`] is used by an ApplicationMaster to start, stop and monitor its containers on
//!   NodeManagers.
//!
//! # Usage
//!
//! Create a client to the ResourceManager in the default configs
//! ```rust
//! use yarn_native::YarnClientBuilder;
//! let client = YarnClientBuilder::new().build().unwrap();
//! ```
//!
//! Create a client for ResourceManager HA
//! ```rust
//! use yarn_native::YarnClientBuilder;
//! let client = YarnClientBuilder::new()
//!     .with_config(vec![
//!         ("yarn.resourcemanager.ha.enabled", "true"),
//!         ("yarn.resourcemanager.ha.rm-ids", "rm1,rm2"),
//!         ("yarn.resourcemanager.hostname.rm1", "rm-1"),
//!         ("yarn.resourcemanager.hostname.rm2", "rm-2"),
//!     ])
//!     .build()
//!     .unwrap();
//! ```
//!
//! Inside an ApplicationMaster container, read the AMRMToken from the container's token file
//! and share NMTokens with the NodeManager client
//! ```rust,no_run
//! use yarn_native::{AmRmClientBuilder, NmClientBuilder};
//! let am_client = AmRmClientBuilder::new().build().unwrap();
//! let nm_client = NmClientBuilder::new()
//!     .with_nm_token_cache(am_client.nm_token_cache())
//!     .build()
//!     .unwrap();
//! ```
pub mod am;
pub mod client;
pub(crate) mod config;
pub(crate) mod error;
pub mod ids;
#[cfg(feature = "integration-test")]
pub mod miniyarn;
pub mod nm;
pub(crate) mod nodemanager;
pub(crate) mod proto;
pub(crate) mod proxy;
pub mod records;
pub(crate) mod resourcemanager;

pub use am::{AmRmClient, AmRmClientBuilder, NmTokenCache};
pub use client::{YarnClient, YarnClientBuilder};
pub use error::Result;
pub use error::YarnError;
pub use hadoop_native::security::user::{Credentials, SerializedFormat, Token};
pub use nm::{NmClient, NmClientBuilder};
