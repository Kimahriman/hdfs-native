//! Native Hadoop YARN client foundations.
//!
//! This crate starts with YARN configuration, ResourceManager RPC proxying,
//! and generated protobuf types. Higher-level client operations can build on
//! these pieces.

pub mod config;
#[cfg(feature = "integration-test")]
pub mod miniyarn;
pub mod proto;
pub mod proxy;

pub use hadoop_native::HadoopError as YarnError;
pub type Result<T> = std::result::Result<T, YarnError>;
