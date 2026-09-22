//! Shared native Hadoop client foundations.
//!
//! This crate contains protocol and configuration functionality that is common
//! to Hadoop clients. Service-specific clients, such as HDFS and YARN, build on
//! top of these primitives.

pub mod config;
pub mod proto;
pub mod rpc;
pub mod security;

mod error;

pub use error::{HadoopError, Result};
