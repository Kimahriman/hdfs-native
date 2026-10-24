use std::io;

use prost::DecodeError;
use thiserror::Error;

/// Errors produced by shared Hadoop client functionality.
#[derive(Error, Debug)]
pub enum HadoopError {
    #[error("IO error occurred while communicating with Hadoop: {0}")]
    IOError(#[from] io::Error),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("failed to decode RPC response: {0}")]
    InvalidRPCResponse(#[from] DecodeError),
    #[error("RPC error: {0} {1}")]
    RPCError(String, String),
    #[error("fatal RPC error: {0} {1}")]
    FatalRPCError(String, String),
    #[error("SASL error: {0}")]
    SASLError(String),
    #[error("GSSAPI error: {0:?} {1} {2}")]
    GSSAPIError(crate::security::gssapi::GssMajorCodes, u32, String),
    #[error("No valid SASL mechanism found")]
    NoSASLMechanism,
    #[error("operation failed: {0}")]
    OperationFailed(String),
    #[error("XML parse error: {0}")]
    XmlParseError(#[from] roxmltree::Error),
}

pub type Result<T> = std::result::Result<T, HadoopError>;
