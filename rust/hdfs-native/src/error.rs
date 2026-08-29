use std::io;

use prost::DecodeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HdfsError {
    #[error(transparent)]
    HadoopError(hadoop_native::HadoopError),
    #[error("IO error occurred while communicating with HDFS: {0}")]
    IOError(#[from] io::Error),
    #[error("data transfer error: {0}")]
    DataTransferError(String),
    #[error("checksums didn't match")]
    ChecksumError,
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("failed to parse URL: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("file already exists: {0}")]
    AlreadyExists(String),
    #[error("operation failed: {0}")]
    OperationFailed(String),
    #[error("file not found: {0}")]
    FileNotFound(String),
    #[error("blocks not found for {0}")]
    BlocksNotFound(String),
    #[error("path is a directory: {0}")]
    IsADirectoryError(String),
    #[error("Trash is not enabled")]
    TrashNotEnabled,
    #[error("unsupported erasure coding policy {0}")]
    UnsupportedErasureCodingPolicy(String),
    #[error("erasure coding error: {0}")]
    ErasureCodingError(String),
    #[error("operation not supported: {0}")]
    UnsupportedFeature(String),
    #[error("interal error, this shouldn't happen: {0}")]
    InternalError(String),
    #[error("failed to decode RPC response: {0}")]
    InvalidRPCResponse(#[from] DecodeError),
    #[error("RPC error: {0} {1}")]
    RPCError(String, String),
    #[error("fatal RPC error: {0} {1}")]
    FatalRPCError(String, String),
    #[error("SASL error: {0}")]
    SASLError(String),
    #[error("GSSAPI error: {0:?} {1} {2}")]
    GSSAPIError(hadoop_native::security::gssapi::GssMajorCodes, u32, String),
    #[error("No valid SASL mechanism found")]
    NoSASLMechanism,
}

pub type Result<T> = std::result::Result<T, HdfsError>;

impl From<hadoop_native::HadoopError> for HdfsError {
    fn from(error: hadoop_native::HadoopError) -> Self {
        use hadoop_native::HadoopError;

        match error {
            HadoopError::IOError(error) => Self::IOError(error),
            HadoopError::InvalidPath(path) => Self::InvalidPath(path),
            HadoopError::InvalidArgument(message) => Self::InvalidArgument(message),
            HadoopError::InvalidRPCResponse(error) => Self::InvalidRPCResponse(error),
            HadoopError::RPCError(class, message) => Self::RPCError(class, message),
            HadoopError::FatalRPCError(class, message) => Self::FatalRPCError(class, message),
            HadoopError::SASLError(message) => Self::SASLError(message),
            HadoopError::GSSAPIError(major, minor, message) => {
                Self::GSSAPIError(major, minor, message)
            }
            HadoopError::NoSASLMechanism => Self::NoSASLMechanism,
            HadoopError::OperationFailed(message) => Self::OperationFailed(message),
            other => Self::HadoopError(other),
        }
    }
}
