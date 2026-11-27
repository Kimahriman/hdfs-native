use std::io;

use prost::DecodeError;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum YarnError {
    #[error(transparent)]
    HadoopError(hadoop_native::HadoopError),
    #[error("IO error occurred while communicating with YARN: {0}")]
    IOError(#[from] io::Error),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("operation failed: {0}")]
    OperationFailed(String),
    #[error("application not found: {0}")]
    ApplicationNotFound(String),
    #[error("application master not registered: {0}")]
    ApplicationMasterNotRegistered(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("container {0} failed: {1} {2}")]
    ContainerError(String, String, String),
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
    #[error("XML parse error: {0}")]
    XmlParseError(#[from] roxmltree::Error),
}

pub type Result<T> = std::result::Result<T, YarnError>;

impl From<hadoop_native::HadoopError> for YarnError {
    fn from(error: hadoop_native::HadoopError) -> Self {
        use hadoop_native::HadoopError;

        match error {
            HadoopError::IOError(error) => Self::IOError(error),
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
            HadoopError::XmlParseError(error) => Self::XmlParseError(error),
            error => Self::HadoopError(error),
        }
    }
}
