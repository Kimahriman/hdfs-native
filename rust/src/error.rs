use std::io;

#[cfg(feature = "kerberos")]
use libgssapi::error::Error as GssapiError;
use prost::DecodeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HdfsError {
    #[error("IO error occurred while communicating with HDFS")]
    IOError(#[from] io::Error),
    #[error("file not found")]
    FileNotFound(String),
    #[error("failed to decode RPC response")]
    InvalidRPCResponse(#[from] DecodeError),
    #[error("RPC error")]
    RPCError(String, String),
    #[error("fatal RPC error")]
    FatalRPCError(String, String),
    #[error("SASL error")]
    SASLError(String),
    #[error("GSSAPI error")]
    GSSAPI(#[from] GssapiError),
    #[error("No valid SASL mechanism found")]
    NoSASLMechanism,
}

pub type Result<T> = std::result::Result<T, HdfsError>;
