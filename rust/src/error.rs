use std::io;

use prost::DecodeError;
use rsasl::prelude::{SASLError, SessionError};
#[cfg(feature = "rsasl2")]
use rsasl2::prelude::SASLError;
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
    RPCError(String),
    #[error("fatal RPC error")]
    FatalRPCError(String),
    #[cfg(feature = "kerberos")]
    #[error("SASL error")]
    RSASLError(#[from] SASLError),
    #[error("SASL session error")]
    RSASLSessionError(#[from] SessionError),
    #[error("SASL error")]
    SASLError(String),
    #[error("No valid SASL mechanism found")]
    NoSASLMechanism,
}

pub type Result<T> = std::result::Result<T, HdfsError>;