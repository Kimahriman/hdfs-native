use hdfs_native::HdfsError;
use pyo3::{exceptions::*, PyErr};

pub struct PythonHdfsError(HdfsError);

impl From<HdfsError> for PythonHdfsError {
    fn from(value: HdfsError) -> Self {
        PythonHdfsError(value)
    }
}

impl From<PythonHdfsError> for PyErr {
    fn from(value: PythonHdfsError) -> Self {
        match value.0 {
            HdfsError::IOError(err) => PyIOError::new_err(err.to_string()),
            HdfsError::FileNotFound(path) => PyFileNotFoundError::new_err(path),
            HdfsError::BlocksNotFound(path) => {
                PyValueError::new_err(format!("Blocks not found for {}", path))
            }
            HdfsError::IsADirectoryError(path) => PyIsADirectoryError::new_err(path),
            HdfsError::UnsupportedFeature(feat) => PyNotImplementedError::new_err(feat),
            HdfsError::RPCError(class, msg) => {
                PyRuntimeError::new_err(format!("RPC error: {}\n{}", class, msg))
            }
            HdfsError::FatalRPCError(class, msg) => {
                PyRuntimeError::new_err(format!("Fatal RPC error: {}\n{}", class, msg))
            }
            HdfsError::SASLError(msg) => PyRuntimeError::new_err(format!("SASL error: {}", msg)),
            HdfsError::GSSAPIError(err) => {
                PyRuntimeError::new_err(format!("GSSAPI error: {}", err.to_string()))
            }
            _ => PyValueError::new_err(value.0.to_string()),
        }
    }
}
