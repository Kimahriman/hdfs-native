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
            HdfsError::IOError(err) => PyIOError::new_err(err),
            HdfsError::FileNotFound(path) => PyFileNotFoundError::new_err(path),
            HdfsError::IsADirectoryError(path) => PyIsADirectoryError::new_err(path),
            HdfsError::UnsupportedFeature(feat) => PyNotImplementedError::new_err(feat),
            _ => PyRuntimeError::new_err(format!("{:?}", value.0)),
        }
    }
}
