use hdfs_native::HdfsError;
use pyo3::{exceptions::PyValueError, PyErr};

pub struct PythonHdfsError(HdfsError);

impl From<HdfsError> for PythonHdfsError {
    fn from(value: HdfsError) -> Self {
        PythonHdfsError(value)
    }
}

impl From<PythonHdfsError> for PyErr {
    fn from(value: PythonHdfsError) -> Self {
        match value.0 {
            _ => PyValueError::new_err("Unknown HDFS error"),
        }
    }
}
