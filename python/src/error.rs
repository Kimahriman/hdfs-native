use pyo3::{create_exception, exceptions::PyException, PyErr};

create_exception!(hdfs_native, HdfsError, PyException);

#[derive(thiserror::Error, Debug)]
pub enum PythonError {
    #[error("Error in HDFS client")]
    HdfsError(#[from] hdfs_native::HdfsError),
}

impl From<PythonError> for PyErr {
    fn from(value: PythonError) -> Self {
        match value {
            PythonError::HdfsError(error) => HdfsError::new_err(error.to_string()),
        }
    }
}
