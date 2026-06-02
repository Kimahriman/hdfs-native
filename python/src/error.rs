use hdfs_native::HdfsError;
use pyo3::{PyErr, create_exception, exceptions::*};

create_exception!(_internal, TrashNotEnabledError, PyException);

pub struct PythonHdfsError(HdfsError);

impl From<HdfsError> for PythonHdfsError {
    fn from(value: HdfsError) -> Self {
        PythonHdfsError(value)
    }
}

impl From<PythonHdfsError> for PyErr {
    fn from(value: PythonHdfsError) -> Self {
        match value.0 {
            HdfsError::RPCError(class, message) if class == "java.io.FileNotFoundException" => {
                PyFileNotFoundError::new_err(message)
            }
            HdfsError::IOError(err) => PyIOError::new_err(err),
            HdfsError::AlreadyExists(path) => PyFileExistsError::new_err(path),
            HdfsError::FileNotFound(path) => PyFileNotFoundError::new_err(path),
            HdfsError::IsADirectoryError(path) => PyIsADirectoryError::new_err(path),
            HdfsError::TrashNotEnabled => TrashNotEnabledError::new_err("Trash is not enabled"),
            HdfsError::UnsupportedFeature(feat) => PyNotImplementedError::new_err(feat),
            _ => PyRuntimeError::new_err(format!("{:?}", value.0)),
        }
    }
}
