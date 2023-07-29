use crate::error::PythonError;
use ::hdfs_native::{client::FileStatus, Client};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use tokio::runtime::Runtime;

mod error;

#[pyclass]
struct RawClient {
    inner: Client,
    rt: Runtime,
}

#[pyclass]
struct PyFileStatus {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    length: usize,
    #[pyo3(get)]
    isdir: bool,
    #[pyo3(get)]
    permission: u16,
    #[pyo3(get)]
    owner: String,
    #[pyo3(get)]
    group: String,
    #[pyo3(get)]
    modification_time: u64,
    #[pyo3(get)]
    access_time: u64,
}

impl From<FileStatus> for PyFileStatus {
    fn from(value: FileStatus) -> Self {
        Self {
            path: value.path,
            length: value.length,
            isdir: value.isdir,
            permission: value.permission,
            owner: value.owner,
            group: value.group,
            modification_time: value.modification_time,
            access_time: value.access_time,
        }
    }
}

#[pymethods]
impl RawClient {
    #[new]
    #[pyo3(signature = (url))]
    pub fn new(url: &str) -> PyResult<Self> {
        Ok(RawClient {
            inner: Client::new(url).map_err(PythonError::from)?,
            rt: tokio::runtime::Runtime::new()
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))?,
        })
    }

    pub fn list_status(&self, path: &str, recursive: bool) -> PyResult<Vec<PyFileStatus>> {
        Ok(self
            .rt
            .block_on(self.inner.list_status(path, recursive))
            .map_err(PythonError::from)?
            .into_iter()
            .map(PyFileStatus::from)
            .collect())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn hdfs_native(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RawClient>()?;
    Ok(())
}
