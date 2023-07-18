use crate::error::PythonError;
use ::hdfs_native::{client::FileStatus, Client};
use pyo3::{exceptions::PyRuntimeError, prelude::*};

mod error;

#[inline]
fn rt() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

#[pyclass]
struct RawClient {
    inner: Client,
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
        })
    }

    pub fn list_status(&self, path: &str) -> PyResult<Vec<PyFileStatus>> {
        Ok(rt()?
            .block_on(self.inner.list_status(path))
            .map_err(PythonError::from)?
            .into_iter()
            .map(PyFileStatus::from)
            .collect())
    }
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn hdfs_native(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<RawClient>()?;
    Ok(())
}
