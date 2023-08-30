use std::borrow::Cow;
use std::sync::Arc;

use ::hdfs_native::file::FileReader;
use ::hdfs_native::{client::FileStatus, Client};
use log::LevelFilter;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use tokio::runtime::Runtime;

mod error;

use crate::error::PythonHdfsError;

type PyHdfsResult<T> = Result<T, PythonHdfsError>;

#[pyclass(get_all, frozen, name = "FileStatus")]
struct PyFileStatus {
    path: String,
    length: usize,
    isdir: bool,
    permission: u16,
    owner: String,
    group: String,
    modification_time: u64,
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

#[pyclass(name = "FileReader")]
struct PyFileReader {
    inner: FileReader,
    rt: Arc<Runtime>,
}

#[pymethods]
impl PyFileReader {
    pub fn file_length(&self) -> usize {
        self.inner.file_length()
    }

    pub fn read(&mut self, len: usize) -> PyHdfsResult<Cow<[u8]>> {
        Ok(Cow::from(self.rt.block_on(self.inner.read(len))?.to_vec()))
    }

    pub fn read_range(&self, offset: usize, len: usize) -> PyHdfsResult<Cow<[u8]>> {
        Ok(Cow::from(
            self.rt
                .block_on(self.inner.read_range(offset, len))?
                .to_vec(),
        ))
    }
}

#[pyclass(name = "Client")]
struct PyClient {
    inner: Client,
    rt: Arc<Runtime>,
}

#[pymethods]
impl PyClient {
    #[new]
    #[pyo3(signature = (url))]
    pub fn new(url: &str) -> PyResult<Self> {
        // Initialize logging, ignore errors if this is called multiple times
        let _ = env_logger::Builder::new()
            .filter_level(LevelFilter::Off)
            .try_init();

        Ok(PyClient {
            inner: Client::new(url).map_err(PythonHdfsError::from)?,
            rt: Arc::new(
                tokio::runtime::Runtime::new()
                    .map_err(|err| PyRuntimeError::new_err(err.to_string()))?,
            ),
        })
    }

    pub fn get_file_info(&self, path: &str) -> PyHdfsResult<PyFileStatus> {
        Ok(self
            .rt
            .block_on(self.inner.get_file_info(path))
            .map(PyFileStatus::from)?)
    }

    pub fn list_status(&self, path: &str, recursive: bool) -> PyHdfsResult<Vec<PyFileStatus>> {
        Ok(self
            .rt
            .block_on(self.inner.list_status(path, recursive))?
            .into_iter()
            .map(PyFileStatus::from)
            .collect())
    }

    pub fn read(&self, path: &str) -> PyHdfsResult<PyFileReader> {
        let file_reader = self.rt.block_on(self.inner.read(path))?;

        Ok(PyFileReader {
            inner: file_reader,
            rt: Arc::clone(&self.rt),
        })
    }

    pub fn mkdirs(&self, path: &str, permission: u32, create_parent: bool) -> PyHdfsResult<()> {
        Ok(self
            .rt
            .block_on(self.inner.mkdirs(path, permission, create_parent))?)
    }

    pub fn rename(&self, src: &str, dst: &str, overwrite: bool) -> PyHdfsResult<()> {
        Ok(self.rt.block_on(self.inner.rename(src, dst, overwrite))?)
    }

    pub fn delete(&self, path: &str, recursive: bool) -> PyHdfsResult<bool> {
        Ok(self.rt.block_on(self.inner.delete(path, recursive))?)
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn hdfs_native(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyClient>()?;
    Ok(())
}
