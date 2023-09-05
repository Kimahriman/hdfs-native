use std::borrow::Cow;
use std::sync::Arc;

use ::hdfs_native::file::{FileReader, FileWriter};
use ::hdfs_native::WriteOptions;
use ::hdfs_native::{client::FileStatus, Client};
use bytes::Bytes;
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

#[pyclass(get_all, set_all, name = "WriteOptions")]
#[derive(Clone)]
struct PyWriteOptions {
    block_size: Option<u64>,
    replication: Option<u32>,
    permission: u32,
    overwrite: bool,
    create_parent: bool,
}

impl From<PyWriteOptions> for WriteOptions {
    fn from(value: PyWriteOptions) -> Self {
        Self {
            block_size: value.block_size,
            replication: value.replication,
            permission: value.permission,
            overwrite: value.overwrite,
            create_parent: value.create_parent,
        }
    }
}

impl From<WriteOptions> for PyWriteOptions {
    fn from(value: WriteOptions) -> Self {
        Self {
            block_size: value.block_size,
            replication: value.replication,
            permission: value.permission,
            overwrite: value.overwrite,
            create_parent: value.create_parent,
        }
    }
}

#[pymethods]
impl PyWriteOptions {
    #[new]
    #[pyo3(signature = ())]
    pub fn new() -> Self {
        Self::from(WriteOptions::default())
    }
}

#[pyclass(name = "FileWriter")]
struct PyFileWriter {
    inner: FileWriter,
    rt: Arc<Runtime>,
}

#[pymethods]
impl PyFileWriter {
    pub fn write(&mut self, buf: Vec<u8>) -> PyHdfsResult<()> {
        Ok(self.rt.block_on(self.inner.write(Bytes::from(buf)))?)
    }

    pub fn close(&mut self) -> PyHdfsResult<()> {
        Ok(self.rt.block_on(self.inner.close())?)
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

    pub fn create(&self, src: &str, write_options: PyWriteOptions) -> PyHdfsResult<PyFileWriter> {
        let file_writer = self
            .rt
            .block_on(self.inner.create(src, WriteOptions::from(write_options)))?;

        Ok(PyFileWriter {
            inner: file_writer,
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
    m.add_class::<PyWriteOptions>()?;
    Ok(())
}
