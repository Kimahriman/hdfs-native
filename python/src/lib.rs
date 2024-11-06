use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use ::hdfs_native::file::{FileReader, FileWriter};
use ::hdfs_native::WriteOptions;
use ::hdfs_native::{
    client::{FileStatus, ListStatusIterator},
    Client,
};
use bytes::Bytes;
use hdfs_native::client::ContentSummary;
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
    replication: Option<u32>,
    blocksize: Option<u64>,
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
            replication: value.replication,
            blocksize: value.blocksize,
        }
    }
}

#[pymethods]
impl PyFileStatus {
    /// Return a dataclass-esque format for the repr
    fn __repr__(&self) -> String {
        format!("FileStatus(path='{}', length={}, isdir={}, permission={}, owner={}, group={}, modification_time={}, access_time={}, replication={}, blocksize={})",
        self.path,
        self.length,
        self.isdir,
        self.permission,
        self.owner,
        self.group,
        self.modification_time,
        self.access_time,
        self.replication.map(|r| r.to_string()).unwrap_or("None".to_string()),
        self.blocksize.map(|r| r.to_string()).unwrap_or("None".to_string())
    )
    }
}

#[pyclass(name = "FileStatusIter")]
struct PyFileStatusIter {
    inner: Arc<ListStatusIterator>,
    rt: Arc<Runtime>,
}

#[pymethods]
impl PyFileStatusIter {
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRefMut<'_, Self>) -> PyHdfsResult<Option<PyFileStatus>> {
        // Kinda dumb, but lets us release the GIL while getting the next value
        let inner = Arc::clone(&slf.inner);
        let rt = Arc::clone(&slf.rt);
        if let Some(result) = slf.py().allow_threads(|| rt.block_on(inner.next())) {
            Ok(Some(PyFileStatus::from(result?)))
        } else {
            Ok(None)
        }
    }
}

#[pyclass(get_all, frozen, name = "ContentSummary")]
struct PyContentSummary {
    length: u64,
    file_count: u64,
    directory_count: u64,
    quota: u64,
    space_consumed: u64,
    space_quota: u64,
}

impl From<ContentSummary> for PyContentSummary {
    fn from(value: ContentSummary) -> Self {
        Self {
            length: value.length,
            file_count: value.file_count,
            directory_count: value.directory_count,
            quota: value.quota,
            space_consumed: value.space_consumed,
            space_quota: value.space_quota,
        }
    }
}

#[pymethods]
impl PyContentSummary {
    /// Return a dataclass-esque format for the repr
    fn __repr__(&self) -> String {
        format!("ContentSummary(length={}, file_count={}, directory_count={}, quota={}, space_consumed={}, space_quota={})",
            self.length,
            self.file_count,
            self.directory_count,
            self.quota,
            self.space_consumed,
            self.space_quota,
        )
    }
}

#[pyclass]
struct RawFileReader {
    inner: FileReader,
    rt: Arc<Runtime>,
}

#[pymethods]
impl RawFileReader {
    pub fn file_length(&self) -> usize {
        self.inner.file_length()
    }

    pub fn seek(&mut self, pos: usize) {
        self.inner.seek(pos);
    }

    pub fn tell(&self) -> usize {
        self.inner.tell()
    }

    pub fn read(&mut self, len: i64, py: Python) -> PyHdfsResult<Cow<[u8]>> {
        let read_len = if len < 0 {
            self.inner.remaining()
        } else {
            len as usize
        };
        Ok(Cow::from(
            py.allow_threads(|| self.rt.block_on(self.inner.read(read_len)))?
                .to_vec(),
        ))
    }

    pub fn read_range(&self, offset: usize, len: usize, py: Python) -> PyHdfsResult<Cow<[u8]>> {
        Ok(Cow::from(
            py.allow_threads(|| self.rt.block_on(self.inner.read_range(offset, len)))?
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
    pub fn new(
        block_size: Option<u64>,
        replication: Option<u32>,
        permission: Option<u32>,
        overwrite: Option<bool>,
        create_parent: Option<bool>,
    ) -> Self {
        let mut write_options = WriteOptions::default();
        if let Some(block_size) = block_size {
            write_options = write_options.block_size(block_size);
        }
        if let Some(replication) = replication {
            write_options = write_options.replication(replication);
        }
        if let Some(permission) = permission {
            write_options = write_options.permission(permission);
        }
        if let Some(overwrite) = overwrite {
            write_options = write_options.overwrite(overwrite);
        }
        if let Some(create_parent) = create_parent {
            write_options = write_options.create_parent(create_parent);
        }

        PyWriteOptions::from(write_options)
    }

    /// Return a dataclass-esque format for the repr
    fn __repr__(&self) -> String {
        format!("WriteOptions(block_size={}, replication={}, permission={}, overwrite={}, create_parent={})",
            self.block_size.map(|x| x.to_string()).unwrap_or("None".to_string()),
            self.replication.map(|x| x.to_string()).unwrap_or("None".to_string()),
            self.permission,
            self.overwrite,
            self.create_parent
        )
    }
}

#[pyclass]
struct RawFileWriter {
    inner: FileWriter,
    rt: Arc<Runtime>,
}

#[pymethods]
impl RawFileWriter {
    pub fn write(&mut self, buf: Vec<u8>, py: Python) -> PyHdfsResult<usize> {
        Ok(py.allow_threads(|| self.rt.block_on(self.inner.write(Bytes::from(buf))))?)
    }

    pub fn close(&mut self, py: Python) -> PyHdfsResult<()> {
        Ok(py.allow_threads(|| self.rt.block_on(self.inner.close()))?)
    }
}

#[pyclass(name = "RawClient", subclass)]
struct RawClient {
    inner: Client,
    rt: Arc<Runtime>,
}

#[pymethods]
impl RawClient {
    #[new]
    #[pyo3(signature = (url, config))]
    pub fn new(url: Option<&str>, config: Option<HashMap<String, String>>) -> PyResult<Self> {
        // Initialize logging, ignore errors if this is called multiple times
        let _ = env_logger::try_init();

        let config = config.unwrap_or_default();

        let inner = if let Some(url) = url {
            Client::new_with_config(url, config).map_err(PythonHdfsError::from)?
        } else {
            Client::default_with_config(config).map_err(PythonHdfsError::from)?
        };

        Ok(RawClient {
            inner,
            rt: Arc::new(
                tokio::runtime::Runtime::new()
                    .map_err(|err| PyRuntimeError::new_err(err.to_string()))?,
            ),
        })
    }

    pub fn get_file_info(&self, path: &str, py: Python) -> PyHdfsResult<PyFileStatus> {
        Ok(py.allow_threads(|| {
            self.rt
                .block_on(self.inner.get_file_info(path))
                .map(PyFileStatus::from)
        })?)
    }

    pub fn list_status(&self, path: &str, recursive: bool) -> PyFileStatusIter {
        let inner = self.inner.list_status_iter(path, recursive);
        PyFileStatusIter {
            inner: Arc::new(inner),
            rt: Arc::clone(&self.rt),
        }
    }

    pub fn read(&self, path: &str, py: Python) -> PyHdfsResult<RawFileReader> {
        let file_reader = py.allow_threads(|| self.rt.block_on(self.inner.read(path)))?;

        Ok(RawFileReader {
            inner: file_reader,
            rt: Arc::clone(&self.rt),
        })
    }

    pub fn create(
        &self,
        src: &str,
        write_options: PyWriteOptions,
        py: Python,
    ) -> PyHdfsResult<RawFileWriter> {
        let file_writer = py.allow_threads(|| {
            self.rt
                .block_on(self.inner.create(src, WriteOptions::from(write_options)))
        })?;

        Ok(RawFileWriter {
            inner: file_writer,
            rt: Arc::clone(&self.rt),
        })
    }

    pub fn append(&self, src: &str, py: Python) -> PyHdfsResult<RawFileWriter> {
        let file_writer = py.allow_threads(|| self.rt.block_on(self.inner.append(src)))?;

        Ok(RawFileWriter {
            inner: file_writer,
            rt: Arc::clone(&self.rt),
        })
    }

    pub fn mkdirs(
        &self,
        path: &str,
        permission: u32,
        create_parent: bool,
        py: Python,
    ) -> PyHdfsResult<()> {
        Ok(py.allow_threads(|| {
            self.rt
                .block_on(self.inner.mkdirs(path, permission, create_parent))
        })?)
    }

    pub fn rename(&self, src: &str, dst: &str, overwrite: bool, py: Python) -> PyHdfsResult<()> {
        Ok(py.allow_threads(|| self.rt.block_on(self.inner.rename(src, dst, overwrite)))?)
    }

    pub fn delete(&self, path: &str, recursive: bool, py: Python) -> PyHdfsResult<bool> {
        Ok(py.allow_threads(|| self.rt.block_on(self.inner.delete(path, recursive)))?)
    }

    pub fn set_times(&self, path: &str, mtime: u64, atime: u64, py: Python) -> PyHdfsResult<()> {
        Ok(py.allow_threads(|| self.rt.block_on(self.inner.set_times(path, mtime, atime)))?)
    }

    pub fn set_owner(
        &self,
        path: &str,
        owner: Option<&str>,
        group: Option<&str>,
        py: Python,
    ) -> PyHdfsResult<()> {
        Ok(py.allow_threads(|| self.rt.block_on(self.inner.set_owner(path, owner, group)))?)
    }

    pub fn set_permission(&self, path: &str, permission: u32, py: Python) -> PyHdfsResult<()> {
        Ok(py.allow_threads(|| {
            self.rt
                .block_on(self.inner.set_permission(path, permission))
        })?)
    }

    pub fn set_replication(&self, path: &str, replication: u32, py: Python) -> PyHdfsResult<bool> {
        Ok(py.allow_threads(|| {
            self.rt
                .block_on(self.inner.set_replication(path, replication))
        })?)
    }

    pub fn get_content_summary(&self, path: &str, py: Python) -> PyHdfsResult<PyContentSummary> {
        Ok(py
            .allow_threads(|| self.rt.block_on(self.inner.get_content_summary(path)))?
            .into())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RawClient>()?;
    m.add_class::<PyWriteOptions>()?;
    Ok(())
}
