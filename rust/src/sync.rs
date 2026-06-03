//! Synchronous wrappers around the asynchronous HDFS client.
//!
//! The sync client owns a Tokio runtime and delegates operations to the async
//! [`crate::Client`]. This is intended for applications that want blocking APIs
//! without managing an async runtime directly.

use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::runtime::Runtime;

use crate::acl::{AclEntry, AclStatus};
use crate::client::{self, ContentSummary, FileStatus, WriteOptions};
use crate::file::{FileReader as AsyncFileReader, FileWriter as AsyncFileWriter};
use crate::{Result, client::IORuntime};

/// Builds a new synchronous [`Client`] instance.
#[derive(Default)]
pub struct ClientBuilder {
    inner: client::ClientBuilder,
}

impl ClientBuilder {
    /// Create a new [`ClientBuilder`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the URL to connect to.
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.inner = self.inner.with_url(url);
        self
    }

    /// Set configs to use for the client.
    pub fn with_config(
        mut self,
        config: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.inner = self.inner.with_config(config);
        self
    }

    /// Set the configuration directory path to read from.
    pub fn with_config_dir(mut self, config_dir: impl Into<String>) -> Self {
        self.inner = self.inner.with_config_dir(config_dir);
        self
    }

    /// Create the synchronous [`Client`] from the provided settings.
    pub fn build(self) -> Result<Client> {
        let rt = Arc::new(Runtime::new()?);
        let inner = self
            .inner
            .with_io_runtime(IORuntime::from(rt.handle().clone()))
            .build()?;
        Ok(Client { inner, rt })
    }
}

/// A blocking HDFS client.
#[derive(Clone, Debug)]
pub struct Client {
    inner: client::Client,
    rt: Arc<Runtime>,
}

impl Client {
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }

    /// Retrieve the file status for the file at `path`.
    pub fn get_file_info(&self, path: &str) -> Result<FileStatus> {
        self.block_on(self.inner.get_file_info(path))
    }

    /// Retrieve all file statuses under `path`.
    pub fn list_status(&self, path: &str, recursive: bool) -> Result<Vec<FileStatus>> {
        self.block_on(self.inner.list_status(path, recursive))
    }

    /// Retrieve a blocking iterator of all files in directories located at `path`.
    pub fn list_status_iter(&self, path: &str, recursive: bool) -> ListStatusIterator {
        ListStatusIterator {
            inner: self.inner.list_status_iter(path, recursive),
            rt: Arc::clone(&self.rt),
        }
    }

    /// Opens a file reader for the file at `path`.
    pub fn read(&self, path: &str) -> Result<FileReader> {
        Ok(FileReader {
            inner: self.block_on(self.inner.read(path))?,
            rt: Arc::clone(&self.rt),
        })
    }

    /// Opens a new file for writing.
    pub fn create(&self, src: &str, write_options: impl AsRef<WriteOptions>) -> Result<FileWriter> {
        Ok(FileWriter {
            inner: self.block_on(self.inner.create(src, write_options))?,
            rt: Arc::clone(&self.rt),
        })
    }

    /// Opens an existing file for appending.
    pub fn append(&self, src: &str) -> Result<FileWriter> {
        Ok(FileWriter {
            inner: self.block_on(self.inner.append(src))?,
            rt: Arc::clone(&self.rt),
        })
    }

    /// Create a new directory at `path` with the given permission.
    pub fn mkdirs(&self, path: &str, permission: u32, create_parent: bool) -> Result<()> {
        self.block_on(self.inner.mkdirs(path, permission, create_parent))
    }

    /// Rename `src` to `dst`.
    pub fn rename(&self, src: &str, dst: &str, overwrite: bool) -> Result<()> {
        self.block_on(self.inner.rename(src, dst, overwrite))
    }

    /// Delete the file or directory at `path`.
    pub fn delete(&self, path: &str, recursive: bool) -> Result<bool> {
        self.block_on(self.inner.delete(path, recursive))
    }

    /// Move a file or directory at `path` into the user's trash.
    pub fn trash(&self, path: &str) -> Result<Option<String>> {
        self.block_on(self.inner.trash(path))
    }

    /// Set modified and access times for a file.
    pub fn set_times(&self, path: &str, mtime: u64, atime: u64) -> Result<()> {
        self.block_on(self.inner.set_times(path, mtime, atime))
    }

    /// Optionally set the owner and group for a file.
    pub fn set_owner(&self, path: &str, owner: Option<&str>, group: Option<&str>) -> Result<()> {
        self.block_on(self.inner.set_owner(path, owner, group))
    }

    /// Set permissions for a file.
    pub fn set_permission(&self, path: &str, permission: u32) -> Result<()> {
        self.block_on(self.inner.set_permission(path, permission))
    }

    /// Set replication for a file.
    pub fn set_replication(&self, path: &str, replication: u32) -> Result<bool> {
        self.block_on(self.inner.set_replication(path, replication))
    }

    /// Get a content summary for a file or directory rooted at `path`.
    pub fn get_content_summary(&self, path: &str) -> Result<ContentSummary> {
        self.block_on(self.inner.get_content_summary(path))
    }

    /// Update ACL entries for file or directory at `path`.
    pub fn modify_acl_entries(&self, path: &str, acl_spec: Vec<AclEntry>) -> Result<()> {
        self.block_on(self.inner.modify_acl_entries(path, acl_spec))
    }

    /// Remove specific ACL entries for file or directory at `path`.
    pub fn remove_acl_entries(&self, path: &str, acl_spec: Vec<AclEntry>) -> Result<()> {
        self.block_on(self.inner.remove_acl_entries(path, acl_spec))
    }

    /// Remove all default ACL entries for file or directory at `path`.
    pub fn remove_default_acl(&self, path: &str) -> Result<()> {
        self.block_on(self.inner.remove_default_acl(path))
    }

    /// Remove all ACL entries for file or directory at `path`.
    pub fn remove_acl(&self, path: &str) -> Result<()> {
        self.block_on(self.inner.remove_acl(path))
    }

    /// Override ACL entries for file or directory at `path`.
    pub fn set_acl(&self, path: &str, acl_spec: Vec<AclEntry>) -> Result<()> {
        self.block_on(self.inner.set_acl(path, acl_spec))
    }

    /// Get ACL status for the file or directory at `path`.
    pub fn get_acl_status(&self, path: &str) -> Result<AclStatus> {
        self.block_on(self.inner.get_acl_status(path))
    }

    /// Get all file statuses matching the glob `pattern`.
    pub fn glob_status(&self, pattern: &str) -> Result<Vec<FileStatus>> {
        self.block_on(self.inner.glob_status(pattern))
    }
}

impl Default for Client {
    fn default() -> Self {
        ClientBuilder::new()
            .build()
            .expect("Failed to create default client")
    }
}

/// A blocking file status iterator.
pub struct ListStatusIterator {
    inner: client::ListStatusIterator,
    rt: Arc<Runtime>,
}

impl Iterator for ListStatusIterator {
    type Item = Result<FileStatus>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rt.block_on(self.inner.next())
    }
}

/// A blocking file reader.
pub struct FileReader {
    inner: AsyncFileReader,
    rt: Arc<Runtime>,
}

impl FileReader {
    /// Returns the total size of the file.
    pub fn file_length(&self) -> usize {
        self.inner.file_length()
    }

    /// Returns the remaining bytes left based on the current cursor position.
    pub fn remaining(&self) -> usize {
        self.inner.remaining()
    }

    /// Sets the cursor position.
    pub fn seek(&mut self, pos: usize) {
        self.inner.seek(pos);
    }

    /// Returns the current cursor position in the file.
    pub fn tell(&self) -> usize {
        self.inner.tell()
    }

    /// Read up to `len` bytes, advancing the internal position.
    pub fn read(&mut self, len: usize) -> Result<Bytes> {
        self.rt.block_on(self.inner.read(len))
    }

    /// Read up to `buf.len()` bytes into the provided slice.
    pub fn read_buf(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.rt.block_on(self.inner.read_buf(buf))
    }

    /// Read up to `len` bytes starting at `offset`.
    pub fn read_range(&self, offset: usize, len: usize) -> Result<Bytes> {
        self.rt.block_on(self.inner.read_range(offset, len))
    }

    /// Read file data into an existing buffer.
    pub fn read_range_buf(&self, buf: &mut [u8], offset: usize) -> Result<()> {
        self.rt.block_on(self.inner.read_range_buf(buf, offset))
    }

    /// Return a blocking stream of `Bytes` objects containing the file content.
    pub fn read_range_stream(&self, offset: usize, len: usize) -> FileReadStream {
        FileReadStream {
            inner: Mutex::new(self.inner.read_range_stream(offset, len).boxed()),
            rt: Arc::clone(&self.rt),
        }
    }
}

/// A blocking stream of file bytes.
pub struct FileReadStream {
    inner: Mutex<BoxStream<'static, Result<Bytes>>>,
    rt: Arc<Runtime>,
}

impl Iterator for FileReadStream {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rt.block_on(self.inner.lock().unwrap().next())
    }
}

/// A blocking file writer.
pub struct FileWriter {
    inner: AsyncFileWriter,
    rt: Arc<Runtime>,
}

impl FileWriter {
    /// Write bytes to the file.
    pub fn write(&mut self, buf: Bytes) -> Result<usize> {
        self.rt.block_on(self.inner.write(buf))
    }

    /// Close the file writer.
    pub fn close(&mut self) -> Result<()> {
        self.rt.block_on(self.inner.close())
    }
}
