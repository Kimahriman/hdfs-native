// #![warn(missing_docs)]
//! Object store implementation for the Native Rust HDFS client
//!
//! # Usage
//!
//! ```rust
//! use hdfs_native::Client;
//! use hdfs_native_object_store::HdfsObjectStore;
//! # use hdfs_native::Result;
//! # fn main() -> Result<()> {
//! let client = Client::new("hdfs://localhost:9000")?;
//! let store = HdfsObjectStore::new(client);
//! # Ok(())
//! # }
//! ```
use std::{
    fmt::{Display, Formatter},
    future,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{NaiveDateTime, TimeZone, Utc};
use futures::stream::{BoxStream, StreamExt};
use hdfs_native::{client::FileStatus, file::FileWriter, Client, HdfsError, WriteOptions};
use object_store::{
    multipart::{PartId, PutPart, WriteMultiPart},
    path::Path,
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta, ObjectStore,
    PutMode, PutOptions, PutResult, Result,
};
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct HdfsObjectStore {
    client: Arc<Client>,
}

impl HdfsObjectStore {
    pub fn new(client: Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }

    async fn internal_copy(&self, from: &Path, to: &Path, overwrite: bool) -> Result<()> {
        let overwrite = match self.client.get_file_info(&make_absolute_file(to)).await {
            Ok(_) if overwrite => true,
            Ok(_) => Err(HdfsError::AlreadyExists(make_absolute_file(to))).to_object_store_err()?,
            Err(HdfsError::FileNotFound(_)) => false,
            Err(e) => Err(e).to_object_store_err()?,
        };

        let write_options = WriteOptions {
            overwrite,
            ..Default::default()
        };

        let file = self
            .client
            .read(&make_absolute_file(from))
            .await
            .to_object_store_err()?;
        let mut stream = file.read_range_stream(0, file.file_length()).boxed();

        let mut new_file = self
            .client
            .create(&make_absolute_file(to), write_options)
            .await
            .to_object_store_err()?;

        while let Some(bytes) = stream.next().await.transpose().to_object_store_err()? {
            new_file.write(bytes).await.to_object_store_err()?;
        }
        new_file.close().await.to_object_store_err()?;

        Ok(())
    }
}

impl Display for HdfsObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HdfsObjectStore")
    }
}

impl From<Client> for HdfsObjectStore {
    fn from(value: Client) -> Self {
        Self::new(value)
    }
}

#[async_trait]
impl ObjectStore for HdfsObjectStore {
    /// Save the provided bytes to the specified location
    ///
    /// To make the operation atomic, we write to a temporary file ".{filename}.tmp" and rename
    /// on a successful write.
    async fn put_opts(&self, location: &Path, bytes: Bytes, opts: PutOptions) -> Result<PutResult> {
        let overwrite = match opts.mode {
            PutMode::Create => false,
            PutMode::Overwrite => true,
            PutMode::Update(_) => {
                return Err(object_store::Error::NotSupported {
                    source: "Update mode not supported".to_string().into(),
                })
            }
        };

        let final_file_path = make_absolute_file(location);
        let path_buf = PathBuf::from(&final_file_path);

        let file_name = path_buf
            .file_name()
            .ok_or(HdfsError::InvalidPath("path missing filename".to_string()))
            .to_object_store_err()?
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))
            .to_object_store_err()?
            .to_string();

        let tmp_filename = path_buf
            .with_file_name(format!(".{}.tmp", file_name))
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))
            .to_object_store_err()?
            .to_string();

        let write_options = WriteOptions {
            overwrite,
            ..Default::default()
        };

        let mut writer = self
            .client
            .create(&tmp_filename, write_options)
            .await
            .to_object_store_err()?;
        writer.write(bytes).await.to_object_store_err()?;
        writer.close().await.to_object_store_err()?;

        self.client
            .rename(&tmp_filename, &final_file_path, overwrite)
            .await
            .to_object_store_err()?;

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    /// Uses the [PutPart] trait to implement an asynchronous writer. We can't actually upload
    /// multiple parts at once, so we simply set a limit of one part at a time.
    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let final_file_path = make_absolute_file(location);
        let path_buf = PathBuf::from(&final_file_path);

        let file_name = path_buf
            .file_name()
            .ok_or(HdfsError::InvalidPath("path missing filename".to_string()))
            .to_object_store_err()?
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))
            .to_object_store_err()?
            .to_string();

        let tmp_filename = path_buf
            .with_file_name(format!(".{}.tmp", file_name))
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))
            .to_object_store_err()?
            .to_string();

        // First we need to check if the tmp file exists so we know whether to overwrite
        let overwrite = match self.client.get_file_info(&tmp_filename).await {
            Ok(_) => true,
            Err(HdfsError::FileNotFound(_)) => false,
            Err(e) => Err(e).to_object_store_err()?,
        };

        let write_options = WriteOptions {
            overwrite,
            ..Default::default()
        };

        let writer = self
            .client
            .create(&tmp_filename, write_options)
            .await
            .to_object_store_err()?;

        Ok((
            tmp_filename.clone(),
            Box::new(WriteMultiPart::new(
                HdfsMultipartWriter::new(
                    Arc::clone(&self.client),
                    writer,
                    &tmp_filename,
                    &final_file_path,
                ),
                1,
            )),
        ))
    }

    /// Attempts to delete the temporary file used for multipart uploads.
    async fn abort_multipart(&self, _location: &Path, multipart_id: &MultipartId) -> Result<()> {
        // The multipart_id is the resolved temporary file name, so we can just delete it
        self.client
            .delete(multipart_id, false)
            .await
            .to_object_store_err()?;
        Ok(())
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
        {
            return Err(object_store::Error::NotImplemented);
        }

        let meta = self.head(location).await?;

        let range = options.range.unwrap_or(0..meta.size);

        let reader = self
            .client
            .read(&make_absolute_file(location))
            .await
            .to_object_store_err()?;
        let stream = reader
            .read_range_stream(range.start, range.end - range.start)
            .map(|b| b.to_object_store_err())
            .boxed();

        let payload = GetResultPayload::Stream(stream);

        Ok(GetResult {
            payload,
            meta,
            range,
        })
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let status = self
            .client
            .get_file_info(&make_absolute_file(location))
            .await
            .to_object_store_err()?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc.from_utc_datetime(
                &NaiveDateTime::from_timestamp_opt(status.modification_time as i64, 0).unwrap(),
            ),
            size: status.length,
            e_tag: None,
            version: None,
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        let result = self
            .client
            .delete(&make_absolute_file(location), false)
            .await
            .to_object_store_err()?;

        if !result {
            Err(HdfsError::OperationFailed(
                "failed to delete object".to_string(),
            ))
            .to_object_store_err()?
        }

        Ok(())
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    ///
    /// Note: the order of returned [`ObjectMeta`] is not guaranteed
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        let status_stream = self
            .client
            .list_status_iter(
                &prefix.map(make_absolute_dir).unwrap_or("".to_string()),
                true,
            )
            .into_stream()
            .filter(|res| {
                let result = match res {
                    Ok(status) => !status.isdir,
                    // Listing by prefix should just return an empty list if the prefix isn't found
                    Err(HdfsError::FileNotFound(_)) => false,
                    _ => true,
                };
                future::ready(result)
            })
            .map(|res| res.map_or_else(|e| Err(e).to_object_store_err(), |s| get_object_meta(&s)));

        Box::pin(status_stream)
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut status_stream = self
            .client
            .list_status_iter(
                &prefix.map(make_absolute_dir).unwrap_or("".to_string()),
                false,
            )
            .into_stream()
            .filter(|res| {
                let result = match res {
                    // Listing by prefix should just return an empty list if the prefix isn't found
                    Err(HdfsError::FileNotFound(_)) => false,
                    _ => true,
                };
                future::ready(result)
            });

        let mut statuses = Vec::<FileStatus>::new();
        while let Some(status) = status_stream.next().await {
            statuses.push(status.to_object_store_err()?);
        }

        let mut dirs: Vec<Path> = Vec::new();
        for status in statuses.iter().filter(|s| s.isdir) {
            dirs.push(Path::parse(&status.path)?)
        }

        let mut files: Vec<ObjectMeta> = Vec::new();
        for status in statuses.iter().filter(|s| !s.isdir) {
            files.push(get_object_meta(status)?)
        }

        Ok(ListResult {
            common_prefixes: dirs,
            objects: files,
        })
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(self
            .client
            .rename(&make_absolute_file(from), &make_absolute_file(to), true)
            .await
            .to_object_store_err()?)
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(self
            .client
            .rename(&make_absolute_file(from), &make_absolute_file(to), false)
            .await
            .to_object_store_err()?)
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.internal_copy(from, to, true).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    ///
    /// Performs an atomic operation if the underlying object storage supports it.
    /// If atomic operations are not supported by the underlying object storage (like S3)
    /// it will return an error.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.internal_copy(from, to, false).await
    }
}

#[cfg(feature = "integration-test")]
pub trait HdfsErrorConvert<T> {
    fn to_object_store_err(self) -> Result<T>;
}

#[cfg(not(feature = "integration-test"))]
trait HdfsErrorConvert<T> {
    fn to_object_store_err(self) -> Result<T>;
}

impl<T> HdfsErrorConvert<T> for hdfs_native::Result<T> {
    fn to_object_store_err(self) -> Result<T> {
        self.map_err(|err| match err {
            HdfsError::FileNotFound(path) => object_store::Error::NotFound {
                path: path.clone(),
                source: Box::new(HdfsError::FileNotFound(path)),
            },
            HdfsError::AlreadyExists(path) => object_store::Error::AlreadyExists {
                path: path.clone(),
                source: Box::new(HdfsError::AlreadyExists(path)),
            },
            _ => object_store::Error::Generic {
                store: "HdfsObjectStore",
                source: Box::new(err),
            },
        })
    }
}

// Create a fake multipart writer that assumes only one part will be
// written at a time. It would be better if we figured out how to implement
// AsyncWrite for the FileWriter
struct HdfsMultipartWriter {
    // FileWriter is stateful, but put_part doesn't allow a mutable borrow so we
    // have to wrap in an async mutex
    client: Arc<Client>,
    inner: Arc<tokio::sync::Mutex<FileWriter>>,
    tmp_filename: String,
    final_filename: String,
    next_part: AtomicUsize,
}

impl HdfsMultipartWriter {
    fn new(
        client: Arc<Client>,
        inner: FileWriter,
        tmp_filename: &str,
        final_filename: &str,
    ) -> Self {
        Self {
            client,
            inner: Arc::new(tokio::sync::Mutex::new(inner)),
            tmp_filename: tmp_filename.to_string(),
            final_filename: final_filename.to_string(),
            next_part: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl PutPart for HdfsMultipartWriter {
    /// Upload a single part
    async fn put_part(&self, buf: Vec<u8>, part_idx: usize) -> Result<PartId> {
        if part_idx != self.next_part.load(Ordering::SeqCst) {
            return Err(object_store::Error::NotSupported {
                source: "Part received out of order".to_string().into(),
            });
        }

        self.inner
            .lock()
            .await
            .write(buf.into())
            .await
            .to_object_store_err()?;

        self.next_part.fetch_add(1, Ordering::SeqCst);

        Ok(PartId {
            content_id: part_idx.to_string(),
        })
    }

    /// Complete the upload with the provided parts
    ///
    /// `completed_parts` is in order of part number
    async fn complete(&self, _completed_parts: Vec<PartId>) -> Result<()> {
        self.inner
            .lock()
            .await
            .close()
            .await
            .to_object_store_err()?;
        self.client
            .rename(&self.tmp_filename, &self.final_filename, true)
            .await
            .to_object_store_err()?;
        Ok(())
    }
}

/// ObjectStore paths always remove the leading slash, so add it back
fn make_absolute_file(path: &Path) -> String {
    format!("/{}", path.as_ref())
}

fn make_absolute_dir(path: &Path) -> String {
    if path.parts().count() > 0 {
        format!("/{}/", path.as_ref())
    } else {
        "/".to_string()
    }
}

fn get_object_meta(status: &FileStatus) -> Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: Path::parse(&status.path)?,
        last_modified: Utc.from_utc_datetime(
            &NaiveDateTime::from_timestamp_opt(status.modification_time as i64, 0).unwrap(),
        ),
        size: status.length,
        e_tag: None,
        version: None,
    })
}
