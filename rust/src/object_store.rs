use std::{
    fmt::{Display, Formatter},
    future,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{client::FileStatus, file::FileWriter, Client, HdfsError, WriteOptions};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{NaiveDateTime, TimeZone, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use object_store::{
    multipart::{PartId, PutPart, WriteMultiPart},
    path::Path,
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result,
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
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let final_file_path = make_absolute_file(location);
        let path_buf = PathBuf::from(&final_file_path);

        let file_name = path_buf
            .file_name()
            .ok_or(HdfsError::InvalidPath("path missing filename".to_string()))?
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))?
            .to_string();

        let tmp_filename = path_buf
            .with_file_name(format!(".{}.tmp", file_name))
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))?
            .to_string();

        // First we need to check if the tmp file exists so we know whether to overwrite
        let overwrite = match self.client.get_file_info(&tmp_filename).await {
            Ok(_) => true,
            Err(HdfsError::FileNotFound(_)) => false,
            Err(e) => Err(e)?,
        };

        let mut write_options = WriteOptions::default();
        write_options.overwrite = overwrite;

        let mut writer = self.client.create(&tmp_filename, write_options).await?;
        writer.write(bytes).await?;
        writer.close().await?;

        self.client
            .rename(&tmp_filename, &final_file_path, true)
            .await?;

        Ok(())
    }

    /// Currently not implemented. Once object_store is upgraded to 0.7, we can implement this
    /// using the PutPart trait in the multipart mod that was made public.
    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let final_file_path = make_absolute_file(location);
        let path_buf = PathBuf::from(&final_file_path);

        let file_name = path_buf
            .file_name()
            .ok_or(HdfsError::InvalidPath("path missing filename".to_string()))?
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))?
            .to_string();

        let tmp_filename = path_buf
            .with_file_name(format!(".{}.tmp", file_name))
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))?
            .to_string();

        // First we need to check if the tmp file exists so we know whether to overwrite
        let overwrite = match self.client.get_file_info(&tmp_filename).await {
            Ok(_) => true,
            Err(HdfsError::FileNotFound(_)) => false,
            Err(e) => Err(e)?,
        };

        let mut write_options = WriteOptions::default();
        write_options.overwrite = overwrite;

        let writer = self.client.create(&tmp_filename, write_options).await?;

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

    /// Cleanup an aborted upload.
    ///
    /// See documentation for individual stores for exact behavior, as capabilities
    /// vary by object store.
    async fn abort_multipart(&self, _location: &Path, multipart_id: &MultipartId) -> Result<()> {
        // The multipart_id is the resolved temporary file name, so we can just delete it
        self.client.delete(multipart_id, false).await?;
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

        let reader = self.client.read(&make_absolute_file(location)).await?;
        let bytes = reader
            .read_range(range.start, range.end - range.start)
            .await?;

        let payload = GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed());

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
            .await?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc.from_utc_datetime(
                &NaiveDateTime::from_timestamp_opt(status.modification_time as i64, 0).unwrap(),
            ),
            size: status.length,
            e_tag: None,
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        let result = self
            .client
            .delete(&make_absolute_file(location), false)
            .await?;

        if !result {
            Err(HdfsError::OperationFailed(
                "failed to delete object".to_string(),
            ))?
        }

        Ok(())
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    ///
    /// Note: the order of returned [`ObjectMeta`] is not guaranteed
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let status_stream = self
            .client
            .list_status_iter(
                &prefix
                    .map(|p| make_absolute_dir(p))
                    .unwrap_or("".to_string()),
                true,
            )
            .into_stream()
            .filter(|res| {
                let result = if let Ok(status) = res {
                    !status.isdir
                } else {
                    true
                };
                future::ready(result)
            })
            .map(move |res| {
                res.map(|s| s.into())
                    .map_err(|err| object_store::Error::from(err))
            });

        Ok(Box::pin(status_stream))
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut status_stream = self.client.list_status_iter(
            &prefix
                .map(|p| make_absolute_dir(p))
                .unwrap_or("".to_string()),
            false,
        );

        let mut statuses = Vec::<FileStatus>::new();
        while let Some(status) = status_stream.next().await {
            statuses.push(status?);
        }

        let dirs: Vec<Path> = statuses
            .iter()
            .filter(|s| s.isdir)
            .map(|s| Path::from(s.path.as_ref()))
            .collect();
        let files: Vec<ObjectMeta> = statuses
            .iter()
            .filter(|s| !s.isdir)
            .map(|s| s.into())
            .collect();

        Ok(ListResult {
            common_prefixes: dirs,
            objects: files,
        })
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(self
            .client
            .rename(&make_absolute_file(from), &make_absolute_file(to), true)
            .await?)
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(self
            .client
            .rename(&make_absolute_file(from), &make_absolute_file(to), false)
            .await?)
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    ///
    /// Performs an atomic operation if the underlying object storage supports it.
    /// If atomic operations are not supported by the underlying object storage (like S3)
    /// it will return an error.
    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }
}

impl From<HdfsError> for object_store::Error {
    fn from(value: HdfsError) -> Self {
        match value {
            HdfsError::FileNotFound(path) => object_store::Error::NotFound {
                path: path.clone(),
                source: Box::new(HdfsError::FileNotFound(path)),
            },
            _ => object_store::Error::Generic {
                store: "HdfsObjectStore",
                source: Box::new(value),
            },
        }
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

        self.inner.lock().await.write(buf.into()).await?;

        self.next_part.fetch_add(1, Ordering::SeqCst);

        Ok(PartId {
            content_id: part_idx.to_string(),
        })
    }

    /// Complete the upload with the provided parts
    ///
    /// `completed_parts` is in order of part number
    async fn complete(&self, _completed_parts: Vec<PartId>) -> Result<()> {
        self.inner.lock().await.close().await?;
        self.client
            .rename(&self.tmp_filename, &self.final_filename, true)
            .await?;
        Ok(())
    }
}

/// ObjectStore paths always remove the leading slash, so add it back
fn make_absolute_file(path: &Path) -> String {
    format!("/{}", path.as_ref())
}

fn make_absolute_dir(path: &Path) -> String {
    format!("/{}/", path.as_ref())
}

impl From<&FileStatus> for ObjectMeta {
    fn from(status: &FileStatus) -> Self {
        ObjectMeta {
            location: Path::from(status.path.clone()),
            last_modified: Utc.from_utc_datetime(
                &NaiveDateTime::from_timestamp_opt(status.modification_time as i64, 0).unwrap(),
            ),
            size: status.length,
            e_tag: None,
        }
    }
}

impl From<FileStatus> for ObjectMeta {
    fn from(status: FileStatus) -> Self {
        (&status).into()
    }
}
