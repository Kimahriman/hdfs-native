use std::{
    fmt::{Display, Formatter},
    future,
};

use crate::{client::FileStatus, Client, HdfsError};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct HdfsObjectStore {
    client: Client,
}

impl HdfsObjectStore {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// ObjectStore paths always remove the leading slash, so add it back
    fn make_absolute_file(path: &Path) -> String {
        format!("/{}", path.as_ref())
    }

    fn make_absolute_dir(path: &Path) -> String {
        format!("/{}/", path.as_ref())
    }
}

impl Display for HdfsObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HdfsObjectStore")
    }
}

#[async_trait]
impl ObjectStore for HdfsObjectStore {
    /// Save the provided bytes to the specified location
    ///
    /// The operation is guaranteed to be atomic, it will either successfully
    /// write the entirety of `bytes` to `location`, or fail. No clients
    /// should be able to observe a partially written object
    async fn put(&self, _location: &Path, _bytes: Bytes) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    /// Get a multi-part upload that allows writing data in chunks
    ///
    /// Most cloud-based uploads will buffer and upload parts in parallel.
    ///
    /// To complete the upload, [AsyncWrite::poll_shutdown] must be called
    /// to completion. This operation is guaranteed to be atomic, it will either
    /// make all the written data available at `location`, or fail. No clients
    /// should be able to observe a partially written object
    ///
    /// For some object stores (S3, GCS, and local in particular), if the
    /// writer fails or panics, you must call [ObjectStore::abort_multipart]
    /// to clean up partially written data.
    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(object_store::Error::NotImplemented)
    }

    /// Cleanup an aborted upload.
    ///
    /// See documentation for individual stores for exact behavior, as capabilities
    /// vary by object store.
    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
        {
            return Err(object_store::Error::NotImplemented);
        }

        let reader = self
            .client
            .read(&Self::make_absolute_file(location))
            .await?;
        let bytes = if let Some(range) = options.range {
            reader
                .read_range(range.start, range.end - range.start)
                .await?
        } else {
            reader.read().await?
        };

        Ok(GetResult::Stream(
            stream::once(async move { Ok(bytes) }).boxed(),
        ))
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let status = self
            .client
            .get_file_info(&Self::make_absolute_file(location))
            .await?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp_opt(status.modification_time as i64, 0).unwrap(),
                Utc,
            ),
            size: status.length,
            e_tag: None,
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, _location: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
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
                    .map(|p| Self::make_absolute_dir(p))
                    .unwrap_or("".to_string()),
                true,
            )
            .filter(|res| {
                let result = if let Ok(status) = res {
                    !status.isdir
                } else {
                    true
                };
                future::ready(result)
            })
            .map(|res| {
                res.map(|s| create_object_meta(&s))
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
                .map(|p| Self::make_absolute_dir(p))
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
            .map(create_object_meta)
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
            .rename(
                &Self::make_absolute_file(from),
                &Self::make_absolute_file(to),
                true,
            )
            .await?)
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(self
            .client
            .rename(
                &Self::make_absolute_file(from),
                &Self::make_absolute_file(to),
                false,
            )
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

fn create_object_meta(status: &FileStatus) -> ObjectMeta {
    ObjectMeta {
        location: Path::from(status.path.as_ref()),
        last_modified: DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(status.modification_time as i64, 0).unwrap(),
            Utc,
        ),
        size: status.length,
        e_tag: None,
    }
}
