use std::fmt::{Display, Formatter};

use crate::Client;
use crate::HdfsError;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::BoxStream;
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
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        todo!()
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
        todo!()
    }

    /// Cleanup an aborted upload.
    ///
    /// See documentation for individual stores for exact behavior, as capabilities
    /// vary by object store.
    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        todo!()
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        todo!()
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        todo!()
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        todo!()
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    ///
    /// Note: the order of returned [`ObjectMeta`] is not guaranteed
    /// TODO: Make this lazy with a ListStatusIterator and needs to be recursive?
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let statuses = self
            .client
            .list_status(prefix.map(|p| p.to_string().as_ref()).unwrap_or(""))
            .await
            .unwrap();

        let object_metas = statuses
            .iter()
            .filter(|status| !status.isdir)
            .map(|status| {
                Ok(ObjectMeta {
                    location: Path::from(status.path),
                    last_modified: DateTime::<Utc>::from_utc(
                        NaiveDateTime::from_timestamp_opt(status.modification_time, 0).unwrap(),
                        Utc,
                    ),
                    size: status.length,
                    e_tag: None,
                })
            });

        Ok(futures::stream::iter(object_metas).boxed())
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let statuses = self
            .client
            .list_status(prefix.map(|p| p.to_string().as_ref()).unwrap_or(""))
            .await
            .unwrap();

        Ok(futures::stream::iter(statuses.iter().map(|status| {
            Ok(ObjectMeta {
                location: Path::from(status.path),
                last_modified: DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp_opt(status.modification_time, 0).unwrap(),
                    Utc,
                ),
                size: status.length,
                e_tag: None,
            })
        }))
        .boxed())
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        todo!()
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(self
            .client
            .rename(&from.to_string(), &to.to_string(), true)
            .await?)
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(self
            .client
            .rename(&from.to_string(), &to.to_string(), false)
            .await?)
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    ///
    /// Performs an atomic operation if the underlying object storage supports it.
    /// If atomic operations are not supported by the underlying object storage (like S3)
    /// it will return an error.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        todo!()
    }
}

impl From<HdfsError> for object_store::Error {
    fn from(value: HdfsError) -> Self {
        match value {
            HdfsError::FileNotFound => object_store::Error::NotFound {
                path: "".to_string(),
                source: Box::new(value),
            },
            _ => object_store::Error::Generic {
                store: "HdfsObjectStore",
                source: Box::new(value),
            },
        }
    }
}
