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
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{NaiveDateTime, TimeZone, Utc};
use futures::{
    stream::{BoxStream, StreamExt},
    FutureExt,
};
use hdfs_native::{client::FileStatus, file::FileWriter, Client, HdfsError, WriteOptions};
use object_store::{
    path::Path, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
    UploadPart,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::{self, JoinHandle},
};

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

    async fn open_tmp_file(&self, file_path: &str) -> Result<(FileWriter, String)> {
        let path_buf = PathBuf::from(file_path);

        let file_name = path_buf
            .file_name()
            .ok_or(HdfsError::InvalidPath("path missing filename".to_string()))
            .to_object_store_err()?
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))
            .to_object_store_err()?
            .to_string();

        let tmp_file_path = path_buf
            .with_file_name(format!(".{}.tmp", file_name))
            .to_str()
            .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))
            .to_object_store_err()?
            .to_string();

        let writer = self
            .client
            .create(&tmp_file_path, WriteOptions::default())
            .await
            .to_object_store_err()?;

        Ok((writer, tmp_file_path))
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
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
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

        // If we're not overwriting, do an upfront check to see if the file already
        // exists. Otherwise we have to write the whole file and try to rename before
        // finding out.
        if !overwrite && self.client.get_file_info(&final_file_path).await.is_ok() {
            return Err(HdfsError::AlreadyExists(final_file_path)).to_object_store_err();
        }

        let (mut tmp_file, tmp_file_path) = self.open_tmp_file(&final_file_path).await?;

        for bytes in payload {
            tmp_file.write(bytes).await.to_object_store_err()?;
        }
        tmp_file.close().await.to_object_store_err()?;

        self.client
            .rename(&tmp_file_path, &final_file_path, overwrite)
            .await
            .to_object_store_err()?;

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    /// Uses the [PutPart] trait to implement an asynchronous writer. We can't actually upload
    /// multiple parts at once, so we simply set a limit of one part at a time.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let final_file_path = make_absolute_file(location);

        let (tmp_file, tmp_file_path) = self.open_tmp_file(&final_file_path).await?;

        Ok(Box::new(HdfsMultipartWriter::new(
            Arc::clone(&self.client),
            tmp_file,
            &tmp_file_path,
            &final_file_path,
        )))
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

        let range = options
            .range
            .map(|r| match r {
                GetRange::Bounded(range) => range,
                GetRange::Offset(offset) => offset..meta.size,
                GetRange::Suffix(suffix) => meta.size.saturating_sub(suffix)..meta.size,
            })
            .unwrap_or(0..meta.size);

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
            attributes: Default::default(),
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

type PartSender = mpsc::UnboundedSender<(oneshot::Sender<Result<()>>, PutPayload)>;

// Create a fake multipart writer the creates an uploader to a temp file as a background
// task, and submits new parts to be uploaded to a queue for this task.
// A once cell is used to track whether a part has finished writing or not.
// On completing, rename the file to the actual target.
struct HdfsMultipartWriter {
    client: Arc<Client>,
    sender: Option<(JoinHandle<Result<()>>, PartSender)>,
    tmp_filename: String,
    final_filename: String,
}

impl HdfsMultipartWriter {
    fn new(
        client: Arc<Client>,
        writer: FileWriter,
        tmp_filename: &str,
        final_filename: &str,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        Self {
            client,
            sender: Some((Self::start_writer_task(writer, receiver), sender)),
            tmp_filename: tmp_filename.to_string(),
            final_filename: final_filename.to_string(),
        }
    }

    fn start_writer_task(
        mut writer: FileWriter,
        mut part_receiver: mpsc::UnboundedReceiver<(oneshot::Sender<Result<()>>, PutPayload)>,
    ) -> JoinHandle<Result<()>> {
        task::spawn(async move {
            'outer: loop {
                match part_receiver.recv().await {
                    Some((sender, part)) => {
                        for bytes in part {
                            if let Err(e) = writer.write(bytes).await.to_object_store_err() {
                                let _ = sender.send(Err(e));
                                break 'outer;
                            }
                        }
                        let _ = sender.send(Ok(()));
                    }
                    None => {
                        return writer.close().await.to_object_store_err();
                    }
                }
            }

            // If we've reached here, a write task failed so just return Err's for all new parts that come in
            while let Some((sender, _)) = part_receiver.recv().await {
                let _ = sender.send(
                    Err(HdfsError::OperationFailed(
                        "Write failed during one of the parts".to_string(),
                    ))
                    .to_object_store_err(),
                );
            }
            Err(HdfsError::OperationFailed(
                "Write failed during one of the parts".to_string(),
            ))
            .to_object_store_err()
        })
    }
}

impl std::fmt::Debug for HdfsMultipartWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsMultipartWriter")
            .field("tmp_filename", &self.tmp_filename)
            .field("final_filename", &self.final_filename)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for HdfsMultipartWriter {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let (result_sender, result_receiver) = oneshot::channel();

        if let Some((_, payload_sender)) = self.sender.as_ref() {
            payload_sender.send((result_sender, payload)).unwrap();
        } else {
            result_sender
                .send(
                    Err(HdfsError::OperationFailed(
                        "Cannot put part after completing or aborting".to_string(),
                    ))
                    .to_object_store_err(),
                )
                .unwrap();
        }

        async { result_receiver.await.unwrap() }.boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // Drop the sender so the task knows no more data is coming
        if let Some((handle, sender)) = self.sender.take() {
            drop(sender);

            // Wait for the writer task to finish
            handle.await??;

            self.client
                .rename(&self.tmp_filename, &self.final_filename, true)
                .await
                .to_object_store_err()?;

            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        } else {
            Err(object_store::Error::NotSupported {
                source: "Cannot call abort or complete multiple times".into(),
            })
        }
    }

    async fn abort(&mut self) -> Result<()> {
        // Drop the sender so the task knows no more data is coming
        if let Some((handle, sender)) = self.sender.take() {
            drop(sender);

            // Wait for the writer task to finish
            handle.abort();

            self.client
                .delete(&self.tmp_filename, false)
                .await
                .to_object_store_err()?;

            Ok(())
        } else {
            Err(object_store::Error::NotSupported {
                source: "Cannot call abort or complete multiple times".into(),
            })
        }
    }
    // /// Upload a single part
    // async fn put_part(&self, buf: Vec<u8>, part_idx: usize) -> Result<PartId> {
    //     if part_idx != self.next_part.load(Ordering::SeqCst) {
    //         return Err(object_store::Error::NotSupported {
    //             source: "Part received out of order".to_string().into(),
    //         });
    //     }

    //     self.inner
    //         .lock()
    //         .await
    //         .write(buf.into())
    //         .await
    //         .to_object_store_err()?;

    //     self.next_part.fetch_add(1, Ordering::SeqCst);

    //     Ok(PartId {
    //         content_id: part_idx.to_string(),
    //     })
    // }

    // /// Complete the upload with the provided parts
    // ///
    // /// `completed_parts` is in order of part number
    // async fn complete(&self, _completed_parts: Vec<PartId>) -> Result<()> {
    //     self.inner
    //         .lock()
    //         .await
    //         .close()
    //         .await
    //         .to_object_store_err()?;
    //     self.client
    //         .rename(&self.tmp_filename, &self.final_filename, true)
    //         .await
    //         .to_object_store_err()?;
    //     Ok(())
    // }
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
