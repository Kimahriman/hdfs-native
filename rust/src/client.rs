use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use log::debug;
use url::Url;

use crate::common::config::Configuration;
use crate::error::{HdfsError, Result};
use crate::file::{FileReader, FileWriter};
use crate::hdfs::ec::resolve_ec_policy;
use crate::hdfs::protocol::NamenodeProtocol;
use crate::hdfs::proxy::NameServiceProxy;
use crate::proto::hdfs::hdfs_file_status_proto::FileType;

use crate::proto::hdfs::HdfsFileStatusProto;

#[derive(Clone)]
pub struct WriteOptions {
    // Block size. Default is retrieved from the server.
    pub block_size: Option<u64>,
    // Replication factor. Default is retrieved from the server.
    pub replication: Option<u32>,
    // Unix file permission, defaults to 755
    pub permission: u32,
    // Whether to overwrite the file, defaults to false
    pub overwrite: bool,
    // Whether to create any missing parent directories, defaults to true
    pub create_parent: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            block_size: None,
            replication: None,
            permission: 0o755,
            overwrite: false,
            create_parent: true,
        }
    }
}

#[derive(Debug)]
pub struct Client {
    protocol: Arc<NamenodeProtocol>,
}

impl Client {
    /// Creates a new HDFS Client. The URL must include the protocol and host, and optionally a port.
    /// If a port is included, the host is treated as a single NameNode. If no port is included, the
    /// host is treated as a name service that will be resolved using the HDFS config.
    ///
    /// viewfs schemes are not currently supported.
    pub fn new(url: &str) -> Result<Self> {
        let parsed_url = Url::parse(url).expect("Failed to parse provided URL");

        assert_eq!(
            parsed_url.scheme(),
            "hdfs",
            "Only hdfs:// scheme is currently supported"
        );
        assert!(parsed_url.host().is_some(), "Host must be specified");

        let config = Configuration::new()?;

        let proxy = NameServiceProxy::new(&parsed_url, &config);
        let protocol = Arc::new(NamenodeProtocol::new(proxy));
        Ok(Client { protocol })
    }

    /// Retrieve the file status for the file at `path`.
    pub async fn get_file_info(&self, path: &str) -> Result<FileStatus> {
        match self.protocol.get_file_info(path).await?.fs {
            Some(status) => Ok(FileStatus::from(status, path)),
            None => Err(HdfsError::FileNotFound(path.to_string())),
        }
    }

    /// Retrives a list of all files in directories located at `path`. Wrapper around `list_status_iter` that
    /// returns Err if any part of the stream fails, or Ok if all file statuses were found successfully.
    pub async fn list_status(&self, path: &str, recursive: bool) -> Result<Vec<FileStatus>> {
        let iter = self.list_status_iter(path, recursive);
        let statuses = iter
            .into_stream()
            .collect::<Vec<Result<FileStatus>>>()
            .await;

        let mut resolved_statues = Vec::<FileStatus>::with_capacity(statuses.len());
        for status in statuses.into_iter() {
            resolved_statues.push(status?);
        }

        Ok(resolved_statues)
    }

    /// Retrives an iterator of all files in directories located at `path`.
    pub fn list_status_iter(&self, path: &str, recursive: bool) -> ListStatusIterator {
        ListStatusIterator::new(path.to_string(), self.protocol.clone(), recursive)
    }

    /// Opens a file reader for the file at `path`. Path should not include a scheme.
    pub async fn read(&self, path: &str) -> Result<FileReader> {
        let located_info = self.protocol.get_located_file_info(path).await?;
        match located_info.fs {
            Some(mut status) => {
                let ec_schema = if let Some(ec_policy) = status.ec_policy.as_ref() {
                    Some(resolve_ec_policy(ec_policy)?)
                } else {
                    None
                };

                if status.file_encryption_info.is_some() {
                    return Err(HdfsError::UnsupportedFeature("File encryption".to_string()));
                }
                if status.file_type() == FileType::IsDir {
                    return Err(HdfsError::IsADirectoryError(path.to_string()));
                }

                if let Some(locations) = status.locations.take() {
                    Ok(FileReader::new(status, locations, ec_schema))
                } else {
                    Err(HdfsError::BlocksNotFound(path.to_string()))
                }
            }
            None => Err(HdfsError::FileNotFound(path.to_string())),
        }
    }

    pub async fn create(&self, src: &str, write_options: WriteOptions) -> Result<FileWriter> {
        let server_defaults = self.protocol.get_server_defaults().await?.server_defaults;

        let block_size = write_options
            .block_size
            .unwrap_or(server_defaults.block_size);
        let replication = write_options
            .replication
            .unwrap_or(server_defaults.replication);

        let create_response = self
            .protocol
            .create(
                src,
                write_options.permission,
                write_options.overwrite,
                write_options.create_parent,
                replication,
                block_size,
            )
            .await?;

        match create_response.fs {
            Some(status) => {
                if status.ec_policy.is_some() {
                    return Err(HdfsError::UnsupportedFeature("Erasure coding".to_string()));
                }
                if status.file_encryption_info.is_some() {
                    return Err(HdfsError::UnsupportedFeature("File encryption".to_string()));
                }

                Ok(FileWriter::new(
                    Arc::clone(&self.protocol),
                    src.to_string(),
                    status,
                    server_defaults,
                ))
            }
            None => Err(HdfsError::FileNotFound(src.to_string())),
        }
    }

    /// Create a new directory at `path` with the given `permission`. If `create_parent` is true,
    /// any missing parent directories will be created as well, otherwise an error will be returned
    /// if the parent directory doesn't already exist.
    pub async fn mkdirs(&self, path: &str, permission: u32, create_parent: bool) -> Result<()> {
        self.protocol
            .mkdirs(path, permission, create_parent)
            .await
            .map(|_| ())
    }

    /// Renames `src` to `dst`. Returns Ok(()) on success, and Err otherwise.
    pub async fn rename(&self, src: &str, dst: &str, overwrite: bool) -> Result<()> {
        debug!("Renaming {} to {}", src, dst);
        self.protocol.rename(src, dst, overwrite).await.map(|_| ())
    }

    /// Deletes the file or directory at `path`. If `recursive` is false and `path` is a non-empty
    /// directory, this will fail. Returns `Ok(true)` if it was successfully deleted.
    pub async fn delete(&self, path: &str, recursive: bool) -> Result<bool> {
        self.protocol
            .delete(path, recursive)
            .await
            .map(|r| r.result)
    }
}

pub(crate) struct DirListingIterator {
    path: String,
    protocol: Arc<NamenodeProtocol>,
    files_only: bool,
    partial_listing: VecDeque<HdfsFileStatusProto>,
    remaining: u32,
    last_seen: Vec<u8>,
}

impl DirListingIterator {
    fn new(path: String, protocol: Arc<NamenodeProtocol>, files_only: bool) -> Self {
        DirListingIterator {
            path,
            protocol,
            files_only,
            partial_listing: VecDeque::new(),
            remaining: 1,
            last_seen: Vec::new(),
        }
    }

    async fn get_next_batch(&mut self) -> Result<bool> {
        let listing = self
            .protocol
            .get_listing(&self.path, self.last_seen.clone(), false)
            .await?;

        if let Some(dir_list) = listing.dir_list {
            self.last_seen = dir_list
                .partial_listing
                .last()
                .map(|p| p.path.clone())
                .unwrap_or(Vec::new());

            self.remaining = dir_list.remaining_entries;

            self.partial_listing = dir_list
                .partial_listing
                .into_iter()
                .filter(|s| !self.files_only || s.file_type() != FileType::IsDir)
                .collect();
            Ok(self.partial_listing.len() > 0)
        } else {
            Err(HdfsError::FileNotFound(self.path.clone()))
        }
    }

    pub async fn next(&mut self) -> Option<Result<FileStatus>> {
        if self.partial_listing.len() == 0 && self.remaining > 0 {
            if let Err(error) = self.get_next_batch().await {
                self.remaining = 0;
                return Some(Err(error));
            }
        }
        if let Some(next) = self.partial_listing.pop_front() {
            Some(Ok(FileStatus::from(next, &self.path)))
        } else {
            None
        }
    }
}

pub struct ListStatusIterator {
    protocol: Arc<NamenodeProtocol>,
    recursive: bool,
    iters: Vec<DirListingIterator>,
}

impl ListStatusIterator {
    fn new(path: String, protocol: Arc<NamenodeProtocol>, recursive: bool) -> Self {
        let initial = DirListingIterator::new(path.clone(), Arc::clone(&protocol), false);

        ListStatusIterator {
            protocol,
            recursive,
            iters: vec![initial],
        }
    }

    pub async fn next(&mut self) -> Option<Result<FileStatus>> {
        let mut next_file: Option<Result<FileStatus>> = None;
        while next_file.is_none() {
            if let Some(iter) = self.iters.last_mut() {
                if let Some(file_result) = iter.next().await {
                    if let Ok(file) = file_result {
                        // Return the directory as the next result, but start traversing into that directory
                        // next if we're doing a recursive listing
                        if file.isdir && self.recursive {
                            self.iters.push(DirListingIterator::new(
                                file.path.clone(),
                                Arc::clone(&self.protocol),
                                false,
                            ))
                        }
                        next_file = Some(Ok(file));
                    } else {
                        // Error, return that as the next element
                        next_file = Some(file_result)
                    }
                } else {
                    // We've exhausted this directory
                    self.iters.pop();
                }
            } else {
                // There's nothing left, just return None
                break;
            }
        }

        next_file
    }

    pub fn into_stream(self) -> BoxStream<'static, Result<FileStatus>> {
        let listing = stream::unfold(self, |mut state| async move {
            let next = state.next().await;
            next.map(|n| (n, state))
        });
        Box::pin(listing)
    }
}

#[derive(Debug)]
pub struct FileStatus {
    pub path: String,
    pub length: usize,
    pub isdir: bool,
    pub permission: u16,
    pub owner: String,
    pub group: String,
    pub modification_time: u64,
    pub access_time: u64,
}

impl FileStatus {
    fn from(value: HdfsFileStatusProto, base_path: &str) -> Self {
        let mut path = PathBuf::from(base_path);
        if let Ok(relative_path) = std::str::from_utf8(&value.path) {
            if relative_path.len() > 0 {
                path.push(relative_path)
            }
        }

        FileStatus {
            isdir: value.file_type() == FileType::IsDir,
            path: path
                .to_str()
                .map(|x| x.to_string())
                .unwrap_or(String::new()),
            length: value.length as usize,
            permission: value.permission.perm as u16,
            owner: value.owner,
            group: value.group,
            modification_time: value.modification_time,
            access_time: value.access_time,
        }
    }
}
