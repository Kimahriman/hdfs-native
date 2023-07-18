use std::collections::VecDeque;
use std::sync::Arc;

use log::debug;
use url::Url;

use crate::common::config::Configuration;
use crate::error::{HdfsError, Result};
use crate::hdfs::file::HdfsFileReader;
use crate::hdfs::protocol::NamenodeProtocol;
use crate::hdfs::proxy::NameServiceProxy;
use crate::proto::hdfs::hdfs_file_status_proto::FileType;

use crate::proto::hdfs::HdfsFileStatusProto;

#[derive(Debug)]
pub struct Client {
    protocol: Arc<NamenodeProtocol>,
}

impl Client {
    /// Creates a new HDFS Client. The URL must include the protocol and host, and optionally a port.
    /// If a port is included, the host is treated as a single NameNode. If no port is included, the
    /// host is treated as a name service that will be resolved using the HDFS config.
    ///
    /// viewfs schemes and name services are not currently supported.
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

    /// Retrieve the file status for the file at `src`.
    pub async fn get_file_info(&self, path: &str) -> Result<FileStatus> {
        match self.protocol.get_file_info(path).await?.fs {
            Some(status) => Ok(FileStatus::from(status)),
            None => Err(HdfsError::FileNotFound(path.to_string())),
        }
    }

    /// Retrieves a list of file status for all files in `path`. This does not recurse into directories.
    pub async fn list_status(&self, path: &str) -> Result<Vec<FileStatus>> {
        let mut results = Vec::<FileStatus>::new();
        let mut start_after = Vec::<u8>::new();
        loop {
            let partial_listing = self.protocol.get_listing(path, start_after, true).await?;
            match partial_listing.dir_list {
                None => return Err(HdfsError::FileNotFound(path.to_string())),
                Some(dir_list) => {
                    start_after = dir_list
                        .partial_listing
                        .last()
                        .map(|p| p.path.clone())
                        .unwrap_or(Vec::new());
                    let file_statuses = dir_list.partial_listing.into_iter().map(FileStatus::from);
                    results.extend(file_statuses);
                    if dir_list.remaining_entries == 0 {
                        break;
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn list_status_iterator(
        &self,
        path: &str,
        files_only: bool,
        recursive: bool,
    ) -> ListStatusIterator {
        ListStatusIterator::new(
            path.to_string(),
            self.protocol.clone(),
            files_only,
            recursive,
        )
    }

    /// Opens a file reader for the file at `path`. Path should not include a scheme.
    pub async fn read(&self, path: &str) -> Result<HdfsFileReader> {
        let located_info = self.protocol.get_located_file_info(path).await?;
        match located_info.fs {
            Some(status) => Ok(HdfsFileReader::new(status.locations.unwrap())),
            None => Err(HdfsError::FileNotFound(path.to_string())),
        }
    }

    /// Renames `src` to `dst`. Returns Ok(()) on success, and Err otherwise.
    pub async fn rename(&self, src: &str, dst: &str, overwrite: bool) -> Result<()> {
        debug!("Renaming {} to {}", src, dst);
        self.protocol.rename(src, dst, overwrite).await.map(|_| ())
    }
}

pub struct ListStatusIterator {
    path: String,
    protocol: Arc<NamenodeProtocol>,
    files_only: bool,
    recursive: bool,
    partial_listing: VecDeque<HdfsFileStatusProto>,
    listing_position: usize,
    remaining: u32,
    last_seen: Vec<u8>,
}

impl ListStatusIterator {
    fn new(
        path: String,
        protocol: Arc<NamenodeProtocol>,
        files_only: bool,
        recursive: bool,
    ) -> Self {
        ListStatusIterator {
            path,
            protocol,
            files_only,
            recursive,
            partial_listing: VecDeque::new(),
            listing_position: 0,
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
            self.listing_position = 0;
            self.remaining = dir_list.remaining_entries;
            self.partial_listing = dir_list
                .partial_listing
                .into_iter()
                .filter(|s| !self.files_only || s.file_type() != FileType::IsDir)
                .collect();
            Ok(self.partial_listing.len() > 0)
        } else {
            Ok(false)
        }
    }

    pub async fn next(&mut self) -> Option<Result<FileStatus>> {
        if self.partial_listing.len() == 0 && self.remaining > 0 {
            if let Err(error) = self.get_next_batch().await {
                return Some(Err(error));
            }
        }
        if let Some(next) = self.partial_listing.pop_front() {
            Some(Ok(FileStatus::from(next)))
        } else {
            None
        }
    }
}

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

impl From<HdfsFileStatusProto> for FileStatus {
    fn from(value: HdfsFileStatusProto) -> Self {
        FileStatus {
            path: std::str::from_utf8(value.path.as_slice())
                .unwrap()
                .to_string(),
            length: value.length as usize,
            isdir: value.file_type() == FileType::IsDir,
            permission: value.permission.perm as u16,
            owner: value.owner,
            group: value.group,
            modification_time: value.modification_time,
            access_time: value.access_time,
        }
    }
}
