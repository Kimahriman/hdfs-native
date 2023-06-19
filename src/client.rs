use std::sync::Arc;

use url::Url;

use crate::common::config::Configuration;
use crate::hdfs::file::HdfsFileReader;
use crate::hdfs::protocol::NamenodeProtocol;
use crate::hdfs::proxy::NameServiceProxy;
use crate::{connection::RpcConnection, proto::hdfs::hdfs_file_status_proto::FileType};

use std::io::{Error, ErrorKind, Result};

use crate::proto::hdfs::HdfsFileStatusProto;

// pub struct ListStatusIterator {
//     path: String,
//     protocol: Arc<NamenodeProtocol<NamenodeConnection>>,
//     partial_listing: Vec<hdfs::HdfsFileStatusProto>,
//     listing_position: usize,
//     has_more: bool,
//     last_seen: Vec<u8>,
// }

// impl ListStatusIterator {
//     fn new(path: String, protocol: Arc<NamenodeProtocol<NamenodeConnection>>) -> Self {
//         ListStatusIterator {
//             path,
//             protocol,
//             partial_listing: Vec::new(),
//             listing_position: 0,
//             has_more: true,
//             last_seen: Vec::new(),
//         }
//     }
// }

// impl Iterator for ListStatusIterator {

//     type Item = hdfs::HdfsFileStatusProto;

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.listing_position >= self.partial_listing.len() && self.has_more {

//             let listing = self.protocol.get_listing(&self.path, self.last_seen, false)?.get()?;
//             match listing {
//                 None => Err(std::io::Error(ErrorKind::NotFound, "File not found"))
//                 Some(partial_listing) => {

//                 }
//             }
//         }
//     }
// }

pub struct Client {
    protocol: Arc<NamenodeProtocol<NameServiceProxy>>,
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

    pub fn list_status(&self, path: &str) -> Result<Vec<FileStatus>> {
        let mut results = Vec::<FileStatus>::new();
        let mut start_after = Vec::<u8>::new();
        loop {
            let partial_listing = self.protocol.get_listing(path, start_after, true)?;
            match partial_listing.dir_list {
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "File not found",
                    ))
                }
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

    // pub fn list_status_iterator(&self, path: &str) -> ListStatusIterator {
    //     ListStatusIterator::new(path.to_string(), self.protocol.clone())
    // }

    pub fn read(&self, path: &str) -> Result<HdfsFileReader> {
        let located_info = self.protocol.get_located_file_info(path)?;
        match located_info.fs {
            Some(status) => Ok(HdfsFileReader::new(status.locations.unwrap())),
            None => Err(Error::new(ErrorKind::NotFound, "File not found")),
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
