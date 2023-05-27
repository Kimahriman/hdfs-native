use std::sync::Arc;

use url::Url;

use crate::connection::NamenodeConnection;
use crate::hdfs::file::HdfsFileReader;
use crate::hdfs::protocol::NamenodeProtocol;

use std::io::{Error, ErrorKind, Result};

use crate::proto::hdfs;

pub struct ListStatusIterator {
    path: String,
    protocol: Arc<NamenodeProtocol<NamenodeConnection>>,
    partial_listing: Vec<hdfs::HdfsFileStatusProto>,
    listing_position: usize,
    has_more: bool,
    last_seen: Vec<u8>,
}

impl ListStatusIterator {
    fn new(path: String, protocol: Arc<NamenodeProtocol<NamenodeConnection>>) -> Self {
        ListStatusIterator {
            path,
            protocol,
            partial_listing: Vec::new(),
            listing_position: 0,
            has_more: true,
            last_seen: Vec::new(),
        }
    }
}

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
    protocol: Arc<NamenodeProtocol<NamenodeConnection>>,
}

impl Client {
    pub fn new(url: &str) -> Result<Self> {
        let parsed_url = Url::parse(url).expect("Failed to parse provided URL");

        assert_eq!(
            parsed_url.scheme(),
            "hdfs",
            "Only hdfs:// scheme is currently supported"
        );
        assert!(parsed_url.host().is_some(), "Host must be specified");
        assert!(
            parsed_url.port().is_some(),
            "Name services are not currently supported, port is required for host"
        );

        let engine = NamenodeConnection::connect(format!(
            "{}:{}",
            parsed_url.host().unwrap(),
            parsed_url.port().unwrap()
        ))?;
        let protocol = Arc::new(NamenodeProtocol::new(engine));
        Ok(Client { protocol })
    }

    pub fn list_status(&self, path: &str) -> Result<Vec<hdfs::HdfsFileStatusProto>> {
        let mut results = Vec::<hdfs::HdfsFileStatusProto>::new();
        let mut start_after = Vec::<u8>::new();
        loop {
            let partial_listing = self.protocol.get_listing(path, start_after, true)?.get()?;
            match partial_listing.dir_list {
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "File not found",
                    ))
                }
                Some(dir_list) => {
                    start_after = dir_list.partial_listing.last().unwrap().path.clone();
                    results.extend(dir_list.partial_listing);
                    if dir_list.remaining_entries == 0 {
                        break;
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn list_status_iterator(&self, path: &str) -> ListStatusIterator {
        ListStatusIterator::new(path.to_string(), self.protocol.clone())
    }

    pub fn read(&self, path: &str) -> Result<HdfsFileReader> {
        let located_info = self.protocol.get_located_file_info(path)?.get()?;
        match located_info.fs {
            Some(status) => Ok(HdfsFileReader::new(status.locations.unwrap())),
            None => Err(Error::new(ErrorKind::NotFound, "File not found")),
        }
    }
}
