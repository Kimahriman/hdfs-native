use std::io::Result;

use bytes::{BufMut, Bytes, BytesMut};

use crate::connection::Op;
use crate::connection::{CallResult, DatanodeConnection, RpcEngine};
use crate::proto::common;
use crate::proto::hdfs;

pub struct NamenodeProtocol<T: RpcEngine> {
    engine: T,
}

impl<T: RpcEngine> NamenodeProtocol<T> {
    pub fn new(engine: T) -> Self {
        NamenodeProtocol { engine }
    }

    pub fn get_listing(
        &self,
        src: &str,
        start_after: Vec<u8>,
        need_location: bool,
    ) -> Result<CallResult<hdfs::GetListingResponseProto>> {
        let mut message = hdfs::GetListingRequestProto::default();
        message.src = src.to_string();
        message.start_after = start_after;
        message.need_location = need_location;
        self.engine.call("getListing", &message)
    }

    pub fn get_located_file_info(
        &self,
        src: &str,
    ) -> Result<CallResult<hdfs::GetLocatedFileInfoResponseProto>> {
        let mut message = hdfs::GetLocatedFileInfoRequestProto::default();
        message.src = Some(src.to_string());
        message.need_block_token = Some(true);
        self.engine.call("getLocatedFileInfo", &message)
    }
}
