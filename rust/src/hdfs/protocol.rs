use prost::Message;

use crate::proto::hdfs;
use crate::Result;

use super::proxy::NameServiceProxy;

#[derive(Debug)]
pub(crate) struct NamenodeProtocol {
    proxy: NameServiceProxy,
}

impl NamenodeProtocol {
    pub(crate) fn new(proxy: NameServiceProxy) -> Self {
        NamenodeProtocol { proxy }
    }

    pub(crate) async fn get_file_info(&self, src: &str) -> Result<hdfs::GetFileInfoResponseProto> {
        let mut message = hdfs::GetFileInfoRequestProto::default();
        message.src = src.to_string();

        let response = self
            .proxy
            .call("getFileInfo", message.encode_length_delimited_to_vec())
            .await?;
        Ok(hdfs::GetFileInfoResponseProto::decode_length_delimited(
            response,
        )?)
    }

    pub(crate) async fn get_listing(
        &self,
        src: &str,
        start_after: Vec<u8>,
        need_location: bool,
    ) -> Result<hdfs::GetListingResponseProto> {
        let mut message = hdfs::GetListingRequestProto::default();
        message.src = src.to_string();
        message.start_after = start_after;
        message.need_location = need_location;

        let response = self
            .proxy
            .call("getListing", message.encode_length_delimited_to_vec())
            .await?;
        Ok(hdfs::GetListingResponseProto::decode_length_delimited(
            response,
        )?)
    }

    pub(crate) async fn get_located_file_info(
        &self,
        src: &str,
    ) -> Result<hdfs::GetLocatedFileInfoResponseProto> {
        let mut message = hdfs::GetLocatedFileInfoRequestProto::default();
        message.src = Some(src.to_string());
        message.need_block_token = Some(true);

        let response = self
            .proxy
            .call(
                "getLocatedFileInfo",
                message.encode_length_delimited_to_vec(),
            )
            .await?;
        Ok(hdfs::GetLocatedFileInfoResponseProto::decode_length_delimited(response)?)
    }

    pub(crate) async fn mkdirs(
        &self,
        src: &str,
        permission: u32,
        create_parent: bool,
    ) -> Result<hdfs::MkdirsResponseProto> {
        let mut masked = hdfs::FsPermissionProto::default();
        masked.perm = permission;

        let mut message = hdfs::MkdirsRequestProto::default();
        message.src = src.to_string();
        message.masked = masked;
        message.create_parent = create_parent;

        let response = self
            .proxy
            .call("mkdirs", message.encode_length_delimited_to_vec())
            .await?;
        Ok(hdfs::MkdirsResponseProto::decode_length_delimited(
            response,
        )?)
    }

    pub(crate) async fn rename(
        &self,
        src: &str,
        dst: &str,
        overwrite: bool,
    ) -> Result<hdfs::Rename2ResponseProto> {
        let mut message = hdfs::Rename2RequestProto::default();
        message.src = src.to_string();
        message.dst = dst.to_string();
        message.overwrite_dest = overwrite;

        let response = self
            .proxy
            .call("rename2", message.encode_length_delimited_to_vec())
            .await?;
        Ok(hdfs::Rename2ResponseProto::decode_length_delimited(
            response,
        )?)
    }

    pub(crate) async fn delete(
        &self,
        src: &str,
        recursive: bool,
    ) -> Result<hdfs::DeleteResponseProto> {
        let mut message = hdfs::DeleteRequestProto::default();
        message.src = src.to_string();
        message.recursive = recursive;

        let response = self
            .proxy
            .call("delete", message.encode_length_delimited_to_vec())
            .await?;
        Ok(hdfs::DeleteResponseProto::decode_length_delimited(
            response,
        )?)
    }
}
