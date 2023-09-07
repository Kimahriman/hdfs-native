use log::debug;
use prost::Message;
use uuid::Uuid;

use crate::proto::hdfs;
use crate::Result;

use super::proxy::NameServiceProxy;

#[derive(Debug)]
pub(crate) struct NamenodeProtocol {
    proxy: NameServiceProxy,
    client_name: String,
}

impl NamenodeProtocol {
    pub(crate) fn new(proxy: NameServiceProxy) -> Self {
        let client_name = format!(
            "hdfs_native_client-{}",
            Uuid::new_v4().as_hyphenated().to_string()
        );
        NamenodeProtocol { proxy, client_name }
    }

    pub(crate) async fn get_file_info(&self, src: &str) -> Result<hdfs::GetFileInfoResponseProto> {
        let mut message = hdfs::GetFileInfoRequestProto::default();
        message.src = src.to_string();
        debug!("get_file_info request: {:?}", &message);

        let response = self
            .proxy
            .call("getFileInfo", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::GetFileInfoResponseProto::decode_length_delimited(response)?;
        debug!("get_file_info response: {:?}", &decoded);

        Ok(decoded)
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
        debug!("get_listing request: {:?}", &message);

        let response = self
            .proxy
            .call("getListing", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::GetListingResponseProto::decode_length_delimited(response)?;
        debug!("get_listing response: {:?}", &decoded);
        Ok(decoded)
    }

    pub(crate) async fn get_located_file_info(
        &self,
        src: &str,
    ) -> Result<hdfs::GetLocatedFileInfoResponseProto> {
        let mut message = hdfs::GetLocatedFileInfoRequestProto::default();
        message.src = Some(src.to_string());
        message.need_block_token = Some(true);
        debug!("get_located_block_info response: {:?}", &message);

        let response = self
            .proxy
            .call(
                "getLocatedFileInfo",
                message.encode_length_delimited_to_vec(),
            )
            .await?;

        let decoded = hdfs::GetLocatedFileInfoResponseProto::decode_length_delimited(response)?;
        debug!("get_located_block_info response: {:?}", &decoded);
        Ok(decoded)
    }

    pub(crate) async fn get_server_defaults(&self) -> Result<hdfs::GetServerDefaultsResponseProto> {
        let message = hdfs::GetServerDefaultsRequestProto::default();

        let response = self
            .proxy
            .call(
                "getServerDefaults",
                message.encode_length_delimited_to_vec(),
            )
            .await?;

        let decoded = hdfs::GetServerDefaultsResponseProto::decode_length_delimited(response)?;
        debug!("get_server_defaults response: {:?}", &decoded);
        Ok(decoded)
    }

    pub(crate) async fn create(
        &self,
        src: &str,
        permission: u32,
        overwrite: bool,
        create_parent: bool,
        replication: u32,
        block_size: u64,
    ) -> Result<hdfs::CreateResponseProto> {
        let mut masked = hdfs::FsPermissionProto::default();
        masked.perm = permission;

        let mut message = hdfs::CreateRequestProto::default();
        message.src = src.to_string();
        message.masked = masked;
        message.client_name = self.client_name.clone();
        if overwrite {
            message.create_flag = hdfs::CreateFlagProto::Overwrite as u32;
        } else {
            message.create_flag = hdfs::CreateFlagProto::Create as u32;
        }
        message.create_parent = create_parent;
        message.replication = replication;
        message.block_size = block_size;

        debug!("create request: {:?}", &message);

        let response = self
            .proxy
            .call("create", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::CreateResponseProto::decode_length_delimited(response)?;
        debug!("create response: {:?}", &decoded);
        Ok(decoded)
    }

    pub(crate) async fn add_block(
        &self,
        src: &str,
        previous: Option<hdfs::ExtendedBlockProto>,
        file_id: Option<u64>,
    ) -> Result<hdfs::AddBlockResponseProto> {
        let mut message = hdfs::AddBlockRequestProto::default();
        message.src = src.to_string();
        message.client_name = self.client_name.clone();
        message.previous = previous;
        message.file_id = file_id;

        debug!("add_block request: {:?}", &message);

        let response = self
            .proxy
            .call("addBlock", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::AddBlockResponseProto::decode_length_delimited(response)?;
        debug!("add_block response: {:?}", &decoded);
        Ok(decoded)
    }

    pub(crate) async fn complete(
        &self,
        src: &str,
        last: Option<hdfs::ExtendedBlockProto>,
        file_id: Option<u64>,
    ) -> Result<hdfs::CompleteResponseProto> {
        let mut message = hdfs::CompleteRequestProto::default();
        message.src = src.to_string();
        message.client_name = self.client_name.clone();
        message.last = last;
        message.file_id = file_id;

        debug!("complete request: {:?}", &message);

        let response = self
            .proxy
            .call("complete", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::CompleteResponseProto::decode_length_delimited(response)?;
        debug!("complete response: {:?}", &decoded);
        Ok(decoded)
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

        debug!("mkdirs request: {:?}", &message);

        let response = self
            .proxy
            .call("mkdirs", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::MkdirsResponseProto::decode_length_delimited(response)?;
        debug!("mkdirs response: {:?}", &decoded);
        Ok(decoded)
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

        debug!("rename request: {:?}", &message);

        let response = self
            .proxy
            .call("rename2", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::Rename2ResponseProto::decode_length_delimited(response)?;
        debug!("rename response: {:?}", &decoded);
        Ok(decoded)
    }

    pub(crate) async fn delete(
        &self,
        src: &str,
        recursive: bool,
    ) -> Result<hdfs::DeleteResponseProto> {
        let mut message = hdfs::DeleteRequestProto::default();
        message.src = src.to_string();
        message.recursive = recursive;
        debug!("delete request: {:?}", &message);

        let response = self
            .proxy
            .call("delete", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::DeleteResponseProto::decode_length_delimited(response)?;
        debug!("delete response: {:?}", &decoded);
        Ok(decoded)
    }
}
