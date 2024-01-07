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
        let client_name = format!("hdfs_native_client-{}", Uuid::new_v4().as_hyphenated());
        NamenodeProtocol { proxy, client_name }
    }

    pub(crate) async fn get_file_info(&self, src: &str) -> Result<hdfs::GetFileInfoResponseProto> {
        let message = hdfs::GetFileInfoRequestProto {
            src: src.to_string(),
        };
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
        let message = hdfs::GetListingRequestProto {
            src: src.to_string(),
            start_after,
            need_location,
        };
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
        let message = hdfs::GetLocatedFileInfoRequestProto {
            src: Some(src.to_string()),
            need_block_token: Some(true),
        };
        debug!("getLocatedFileInfo request: {:?}", &message);

        let response = self
            .proxy
            .call(
                "getLocatedFileInfo",
                message.encode_length_delimited_to_vec(),
            )
            .await?;

        let decoded = hdfs::GetLocatedFileInfoResponseProto::decode_length_delimited(response)?;
        debug!("getLocatedFileInfo response: {:?}", &decoded);
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
        let masked = hdfs::FsPermissionProto { perm: permission };
        let mut create_flag = hdfs::CreateFlagProto::Create as u32;
        if overwrite {
            create_flag |= hdfs::CreateFlagProto::Overwrite as u32;
        }

        let message = hdfs::CreateRequestProto {
            src: src.to_string(),
            masked,
            client_name: self.client_name.clone(),
            create_parent,
            replication,
            block_size,
            create_flag,
            ..Default::default()
        };

        debug!("create request: {:?}", &message);

        let response = self
            .proxy
            .call("create", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::CreateResponseProto::decode_length_delimited(response)?;
        debug!("create response: {:?}", &decoded);
        Ok(decoded)
    }

    pub(crate) async fn append(
        &self,
        src: &str,
        new_block: bool,
    ) -> Result<hdfs::AppendResponseProto> {
        let mut flag = hdfs::CreateFlagProto::Append as u32;
        if new_block {
            flag |= hdfs::CreateFlagProto::NewBlock as u32;
        }

        let message = hdfs::AppendRequestProto {
            src: src.to_string(),
            client_name: self.client_name.clone(),
            flag: Some(flag),
        };

        debug!("append request: {:?}", &message);

        let response = self
            .proxy
            .call("append", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::AppendResponseProto::decode_length_delimited(response)?;
        debug!("append response: {:?}", &decoded);
        Ok(decoded)
    }

    pub(crate) async fn add_block(
        &self,
        src: &str,
        previous: Option<hdfs::ExtendedBlockProto>,
        file_id: Option<u64>,
    ) -> Result<hdfs::AddBlockResponseProto> {
        let message = hdfs::AddBlockRequestProto {
            src: src.to_string(),
            client_name: self.client_name.clone(),
            previous,
            file_id,
            ..Default::default()
        };

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
        let message = hdfs::CompleteRequestProto {
            src: src.to_string(),
            client_name: self.client_name.clone(),
            last,
            file_id,
        };
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
        let masked = hdfs::FsPermissionProto { perm: permission };

        let message = hdfs::MkdirsRequestProto {
            src: src.to_string(),
            masked,
            create_parent,
            ..Default::default()
        };
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
        let message = hdfs::Rename2RequestProto {
            src: src.to_string(),
            dst: dst.to_string(),
            overwrite_dest: overwrite,
            ..Default::default()
        };
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
        let message = hdfs::DeleteRequestProto {
            src: src.to_string(),
            recursive,
        };
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
