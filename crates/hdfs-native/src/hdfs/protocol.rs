use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use chrono::Utc;
use log::{debug, warn};
use prost::Message;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::proto::hdfs::{
    self, DataEncryptionKeyProto, FsServerDefaultsProto, GetDataEncryptionKeyResponseProto,
};
use crate::Result;

use super::proxy::NameServiceProxy;

const LEASE_RENEWAL_INTERVAL_SECS: u64 = 30;

#[derive(Debug, Eq, PartialEq, Hash)]
struct LeasedFile {
    file_id: u64,
    namespace: Option<String>,
}

#[derive(Debug)]
pub(crate) struct NamenodeProtocol {
    proxy: NameServiceProxy,
    client_name: String,
    // Stores files currently opened for writing for lease renewal purposes
    open_files: Arc<Mutex<HashSet<LeasedFile>>>,
    lease_renewer: Mutex<Option<JoinHandle<()>>>,
    // We need a sync mutex for these
    server_defaults: tokio::sync::Mutex<Option<FsServerDefaultsProto>>,
    encryption_key: tokio::sync::Mutex<Option<DataEncryptionKeyProto>>,
}

impl NamenodeProtocol {
    pub(crate) fn new(proxy: NameServiceProxy) -> Self {
        let client_name = format!("hdfs_native_client-{}", Uuid::new_v4().as_hyphenated());
        NamenodeProtocol {
            proxy,
            client_name,
            open_files: Arc::new(Mutex::new(HashSet::new())),
            lease_renewer: Mutex::new(None),
            server_defaults: tokio::sync::Mutex::new(None),
            encryption_key: tokio::sync::Mutex::new(None),
        }
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

    pub(crate) async fn get_cached_server_defaults(&self) -> Result<FsServerDefaultsProto> {
        let mut server_defaults = self.server_defaults.lock().await;
        if let Some(defaults) = server_defaults.as_ref() {
            Ok(defaults.clone())
        } else {
            let defaults = self.get_server_defaults().await?.server_defaults;
            *server_defaults = Some(defaults.clone());
            Ok(defaults)
        }
    }

    pub(crate) async fn get_data_encryption_key(
        &self,
    ) -> Result<GetDataEncryptionKeyResponseProto> {
        let message = hdfs::GetDataEncryptionKeyRequestProto::default();

        let response = self
            .proxy
            .call(
                "getDataEncryptionKey",
                message.encode_length_delimited_to_vec(),
            )
            .await?;

        let decoded = hdfs::GetDataEncryptionKeyResponseProto::decode_length_delimited(response)?;
        debug!("get_data_encryption_key response: {:?}", &decoded);
        Ok(decoded)
    }

    pub(crate) async fn get_cached_data_encryption_key(
        &self,
    ) -> Result<Option<DataEncryptionKeyProto>> {
        let server_defaults = self.get_cached_server_defaults().await?;
        if server_defaults.encrypt_data_transfer() {
            let mut encryption_key = self.encryption_key.lock().await;
            // Check if we have an encryption key and that it's not expired (with a 10 second buffer)
            if encryption_key
                .as_ref()
                .is_some_and(|key| (key.expiry_date as i64) > Utc::now().timestamp_millis() + 10000)
            {
                Ok(encryption_key.clone())
            } else {
                let key = self.get_data_encryption_key().await?.data_encryption_key;
                *encryption_key = key.clone();
                Ok(key)
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn create(
        &self,
        src: &str,
        permission: u32,
        overwrite: bool,
        create_parent: bool,
        replication: Option<u32>,
        block_size: Option<u64>,
    ) -> Result<hdfs::CreateResponseProto> {
        let masked = hdfs::FsPermissionProto { perm: permission };
        let mut create_flag = hdfs::CreateFlagProto::Create as u32;
        if overwrite {
            create_flag |= hdfs::CreateFlagProto::Overwrite as u32;
        }

        let server_defaults = self.get_cached_server_defaults().await?;
        let replication = replication.unwrap_or(server_defaults.replication);
        let block_size = block_size.unwrap_or(server_defaults.block_size);

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

    pub(crate) async fn renew_lease(
        &self,
        namespaces: Vec<String>,
    ) -> Result<hdfs::RenewLeaseResponseProto> {
        let message = hdfs::RenewLeaseRequestProto {
            client_name: self.client_name.clone(),
            namespaces,
        };
        debug!("renewLease request: {:?}", &message);

        let response = self
            .proxy
            .call("renewLease", message.encode_length_delimited_to_vec())
            .await?;

        let decoded = hdfs::RenewLeaseResponseProto::decode_length_delimited(response)?;
        debug!("renewLease response: {:?}", &decoded);
        Ok(decoded)
    }
}

impl Drop for NamenodeProtocol {
    fn drop(&mut self) {
        if let Some(handle) = self.lease_renewer.lock().unwrap().take() {
            handle.abort();
        }
    }
}

// This is a little awkward but this needs to be implemented for Arc<NamenodeProtocol>
// so we can spawn the renewal thread lazily. Maybe there's a better way to handle this
pub(crate) trait LeaseTracker {
    fn add_file_lease(&self, file_id: u64, namespace: Option<String>);

    fn remove_file_lease(&self, file_id: u64, namespace: Option<String>);
}

impl LeaseTracker for Arc<NamenodeProtocol> {
    fn add_file_lease(&self, file_id: u64, namespace: Option<String>) {
        self.open_files
            .lock()
            .unwrap()
            .insert(LeasedFile { file_id, namespace });

        let mut lease_renewer = self.lease_renewer.lock().unwrap();
        if lease_renewer.is_none() {
            *lease_renewer = Some(start_lease_renewal(Arc::clone(self)));
        }
    }

    fn remove_file_lease(&self, file_id: u64, namespace: Option<String>) {
        self.open_files
            .lock()
            .unwrap()
            .remove(&LeasedFile { file_id, namespace });
    }
}

fn start_lease_renewal(protocol: Arc<NamenodeProtocol>) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Track renewal times for each protocol
        let mut last_renewal: Option<SystemTime> = None;

        loop {
            let (writing, namespaces) = {
                let files = protocol.open_files.lock().unwrap();
                let namespaces: HashSet<String> = files
                    .iter()
                    .flat_map(|f| f.namespace.as_ref())
                    .cloned()
                    .collect();

                (!files.is_empty(), namespaces)
            };

            if !writing {
                last_renewal = None;
            } else {
                if last_renewal.is_some_and(|last| {
                    last.elapsed()
                        .is_ok_and(|elapsed| elapsed.as_secs() > LEASE_RENEWAL_INTERVAL_SECS)
                }) {
                    if let Err(err) = protocol.renew_lease(namespaces.into_iter().collect()).await {
                        warn!("Failed to renew lease: {:?}", err);
                    }
                    last_renewal = Some(SystemTime::now());
                }

                if last_renewal.is_none() {
                    last_renewal = Some(SystemTime::now());
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}
