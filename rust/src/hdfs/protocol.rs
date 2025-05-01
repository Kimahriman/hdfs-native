use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use chrono::Utc;
use log::{debug, warn};
use prost::Message;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::acl::AclEntry;
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

    async fn call<T: Message + Default>(
        &self,
        method_name: &'static str,
        message: impl Message,
        write: bool,
    ) -> Result<T> {
        debug!("{} request: {:?}", method_name, &message);

        let response = self
            .proxy
            .call(method_name, message.encode_length_delimited_to_vec(), write)
            .await?;

        let decoded = T::decode_length_delimited(response)?;
        debug!("{} response: {:?}", method_name, &decoded);

        Ok(decoded)
    }

    pub(crate) async fn get_file_info(&self, src: &str) -> Result<hdfs::GetFileInfoResponseProto> {
        let message = hdfs::GetFileInfoRequestProto {
            src: src.to_string(),
        };
        self.call("getFileInfo", message, false).await
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

        self.call("getListing", message, false).await
    }

    pub(crate) async fn get_block_locations(
        &self,
        src: &str,
        offset: u64,
        length: u64,
    ) -> Result<hdfs::GetBlockLocationsResponseProto> {
        let message = hdfs::GetBlockLocationsRequestProto {
            src: src.to_string(),
            offset,
            length,
        };
        self.call("getBlockLocations", message, false).await
    }

    pub(crate) async fn get_server_defaults(&self) -> Result<hdfs::GetServerDefaultsResponseProto> {
        let message = hdfs::GetServerDefaultsRequestProto::default();

        self.call("getServerDefaults", message, false).await
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

        self.call("getDataEncryptionKey", message, false).await
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
                encryption_key.clone_from(&key);
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

        self.call("create", message, true).await
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

        self.call("append", message, true).await
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

        self.call("addBlock", message, true).await
    }

    pub(crate) async fn update_block_for_pipeline(
        &self,
        block: hdfs::ExtendedBlockProto,
    ) -> Result<hdfs::UpdateBlockForPipelineResponseProto> {
        let message = hdfs::UpdateBlockForPipelineRequestProto {
            block,
            client_name: self.client_name.clone(),
        };

        self.call("updateBlockForPipeline", message, true).await
    }

    pub(crate) async fn update_pipeline(
        &self,
        old_block: hdfs::ExtendedBlockProto,
        new_block: hdfs::ExtendedBlockProto,
        new_nodes: Vec<hdfs::DatanodeIdProto>,
        storage_i_ds: Vec<String>,
    ) -> Result<hdfs::UpdatePipelineResponseProto> {
        let message = hdfs::UpdatePipelineRequestProto {
            client_name: self.client_name.clone(),
            old_block,
            new_block,
            new_nodes,
            storage_i_ds,
        };

        self.call("updatePipeline", message, true).await
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
        self.call("complete", message, true).await
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
        self.call("mkdirs", message, true).await
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
        self.call("rename2", message, true).await
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
        self.call("delete", message, true).await
    }

    pub(crate) async fn renew_lease(
        &self,
        namespaces: Vec<String>,
    ) -> Result<hdfs::RenewLeaseResponseProto> {
        let message = hdfs::RenewLeaseRequestProto {
            client_name: self.client_name.clone(),
            namespaces,
        };
        self.call("renewLease", message, true).await
    }

    pub(crate) async fn set_times(
        &self,
        src: &str,
        mtime: u64,
        atime: u64,
    ) -> Result<hdfs::SetTimesResponseProto> {
        let message = hdfs::SetTimesRequestProto {
            src: src.to_string(),
            mtime,
            atime,
        };
        self.call("setTimes", message, true).await
    }

    pub(crate) async fn set_owner(
        &self,
        src: &str,
        owner: Option<&str>,
        group: Option<&str>,
    ) -> Result<hdfs::SetOwnerResponseProto> {
        let message = hdfs::SetOwnerRequestProto {
            src: src.to_string(),
            username: owner.map(str::to_string),
            groupname: group.map(str::to_string),
        };

        self.call("setOwner", message, true).await
    }

    pub(crate) async fn set_permission(
        &self,
        src: &str,
        permission: u32,
    ) -> Result<hdfs::SetPermissionResponseProto> {
        let message = hdfs::SetPermissionRequestProto {
            src: src.to_string(),
            permission: hdfs::FsPermissionProto { perm: permission },
        };
        self.call("setPermission", message, true).await
    }

    pub(crate) async fn set_replication(
        &self,
        src: &str,
        replication: u32,
    ) -> Result<hdfs::SetReplicationResponseProto> {
        let message = hdfs::SetReplicationRequestProto {
            src: src.to_string(),
            replication,
        };
        self.call("setReplication", message, true).await
    }

    pub(crate) async fn get_content_summary(
        &self,
        path: &str,
    ) -> Result<hdfs::GetContentSummaryResponseProto> {
        let message = hdfs::GetContentSummaryRequestProto {
            path: path.to_string(),
        };
        self.call("getContentSummary", message, false).await
    }

    pub(crate) async fn modify_acl_entries(
        &self,
        path: &str,
        acl_spec: Vec<AclEntry>,
    ) -> Result<hdfs::ModifyAclEntriesResponseProto> {
        let message = hdfs::ModifyAclEntriesRequestProto {
            src: path.to_string(),
            acl_spec: acl_spec.into_iter().collect(),
        };

        self.call("modifyAclEntries", message, false).await
    }

    pub(crate) async fn remove_acl_entries(
        &self,
        path: &str,
        acl_spec: Vec<AclEntry>,
    ) -> Result<hdfs::RemoveAclEntriesResponseProto> {
        let message = hdfs::RemoveAclEntriesRequestProto {
            src: path.to_string(),
            acl_spec: acl_spec.into_iter().collect(),
        };

        self.call("removeAclEntries", message, false).await
    }

    pub(crate) async fn remove_default_acl(
        &self,
        path: &str,
    ) -> Result<hdfs::RemoveDefaultAclResponseProto> {
        let message = hdfs::RemoveDefaultAclRequestProto {
            src: path.to_string(),
        };
        self.call("removeDefaultAcl", message, false).await
    }

    pub(crate) async fn remove_acl(&self, path: &str) -> Result<hdfs::RemoveAclResponseProto> {
        let message = hdfs::RemoveAclRequestProto {
            src: path.to_string(),
        };
        self.call("removeAcl", message, false).await
    }

    pub(crate) async fn set_acl(
        &self,
        path: &str,
        acl_spec: Vec<AclEntry>,
    ) -> Result<hdfs::SetAclResponseProto> {
        let message = hdfs::SetAclRequestProto {
            src: path.to_string(),
            acl_spec: acl_spec.into_iter().collect(),
        };
        self.call("setAcl", message, false).await
    }

    pub(crate) async fn get_acl_status(
        &self,
        path: &str,
    ) -> Result<hdfs::GetAclStatusResponseProto> {
        let message = hdfs::GetAclStatusRequestProto {
            src: path.to_string(),
        };
        self.call("getAclStatus", message, false).await
    }

    pub(crate) async fn get_additional_datanode(
        &self,
        path: &str,
        block: &hdfs::ExtendedBlockProto,
        existing_nodes: &[hdfs::DatanodeInfoProto],
        exclude_nodes: &[hdfs::DatanodeInfoProto],
        existing_storage_uuids: &[String],
        num_additional_nodes: u32,
    ) -> Result<hdfs::LocatedBlockProto> {
        let message = hdfs::GetAdditionalDatanodeRequestProto {
            src: path.to_string(),
            blk: block.clone(),
            existings: existing_nodes.to_vec(),
            excludes: exclude_nodes.to_vec(),
            num_additional_nodes,
            client_name: self.client_name.clone(),
            existing_storage_uuids: existing_storage_uuids.to_vec(),
            ..Default::default()
        };

        let response: hdfs::GetAdditionalDatanodeResponseProto =
            self.call("getAdditionalDatanode", message, true).await?;

        Ok(response.block)
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
