use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use url::Url;

use crate::acl::{AclEntry, AclStatus};
use crate::common::config::{self, Configuration};
use crate::ec::resolve_ec_policy;
use crate::error::{HdfsError, Result};
use crate::file::{FileReader, FileWriter};
use crate::hdfs::protocol::NamenodeProtocol;
use crate::hdfs::proxy::NameServiceProxy;
use crate::proto::hdfs::hdfs_file_status_proto::FileType;

use crate::proto::hdfs::{ContentSummaryProto, HdfsFileStatusProto};

#[derive(Clone)]
pub struct WriteOptions {
    /// Block size. Default is retrieved from the server.
    pub block_size: Option<u64>,
    /// Replication factor. Default is retrieved from the server.
    pub replication: Option<u32>,
    /// Unix file permission, defaults to 0o644, which is "rw-r--r--" as a Unix permission.
    /// This is the raw octal value represented in base 10.
    pub permission: u32,
    /// Whether to overwrite the file, defaults to false. If true and the
    /// file does not exist, it will result in an error.
    pub overwrite: bool,
    /// Whether to create any missing parent directories, defaults to true. If false
    /// and the parent directory does not exist, an error will be returned.
    pub create_parent: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            block_size: None,
            replication: None,
            permission: 0o644,
            overwrite: false,
            create_parent: true,
        }
    }
}

impl AsRef<WriteOptions> for WriteOptions {
    fn as_ref(&self) -> &WriteOptions {
        self
    }
}

impl WriteOptions {
    /// Set the block_size for the new file
    pub fn block_size(mut self, block_size: u64) -> Self {
        self.block_size = Some(block_size);
        self
    }

    /// Set the replication for the new file
    pub fn replication(mut self, replication: u32) -> Self {
        self.replication = Some(replication);
        self
    }

    /// Set the raw octal permission value for the new file
    pub fn permission(mut self, permission: u32) -> Self {
        self.permission = permission;
        self
    }

    /// Set whether to overwrite an existing file
    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Set whether to create all missing parent directories
    pub fn create_parent(mut self, create_parent: bool) -> Self {
        self.create_parent = create_parent;
        self
    }
}

#[derive(Debug, Clone)]
struct MountLink {
    viewfs_path: String,
    hdfs_path: String,
    protocol: Arc<NamenodeProtocol>,
}

impl MountLink {
    fn new(viewfs_path: &str, hdfs_path: &str, protocol: Arc<NamenodeProtocol>) -> Self {
        // We should never have an empty path, we always want things mounted at root ("/") by default.
        Self {
            viewfs_path: viewfs_path.trim_end_matches("/").to_string(),
            hdfs_path: hdfs_path.trim_end_matches("/").to_string(),
            protocol,
        }
    }
    /// Convert a viewfs path into a name service path if it matches this link
    fn resolve(&self, path: &str) -> Option<String> {
        // Make sure we don't partially match the last component. It either needs to be an exact
        // match to a viewfs path, or needs to match with a trailing slash
        if path == self.viewfs_path {
            Some(self.hdfs_path.clone())
        } else {
            path.strip_prefix(&format!("{}/", self.viewfs_path))
                .map(|relative_path| format!("{}/{}", &self.hdfs_path, relative_path))
        }
    }
}

#[derive(Debug)]
struct MountTable {
    mounts: Vec<MountLink>,
    fallback: MountLink,
}

impl MountTable {
    fn resolve(&self, src: &str) -> (&MountLink, String) {
        for link in self.mounts.iter() {
            if let Some(resolved) = link.resolve(src) {
                return (link, resolved);
            }
        }
        (&self.fallback, self.fallback.resolve(src).unwrap())
    }
}

#[derive(Debug)]
pub struct Client {
    mount_table: Arc<MountTable>,
    config: Arc<Configuration>,
}

impl Client {
    /// Creates a new HDFS Client. The URL must include the protocol and host, and optionally a port.
    /// If a port is included, the host is treated as a single NameNode. If no port is included, the
    /// host is treated as a name service that will be resolved using the HDFS config.
    pub fn new(url: &str) -> Result<Self> {
        let parsed_url = Url::parse(url)?;
        Self::with_config(&parsed_url, Configuration::new()?)
    }

    pub fn new_with_config(url: &str, config: HashMap<String, String>) -> Result<Self> {
        let parsed_url = Url::parse(url)?;
        Self::with_config(&parsed_url, Configuration::new_with_config(config)?)
    }

    pub fn default_with_config(config: HashMap<String, String>) -> Result<Self> {
        let config = Configuration::new_with_config(config)?;
        Self::with_config(&Self::default_fs(&config)?, config)
    }

    fn default_fs(config: &Configuration) -> Result<Url> {
        let url = config
            .get(config::DEFAULT_FS)
            .ok_or(HdfsError::InvalidArgument(format!(
                "No {} setting found",
                config::DEFAULT_FS
            )))?;
        Ok(Url::parse(&url)?)
    }

    fn with_config(url: &Url, config: Configuration) -> Result<Self> {
        let resolved_url = if !url.has_host() {
            let default_url = Self::default_fs(&config)?;
            if url.scheme() != default_url.scheme() || !default_url.has_host() {
                return Err(HdfsError::InvalidArgument(
                    "URL must contain a host".to_string(),
                ));
            }
            default_url
        } else {
            url.clone()
        };

        let mount_table = match url.scheme() {
            "hdfs" => {
                let proxy = NameServiceProxy::new(&resolved_url, &config)?;
                let protocol = Arc::new(NamenodeProtocol::new(proxy));

                MountTable {
                    mounts: Vec::new(),
                    fallback: MountLink::new("/", "/", protocol),
                }
            }
            "viewfs" => Self::build_mount_table(resolved_url.host_str().unwrap(), &config)?,
            _ => {
                return Err(HdfsError::InvalidArgument(
                    "Only `hdfs` and `viewfs` schemes are supported".to_string(),
                ))
            }
        };

        Ok(Self {
            mount_table: Arc::new(mount_table),
            config: Arc::new(config),
        })
    }

    fn build_mount_table(host: &str, config: &Configuration) -> Result<MountTable> {
        let mut mounts: Vec<MountLink> = Vec::new();
        let mut fallback: Option<MountLink> = None;

        for (viewfs_path, hdfs_url) in config.get_mount_table(host).iter() {
            let url = Url::parse(hdfs_url)?;
            if !url.has_host() {
                return Err(HdfsError::InvalidArgument(
                    "URL must contain a host".to_string(),
                ));
            }
            if url.scheme() != "hdfs" {
                return Err(HdfsError::InvalidArgument(
                    "Only hdfs mounts are supported for viewfs".to_string(),
                ));
            }
            let proxy = NameServiceProxy::new(&url, config)?;
            let protocol = Arc::new(NamenodeProtocol::new(proxy));

            if let Some(prefix) = viewfs_path {
                mounts.push(MountLink::new(prefix, url.path(), protocol));
            } else {
                if fallback.is_some() {
                    return Err(HdfsError::InvalidArgument(
                        "Multiple viewfs fallback links found".to_string(),
                    ));
                }
                fallback = Some(MountLink::new("/", url.path(), protocol));
            }
        }

        if let Some(fallback) = fallback {
            // Sort the mount table from longest viewfs path to shortest. This makes sure more specific paths are considered first.
            mounts.sort_by_key(|m| m.viewfs_path.chars().filter(|c| *c == '/').count());
            mounts.reverse();

            Ok(MountTable { mounts, fallback })
        } else {
            Err(HdfsError::InvalidArgument(
                "No viewfs fallback mount found".to_string(),
            ))
        }
    }

    /// Retrieve the file status for the file at `path`.
    pub async fn get_file_info(&self, path: &str) -> Result<FileStatus> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        match link.protocol.get_file_info(&resolved_path).await?.fs {
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
        ListStatusIterator::new(path.to_string(), Arc::clone(&self.mount_table), recursive)
    }

    /// Opens a file reader for the file at `path`. Path should not include a scheme.
    pub async fn read(&self, path: &str) -> Result<FileReader> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        // Get all block locations. Length is actually a signed value, but the proto uses an unsigned value
        let located_info = link
            .protocol
            .get_block_locations(&resolved_path, 0, i64::MAX as u64)
            .await?;

        if let Some(locations) = located_info.locations {
            let ec_schema = if let Some(ec_policy) = locations.ec_policy.as_ref() {
                Some(resolve_ec_policy(ec_policy)?)
            } else {
                None
            };

            if locations.file_encryption_info.is_some() {
                return Err(HdfsError::UnsupportedFeature("File encryption".to_string()));
            }

            Ok(FileReader::new(
                Arc::clone(&link.protocol),
                locations,
                ec_schema,
            ))
        } else {
            Err(HdfsError::FileNotFound(path.to_string()))
        }
    }

    /// Opens a new file for writing. See [WriteOptions] for options and behavior for different
    /// scenarios.
    pub async fn create(
        &self,
        src: &str,
        write_options: impl AsRef<WriteOptions>,
    ) -> Result<FileWriter> {
        let write_options = write_options.as_ref();

        let (link, resolved_path) = self.mount_table.resolve(src);

        let create_response = link
            .protocol
            .create(
                &resolved_path,
                write_options.permission,
                write_options.overwrite,
                write_options.create_parent,
                write_options.replication,
                write_options.block_size,
            )
            .await?;

        match create_response.fs {
            Some(status) => {
                if status.file_encryption_info.is_some() {
                    let _ = self.delete(src, false).await;
                    return Err(HdfsError::UnsupportedFeature("File encryption".to_string()));
                }

                Ok(FileWriter::new(
                    Arc::clone(&link.protocol),
                    resolved_path,
                    status,
                    Arc::clone(&self.config),
                    None,
                ))
            }
            None => Err(HdfsError::FileNotFound(src.to_string())),
        }
    }

    fn needs_new_block(class: &str, msg: &str) -> bool {
        class == "java.lang.UnsupportedOperationException" && msg.contains("NEW_BLOCK")
    }

    /// Opens an existing file for appending. An Err will be returned if the file does not exist. If the
    /// file is replicated, the current block will be appended to until it is full. If the file is erasure
    /// coded, a new block will be created.
    pub async fn append(&self, src: &str) -> Result<FileWriter> {
        let (link, resolved_path) = self.mount_table.resolve(src);

        // Assume the file is replicated and try to append to the current block. If the file is
        // erasure coded, then try again by appending to a new block.
        let append_response = match link.protocol.append(&resolved_path, false).await {
            Err(HdfsError::RPCError(class, msg)) if Self::needs_new_block(&class, &msg) => {
                link.protocol.append(&resolved_path, true).await?
            }
            resp => resp?,
        };

        match append_response.stat {
            Some(status) => {
                if status.file_encryption_info.is_some() {
                    let _ = link
                        .protocol
                        .complete(src, append_response.block.map(|b| b.b), status.file_id)
                        .await;
                    return Err(HdfsError::UnsupportedFeature("File encryption".to_string()));
                }

                Ok(FileWriter::new(
                    Arc::clone(&link.protocol),
                    resolved_path,
                    status,
                    Arc::clone(&self.config),
                    append_response.block,
                ))
            }
            None => Err(HdfsError::FileNotFound(src.to_string())),
        }
    }

    /// Create a new directory at `path` with the given `permission`.
    ///
    /// `permission` is the raw octal value representing the Unix style permission. For example, to
    /// set 755 (`rwxr-x-rx`) permissions, use 0o755.
    ///
    /// If `create_parent` is true, any missing parent directories will be created as well,
    /// otherwise an error will be returned if the parent directory doesn't already exist.
    pub async fn mkdirs(&self, path: &str, permission: u32, create_parent: bool) -> Result<()> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol
            .mkdirs(&resolved_path, permission, create_parent)
            .await
            .map(|_| ())
    }

    /// Renames `src` to `dst`. Returns Ok(()) on success, and Err otherwise.
    pub async fn rename(&self, src: &str, dst: &str, overwrite: bool) -> Result<()> {
        let (src_link, src_resolved_path) = self.mount_table.resolve(src);
        let (dst_link, dst_resolved_path) = self.mount_table.resolve(dst);
        if src_link.viewfs_path == dst_link.viewfs_path {
            src_link
                .protocol
                .rename(&src_resolved_path, &dst_resolved_path, overwrite)
                .await
                .map(|_| ())
        } else {
            Err(HdfsError::InvalidArgument(
                "Cannot rename across different name services".to_string(),
            ))
        }
    }

    /// Deletes the file or directory at `path`. If `recursive` is false and `path` is a non-empty
    /// directory, this will fail. Returns `Ok(true)` if it was successfully deleted.
    pub async fn delete(&self, path: &str, recursive: bool) -> Result<bool> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol
            .delete(&resolved_path, recursive)
            .await
            .map(|r| r.result)
    }

    /// Sets the modified and access times for a file. Times should be in milliseconds from the epoch.
    pub async fn set_times(&self, path: &str, mtime: u64, atime: u64) -> Result<()> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol
            .set_times(&resolved_path, mtime, atime)
            .await?;
        Ok(())
    }

    /// Optionally sets the owner and group for a file.
    pub async fn set_owner(
        &self,
        path: &str,
        owner: Option<&str>,
        group: Option<&str>,
    ) -> Result<()> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol
            .set_owner(&resolved_path, owner, group)
            .await?;
        Ok(())
    }

    /// Sets permissions for a file. Permission should be an octal number reprenting the Unix style
    /// permission.
    ///
    /// For example, to set permissions to rwxr-xr-x:
    /// ```rust
    /// # async fn func() {
    /// # let client = hdfs_native::Client::new("localhost:9000").unwrap();
    /// client.set_permission("/path", 0o755).await.unwrap();
    /// }
    /// ```
    pub async fn set_permission(&self, path: &str, permission: u32) -> Result<()> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol
            .set_permission(&resolved_path, permission)
            .await?;
        Ok(())
    }

    /// Sets the replication for a file.
    pub async fn set_replication(&self, path: &str, replication: u32) -> Result<bool> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        let result = link
            .protocol
            .set_replication(&resolved_path, replication)
            .await?
            .result;

        Ok(result)
    }

    /// Gets a content summary for a file or directory rooted at `path`.
    pub async fn get_content_summary(&self, path: &str) -> Result<ContentSummary> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        let result = link
            .protocol
            .get_content_summary(&resolved_path)
            .await?
            .summary;

        Ok(result.into())
    }

    /// Update ACL entries for file or directory at `path`. Existing entries will remain.
    pub async fn modify_acl_entries(&self, path: &str, acl_spec: Vec<AclEntry>) -> Result<()> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol
            .modify_acl_entries(&resolved_path, acl_spec)
            .await?;

        Ok(())
    }

    /// Remove specific ACL entries for file or directory at `path`.
    pub async fn remove_acl_entries(&self, path: &str, acl_spec: Vec<AclEntry>) -> Result<()> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol
            .remove_acl_entries(&resolved_path, acl_spec)
            .await?;

        Ok(())
    }

    /// Remove all default ACLs for file or directory at `path`.
    pub async fn remove_default_acl(&self, path: &str) -> Result<()> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol.remove_default_acl(&resolved_path).await?;

        Ok(())
    }

    /// Remove all ACL entries for file or directory at `path`.
    pub async fn remove_acl(&self, path: &str) -> Result<()> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol.remove_acl(&resolved_path).await?;

        Ok(())
    }

    /// Override all ACL entries for file or directory at `path`. If only access ACLs are provided,
    /// default ACLs are maintained. Likewise if only default ACLs are provided, access ACLs are
    /// maintained.
    pub async fn set_acl(&self, path: &str, acl_spec: Vec<AclEntry>) -> Result<()> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        link.protocol.set_acl(&resolved_path, acl_spec).await?;

        Ok(())
    }

    /// Get the ACL status for the file or directory at `path`.
    pub async fn get_acl_status(&self, path: &str) -> Result<AclStatus> {
        let (link, resolved_path) = self.mount_table.resolve(path);
        Ok(link
            .protocol
            .get_acl_status(&resolved_path)
            .await?
            .result
            .into())
    }
}

impl Default for Client {
    /// Creates a new HDFS Client based on the fs.defaultFS setting. Panics if the config files fail to load,
    /// no defaultFS is defined, or the defaultFS is invalid.
    fn default() -> Self {
        Self::default_with_config(Default::default()).expect("Failed to create default client")
    }
}

pub(crate) struct DirListingIterator {
    path: String,
    resolved_path: String,
    link: MountLink,
    files_only: bool,
    partial_listing: VecDeque<HdfsFileStatusProto>,
    remaining: u32,
    last_seen: Vec<u8>,
}

impl DirListingIterator {
    fn new(path: String, mount_table: &Arc<MountTable>, files_only: bool) -> Self {
        let (link, resolved_path) = mount_table.resolve(&path);

        DirListingIterator {
            path,
            resolved_path,
            link: link.clone(),
            files_only,
            partial_listing: VecDeque::new(),
            remaining: 1,
            last_seen: Vec::new(),
        }
    }

    async fn get_next_batch(&mut self) -> Result<bool> {
        let listing = self
            .link
            .protocol
            .get_listing(&self.resolved_path, self.last_seen.clone(), false)
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
            Ok(!self.partial_listing.is_empty())
        } else {
            Err(HdfsError::FileNotFound(self.path.clone()))
        }
    }

    pub async fn next(&mut self) -> Option<Result<FileStatus>> {
        if self.partial_listing.is_empty() && self.remaining > 0 {
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
    mount_table: Arc<MountTable>,
    recursive: bool,
    iters: Arc<tokio::sync::Mutex<Vec<DirListingIterator>>>,
}

impl ListStatusIterator {
    fn new(path: String, mount_table: Arc<MountTable>, recursive: bool) -> Self {
        let initial = DirListingIterator::new(path.clone(), &mount_table, false);

        ListStatusIterator {
            mount_table,
            recursive,
            iters: Arc::new(tokio::sync::Mutex::new(vec![initial])),
        }
    }

    pub async fn next(&self) -> Option<Result<FileStatus>> {
        let mut next_file: Option<Result<FileStatus>> = None;
        let mut iters = self.iters.lock().await;
        while next_file.is_none() {
            if let Some(iter) = iters.last_mut() {
                if let Some(file_result) = iter.next().await {
                    if let Ok(file) = file_result {
                        // Return the directory as the next result, but start traversing into that directory
                        // next if we're doing a recursive listing
                        if file.isdir && self.recursive {
                            iters.push(DirListingIterator::new(
                                file.path.clone(),
                                &self.mount_table,
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
                    iters.pop();
                }
            } else {
                // There's nothing left, just return None
                break;
            }
        }

        next_file
    }

    pub fn into_stream(self) -> BoxStream<'static, Result<FileStatus>> {
        let listing = stream::unfold(self, |state| async move {
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
    pub replication: Option<u32>,
    pub blocksize: Option<u64>,
}

impl FileStatus {
    fn from(value: HdfsFileStatusProto, base_path: &str) -> Self {
        let mut path = base_path.trim_end_matches("/").to_string();
        let relative_path = std::str::from_utf8(&value.path).unwrap();
        if !relative_path.is_empty() {
            path.push('/');
            path.push_str(relative_path);
        }

        FileStatus {
            isdir: value.file_type() == FileType::IsDir,
            path,
            length: value.length as usize,
            permission: value.permission.perm as u16,
            owner: value.owner,
            group: value.group,
            modification_time: value.modification_time,
            access_time: value.access_time,
            replication: value.block_replication,
            blocksize: value.blocksize,
        }
    }
}

#[derive(Debug)]
pub struct ContentSummary {
    pub length: u64,
    pub file_count: u64,
    pub directory_count: u64,
    pub quota: u64,
    pub space_consumed: u64,
    pub space_quota: u64,
}

impl From<ContentSummaryProto> for ContentSummary {
    fn from(value: ContentSummaryProto) -> Self {
        ContentSummary {
            length: value.length,
            file_count: value.file_count,
            directory_count: value.directory_count,
            quota: value.quota,
            space_consumed: value.space_consumed,
            space_quota: value.space_quota,
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use url::Url;

    use crate::{
        common::config::Configuration,
        hdfs::{protocol::NamenodeProtocol, proxy::NameServiceProxy},
        Client,
    };

    use super::{MountLink, MountTable};

    fn create_protocol(url: &str) -> Arc<NamenodeProtocol> {
        let proxy =
            NameServiceProxy::new(&Url::parse(url).unwrap(), &Configuration::new().unwrap())
                .unwrap();
        Arc::new(NamenodeProtocol::new(proxy))
    }

    #[test]
    fn test_default_fs() {
        assert!(Client::default_with_config(
            vec![("fs.defaultFS".to_string(), "hdfs://test:9000".to_string())]
                .into_iter()
                .collect(),
        )
        .is_ok());

        assert!(Client::default_with_config(
            vec![("fs.defaultFS".to_string(), "hdfs://".to_string())]
                .into_iter()
                .collect(),
        )
        .is_err());

        assert!(Client::new_with_config(
            "hdfs://",
            vec![("fs.defaultFS".to_string(), "hdfs://test:9000".to_string())]
                .into_iter()
                .collect(),
        )
        .is_ok());

        assert!(Client::new_with_config(
            "hdfs://",
            vec![("fs.defaultFS".to_string(), "hdfs://".to_string())]
                .into_iter()
                .collect(),
        )
        .is_err());

        assert!(Client::new_with_config(
            "hdfs://",
            vec![("fs.defaultFS".to_string(), "viewfs://test".to_string())]
                .into_iter()
                .collect(),
        )
        .is_err());
    }

    #[test]
    fn test_mount_link_resolve() {
        let protocol = create_protocol("hdfs://127.0.0.1:9000");
        let link = MountLink::new("/view", "/hdfs", protocol);

        assert_eq!(link.resolve("/view/dir/file").unwrap(), "/hdfs/dir/file");
        assert_eq!(link.resolve("/view").unwrap(), "/hdfs");
        assert!(link.resolve("/hdfs/path").is_none());
    }

    #[test]
    fn test_fallback_link() {
        let protocol = create_protocol("hdfs://127.0.0.1:9000");
        let link = MountLink::new("", "/hdfs", Arc::clone(&protocol));

        assert_eq!(link.resolve("/path/to/file").unwrap(), "/hdfs/path/to/file");
        assert_eq!(link.resolve("/").unwrap(), "/hdfs/");
        assert_eq!(link.resolve("/hdfs/path").unwrap(), "/hdfs/hdfs/path");

        let link = MountLink::new("", "", protocol);
        assert_eq!(link.resolve("/").unwrap(), "/");
    }

    #[test]
    fn test_mount_table_resolve() {
        let link1 = MountLink::new(
            "/mount1",
            "/path1/nested",
            create_protocol("hdfs://127.0.0.1:9000"),
        );
        let link2 = MountLink::new(
            "/mount2",
            "/path2",
            create_protocol("hdfs://127.0.0.1:9001"),
        );
        let link3 = MountLink::new(
            "/mount3/nested",
            "/path3",
            create_protocol("hdfs://127.0.0.1:9002"),
        );
        let fallback = MountLink::new("/", "/path4", create_protocol("hdfs://127.0.0.1:9003"));

        let mount_table = MountTable {
            mounts: vec![link1, link2, link3],
            fallback,
        };

        // Exact mount path resolves to the exact HDFS path
        let (link, resolved) = mount_table.resolve("/mount1");
        assert_eq!(link.viewfs_path, "/mount1");
        assert_eq!(resolved, "/path1/nested");

        // Trailing slash is treated the same
        let (link, resolved) = mount_table.resolve("/mount1/");
        assert_eq!(link.viewfs_path, "/mount1");
        assert_eq!(resolved, "/path1/nested/");

        // Doesn't do partial matches on a directory name
        let (link, resolved) = mount_table.resolve("/mount12");
        assert_eq!(link.viewfs_path, "");
        assert_eq!(resolved, "/path4/mount12");

        let (link, resolved) = mount_table.resolve("/mount3/file");
        assert_eq!(link.viewfs_path, "");
        assert_eq!(resolved, "/path4/mount3/file");

        let (link, resolved) = mount_table.resolve("/mount3/nested/file");
        assert_eq!(link.viewfs_path, "/mount3/nested");
        assert_eq!(resolved, "/path3/file");
    }
}
