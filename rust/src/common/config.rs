use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};

use dns_lookup::lookup_addr;
use log::debug;
use rand::rng;
use rand::seq::SliceRandom;

use crate::Result;

const HADOOP_CONF_DIR: &str = "HADOOP_CONF_DIR";
const HADOOP_HOME: &str = "HADOOP_HOME";

pub(crate) const DEFAULT_FS: &str = "fs.defaultFS";

// Name Service settings
const HA_NAMENODES_PREFIX: &str = "dfs.ha.namenodes";
const HA_NAMENODE_RPC_ADDRESS_PREFIX: &str = "dfs.namenode.rpc-address";
const DFS_CLIENT_FAILOVER_RESOLVE_NEEDED: &str = "dfs.client.failover.resolve-needed";
const DFS_CLIENT_FAILOVER_RESOLVER_USE_FQDN: &str = "dfs.client.failover.resolver.useFQDN";
const DFS_CLIENT_FAILOVER_RANDOM_ORDER: &str = "dfs.client.failover.random.order";
const DFS_CLIENT_FAILOVER_PROXY_PROVIDER: &str = "dfs.client.failover.proxy.provider";
const DFS_DATA_TRANSFER_PROTECTION: &str = "dfs.data.transfer.protection";

const HADOOP_SECURITY_AUTHENTICATION: &str = "hadoop.security.authentication";

// Viewfs settings
const VIEWFS_MOUNTTABLE_PREFIX: &str = "fs.viewfs.mounttable";

const DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY: &str =
    "dfs.client.block.write.replace-datanode-on-failure.enable";
const DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY: &str =
    "dfs.client.block.write.replace-datanode-on-failure.policy";
const DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY: &str =
    "dfs.client.block.write.replace-datanode-on-failure.best-effort";

#[derive(Debug, Clone)]
pub struct Configuration {
    map: HashMap<String, String>,
}

impl Configuration {
    pub fn new() -> Result<Self> {
        let mut map: HashMap<String, String> = HashMap::new();

        if let Some(conf_dir) = Self::get_conf_dir() {
            for file in ["core-site.xml", "hdfs-site.xml"] {
                let config_path = conf_dir.join(file);
                if config_path.as_path().exists() {
                    Self::read_from_file(config_path.as_path())?
                        .into_iter()
                        .for_each(|(key, value)| {
                            map.insert(key, value);
                        })
                }
            }
        }

        Ok(Configuration { map })
    }

    pub fn new_with_config(conf_map: HashMap<String, String>) -> Result<Self> {
        let mut conf = Self::new()?;
        conf.map.extend(conf_map);
        Ok(conf)
    }

    /// Get a value from the config, returning None if the key wasn't defined.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(|s| s.as_ref())
    }

    fn get_boolean(&self, key: &str, default: bool) -> bool {
        self.get(key)
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(default)
    }

    pub(crate) fn security_enabled(&self) -> bool {
        self.get(HADOOP_SECURITY_AUTHENTICATION)
            .is_some_and(|c| c != "simple")
    }

    pub(crate) fn data_transfer_protection_enabled(&self) -> bool {
        self.get(DFS_DATA_TRANSFER_PROTECTION).is_some()
    }

    pub(crate) fn get_urls_for_nameservice(&self, nameservice: &str) -> Result<Vec<String>> {
        let urls: Vec<String> = self
            .map
            .get(&format!("{HA_NAMENODES_PREFIX}.{nameservice}"))
            .into_iter()
            .flat_map(|namenodes| {
                namenodes.split(',').flat_map(|namenode_id| {
                    self.map
                        .get(&format!(
                            "{HA_NAMENODE_RPC_ADDRESS_PREFIX}.{nameservice}.{namenode_id}"
                        ))
                        .map(|s| s.to_string())
                })
            })
            .collect();

        let mut urls = if self.get_boolean(
            &format!("{DFS_CLIENT_FAILOVER_RESOLVE_NEEDED}.{nameservice}"),
            false,
        ) {
            let use_fqdn = self.get_boolean(
                &format!("{DFS_CLIENT_FAILOVER_RESOLVER_USE_FQDN}.{nameservice}"),
                true,
            );

            let mut resolved_urls: Vec<String> = Vec::new();
            for url in urls {
                for socket_addr in url.to_socket_addrs()? {
                    if socket_addr.is_ipv4() {
                        if use_fqdn {
                            let fqdn = lookup_addr(&socket_addr.ip())?;
                            resolved_urls.push(format!("{}:{}", fqdn, socket_addr.port()));
                        } else {
                            resolved_urls.push(socket_addr.to_string());
                        }
                    }
                }
            }
            debug!(
                "Namenodes for {} resolved to {:?}",
                nameservice, resolved_urls
            );

            resolved_urls
        } else {
            debug!("Namenodes for {} without resolving {:?}", nameservice, urls);
            urls
        };

        if self.get_boolean(
            &format!("{DFS_CLIENT_FAILOVER_RANDOM_ORDER}.{nameservice}"),
            false,
        ) {
            urls.shuffle(&mut rng());
        }
        Ok(urls)
    }

    pub(crate) fn get_proxy_for_nameservice(&self, nameservice: &str) -> Option<&str> {
        self.get(&format!(
            "{DFS_CLIENT_FAILOVER_PROXY_PROVIDER}.{nameservice}"
        ))
    }

    pub(crate) fn get_mount_table(&self, cluster: &str) -> Vec<(Option<String>, String)> {
        self.map
            .iter()
            .flat_map(|(key, value)| {
                if let Some(path) =
                    key.strip_prefix(&format!("{VIEWFS_MOUNTTABLE_PREFIX}.{cluster}.link."))
                {
                    Some((Some(path.to_string()), value.to_string()))
                } else if key == &format!("{VIEWFS_MOUNTTABLE_PREFIX}.{cluster}.linkFallback") {
                    Some((None, value.to_string()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get the replace datanode on failure policy from configuration
    pub fn get_replace_datanode_on_failure_policy(
        &self,
    ) -> crate::hdfs::replace_datanode::ReplaceDatanodeOnFailure {
        use crate::hdfs::replace_datanode::{Policy, ReplaceDatanodeOnFailure};

        let enabled = self.get_boolean(
            DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY,
            true,
        );
        if !enabled {
            return ReplaceDatanodeOnFailure::new(Policy::Disable, false);
        }

        let best_effort = self.get_boolean(
            DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY,
            false,
        );

        let policy_str = self
            .get(DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY)
            .unwrap_or("DEFAULT")
            .to_uppercase();

        let policy = match policy_str.as_str() {
            "NEVER" => Policy::Never,
            "DEFAULT" => Policy::Default,
            "ALWAYS" => Policy::Always,
            _ => Policy::Default,
        };

        ReplaceDatanodeOnFailure::new(policy, best_effort)
    }

    fn read_from_file(path: &Path) -> Result<Vec<(String, String)>> {
        let content = fs::read_to_string(path)?;

        let resolver = EntityResolver::new(path)?;
        let entity_resolver = |_: Option<&str>, uri: &str| resolver.resolve(uri);
        let opts = roxmltree::ParsingOptions {
            allow_dtd: true,
            entity_resolver: Some(&entity_resolver),
            ..Default::default()
        };
        let tree = roxmltree::Document::parse_with_options(&content, opts)?;

        let pairs = tree
            .root()
            .children()
            .find(|d| d.tag_name().name() == "configuration")
            .into_iter()
            .flat_map(|config| {
                config
                    .children()
                    .filter(|c| c.tag_name().name() == "property")
            })
            .flat_map(|property| {
                let name = property.children().find(|n| n.tag_name().name() == "name");
                let value = property.children().find(|n| n.tag_name().name() == "value");

                match (name, value) {
                    (Some(name), Some(value)) => match (name.text(), value.text()) {
                        (Some(name), Some(text)) => Some((name.to_string(), text.to_string())),
                        _ => None,
                    },
                    _ => None,
                }
            });

        Ok(pairs.collect())
    }

    fn get_conf_dir() -> Option<PathBuf> {
        match env::var(HADOOP_CONF_DIR) {
            Ok(dir) => Some(PathBuf::from(dir)),
            Err(_) => match env::var(HADOOP_HOME) {
                Ok(dir) => Some([&dir, "etc/hadoop"].iter().collect()),
                Err(_) => None,
            },
        }
    }
}

/// A simple entity resolver to resolve the XML system entities.
struct EntityResolver {
    basepath: PathBuf,
    bump: bumpalo::Bump,

    file_length_limit: u64,
    allocated_size_limit: u64,
}

impl EntityResolver {
    fn new(config_file_path: &Path) -> Result<Self> {
        let config_file_path = config_file_path.canonicalize()?;
        let basepath = match config_file_path.parent() {
            Some(p) => p.to_path_buf(),
            None => {
                // This should never happen, because the `config_file_path` is
                // expected to be a file. And so its parent should be a valid
                // directory.
                return Err(crate::HdfsError::InvalidPath(format!(
                    "invalid base path for configuration file: {}",
                    config_file_path.display()
                )));
            }
        };

        Ok(Self {
            basepath,
            bump: bumpalo::Bump::new(),

            file_length_limit: 16 * 1024 * 1024,    // 16 MiB
            allocated_size_limit: 16 * 1024 * 1024, // 16 MiB
        })
    }

    fn resolve<'a>(&'a self, uri: &str) -> core::result::Result<Option<&'a str>, String> {
        // Load full path.
        let full_path = self.resolve_full_path(uri)?;

        // Make sure the file exists.
        if !full_path.exists() {
            return Ok(None);
        }

        // Get metadata.
        let entity_file_metadata = match fs::metadata(&full_path) {
            Ok(m) => m,
            Err(e) => {
                return Err(format!(
                    "failed to get metadata of entity file {}: {}",
                    full_path.display(),
                    e
                ))
            }
        };
        let entity_file_size = entity_file_metadata.len();

        // Make sure the file size is reasonable.
        if entity_file_size > self.file_length_limit {
            return Err(format!(
                "entity file {} is too large ({} bytes)",
                full_path.display(),
                entity_file_size
            ));
        }

        // Make sure the allocated size is reasonable.
        let entity_file_allocated_size = self.bump.allocated_bytes() as u64;
        if entity_file_allocated_size + entity_file_size > self.allocated_size_limit {
            return Err(format!(
                "entity resolver has no more memory (allocated {} bytes, entity file size {} bytes)",
                entity_file_allocated_size,
                entity_file_size,
            ));
        }

        // Read the file content and move it into the bump arena.
        let entity_file_content = fs::read_to_string(&full_path).map_err(|e| {
            format!(
                "read entity file content (path {}): {}",
                full_path.display(),
                e
            )
        })?;
        let entity_file_content = self.bump.alloc_str(&entity_file_content);

        // Return the content.
        Ok(Some(entity_file_content))
    }

    fn resolve_full_path(&self, uri: &str) -> core::result::Result<PathBuf, String> {
        use std::path::{Component, Path};

        // Build the full path to the entity file.
        //
        // To make sure it is safe, we only allow relative path with `.` or
        // normal components.
        let path = Path::new(uri);
        let mut full_path = self.basepath.clone();
        for c in path.components() {
            match c {
                Component::CurDir => {}
                Component::ParentDir => {
                    return Err(
                        "parent directory components `..` are not allowed in entity URIs"
                            .to_string(),
                    );
                }
                Component::Normal(p) => full_path.push(p),
                Component::RootDir => {
                    return Err("absolute paths are not allowed in entity URIs".to_string())
                }
                Component::Prefix(p) => {
                    return Err(format!(
                        "absolute paths (with prefix {p:?}) are not allowed in entity URIs"
                    ))
                }
            }
        }

        Ok(full_path)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::net::IpAddr;

    use dns_lookup::lookup_addr;

    use crate::common::config::DFS_CLIENT_FAILOVER_RESOLVER_USE_FQDN;

    use super::{
        Configuration, EntityResolver, DFS_CLIENT_FAILOVER_RESOLVE_NEEDED,
        DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY,
        DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY,
        DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY, HA_NAMENODES_PREFIX,
        HA_NAMENODE_RPC_ADDRESS_PREFIX, VIEWFS_MOUNTTABLE_PREFIX,
    };

    #[test]
    fn test_mount_table_config() {
        let mounts = [
            ("clusterX", "/view1", "/hdfs1"),
            ("clusterX", "/view2", "/hdfs2"),
            ("clusterY", "/view3", "/hdfs3"),
        ];

        let fallbacks = [("clusterX", "/hdfs4"), ("clusterY", "/hdfs5")];

        let config = Configuration {
            map: mounts
                .iter()
                .map(|(cluster, viewfs_path, hdfs_path)| {
                    (
                        format!("{VIEWFS_MOUNTTABLE_PREFIX}.{cluster}.link.{viewfs_path}"),
                        format!("hdfs://127.0.0.1:9000{hdfs_path}"),
                    )
                })
                .chain(fallbacks.iter().map(|(cluster, hdfs_path)| {
                    (
                        format!("{VIEWFS_MOUNTTABLE_PREFIX}.{cluster}.linkFallback"),
                        format!("hdfs://127.0.0.1:9000{hdfs_path}"),
                    )
                }))
                .collect(),
        };

        let mut mount_table = config.get_mount_table("clusterX");
        mount_table.sort();
        assert_eq!(
            vec![
                (None, "hdfs://127.0.0.1:9000/hdfs4".to_string()),
                (
                    Some("/view1".to_string()),
                    "hdfs://127.0.0.1:9000/hdfs1".to_string()
                ),
                (
                    Some("/view2".to_string()),
                    "hdfs://127.0.0.1:9000/hdfs2".to_string()
                )
            ],
            mount_table
        );

        let mut mount_table = config.get_mount_table("clusterY");
        mount_table.sort();
        assert_eq!(
            mount_table,
            vec![
                (None, "hdfs://127.0.0.1:9000/hdfs5".to_string()),
                (
                    Some("/view3".to_string()),
                    "hdfs://127.0.0.1:9000/hdfs3".to_string()
                )
            ]
        );
    }

    #[test]
    fn test_namenode_resolving() {
        let mut config = Configuration {
            map: vec![
                (
                    format!("{}.{}", HA_NAMENODES_PREFIX, "test"),
                    "namenode".to_string(),
                ),
                (
                    format!(
                        "{}.{}.{}",
                        HA_NAMENODE_RPC_ADDRESS_PREFIX, "test", "namenode"
                    ),
                    "localhost:9000".to_string(),
                ),
                (
                    format!("{}.{}", DFS_CLIENT_FAILOVER_RESOLVE_NEEDED, "test"),
                    "true".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        };

        let urls = config.get_urls_for_nameservice("test").unwrap();
        let fqdn = lookup_addr(&IpAddr::from([127, 0, 0, 1])).unwrap();
        assert_eq!(urls.len(), 1, "{urls:?}");
        assert_eq!(urls[0], format!("{fqdn}:9000"));

        config.map.insert(
            format!("{}.{}", DFS_CLIENT_FAILOVER_RESOLVER_USE_FQDN, "test"),
            "false".to_string(),
        );

        let urls = config.get_urls_for_nameservice("test").unwrap();
        assert_eq!(urls.len(), 1, "{urls:?}");
        assert_eq!(urls[0], "127.0.0.1:9000");
    }

    #[test]
    fn test_replace_datanode_policy_config() {
        // Test default policy
        let config = Configuration {
            map: HashMap::new(),
        };
        let policy = config.get_replace_datanode_on_failure_policy();
        assert!(!policy.is_best_effort());

        // Test disabled policy
        let config = Configuration {
            map: [(
                DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY.to_string(),
                "false".to_string(),
            )]
            .into_iter()
            .collect(),
        };
        let policy = config.get_replace_datanode_on_failure_policy();
        assert!(!policy.is_best_effort());

        // Test NEVER policy
        let config = Configuration {
            map: [
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY.to_string(),
                    "true".to_string(),
                ),
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY.to_string(),
                    "NEVER".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let policy = config.get_replace_datanode_on_failure_policy();
        assert!(!policy.is_best_effort());

        // Test ALWAYS policy
        let config = Configuration {
            map: [
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY.to_string(),
                    "true".to_string(),
                ),
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY.to_string(),
                    "ALWAYS".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let policy = config.get_replace_datanode_on_failure_policy();
        assert!(!policy.is_best_effort());

        // Test best-effort disabled
        let config = Configuration {
            map: [
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY.to_string(),
                    "true".to_string(),
                ),
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY.to_string(),
                    "false".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let policy = config.get_replace_datanode_on_failure_policy();
        assert!(!policy.is_best_effort());

        // Test best-effort enabled (explicit)
        let config = Configuration {
            map: [
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY.to_string(),
                    "true".to_string(),
                ),
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY.to_string(),
                    "true".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let policy = config.get_replace_datanode_on_failure_policy();
        assert!(policy.is_best_effort());

        // Test best-effort with invalid value (should use default false)
        let config = Configuration {
            map: [
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY.to_string(),
                    "true".to_string(),
                ),
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY.to_string(),
                    "invalid".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let policy = config.get_replace_datanode_on_failure_policy();
        assert!(!policy.is_best_effort()); // Invalid values treated as false

        // Test policy with both policy and best-effort config
        let config = Configuration {
            map: [
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY.to_string(),
                    "true".to_string(),
                ),
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY.to_string(),
                    "ALWAYS".to_string(),
                ),
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY.to_string(),
                    "false".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let policy = config.get_replace_datanode_on_failure_policy();
        assert!(!policy.is_best_effort());
    }

    #[test]
    fn test_resolve_full_path() {
        use tempfile::tempdir;

        // Create a temporary directory for testing
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let basepath = temp_dir.path().to_path_buf();

        let resolver = EntityResolver {
            basepath: basepath.clone(),
            bump: bumpalo::Bump::new(),
            file_length_limit: 16 * 1024 * 1024,
            allocated_size_limit: 16 * 1024 * 1024,
        };

        // Test normal relative path
        let result = resolver.resolve_full_path("config.xml");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), basepath.join("config.xml"));

        // Test relative path with subdirectory
        let result = resolver.resolve_full_path("subdir/config.xml");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), basepath.join("subdir").join("config.xml"));

        // Test path with current directory component
        let result = resolver.resolve_full_path("./config.xml");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), basepath.join("config.xml"));

        // Test path with multiple current directory components
        let result = resolver.resolve_full_path("./subdir/./config.xml");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), basepath.join("subdir").join("config.xml"));

        // Test path with parent directory component (should fail)
        let result = resolver.resolve_full_path("../config.xml");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "parent directory components `..` are not allowed in entity URIs"
        );

        // Test path with parent directory in the middle (should fail)
        let result = resolver.resolve_full_path("subdir/../config.xml");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "parent directory components `..` are not allowed in entity URIs"
        );

        // Test absolute path (should fail)
        let result = resolver.resolve_full_path("/absolute/path/config.xml");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "absolute paths are not allowed in entity URIs"
        );

        // Test absolute path (should fail)
        let result = resolver.resolve_full_path("/etc/passwd");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "absolute paths are not allowed in entity URIs"
        );

        // Test empty path
        let result = resolver.resolve_full_path("");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), basepath);

        // Test path with only current directory
        let result = resolver.resolve_full_path(".");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), basepath);

        // Test nested subdirectories
        let result = resolver.resolve_full_path("dir1/dir2/dir3/config.xml");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            basepath
                .join("dir1")
                .join("dir2")
                .join("dir3")
                .join("config.xml")
        );
    }

    #[test]
    fn test_config_read_from_file() {
        use indoc::indoc;
        use std::fs;
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp dir");

        // The main config file.
        let config_path = temp_dir.path().join("test-config.xml");
        let config_content = indoc! { r#"
            <?xml version="1.0"?>
            <!DOCTYPE configuration [
                <!ENTITY example-entity SYSTEM "example-entity.xml">
            ]>
            <configuration>
                <property>
                    <name>fs.defaultFS</name>
                    <value>hdfs://localhost:9000</value>
                </property>
                <property>
                    <name>dfs.replication</name>
                    <value>3</value>
                </property>
                &example-entity;
            </configuration>
        "# };
        fs::write(&config_path, config_content).expect("Failed to write config file");

        // The entity file.
        let example_entity_path = temp_dir.path().join("example-entity.xml");
        let example_entity_content = indoc! { r#"
            <?xml version="1.0"?>
            <property>
                <name>custom.property</name>
                <value>entity-value</value>
            </property>
        "# };
        fs::write(example_entity_path, example_entity_content)
            .expect("Failed to write entity file");

        // Get the config map from files.
        let map: HashMap<String, String> = {
            let pairs =
                Configuration::read_from_file(&config_path).expect("Failed to read config file");
            let mut res = HashMap::new();
            for (key, value) in pairs {
                res.insert(key, value);
            }
            res
        };
        assert_eq!(
            map.get("fs.defaultFS").map(|s| s.as_str()),
            Some("hdfs://localhost:9000")
        );
        assert_eq!(map.get("dfs.replication").map(|s| s.as_str()), Some("3"));
        assert_eq!(
            map.get("custom.property").map(|s| s.as_str()),
            Some("entity-value")
        );
    }
}
