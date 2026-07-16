use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::ops::Deref;

use dns_lookup::lookup_addr;
use hadoop_native::config::Configuration as HadoopConfiguration;
use log::debug;
use rand::rng;
use rand::seq::SliceRandom;

use crate::Result;

pub(crate) const DEFAULT_FS: &str = "fs.defaultFS";

const HA_NAMENODES_PREFIX: &str = "dfs.ha.namenodes";
const HA_NAMENODE_RPC_ADDRESS_PREFIX: &str = "dfs.namenode.rpc-address";
const DFS_CLIENT_FAILOVER_RESOLVE_NEEDED: &str = "dfs.client.failover.resolve-needed";
const DFS_CLIENT_FAILOVER_RESOLVER_USE_FQDN: &str = "dfs.client.failover.resolver.useFQDN";
const DFS_CLIENT_FAILOVER_RANDOM_ORDER: &str = "dfs.client.failover.random.order";
const DFS_CLIENT_FAILOVER_PROXY_PROVIDER: &str = "dfs.client.failover.proxy.provider";
const DFS_DATA_TRANSFER_PROTECTION: &str = "dfs.data.transfer.protection";
const DFS_CLIENT_USE_DATANODE_HOSTNAME: &str = "dfs.client.use.datanode.hostname";
const VIEWFS_MOUNTTABLE_PREFIX: &str = "fs.viewfs.mounttable";

#[cfg(feature = "kms")]
pub(crate) const HADOOP_SECURITY_KEY_PROVIDER_PATH: &str = "hadoop.security.key.provider.path";

const DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY: &str =
    "dfs.client.block.write.replace-datanode-on-failure.enable";
const DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY: &str =
    "dfs.client.block.write.replace-datanode-on-failure.policy";
const DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY: &str =
    "dfs.client.block.write.replace-datanode-on-failure.best-effort";

#[derive(Debug, Clone)]
pub(crate) struct Configuration(HadoopConfiguration);

impl Configuration {
    pub(crate) fn new(
        conf_dir: Option<String>,
        conf_map: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        Ok(Self(HadoopConfiguration::new(conf_dir, conf_map)?))
    }

    pub(crate) fn data_transfer_protection_enabled(&self) -> bool {
        self.get(DFS_DATA_TRANSFER_PROTECTION).is_some()
    }

    pub(crate) fn use_datanode_hostname(&self) -> bool {
        self.get_boolean(DFS_CLIENT_USE_DATANODE_HOSTNAME, false)
    }

    pub(crate) fn get_urls_for_nameservice(&self, nameservice: &str) -> Result<Vec<String>> {
        let urls: Vec<String> = self
            .get(&format!("{HA_NAMENODES_PREFIX}.{nameservice}"))
            .into_iter()
            .flat_map(|namenodes| {
                namenodes.split(',').filter_map(|namenode_id| {
                    self.get(&format!(
                        "{HA_NAMENODE_RPC_ADDRESS_PREFIX}.{nameservice}.{namenode_id}"
                    ))
                    .map(str::to_owned)
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
            let mut resolved = Vec::new();
            for url in urls {
                for socket_addr in url.to_socket_addrs()? {
                    if socket_addr.is_ipv4() {
                        if use_fqdn {
                            resolved.push(format!(
                                "{}:{}",
                                lookup_addr(&socket_addr.ip())?,
                                socket_addr.port()
                            ));
                        } else {
                            resolved.push(socket_addr.to_string());
                        }
                    }
                }
            }
            debug!("Namenodes for {nameservice} resolved to {resolved:?}");
            resolved
        } else {
            debug!("Namenodes for {nameservice} without resolving {urls:?}");
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
        self.iter()
            .filter_map(|(key, value)| {
                key.strip_prefix(&format!("{VIEWFS_MOUNTTABLE_PREFIX}.{cluster}.link."))
                    .map(|path| (Some(path.to_owned()), value.to_owned()))
                    .or_else(|| {
                        (key == format!("{VIEWFS_MOUNTTABLE_PREFIX}.{cluster}.linkFallback"))
                            .then(|| (None, value.to_owned()))
                    })
            })
            .collect()
    }

    pub fn get_replace_datanode_on_failure_policy(
        &self,
    ) -> crate::datanode::replacement::ReplaceDatanodeOnFailure {
        use crate::datanode::replacement::{Policy, ReplaceDatanodeOnFailure};

        if !self.get_boolean(
            DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY,
            true,
        ) {
            return ReplaceDatanodeOnFailure::new(Policy::Disable, false);
        }
        let best_effort = self.get_boolean(
            DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY,
            false,
        );
        let policy = match self
            .get(DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY)
            .unwrap_or("DEFAULT")
            .to_uppercase()
            .as_str()
        {
            "NEVER" => Policy::Never,
            "ALWAYS" => Policy::Always,
            _ => Policy::Default,
        };
        ReplaceDatanodeOnFailure::new(policy, best_effort)
    }
}

impl Deref for Configuration {
    type Target = HadoopConfiguration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        Configuration, DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY,
        VIEWFS_MOUNTTABLE_PREFIX,
    };

    #[test]
    fn reads_hdfs_mount_and_write_policy_settings() {
        let config = Configuration::new(
            None,
            Some(HashMap::from([
                (
                    format!("{VIEWFS_MOUNTTABLE_PREFIX}.cluster.link./data"),
                    "hdfs://nameservice/data".to_owned(),
                ),
                (
                    DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY.to_owned(),
                    "true".to_owned(),
                ),
            ])),
        )
        .unwrap();

        assert_eq!(
            config.get_mount_table("cluster"),
            vec![(
                Some("/data".to_owned()),
                "hdfs://nameservice/data".to_owned()
            )]
        );
        assert!(
            config
                .get_replace_datanode_on_failure_policy()
                .is_best_effort()
        );
    }
}
