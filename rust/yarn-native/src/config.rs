use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::ops::Deref;

use hadoop_native::config::Configuration as HadoopConfiguration;

use crate::{Result, YarnError};

pub const RM_ADDRESS: &str = "yarn.resourcemanager.address";
pub const RM_HOSTNAME: &str = "yarn.resourcemanager.hostname";
pub const RM_HA_ENABLED: &str = "yarn.resourcemanager.ha.enabled";
pub const RM_HA_IDS: &str = "yarn.resourcemanager.ha.rm-ids";
pub const CLIENT_FAILOVER_MAX_ATTEMPTS: &str = "yarn.client.failover-max-attempts";
pub const CLIENT_FAILOVER_SLEEP_BASE_MS: &str = "yarn.client.failover-sleep-base-ms";
pub const CLIENT_FAILOVER_SLEEP_MAX_MS: &str = "yarn.client.failover-sleep-max-ms";

const DEFAULT_RM_ADDRESS: &str = "0.0.0.0:8032";
const DEFAULT_RM_PORT: u16 = 8032;
const DEFAULT_FAILOVER_MAX_ATTEMPTS: u32 = 15;
const DEFAULT_FAILOVER_SLEEP_BASE_MS: u64 = 500;
const DEFAULT_FAILOVER_SLEEP_MAX_MS: u64 = 15_000;

/// Hadoop configuration with YARN ResourceManager and failover accessors.
#[derive(Debug, Clone)]
pub struct Configuration(HadoopConfiguration);

impl Configuration {
    /// Load Hadoop XML configuration and apply explicit overrides.
    pub fn new(
        conf_dir: Option<String>,
        conf_map: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        Ok(Self(HadoopConfiguration::new(conf_dir, conf_map)?))
    }

    /// Asynchronously load Hadoop XML configuration and apply explicit overrides.
    pub async fn new_async(
        conf_dir: Option<String>,
        conf_map: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        Ok(Self(
            HadoopConfiguration::new_async(conf_dir, conf_map).await?,
        ))
    }

    /// Return configured ResourceManager RPC addresses in failover order.
    pub fn resource_manager_addresses(&self) -> Result<Vec<String>> {
        if !self.get_boolean(RM_HA_ENABLED, false) {
            return Ok(vec![normalize_address(
                self.get(RM_ADDRESS).unwrap_or(DEFAULT_RM_ADDRESS),
                DEFAULT_RM_PORT,
            )?]);
        }

        let rm_ids = self
            .get(RM_HA_IDS)
            .into_iter()
            .flat_map(|value| value.split(','))
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .collect::<Vec<_>>();

        if rm_ids.is_empty() {
            return Err(YarnError::InvalidArgument(format!(
                "{RM_HA_ENABLED} is true but {RM_HA_IDS} is empty"
            )));
        }

        rm_ids
            .into_iter()
            .map(|rm_id| {
                let address_key = format!("{RM_ADDRESS}.{rm_id}");
                if let Some(address) = self.get(&address_key) {
                    return normalize_address(address, DEFAULT_RM_PORT);
                }

                let hostname_key = format!("{RM_HOSTNAME}.{rm_id}");
                let hostname = self.get(&hostname_key).ok_or_else(|| {
                    YarnError::InvalidArgument(format!(
                        "missing {address_key} and {hostname_key} for YARN HA"
                    ))
                })?;
                hostname_with_port(hostname, DEFAULT_RM_PORT)
            })
            .collect()
    }

    pub(crate) fn failover_max_attempts(&self) -> u32 {
        self.get_u64(
            CLIENT_FAILOVER_MAX_ATTEMPTS,
            u64::from(DEFAULT_FAILOVER_MAX_ATTEMPTS),
        )
        .min(u64::from(u32::MAX)) as u32
    }

    pub(crate) fn failover_sleep_base_ms(&self) -> u64 {
        self.get_u64(
            CLIENT_FAILOVER_SLEEP_BASE_MS,
            DEFAULT_FAILOVER_SLEEP_BASE_MS,
        )
    }

    pub(crate) fn failover_sleep_max_ms(&self) -> u64 {
        self.get_u64(CLIENT_FAILOVER_SLEEP_MAX_MS, DEFAULT_FAILOVER_SLEEP_MAX_MS)
    }

    fn get_u64(&self, key: &str, default: u64) -> u64 {
        self.get(key)
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }
}

impl Deref for Configuration {
    type Target = HadoopConfiguration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn normalize_address(address: &str, default_port: u16) -> Result<String> {
    let address = address.trim();
    if address.is_empty() {
        return Err(YarnError::InvalidArgument(
            "YARN ResourceManager address cannot be empty".to_owned(),
        ));
    }

    if address.starts_with('[') {
        if address.rsplit_once(']').is_some_and(|(_, suffix)| {
            suffix.starts_with(':') && suffix[1..].parse::<u16>().is_ok()
        }) {
            return Ok(address.to_owned());
        }
        if address.ends_with(']') {
            return Ok(format!("{address}:{default_port}"));
        }
        return Err(YarnError::InvalidArgument(format!(
            "invalid YARN ResourceManager address {address:?}"
        )));
    }

    if let Ok(ipv6) = address.parse::<Ipv6Addr>() {
        return Ok(format!("[{ipv6}]:{default_port}"));
    }

    if let Some((host, port)) = address.rsplit_once(':') {
        if port.parse::<u16>().is_ok() && !host.is_empty() {
            return if host.contains(':') {
                Ok(format!("[{host}]:{port}"))
            } else {
                Ok(address.to_owned())
            };
        }
        return Err(YarnError::InvalidArgument(format!(
            "invalid YARN ResourceManager address {address:?}"
        )));
    }

    hostname_with_port(address, default_port)
}

fn hostname_with_port(hostname: &str, port: u16) -> Result<String> {
    let hostname = hostname.trim();
    if hostname.is_empty() {
        return Err(YarnError::InvalidArgument(
            "YARN ResourceManager hostname cannot be empty".to_owned(),
        ));
    }

    Ok(if hostname.starts_with('[') && hostname.ends_with(']') {
        format!("{hostname}:{port}")
    } else if hostname.matches(':').count() > 1 {
        format!("[{hostname}]:{port}")
    } else {
        format!("{hostname}:{port}")
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CLIENT_FAILOVER_MAX_ATTEMPTS, CLIENT_FAILOVER_SLEEP_BASE_MS, CLIENT_FAILOVER_SLEEP_MAX_MS,
        Configuration, RM_ADDRESS, RM_HA_ENABLED, RM_HA_IDS, normalize_address,
    };

    fn config(settings: &[(&str, &str)]) -> Configuration {
        Configuration::new(
            Some("/tmp/yarn-native-missing-conf".to_owned()),
            Some(
                settings
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                    .collect(),
            ),
        )
        .unwrap()
    }

    #[test]
    fn resource_manager_address_uses_default_and_configured_address() {
        assert_eq!(
            config(&[]).resource_manager_addresses().unwrap(),
            vec!["0.0.0.0:8032"]
        );
        assert_eq!(
            config(&[(RM_ADDRESS, "rm.example:8042")])
                .resource_manager_addresses()
                .unwrap(),
            vec!["rm.example:8042"]
        );
    }

    #[test]
    fn resource_manager_addresses_follow_ha_ids() {
        let config = config(&[
            (RM_HA_ENABLED, "true"),
            (RM_HA_IDS, "rm1, rm2"),
            ("yarn.resourcemanager.address.rm1", "rm1.example:8032"),
            ("yarn.resourcemanager.hostname.rm2", "rm2.example"),
        ]);

        assert_eq!(
            config.resource_manager_addresses().unwrap(),
            vec!["rm1.example:8032", "rm2.example:8032"]
        );
    }

    #[test]
    fn resource_manager_addresses_reject_incomplete_ha_configuration() {
        let missing_address = config(&[(RM_HA_ENABLED, "true"), (RM_HA_IDS, "rm1")]);
        assert!(missing_address.resource_manager_addresses().is_err());

        let missing_ids = config(&[(RM_HA_ENABLED, "true")]);
        assert!(missing_ids.resource_manager_addresses().is_err());
    }

    #[test]
    fn resource_manager_hostname_accepts_ipv6() {
        assert_eq!(
            config(&[
                (RM_HA_ENABLED, "true"),
                (RM_HA_IDS, "rm1"),
                ("yarn.resourcemanager.hostname.rm1", "::1"),
            ])
            .resource_manager_addresses()
            .unwrap(),
            vec!["[::1]:8032"]
        );
    }

    #[test]
    fn failover_settings_use_yarn_defaults_and_overrides() {
        let defaults = config(&[]);
        assert_eq!(defaults.failover_max_attempts(), 15);
        assert_eq!(defaults.failover_sleep_base_ms(), 500);
        assert_eq!(defaults.failover_sleep_max_ms(), 15_000);

        let overrides = config(&[
            (CLIENT_FAILOVER_MAX_ATTEMPTS, "3"),
            (CLIENT_FAILOVER_SLEEP_BASE_MS, "25"),
            (CLIENT_FAILOVER_SLEEP_MAX_MS, "100"),
        ]);
        assert_eq!(overrides.failover_max_attempts(), 3);
        assert_eq!(overrides.failover_sleep_base_ms(), 25);
        assert_eq!(overrides.failover_sleep_max_ms(), 100);
    }

    #[test]
    fn normalize_address_adds_default_port_and_preserves_explicit_port() {
        assert_eq!(
            normalize_address("rm.example", 8032).unwrap(),
            "rm.example:8032"
        );
        assert_eq!(
            normalize_address("  rm.example:8042  ", 8032).unwrap(),
            "rm.example:8042"
        );
        assert_eq!(
            normalize_address("127.0.0.1", 8032).unwrap(),
            "127.0.0.1:8032"
        );
    }

    #[test]
    fn normalize_address_handles_bracketed_and_unbracketed_ipv6() {
        assert_eq!(normalize_address("::1", 8032).unwrap(), "[::1]:8032");
        assert_eq!(normalize_address("[::1]", 8032).unwrap(), "[::1]:8032");
        assert_eq!(
            normalize_address("[2001:db8::1]:8042", 8032).unwrap(),
            "[2001:db8::1]:8042"
        );
    }

    #[test]
    fn normalize_address_rejects_empty_and_malformed_addresses() {
        for address in ["", "   ", "[::1", "[::1]:bad", "rm.example:", ":8032"] {
            assert!(
                normalize_address(address, 8032).is_err(),
                "expected {address:?} to be rejected"
            );
        }
    }
}
