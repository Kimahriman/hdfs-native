use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::ops::Deref;
use std::time::Duration;

use hadoop_native::config::Configuration as HadoopConfiguration;

use crate::{Result, YarnError};

const RM_HOSTNAME: &str = "yarn.resourcemanager.hostname";
const DEFAULT_RM_HOSTNAME: &str = "0.0.0.0";
pub(crate) const RM_ADDRESS: &str = "yarn.resourcemanager.address";
pub(crate) const DEFAULT_RM_PORT: u16 = 8032;
pub(crate) const RM_SCHEDULER_ADDRESS: &str = "yarn.resourcemanager.scheduler.address";
pub(crate) const DEFAULT_RM_SCHEDULER_PORT: u16 = 8030;
const RM_HA_ENABLED: &str = "yarn.resourcemanager.ha.enabled";
const RM_HA_IDS: &str = "yarn.resourcemanager.ha.rm-ids";

const RM_CONNECT_MAX_WAIT_MS: &str = "yarn.resourcemanager.connect.max-wait.ms";
const DEFAULT_RM_CONNECT_MAX_WAIT_MS: u64 = 15 * 60 * 1000;
const RM_CONNECT_RETRY_INTERVAL_MS: &str = "yarn.resourcemanager.connect.retry-interval.ms";
const DEFAULT_RM_CONNECT_RETRY_INTERVAL_MS: u64 = 30 * 1000;
const CLIENT_FAILOVER_MAX_ATTEMPTS: &str = "yarn.client.failover-max-attempts";
const CLIENT_FAILOVER_SLEEP_BASE_MS: &str = "yarn.client.failover-sleep-base-ms";
const CLIENT_FAILOVER_SLEEP_MAX_MS: &str = "yarn.client.failover-sleep-max-ms";

const NM_CONNECT_MAX_WAIT_MS: &str = "yarn.client.nodemanager-connect.max-wait-ms";
const DEFAULT_NM_CONNECT_MAX_WAIT_MS: u64 = 3 * 60 * 1000;
const NM_CONNECT_RETRY_INTERVAL_MS: &str = "yarn.client.nodemanager-connect.retry-interval-ms";
const DEFAULT_NM_CONNECT_RETRY_INTERVAL_MS: u64 = 10 * 1000;

const POLL_INTERVAL_MS: &str = "yarn.client.application-client-protocol.poll-interval-ms";
const LEGACY_POLL_INTERVAL_MS: &str = "yarn.client.app-submission.poll-interval";
const DEFAULT_POLL_INTERVAL_MS: u64 = 200;
const POLL_TIMEOUT_MS: &str = "yarn.client.application-client-protocol.poll-timeout-ms";

const TOKEN_SERVICE_USE_IP: &str = "hadoop.security.token.service.use_ip";
const RM_PRINCIPAL: &str = "yarn.resourcemanager.principal";
const HOST_PATTERN: &str = "_HOST";

/// Retry settings for one kind of server connection. HA clients cycle through every server
/// before backing off; non-HA clients retry their single server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FailoverSettings {
    pub(crate) max_attempts: u32,
    pub(crate) sleep_base_millis: u64,
    pub(crate) sleep_max_millis: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct Configuration(HadoopConfiguration);

impl Configuration {
    pub(crate) fn new(
        conf_dir: Option<String>,
        conf_map: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        Ok(Self(HadoopConfiguration::new(conf_dir, conf_map)?))
    }

    pub(crate) async fn new_async(
        conf_dir: Option<String>,
        conf_map: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        Ok(Self(
            HadoopConfiguration::new_async(conf_dir, conf_map).await?,
        ))
    }

    fn get_u64(&self, key: &str, default: u64) -> u64 {
        self.get(key)
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(default)
    }

    /// Like [`Self::get_u64`], but a negative value means "no limit".
    fn get_limit(&self, key: &str, default: Option<u64>) -> Option<u64> {
        match self
            .get(key)
            .and_then(|value| value.trim().parse::<i64>().ok())
        {
            Some(value) if value < 0 => None,
            Some(value) => Some(value as u64),
            None => default,
        }
    }

    pub(crate) fn rm_ha_enabled(&self) -> bool {
        self.get_boolean(RM_HA_ENABLED, false)
    }

    /// The addresses of every ResourceManager for one RPC service, in failover order. The key
    /// is the service's address setting, such as `yarn.resourcemanager.address`.
    pub(crate) fn rm_addresses(&self, key: &str, default_port: u16) -> Result<Vec<String>> {
        if !self.rm_ha_enabled() {
            return Ok(vec![self.rm_address(key, default_port, None)]);
        }

        let addresses: Vec<String> = self
            .get(RM_HA_IDS)
            .into_iter()
            .flat_map(|ids| ids.split(','))
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(|id| self.rm_address(key, default_port, Some(id)))
            .collect();

        if addresses.is_empty() {
            Err(YarnError::InvalidArgument(format!(
                "{RM_HA_ENABLED} is true but no {RM_HA_IDS} are configured"
            )))
        } else {
            Ok(addresses)
        }
    }

    /// An explicit address wins. Otherwise use the ResourceManager hostname with the service's
    /// default port, as yarn-default.xml does.
    fn rm_address(&self, key: &str, default_port: u16, rm_id: Option<&str>) -> String {
        let suffixed = |key: &str| match rm_id {
            Some(id) => format!("{key}.{id}"),
            None => key.to_string(),
        };
        let address = self
            .get(&suffixed(key))
            .or_else(|| self.get(&suffixed(RM_HOSTNAME)))
            .unwrap_or(DEFAULT_RM_HOSTNAME);
        with_default_port(address, default_port)
    }

    /// The ResourceManager's Kerberos principal with `_HOST` replaced by the ResourceManager's
    /// hostname, like Hadoop's `Master.getMasterPrincipal`. Delegation tokens given to an
    /// application must name it as their renewer so the ResourceManager can renew them.
    pub(crate) fn rm_principal(&self) -> Result<String> {
        let principal = self
            .get(RM_PRINCIPAL)
            .map(str::trim)
            .filter(|principal| !principal.is_empty())
            .ok_or_else(|| {
                YarnError::InvalidArgument(format!(
                    "{RM_PRINCIPAL} is not set, so the ResourceManager's principal is unknown"
                ))
            })?;
        let address = self
            .rm_addresses(RM_ADDRESS, DEFAULT_RM_PORT)?
            .into_iter()
            .next()
            .expect("rm_addresses returns at least one address");
        Ok(server_principal(principal, &address))
    }

    /// Builds a token service name for an address, like Hadoop's
    /// `SecurityUtil.buildTokenService`, which uses IP addresses unless
    /// `hadoop.security.token.service.use_ip` is false.
    pub(crate) fn token_service(&self, address: &str) -> String {
        if self.get_boolean(TOKEN_SERVICE_USE_IP, true)
            && let Ok(addrs) = address.to_socket_addrs()
        {
            // Prefer IPv4 when a host resolves to both families, matching Java's default
            // `InetAddress.getByName` behavior (`java.net.preferIPv6Addresses=false`).
            let mut first = None;
            for addr in addrs {
                if addr.is_ipv4() {
                    return addr.to_string();
                }
                first.get_or_insert(addr);
            }
            if let Some(resolved) = first {
                return resolved.to_string();
            }
        }
        address.to_string()
    }

    /// The token service for a set of ResourceManager addresses, like Hadoop's
    /// `ClientRMProxy.getTokenService`.
    pub(crate) fn rm_token_service(&self, addresses: &[String]) -> String {
        addresses
            .iter()
            .map(|address| self.token_service(address))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// ResourceManager retry settings, following Hadoop's `RMProxy`: by default a client keeps
    /// trying for `yarn.resourcemanager.connect.max-wait.ms`, pausing
    /// `yarn.resourcemanager.connect.retry-interval.ms` between attempts.
    pub(crate) fn rm_failover_settings(&self) -> FailoverSettings {
        let max_wait = self.get_limit(RM_CONNECT_MAX_WAIT_MS, Some(DEFAULT_RM_CONNECT_MAX_WAIT_MS));
        let interval = self.get_u64(
            RM_CONNECT_RETRY_INTERVAL_MS,
            DEFAULT_RM_CONNECT_RETRY_INTERVAL_MS,
        );
        if !self.rm_ha_enabled() {
            return FailoverSettings {
                max_attempts: attempts_within(max_wait, interval),
                sleep_base_millis: interval,
                sleep_max_millis: interval,
            };
        }

        let base = self.get_u64(CLIENT_FAILOVER_SLEEP_BASE_MS, interval);
        let max = self.get_u64(CLIENT_FAILOVER_SLEEP_MAX_MS, interval);
        let max_attempts = match self.get_limit(CLIENT_FAILOVER_MAX_ATTEMPTS, None) {
            Some(attempts) => u32::try_from(attempts).unwrap_or(u32::MAX),
            None => attempts_within(max_wait, base),
        };
        FailoverSettings {
            max_attempts,
            sleep_base_millis: base,
            sleep_max_millis: max.max(base),
        }
    }

    /// NodeManager retry settings, following Hadoop's `NMProxy`.
    pub(crate) fn nm_failover_settings(&self) -> FailoverSettings {
        let max_wait = self.get_limit(NM_CONNECT_MAX_WAIT_MS, Some(DEFAULT_NM_CONNECT_MAX_WAIT_MS));
        let interval = self.get_u64(
            NM_CONNECT_RETRY_INTERVAL_MS,
            DEFAULT_NM_CONNECT_RETRY_INTERVAL_MS,
        );
        FailoverSettings {
            max_attempts: attempts_within(max_wait, interval),
            sleep_base_millis: interval,
            sleep_max_millis: interval,
        }
    }

    /// How often to poll the ResourceManager while waiting for an application state change.
    pub(crate) fn poll_interval(&self) -> Duration {
        let legacy = self.get_u64(LEGACY_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
        Duration::from_millis(self.get_u64(POLL_INTERVAL_MS, legacy))
    }

    /// How long to wait for an application state change, or `None` to wait forever.
    pub(crate) fn poll_timeout(&self) -> Option<Duration> {
        self.get_limit(POLL_TIMEOUT_MS, None)
            .map(Duration::from_millis)
    }
}

impl Deref for Configuration {
    type Target = HadoopConfiguration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Replaces a `_HOST` host component in a principal like `yarn/_HOST@REALM` with the host of
/// `address`, like Hadoop's `SecurityUtil.getServerPrincipal`.
fn server_principal(principal: &str, address: &str) -> String {
    let (name, realm) = match principal.split_once('@') {
        Some((name, realm)) => (name, Some(realm)),
        None => (principal, None),
    };
    let Some((service, HOST_PATTERN)) = name.split_once('/') else {
        return principal.to_string();
    };
    let host = address
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(address)
        .trim_start_matches('[')
        .trim_end_matches(']');
    let host = if host == "0.0.0.0" {
        whoami::hostname().unwrap_or_else(|_| "localhost".to_string())
    } else {
        host.to_string()
    };
    match realm {
        Some(realm) => format!("{service}/{}@{realm}", host.to_lowercase()),
        None => format!("{service}/{}", host.to_lowercase()),
    }
}

/// The number of retries that fit in `max_wait`, or unlimited when there is no limit.
fn attempts_within(max_wait: Option<u64>, interval: u64) -> u32 {
    max_wait
        .map(|max_wait| u32::try_from(max_wait / interval.max(1)).unwrap_or(u32::MAX))
        .unwrap_or(u32::MAX)
}

/// Appends `default_port` to an address that has none.
fn with_default_port(address: &str, default_port: u16) -> String {
    let address = address.trim();
    let has_port = match address.strip_prefix('[') {
        // [ipv6]:port
        Some(rest) => rest
            .split_once(']')
            .is_some_and(|(_, port)| port.starts_with(':')),
        None => address.matches(':').count() == 1,
    };
    if has_port {
        address.to_string()
    } else if address.contains(':') && !address.starts_with('[') {
        format!("[{address}]:{default_port}")
    } else {
        format!("{address}:{default_port}")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;

    fn config(values: &[(&str, &str)]) -> Configuration {
        Configuration::new(
            None,
            Some(
                values
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<HashMap<_, _>>(),
            ),
        )
        .unwrap()
    }

    #[test]
    fn default_addresses_use_the_resourcemanager_hostname() {
        assert_eq!(
            config(&[])
                .rm_addresses(RM_ADDRESS, DEFAULT_RM_PORT)
                .unwrap(),
            vec!["0.0.0.0:8032"]
        );
        assert_eq!(
            config(&[(RM_HOSTNAME, "rm.example.com")])
                .rm_addresses(RM_SCHEDULER_ADDRESS, DEFAULT_RM_SCHEDULER_PORT)
                .unwrap(),
            vec!["rm.example.com:8030"]
        );
    }

    #[test]
    fn explicit_addresses_win_and_get_default_ports() {
        let config = config(&[
            (RM_HOSTNAME, "ignored"),
            (RM_ADDRESS, "rm.example.com:9032"),
            (RM_SCHEDULER_ADDRESS, "scheduler.example.com"),
        ]);
        assert_eq!(
            config.rm_addresses(RM_ADDRESS, DEFAULT_RM_PORT).unwrap(),
            vec!["rm.example.com:9032"]
        );
        assert_eq!(
            config
                .rm_addresses(RM_SCHEDULER_ADDRESS, DEFAULT_RM_SCHEDULER_PORT)
                .unwrap(),
            vec!["scheduler.example.com:8030"]
        );
    }

    #[test]
    fn ha_addresses_follow_rm_ids() {
        let config = config(&[
            (RM_HA_ENABLED, "true"),
            (RM_HA_IDS, "rm1, rm2"),
            ("yarn.resourcemanager.hostname.rm1", "rm-1.example.com"),
            ("yarn.resourcemanager.address.rm2", "rm-2.example.com:18032"),
            ("yarn.resourcemanager.hostname.rm2", "ignored"),
        ]);
        assert_eq!(
            config.rm_addresses(RM_ADDRESS, DEFAULT_RM_PORT).unwrap(),
            vec!["rm-1.example.com:8032", "rm-2.example.com:18032"]
        );

        assert!(
            super::tests::config(&[(RM_HA_ENABLED, "true")])
                .rm_addresses(RM_ADDRESS, DEFAULT_RM_PORT)
                .is_err()
        );
    }

    #[test]
    fn default_ports_handle_ipv6() {
        assert_eq!(with_default_port("::1", 8030), "[::1]:8030");
        assert_eq!(with_default_port("[::1]", 8030), "[::1]:8030");
        assert_eq!(with_default_port("[::1]:1", 8030), "[::1]:1");
    }

    #[test]
    fn token_services_use_ip_addresses_by_default() {
        assert_eq!(
            config(&[]).token_service("localhost:8030"),
            "127.0.0.1:8030"
        );

        let hostnames = config(&[(TOKEN_SERVICE_USE_IP, "false")]);
        assert_eq!(hostnames.token_service("localhost:8030"), "localhost:8030");
        assert_eq!(
            hostnames.rm_token_service(&["rm1:8030".to_string(), "rm2:8030".to_string()]),
            "rm1:8030,rm2:8030"
        );
    }

    #[test]
    fn retry_settings_follow_rm_proxy_defaults() {
        assert_eq!(
            config(&[]).rm_failover_settings(),
            FailoverSettings {
                max_attempts: 30,
                sleep_base_millis: 30_000,
                sleep_max_millis: 30_000,
            }
        );
        assert_eq!(
            config(&[(RM_CONNECT_MAX_WAIT_MS, "-1")])
                .rm_failover_settings()
                .max_attempts,
            u32::MAX
        );
        assert_eq!(
            config(&[
                (RM_HA_ENABLED, "true"),
                (CLIENT_FAILOVER_SLEEP_BASE_MS, "100"),
                (CLIENT_FAILOVER_SLEEP_MAX_MS, "1000"),
                (CLIENT_FAILOVER_MAX_ATTEMPTS, "4"),
            ])
            .rm_failover_settings(),
            FailoverSettings {
                max_attempts: 4,
                sleep_base_millis: 100,
                sleep_max_millis: 1000,
            }
        );
        assert_eq!(config(&[]).nm_failover_settings().max_attempts, 18);
    }

    #[test]
    fn poll_settings_have_hadoop_defaults() {
        assert_eq!(config(&[]).poll_interval(), Duration::from_millis(200));
        assert_eq!(config(&[]).poll_timeout(), None);
        assert_eq!(
            config(&[(LEGACY_POLL_INTERVAL_MS, "50")]).poll_interval(),
            Duration::from_millis(50)
        );
        // Invalid values fall back to the defaults, like the rest of the client settings.
        assert_eq!(
            config(&[(POLL_INTERVAL_MS, "soon")]).poll_interval(),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn rm_principal_resolves_the_host_pattern() {
        assert_eq!(
            config(&[
                (RM_PRINCIPAL, "yarn/_HOST@EXAMPLE.COM"),
                (RM_ADDRESS, "RM.example.com:8032"),
            ])
            .rm_principal()
            .unwrap(),
            "yarn/rm.example.com@EXAMPLE.COM"
        );
        assert_eq!(
            config(&[
                (RM_PRINCIPAL, "yarn/rm-1@EXAMPLE.COM"),
                (RM_ADDRESS, "rm-2:8032"),
            ])
            .rm_principal()
            .unwrap(),
            "yarn/rm-1@EXAMPLE.COM"
        );
        assert_eq!(
            config(&[
                (RM_PRINCIPAL, "yarn/_HOST"),
                (RM_HA_ENABLED, "true"),
                (RM_HA_IDS, "rm1,rm2"),
                ("yarn.resourcemanager.hostname.rm1", "rm-1"),
                ("yarn.resourcemanager.hostname.rm2", "rm-2"),
            ])
            .rm_principal()
            .unwrap(),
            "yarn/rm-1"
        );
        assert!(config(&[]).rm_principal().is_err());
    }
}
