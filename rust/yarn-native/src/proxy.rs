use std::io::ErrorKind;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use hadoop_native::rpc::{RpcConnection, RpcConnectionOptions};
use hadoop_native::security::ClientAuth;
use hadoop_native::{HadoopError, Result};
use log::warn;
use tokio::runtime::Handle;

use crate::config::Configuration;

const STANDBY_EXCEPTION: &str = "org.apache.hadoop.ipc.StandbyException";
const SASL_EXCEPTION: &str = "javax.security.sasl.SaslException";
const GSS_EXCEPTION: &str = "org.ietf.jgss.GSSException";
const ACCESS_CONTROL_EXCEPTION: &str = "org.apache.hadoop.security.AccessControlException";
const AUTHORIZATION_EXCEPTION: &str = "org.apache.hadoop.security.authorize.AuthorizationException";
const INVALID_TOKEN_EXCEPTION: &str = "org.apache.hadoop.security.token.SecretManager$InvalidToken";

/// Hadoop RPC protocol name for ResourceManager client calls.
pub const APPLICATION_CLIENT_PROTOCOL: &str =
    "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB";
/// Hadoop RPC protocol name for ApplicationMaster to ResourceManager calls.
pub const APPLICATION_MASTER_PROTOCOL: &str =
    "org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB";
/// Hadoop RPC protocol name for ApplicationMaster to NodeManager calls.
pub const CONTAINER_MANAGEMENT_PROTOCOL: &str =
    "org.apache.hadoop.yarn.api.ContainerManagementProtocolPB";
/// Delegation token kind used by ResourceManager client calls.
pub const RM_DELEGATION_TOKEN_KIND: &str = "RM_DELEGATION_TOKEN";

/// RPC identity and token lookup values for a YARN protocol.
#[derive(Debug, Clone)]
pub struct RpcProtocol {
    /// Fully qualified Hadoop RPC protocol class name.
    pub protocol: String,
    /// Hadoop token kind used for authentication.
    pub token_kind: String,
    /// Token service used to select credentials for this protocol.
    pub token_service: String,
}

impl RpcProtocol {
    /// Create protocol settings from the Hadoop RPC name and token metadata.
    pub fn new(
        protocol: impl Into<String>,
        token_kind: impl Into<String>,
        token_service: impl Into<String>,
    ) -> Self {
        Self {
            protocol: protocol.into(),
            token_kind: token_kind.into(),
            token_service: token_service.into(),
        }
    }

    /// ResourceManager client protocol settings for the given token service.
    pub fn application_client(token_service: impl Into<String>) -> Self {
        Self::new(
            APPLICATION_CLIENT_PROTOCOL,
            RM_DELEGATION_TOKEN_KIND,
            token_service,
        )
    }
}

#[derive(Debug)]
struct ProxyConnection {
    address: String,
    inner: tokio::sync::Mutex<Option<RpcConnection<()>>>,
    rpc_protocol: RpcProtocol,
    effective_user: Option<String>,
    auth: Option<Arc<ClientAuth>>,
    config: Arc<Configuration>,
    handle: Handle,
}

#[derive(Debug)]
enum ProxyCallError {
    BeforeRequest(HadoopError),
    AfterRequest(HadoopError),
}

impl ProxyCallError {
    fn before_request(error: HadoopError) -> Self {
        Self::BeforeRequest(error)
    }

    fn after_request(error: HadoopError) -> Self {
        Self::AfterRequest(error)
    }

    fn error(&self) -> &HadoopError {
        match self {
            Self::BeforeRequest(error) | Self::AfterRequest(error) => error,
        }
    }

    fn into_error(self) -> HadoopError {
        match self {
            Self::BeforeRequest(error) | Self::AfterRequest(error) => error,
        }
    }

    fn is_pre_request_connection_error(&self) -> bool {
        matches!(self, Self::BeforeRequest(HadoopError::IOError(_)))
    }
}

impl ProxyConnection {
    fn new(
        address: String,
        rpc_protocol: RpcProtocol,
        effective_user: Option<String>,
        auth: Option<Arc<ClientAuth>>,
        config: Arc<Configuration>,
        handle: Handle,
    ) -> Self {
        Self {
            address,
            inner: tokio::sync::Mutex::new(None),
            rpc_protocol,
            effective_user,
            auth,
            config,
            handle,
        }
    }

    async fn call(
        &self,
        method_name: &str,
        message: &[u8],
        write: bool,
    ) -> std::result::Result<Bytes, ProxyCallError> {
        for attempt in 0..2 {
            let rpc_response = {
                let mut connection = self.inner.lock().await;
                match &mut *connection {
                    Some(connection) if connection.is_alive() => (),
                    cached => {
                        *cached = Some(
                            RpcConnection::connect(
                                &self.address,
                                RpcConnectionOptions {
                                    protocol: &self.rpc_protocol.protocol,
                                    token_kind: &self.rpc_protocol.token_kind,
                                    token_service: &self.rpc_protocol.token_service,
                                    effective_user: self.effective_user.clone(),
                                    auth: self.auth.clone(),
                                },
                                None,
                                &self.config,
                                &self.handle,
                            )
                            .await
                            .map_err(ProxyCallError::before_request)?,
                        );
                    }
                }

                connection.as_ref().unwrap().call(method_name, message)
            };

            match rpc_response.await {
                Ok(bytes) => return Ok(bytes),
                Err(HadoopError::IOError(error)) if !write && attempt == 0 => {
                    warn!("IO error on YARN RPC call, retrying: {error:?}");
                    *self.inner.lock().await = None;
                }
                Err(error) => return Err(ProxyCallError::after_request(error)),
            }
        }

        unreachable!()
    }
}

/// YARN RPC proxy with lazy connections and endpoint failover.
///
/// Calls use the raw protobuf request and response bytes. The `write` flag
/// controls whether a lost response may be retried, since a write may already
/// have been applied by the ResourceManager.
#[derive(Debug)]
pub struct YarnProxy {
    connections: Vec<ProxyConnection>,
    current_active: AtomicUsize,
    failover_max_attempts: u32,
    failover_sleep_base_ms: u64,
    failover_sleep_max_ms: u64,
}

impl YarnProxy {
    /// Create a proxy for the ResourceManager addresses in `config`.
    pub fn new(
        config: Arc<Configuration>,
        rpc_protocol: RpcProtocol,
        effective_user: Option<String>,
        auth: Option<Arc<ClientAuth>>,
        handle: Handle,
    ) -> Result<Self> {
        let addresses = config.resource_manager_addresses()?;
        Self::with_addresses(
            addresses,
            config,
            rpc_protocol,
            effective_user,
            auth,
            handle,
        )
    }

    /// Create a proxy for explicit `host:port` endpoints.
    ///
    /// Use this for YARN services whose endpoint is supplied by another
    /// protocol response, such as a NodeManager returned by the ResourceManager.
    pub fn with_addresses(
        addresses: Vec<String>,
        config: Arc<Configuration>,
        rpc_protocol: RpcProtocol,
        effective_user: Option<String>,
        auth: Option<Arc<ClientAuth>>,
        handle: Handle,
    ) -> Result<Self> {
        if addresses.is_empty() || addresses.iter().any(|address| address.trim().is_empty()) {
            return Err(HadoopError::InvalidArgument(
                "YARN RPC proxy requires non-empty endpoints".to_owned(),
            ));
        }

        let connections = addresses
            .into_iter()
            .map(|address| {
                ProxyConnection::new(
                    address,
                    rpc_protocol.clone(),
                    effective_user.clone(),
                    auth.clone(),
                    Arc::clone(&config),
                    handle.clone(),
                )
            })
            .collect();

        Ok(Self {
            connections,
            current_active: AtomicUsize::new(0),
            failover_max_attempts: config.failover_max_attempts(),
            failover_sleep_base_ms: config.failover_sleep_base_ms(),
            failover_sleep_max_ms: config.failover_sleep_max_ms(),
        })
    }

    /// Send an RPC using the configured protocol identity.
    pub async fn call(&self, method_name: &str, message: &[u8], write: bool) -> Result<Bytes> {
        let mut connection_index = self.current_active.load(Ordering::SeqCst);
        let mut failovers = 0u32;

        loop {
            let result = self.connections[connection_index]
                .call(method_name, message, write)
                .await;

            match result {
                Ok(bytes) => {
                    self.current_active
                        .store(connection_index, Ordering::SeqCst);
                    return Ok(bytes);
                }
                Err(error) if is_authentication_error(error.error()) => {
                    return Err(error.into_error());
                }
                Err(error) if is_non_retriable_rpc_error(error.error()) => {
                    return Err(error.into_error());
                }
                Err(error) if write && !is_safe_write_failover_error(&error) => {
                    return Err(error.into_error());
                }
                Err(error) if failovers >= self.failover_max_attempts => {
                    return Err(error.into_error());
                }
                Err(error) => {
                    if !is_retriable_error(error.error()) {
                        warn!("YARN ResourceManager RPC failed: {:?}", error.error());
                    }
                    failovers += 1;
                    if failovers.is_multiple_of(self.connections.len() as u32) {
                        tokio::time::sleep(exponential_failover_sleep(
                            self.failover_sleep_base_ms,
                            self.failover_sleep_max_ms,
                            failovers / self.connections.len() as u32,
                        ))
                        .await;
                    }
                    connection_index = (connection_index + 1) % self.connections.len();
                }
            }
        }
    }
}

fn is_retriable(exception: &str) -> bool {
    exception == STANDBY_EXCEPTION
}

fn is_retriable_error(error: &HadoopError) -> bool {
    matches!(
        error,
        HadoopError::RPCError(exception, _) | HadoopError::FatalRPCError(exception, _)
            if is_retriable(exception)
    )
}

fn is_authentication_error(error: &HadoopError) -> bool {
    match error {
        HadoopError::SASLError(_) | HadoopError::GSSAPIError(..) | HadoopError::NoSASLMechanism => {
            true
        }
        HadoopError::RPCError(exception, _) | HadoopError::FatalRPCError(exception, _) => {
            matches!(
                exception.as_str(),
                SASL_EXCEPTION
                    | GSS_EXCEPTION
                    | ACCESS_CONTROL_EXCEPTION
                    | AUTHORIZATION_EXCEPTION
                    | INVALID_TOKEN_EXCEPTION
            )
        }
        _ => false,
    }
}

fn is_non_retriable_rpc_error(error: &HadoopError) -> bool {
    matches!(
        error,
        HadoopError::RPCError(exception, _) if !is_retriable(exception)
    )
}

fn is_safe_write_failover_error(error: &ProxyCallError) -> bool {
    if matches!(
        error.error(),
        HadoopError::RPCError(exception, _) | HadoopError::FatalRPCError(exception, _)
            if is_retriable(exception)
    ) || error.is_pre_request_connection_error()
    {
        return true;
    }

    matches!(
        error.error(),
        HadoopError::IOError(error)
            if matches!(
                error.kind(),
                ErrorKind::AddrNotAvailable
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::HostUnreachable
                    | ErrorKind::NetworkUnreachable
                    | ErrorKind::NotConnected
            )
    )
}

fn exponential_failover_sleep(base_ms: u64, max_ms: u64, round: u32) -> Duration {
    let factor = 1u64 << round.min(32);
    Duration::from_millis(base_ms.saturating_mul(factor).min(max_ms))
}

#[cfg(test)]
mod tests {
    use super::{
        ProxyCallError, STANDBY_EXCEPTION, exponential_failover_sleep, is_authentication_error,
        is_retriable_error, is_safe_write_failover_error,
    };
    use hadoop_native::HadoopError;
    use std::time::Duration;

    #[test]
    fn standby_errors_are_retriable() {
        assert!(is_retriable_error(&HadoopError::RPCError(
            STANDBY_EXCEPTION.to_owned(),
            "ResourceManager is standby".to_owned()
        )));
        assert!(is_retriable_error(&HadoopError::FatalRPCError(
            STANDBY_EXCEPTION.to_owned(),
            "ResourceManager is standby".to_owned()
        )));
        assert!(!is_retriable_error(&HadoopError::RPCError(
            "java.io.IOException".to_owned(),
            "request failed".to_owned()
        )));
    }

    #[test]
    fn authentication_errors_are_not_failed_over() {
        assert!(is_authentication_error(&HadoopError::NoSASLMechanism));
        assert!(is_authentication_error(&HadoopError::FatalRPCError(
            "javax.security.sasl.SaslException".to_owned(),
            "authentication failed".to_owned()
        )));
        assert!(!is_authentication_error(&HadoopError::RPCError(
            STANDBY_EXCEPTION.to_owned(),
            "ResourceManager is standby".to_owned()
        )));
    }

    #[test]
    fn write_failover_requires_a_known_safe_error() {
        let standby = ProxyCallError::after_request(HadoopError::RPCError(
            STANDBY_EXCEPTION.to_owned(),
            "ResourceManager is standby".to_owned(),
        ));
        assert!(is_safe_write_failover_error(&standby));

        let connect_error = ProxyCallError::before_request(HadoopError::IOError(
            std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
        ));
        assert!(is_safe_write_failover_error(&connect_error));

        let ambiguous_error = ProxyCallError::after_request(HadoopError::IOError(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        ));
        assert!(!is_safe_write_failover_error(&ambiguous_error));
    }

    #[test]
    fn failover_backoff_scales_and_caps() {
        assert_eq!(
            exponential_failover_sleep(500, 15_000, 0),
            Duration::from_millis(500)
        );
        assert_eq!(
            exponential_failover_sleep(500, 15_000, 1),
            Duration::from_millis(1_000)
        );
        assert_eq!(
            exponential_failover_sleep(500, 15_000, 10),
            Duration::from_millis(15_000)
        );
    }
}
