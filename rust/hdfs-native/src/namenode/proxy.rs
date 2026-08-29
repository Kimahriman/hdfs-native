use std::io::ErrorKind;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use hadoop_native::rpc::{RpcConnection, RpcConnectionOptions};
use hadoop_native::security::ClientAuth;
use log::{debug, warn};
use prost::Message;
use rand::RngExt;
use tokio::runtime::Handle;
use url::Url;

use crate::{
    HdfsError, Result,
    config::Configuration,
    namenode::alignment::AlignmentContext,
    proto::{common::HaServiceStateProto, hdfs},
};

// RPC exceptions that should be tried
const STANDBY_EXCEPTION: &str = "org.apache.hadoop.ipc.StandbyException";
const OBSERVER_RETRY_EXCEPTION: &str = "org.apache.hadoop.ipc.ObserverRetryOnActiveException";
const PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
const TOKEN_KIND: &str = "HDFS_DELEGATION_TOKEN";

// Authentication and authorization failures are local to the client
// credentials and cannot be fixed by connecting to another HA endpoint.
const SASL_EXCEPTION: &str = "javax.security.sasl.SaslException";
const GSS_EXCEPTION: &str = "org.ietf.jgss.GSSException";
const ACCESS_CONTROL_EXCEPTION: &str = "org.apache.hadoop.security.AccessControlException";
const AUTHORIZATION_EXCEPTION: &str = "org.apache.hadoop.security.authorize.AuthorizationException";
const INVALID_TOKEN_EXCEPTION: &str = "org.apache.hadoop.security.token.SecretManager$InvalidToken";

/// Lazily creates a connection to a host, and recreates the connection
/// on fatal errors.
#[derive(Debug)]
struct ProxyConnection {
    url: String,
    inner: Arc<tokio::sync::Mutex<Option<RpcConnection<AlignmentContext>>>>,
    alignment_context: Option<Arc<Mutex<AlignmentContext>>>,
    nameservice: Option<String>,
    effective_user: Option<String>,
    auth: Option<Arc<ClientAuth>>,
    config: Arc<Configuration>,
    handle: Handle,
}

#[derive(Debug)]
enum ProxyCallError {
    BeforeRequest(HdfsError),
    AfterRequest(HdfsError),
}

impl ProxyCallError {
    fn before_request(error: HdfsError) -> Self {
        Self::BeforeRequest(error)
    }

    fn after_request(error: HdfsError) -> Self {
        Self::AfterRequest(error)
    }

    fn error(&self) -> &HdfsError {
        match self {
            Self::BeforeRequest(error) | Self::AfterRequest(error) => error,
        }
    }

    fn into_error(self) -> HdfsError {
        match self {
            Self::BeforeRequest(error) | Self::AfterRequest(error) => error,
        }
    }

    fn is_pre_request_connection_error(&self) -> bool {
        matches!(self, Self::BeforeRequest(HdfsError::IOError(_)))
    }
}

impl ProxyConnection {
    fn new(
        url: String,
        alignment_context: Option<Arc<Mutex<AlignmentContext>>>,
        nameservice: Option<String>,
        effective_user: Option<String>,
        auth: Option<Arc<ClientAuth>>,
        config: Arc<Configuration>,
        handle: Handle,
    ) -> Self {
        ProxyConnection {
            url,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
            alignment_context,
            nameservice,
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
            let receiver = {
                let mut connection = self.inner.lock().await;
                match &mut *connection {
                    Some(c) if c.is_alive() => (),
                    c => {
                        *c = Some(
                            RpcConnection::connect(
                                &self.url,
                                RpcConnectionOptions {
                                    protocol: PROTOCOL,
                                    token_kind: TOKEN_KIND,
                                    token_service: self
                                        .nameservice
                                        .as_ref()
                                        .map(|nameservice| format!("ha-hdfs:{nameservice}"))
                                        .as_deref()
                                        .unwrap_or(&self.url),
                                    effective_user: self.effective_user.clone(),
                                    auth: self.auth.clone(),
                                },
                                self.alignment_context.clone(),
                                &self.config,
                                &self.handle,
                            )
                            .await
                            .map_err(|error| ProxyCallError::before_request(error.into()))?,
                        );
                    }
                }

                connection
                    .as_ref()
                    .unwrap()
                    .call(method_name, message)
                    .await
                    .map_err(|error| ProxyCallError::after_request(error.into()))?
            };
            let result: Result<Bytes> = receiver
                .await
                .map_err(|_| {
                    ProxyCallError::after_request(HdfsError::IOError(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "RPC listener disconnected",
                    )))
                })?
                .map_err(HdfsError::from);

            #[cfg(feature = "integration-test")]
            let result = if result.is_ok()
                && crate::test::NAMENODE_RESPONSE_FAULT_INJECTOR.swap(false, Ordering::SeqCst)
            {
                Err(HdfsError::IOError(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "Injected NameNode response failure",
                )))
            } else {
                result
            };

            match result {
                Ok(bytes) => return Ok(bytes),
                // An ambiguous response can be retried for reads, but retrying a write could
                // replay a mutation that the NameNode already committed.
                Err(HdfsError::IOError(ref e)) if !write && attempt == 0 => {
                    warn!("IO error on RPC call, retrying: {:?}", e);
                    *self.inner.lock().await = None;
                    continue;
                }
                Err(error) => return Err(ProxyCallError::after_request(error)),
            }
        }
        unreachable!()
    }
}

#[derive(Debug)]
pub(crate) struct NameServiceProxy {
    proxy_connections: Vec<ProxyConnection>,
    current_active: AtomicUsize,
    find_observer: bool,
    current_observer: tokio::sync::Mutex<Option<usize>>,
    msynced: Option<tokio::sync::Mutex<bool>>,
    failover_max_attempts: u32,
    failover_sleep_base_millis: u64,
    failover_sleep_max_millis: u64,
}

impl NameServiceProxy {
    /// Creates a new proxy for a name service. If the URL contains a port,
    /// it is assumed to be for a single NameNode.
    pub(crate) fn new(
        nameservice: &Url,
        config: Arc<Configuration>,
        handle: Handle,
        effective_user: Option<String>,
        auth: Option<Arc<ClientAuth>>,
    ) -> Result<Self> {
        let host = nameservice.host_str().ok_or(HdfsError::InvalidArgument(
            "No host for name service".to_string(),
        ))?;

        let (context_enabled, find_observer) = match config.get_proxy_for_nameservice(host) {
            Some("org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider") => {
                (true, true)
            }
            Some(
                "org.apache.hadoop.hdfs.server.namenode.ha.RouterObserverReadConfiguredFailoverProxyProvider",
            ) => (true, false),
            Some("org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
            | None => (false, false),
            Some(provider) => {
                warn!(
                    "Unsupported proxy provider {provider}, falling back to ConfiguredFailoverProxyProvider behavior"
                );
                (false, false)
            }
        };

        let alignment_context = if context_enabled {
            Some(Arc::new(Mutex::new(AlignmentContext::default())))
        } else {
            None
        };

        let proxy_connections = if let Some(port) = nameservice.port() {
            let url = format!("{}:{}", nameservice.host_str().unwrap(), port);
            vec![ProxyConnection::new(
                url,
                alignment_context,
                None,
                effective_user,
                auth,
                Arc::clone(&config),
                handle,
            )]
        } else {
            // TODO: Add check for no configured namenodes
            config
                .get_urls_for_nameservice(host)?
                .into_iter()
                .map(|url| {
                    ProxyConnection::new(
                        url,
                        alignment_context.clone(),
                        Some(host.to_string()),
                        effective_user.clone(),
                        auth.clone(),
                        Arc::clone(&config),
                        handle.clone(),
                    )
                })
                .collect()
        };

        if proxy_connections.is_empty() {
            Err(HdfsError::InvalidArgument(
                "No NameNode hosts found".to_string(),
            ))
        } else {
            Ok(Self {
                proxy_connections,
                current_active: AtomicUsize::new(0),
                find_observer,
                current_observer: tokio::sync::Mutex::new(None),
                msynced: if context_enabled {
                    Some(tokio::sync::Mutex::new(false))
                } else {
                    None
                },
                failover_max_attempts: config.failover_max_attempts(),
                failover_sleep_base_millis: config.failover_sleep_base_millis(),
                failover_sleep_max_millis: config.failover_sleep_max_millis(),
            })
        }
    }

    async fn msync_if_needed(&self) -> Result<()> {
        if let Some(msynced) = self.msynced.as_ref() {
            let mut msynced = msynced.lock().await;
            if !(*msynced) {
                let msync_msg = hdfs::MsyncRequestProto::default();
                self.call_inner("msync", &msync_msg.encode_length_delimited_to_vec(), true)
                    .await
                    .map(|_| ())
                    .inspect(|_| *msynced = true)?;
            }
        }
        Ok(())
    }

    fn is_retriable(exception: &str) -> bool {
        exception == STANDBY_EXCEPTION || exception == OBSERVER_RETRY_EXCEPTION
    }

    fn is_authentication_error(error: &HdfsError) -> bool {
        match error {
            HdfsError::SASLError(_) | HdfsError::GSSAPIError(..) | HdfsError::NoSASLMechanism => {
                true
            }
            HdfsError::RPCError(exception, _) | HdfsError::FatalRPCError(exception, _) => {
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

    fn is_safe_failover_error(error: &HdfsError) -> bool {
        match error {
            HdfsError::RPCError(exception, _) | HdfsError::FatalRPCError(exception, _) => {
                Self::is_retriable(exception)
            }
            _ => false,
        }
    }

    fn is_safe_io_error(error: &HdfsError) -> bool {
        matches!(
            error,
            HdfsError::IOError(error)
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

    fn is_safe_write_failover_error(error: &ProxyCallError) -> bool {
        Self::is_safe_failover_error(error.error())
            || error.is_pre_request_connection_error()
            || Self::is_safe_io_error(error.error())
    }

    pub(crate) async fn call(
        &self,
        method_name: &'static str,
        message: &[u8],
        write: bool,
    ) -> Result<Bytes> {
        if write {
            self.msync_if_needed().await?;
        }
        self.call_inner(method_name, message, write).await
    }

    async fn find_observer(&self) -> std::result::Result<Option<usize>, ProxyCallError> {
        for i in 0..self.proxy_connections.len() {
            let ha_state_msg = hdfs::HaServiceStateRequestProto::default();
            let response = self.proxy_connections[i]
                .call(
                    "getHAServiceState",
                    &ha_state_msg.encode_length_delimited_to_vec(),
                    false,
                )
                .await;

            match response {
                Ok(response) => {
                    if let Ok(ha_state) =
                        hdfs::HaServiceStateResponseProto::decode_length_delimited(response)
                        && matches!(ha_state.state(), HaServiceStateProto::Observer)
                    {
                        return Ok(Some(i));
                    }
                }
                Err(e) => {
                    if Self::is_authentication_error(e.error()) {
                        return Err(e);
                    }
                    debug!("Couldn't get HA service status: {:?}", e.error());
                    continue;
                }
            }
        }
        Ok(None)
    }

    async fn call_observer(
        &self,
        method_name: &'static str,
        message: &[u8],
    ) -> std::result::Result<Bytes, ProxyCallError> {
        let observer_index = {
            let mut observer = self.current_observer.lock().await;
            if let Some(index) = *observer {
                index
            } else if let Some(index) = self.find_observer().await? {
                *observer = Some(index);
                index
            } else {
                return Err(ProxyCallError::before_request(HdfsError::InternalError(
                    "Unable to find observer node".to_string(),
                )));
            }
        };
        let result = self.proxy_connections[observer_index]
            .call(method_name, message, false)
            .await;

        #[cfg(feature = "integration-test")]
        if result.is_ok()
            && let Some(v) = crate::test::PROXY_CALLS.lock().unwrap().as_mut()
        {
            v.push((method_name, true));
        }

        if result.is_err() {
            *self.current_observer.lock().await = None;
        }

        result
    }

    async fn call_inner(
        &self,
        method_name: &'static str,
        message: &[u8],
        write: bool,
    ) -> Result<Bytes> {
        if !write && self.find_observer {
            let result = self.call_observer(method_name, message).await;
            // If it succeeds, return that result, otherwise just fallback to the active
            match result {
                Ok(res) => return Ok(res),
                Err(e) if Self::is_authentication_error(e.error()) => {
                    return Err(e.into_error());
                }
                Err(e) => warn!(
                    "Failed to call observer node, falling back to the active: {:?}",
                    e.error()
                ),
            }
        }
        let mut proxy_index = self.current_active.load(Ordering::SeqCst);
        let mut failovers: u32 = 0;

        loop {
            #[cfg(feature = "integration-test")]
            let standby_injected =
                crate::test::NAMENODE_STANDBY_FAULT_INJECTOR.load(Ordering::SeqCst);
            #[cfg(not(feature = "integration-test"))]
            let standby_injected = false;

            let result = if standby_injected {
                Err(ProxyCallError::after_request(HdfsError::RPCError(
                    STANDBY_EXCEPTION.to_string(),
                    "NameNode standby fault injection".to_string(),
                )))
            } else {
                self.proxy_connections[proxy_index]
                    .call(method_name, message, write)
                    .await
            };

            match result {
                Ok(bytes) => {
                    if write || !self.find_observer {
                        self.current_active.store(proxy_index, Ordering::SeqCst);
                    }

                    #[cfg(feature = "integration-test")]
                    if let Some(v) = crate::test::PROXY_CALLS.lock().unwrap().as_mut() {
                        v.push((method_name, false));
                    }

                    return Ok(bytes);
                }
                // Authentication failures are caused by the client's credentials, not by
                // the endpoint, so matching Hadoop's FailoverOnNetworkExceptionRetry means
                // returning them without trying another NameNode.
                Err(e) if Self::is_authentication_error(e.error()) => {
                    return Err(e.into_error());
                }
                // RPCError indicates the call was successfully attempted but had an error, so should be returned immediately
                Err(e)
                    if matches!(
                        e.error(),
                        HdfsError::RPCError(exception, _) if !Self::is_retriable(exception)
                    ) =>
                {
                    match e.into_error() {
                        HdfsError::RPCError(exception, msg) => {
                            return Err(Self::convert_rpc_error(exception, msg));
                        }
                        _ => unreachable!(),
                    }
                }
                // A write may already have been committed when the response is lost. Only a
                // standby-style response proves that the request was not handled by this node.
                // A connection failure before the RPC was sent is also safe to fail over.
                Err(e) if write && !Self::is_safe_write_failover_error(&e) => {
                    return Err(e.into_error());
                }
                Err(e) if failovers >= self.failover_max_attempts => {
                    return Err(e.into_error());
                }
                Err(e) => {
                    match e.error() {
                        // Retriable error, just try the next connection
                        HdfsError::RPCError(exception, _)
                        | HdfsError::FatalRPCError(exception, _)
                            if Self::is_retriable(exception) => {}
                        // Some other error, we will retry but log the error
                        _ => warn!("{:?}", e.error()),
                    }

                    failovers += 1;
                    let num_proxies = self.proxy_connections.len() as u32;
                    if failovers.is_multiple_of(num_proxies) {
                        tokio::time::sleep(exponential_failover_sleep(
                            self.failover_sleep_base_millis,
                            self.failover_sleep_max_millis,
                            failovers / num_proxies,
                        ))
                        .await;
                    }

                    proxy_index = (proxy_index + 1) % self.proxy_connections.len();
                }
            }
        }
    }

    fn convert_rpc_error(exception: String, msg: String) -> HdfsError {
        match exception.as_ref() {
            "org.apache.hadoop.fs.FileAlreadyExistsException" => HdfsError::AlreadyExists(msg),
            "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException" => {
                HdfsError::AlreadyExists(msg)
            }
            _ => HdfsError::RPCError(exception, msg),
        }
    }
}

fn exponential_failover_sleep(base_millis: u64, max_millis: u64, failovers: u32) -> Duration {
    let uncapped = base_millis.saturating_mul(1u64 << failovers.min(32));
    let jittered = (uncapped as f64 * rand::rng().random_range(0.5..1.5)) as u64;
    Duration::from_millis(jittered.min(max_millis))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hadoop_native::security::gssapi::GssMajorCodes;

    #[test]
    fn test_authentication_errors_are_not_failed_over() {
        let authentication_errors = [
            HdfsError::SASLError("Kerberos authentication failed".to_string()),
            HdfsError::GSSAPIError(
                GssMajorCodes::GSS_S_FAILURE,
                0,
                "No credentials".to_string(),
            ),
            HdfsError::NoSASLMechanism,
            HdfsError::FatalRPCError(
                SASL_EXCEPTION.to_string(),
                "SASL authentication failed".to_string(),
            ),
            HdfsError::FatalRPCError(
                ACCESS_CONTROL_EXCEPTION.to_string(),
                "Access denied".to_string(),
            ),
            HdfsError::FatalRPCError(
                INVALID_TOKEN_EXCEPTION.to_string(),
                "Invalid token".to_string(),
            ),
        ];

        assert!(
            authentication_errors
                .iter()
                .all(NameServiceProxy::is_authentication_error)
        );
    }

    #[test]
    fn test_transport_errors_can_still_fail_over() {
        let transport_errors = [
            HdfsError::IOError(std::io::Error::other("connection refused")),
            HdfsError::RPCError(
                STANDBY_EXCEPTION.to_string(),
                "NameNode is standby".to_string(),
            ),
            HdfsError::FatalRPCError(
                OBSERVER_RETRY_EXCEPTION.to_string(),
                "Retry on active NameNode".to_string(),
            ),
        ];

        assert!(
            transport_errors
                .iter()
                .all(|error| !NameServiceProxy::is_authentication_error(error))
        );
    }

    #[test]
    fn test_writes_only_fail_over_on_explicit_standby_errors() {
        let safe_failover_errors = [
            HdfsError::RPCError(
                STANDBY_EXCEPTION.to_string(),
                "NameNode is standby".to_string(),
            ),
            HdfsError::FatalRPCError(
                OBSERVER_RETRY_EXCEPTION.to_string(),
                "Retry on active NameNode".to_string(),
            ),
        ];
        assert!(
            safe_failover_errors
                .iter()
                .all(NameServiceProxy::is_safe_failover_error)
        );

        let ambiguous_errors = [
            HdfsError::IOError(std::io::Error::other("connection aborted")),
            HdfsError::FatalRPCError(
                "org.apache.hadoop.ipc.FatalConnectionException".to_string(),
                "request may have been processed".to_string(),
            ),
            HdfsError::SASLError("authentication failed".to_string()),
        ];
        assert!(
            ambiguous_errors
                .iter()
                .all(|error| !NameServiceProxy::is_safe_failover_error(error))
        );
    }

    #[test]
    fn test_pre_request_connection_errors_can_fail_over_writes() {
        let connection_error = ProxyCallError::before_request(HdfsError::IOError(
            std::io::Error::other("connection refused"),
        ));
        assert!(NameServiceProxy::is_safe_write_failover_error(
            &connection_error
        ));

        let ambiguous_error = ProxyCallError::after_request(HdfsError::IOError(
            std::io::Error::other("connection aborted"),
        ));
        assert!(!NameServiceProxy::is_safe_write_failover_error(
            &ambiguous_error
        ));
    }

    #[test]
    fn test_known_unreachable_io_errors_can_fail_over_writes() {
        let safe_errors = [
            ErrorKind::AddrNotAvailable,
            ErrorKind::ConnectionRefused,
            ErrorKind::HostUnreachable,
            ErrorKind::NetworkUnreachable,
            ErrorKind::NotConnected,
        ];
        for kind in safe_errors {
            let error = ProxyCallError::after_request(HdfsError::IOError(kind.into()));
            assert!(NameServiceProxy::is_safe_write_failover_error(&error));
        }

        let ambiguous_errors = [
            ErrorKind::BrokenPipe,
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionReset,
            ErrorKind::TimedOut,
            ErrorKind::UnexpectedEof,
        ];
        for kind in ambiguous_errors {
            let error = ProxyCallError::after_request(HdfsError::IOError(kind.into()));
            assert!(!NameServiceProxy::is_safe_write_failover_error(&error));
        }
    }

    #[test]
    fn test_exponential_failover_sleep() {
        for _ in 0..100 {
            let first_backoff = exponential_failover_sleep(500, 15000, 1);

            assert!(first_backoff >= Duration::from_millis(500));
            assert!(first_backoff <= Duration::from_millis(1500));
            assert_eq!(
                exponential_failover_sleep(500, 15000, 20),
                Duration::from_millis(15000)
            );
        }
    }
}
