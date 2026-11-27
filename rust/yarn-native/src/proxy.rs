use std::io::ErrorKind;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use hadoop_native::rpc::{RpcConnection, RpcConnectionOptions};
use hadoop_native::security::ClientAuth;
use hadoop_native::security::user::Token;
use log::{debug, warn};
use prost::Message;
use rand::RngExt;
use tokio::runtime::Handle;

use crate::config::{Configuration, FailoverSettings};
use crate::{Result, YarnError};

// RPC exceptions that should be tried
const STANDBY_EXCEPTION: &str = "org.apache.hadoop.ipc.StandbyException";
const RETRIABLE_EXCEPTION: &str = "org.apache.hadoop.ipc.RetriableException";

// Authentication and authorization failures are local to the client
// credentials and cannot be fixed by connecting to another HA endpoint.
const SASL_EXCEPTION: &str = "javax.security.sasl.SaslException";
const GSS_EXCEPTION: &str = "org.ietf.jgss.GSSException";
const ACCESS_CONTROL_EXCEPTION: &str = "org.apache.hadoop.security.AccessControlException";
const AUTHORIZATION_EXCEPTION: &str = "org.apache.hadoop.security.authorize.AuthorizationException";
const INVALID_TOKEN_EXCEPTION: &str = "org.apache.hadoop.security.token.SecretManager$InvalidToken";

const APPLICATION_NOT_FOUND_EXCEPTION: &str =
    "org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException";
const APPLICATION_MASTER_NOT_REGISTERED_EXCEPTION: &str =
    "org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException";
const APPLICATION_ATTEMPT_NOT_FOUND_EXCEPTION: &str =
    "org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException";

/// Static description of a Hadoop RPC protocol.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Protocol {
    pub(crate) name: &'static str,
    pub(crate) token_kind: &'static str,
}

/// Lazily creates a connection to a host, and recreates the connection
/// on fatal errors.
#[derive(Debug)]
struct ProxyConnection {
    address: String,
    protocol: Protocol,
    token_service: String,
    inner: Arc<tokio::sync::Mutex<Option<RpcConnection<()>>>>,
    token: Arc<Mutex<Option<Token>>>,
    effective_user: Option<String>,
    auth: Option<Arc<ClientAuth>>,
    config: Arc<Configuration>,
    handle: Handle,
}

#[derive(Debug)]
enum ProxyCallError {
    BeforeRequest(YarnError),
    AfterRequest(YarnError),
}

impl ProxyCallError {
    fn before_request(error: YarnError) -> Self {
        Self::BeforeRequest(error)
    }

    fn after_request(error: YarnError) -> Self {
        Self::AfterRequest(error)
    }

    fn error(&self) -> &YarnError {
        match self {
            Self::BeforeRequest(error) | Self::AfterRequest(error) => error,
        }
    }

    /// Converts this into the error that [`ServiceProxy::call`] returns.
    ///
    /// Only an exception the server raised for the request is mapped to a dedicated variant,
    /// such as [`YarnError::ApplicationNotFound`]. Connection and authentication errors are
    /// returned unchanged.
    fn into_error(self) -> YarnError {
        match self {
            Self::AfterRequest(YarnError::RPCError(exception, msg)) => {
                ServiceProxy::convert_rpc_error(exception, msg)
            }
            Self::BeforeRequest(error) | Self::AfterRequest(error) => error,
        }
    }

    fn is_pre_request_connection_error(&self) -> bool {
        matches!(self, Self::BeforeRequest(YarnError::IOError(_)))
    }
}

impl ProxyConnection {
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
                    Some(c) if c.is_alive() => (),
                    c => {
                        let token = self.token.lock().unwrap().clone();
                        *c = Some(
                            RpcConnection::connect(
                                &self.address,
                                RpcConnectionOptions {
                                    protocol: self.protocol.name,
                                    token_kind: self.protocol.token_kind,
                                    token_service: &self.token_service,
                                    effective_user: self.effective_user.clone(),
                                    auth: self.auth.clone(),
                                    token,
                                },
                                None,
                                &self.config,
                                &self.handle,
                            )
                            .await
                            .map_err(|error| ProxyCallError::before_request(error.into()))?,
                        );
                    }
                }

                connection.as_ref().unwrap().call(method_name, message)
            };
            let result: Result<Bytes> = rpc_response.await.map_err(YarnError::from);

            match result {
                Ok(bytes) => return Ok(bytes),
                // An ambiguous response can be retried for reads, but retrying a write could
                // replay a mutation that the server already committed.
                Err(YarnError::IOError(ref e)) if !write && attempt == 0 => {
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

/// A proxy to a service that runs on one or more servers, such as the ResourceManagers of an
/// HA cluster or a single NodeManager. Calls go to the server that last succeeded and fail over
/// to the next one on errors that show the request can safely go elsewhere.
#[derive(Debug)]
pub(crate) struct ServiceProxy {
    proxy_connections: Vec<ProxyConnection>,
    current_active: AtomicUsize,
    token: Arc<Mutex<Option<Token>>>,
    token_service: String,
    failover: FailoverSettings,
}

impl ServiceProxy {
    /// Creates a proxy to the servers at `addresses`, in failover order. `token` is used to
    /// authenticate new connections when set; otherwise each connection looks for a token of
    /// the protocol's kind for its server's service in `HADOOP_TOKEN_FILE_LOCATION`, such as
    /// one for `token_service`, the comma-separated services of all the servers.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        protocol: Protocol,
        addresses: Vec<String>,
        token_service: String,
        token: Option<Token>,
        failover: FailoverSettings,
        config: Arc<Configuration>,
        handle: Handle,
        effective_user: Option<String>,
        auth: Option<Arc<ClientAuth>>,
    ) -> Result<Self> {
        if addresses.is_empty() {
            return Err(YarnError::InvalidArgument(format!(
                "No server addresses found for {}",
                protocol.name
            )));
        }
        debug!(
            "Creating {} proxy to {:?} with token service {}",
            protocol.name, addresses, token_service
        );

        let token = Arc::new(Mutex::new(token));
        let proxy_connections: Vec<ProxyConnection> = addresses
            .into_iter()
            .map(|address| ProxyConnection {
                // Like Hadoop's token selectors, look tokens up by this server's own service
                token_service: config.token_service(&address),
                address,
                protocol,
                inner: Arc::new(tokio::sync::Mutex::new(None)),
                token: Arc::clone(&token),
                effective_user: effective_user.clone(),
                auth: auth.clone(),
                config: Arc::clone(&config),
                handle: handle.clone(),
            })
            .collect();

        // Try every server at least once, even when the retry settings allow no failovers
        let failover = FailoverSettings {
            max_attempts: failover
                .max_attempts
                .max(proxy_connections.len() as u32 - 1),
            ..failover
        };

        Ok(Self {
            proxy_connections,
            current_active: AtomicUsize::new(0),
            token,
            token_service,
            failover,
        })
    }

    /// Replaces the token used for new connections. Open connections stay authenticated with
    /// the token they were opened with, as in Hadoop's Java client.
    pub(crate) fn set_token(&self, token: Token) {
        *self.token.lock().unwrap() = Some(token);
    }

    /// The token used for new connections.
    pub(crate) fn token(&self) -> Option<Token> {
        self.token.lock().unwrap().clone()
    }

    /// The service name for tokens to these servers, which covers all of them.
    pub(crate) fn token_service(&self) -> &str {
        &self.token_service
    }

    fn is_retriable(exception: &str) -> bool {
        exception == STANDBY_EXCEPTION || exception == RETRIABLE_EXCEPTION
    }

    fn is_authentication_error(error: &YarnError) -> bool {
        match error {
            YarnError::SASLError(_) | YarnError::GSSAPIError(..) | YarnError::NoSASLMechanism => {
                true
            }
            YarnError::RPCError(exception, _) | YarnError::FatalRPCError(exception, _) => {
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

    fn is_safe_failover_error(error: &YarnError) -> bool {
        match error {
            YarnError::RPCError(exception, _) | YarnError::FatalRPCError(exception, _) => {
                Self::is_retriable(exception)
            }
            _ => false,
        }
    }

    fn is_safe_io_error(error: &YarnError) -> bool {
        matches!(
            error,
            YarnError::IOError(error)
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

    /// Calls a protocol method. `write` marks calls that change server state: they are only
    /// retried when the error shows the server did not handle the request.
    pub(crate) async fn call<Req, Res>(
        &self,
        method_name: &'static str,
        message: Req,
        write: bool,
    ) -> Result<Res>
    where
        Req: Message + Default + std::fmt::Debug,
        Res: Message + Default + std::fmt::Debug,
    {
        debug!("{} request: {:?}", method_name, message);

        let response = self
            .call_inner(
                method_name,
                &message.encode_length_delimited_to_vec(),
                write,
            )
            .await?;

        let decoded = Res::decode_length_delimited(response)?;
        debug!("{} response: {:?}", method_name, decoded);

        Ok(decoded)
    }

    async fn call_inner(
        &self,
        method_name: &'static str,
        message: &[u8],
        write: bool,
    ) -> Result<Bytes> {
        let mut proxy_index = self.current_active.load(Ordering::SeqCst);
        let mut failovers: u32 = 0;

        loop {
            let result = self.proxy_connections[proxy_index]
                .call(method_name, message, write)
                .await;

            match result {
                Ok(bytes) => {
                    self.current_active.store(proxy_index, Ordering::SeqCst);
                    return Ok(bytes);
                }
                // Authentication failures are caused by the client's credentials, not by
                // the endpoint, so matching Hadoop's FailoverOnNetworkExceptionRetry means
                // returning them without trying another server.
                Err(e) if Self::is_authentication_error(e.error()) => {
                    return Err(e.into_error());
                }
                // RPCError indicates the call was successfully attempted but had an error, so should be returned immediately
                Err(e)
                    if matches!(
                        e.error(),
                        YarnError::RPCError(exception, _) if !Self::is_retriable(exception)
                    ) =>
                {
                    return Err(e.into_error());
                }
                // A write may already have been committed when the response is lost. Only a
                // standby-style response proves that the request was not handled by this server.
                // A connection failure before the RPC was sent is also safe to fail over.
                Err(e) if write && !Self::is_safe_write_failover_error(&e) => {
                    return Err(e.into_error());
                }
                Err(e) if failovers >= self.failover.max_attempts => {
                    return Err(e.into_error());
                }
                Err(e) => {
                    match e.error() {
                        // Retriable error, just try the next connection
                        YarnError::RPCError(exception, _)
                        | YarnError::FatalRPCError(exception, _)
                            if Self::is_retriable(exception) => {}
                        // Some other error, we will retry but log the error
                        _ => warn!("{:?}", e.error()),
                    }

                    failovers += 1;
                    let num_proxies = self.proxy_connections.len() as u32;
                    if failovers.is_multiple_of(num_proxies) {
                        tokio::time::sleep(exponential_failover_sleep(
                            self.failover.sleep_base_millis,
                            self.failover.sleep_max_millis,
                            failovers / num_proxies,
                        ))
                        .await;
                    }

                    proxy_index = (proxy_index + 1) % self.proxy_connections.len();
                }
            }
        }
    }

    fn convert_rpc_error(exception: String, msg: String) -> YarnError {
        match exception.as_ref() {
            APPLICATION_NOT_FOUND_EXCEPTION | APPLICATION_ATTEMPT_NOT_FOUND_EXCEPTION => {
                YarnError::ApplicationNotFound(msg)
            }
            APPLICATION_MASTER_NOT_REGISTERED_EXCEPTION => {
                YarnError::ApplicationMasterNotRegistered(msg)
            }
            ACCESS_CONTROL_EXCEPTION => YarnError::PermissionDenied(msg),
            _ => YarnError::RPCError(exception, msg),
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
    use std::collections::HashMap;

    use super::*;
    use crate::proto::yarn::{GetClusterMetricsRequestProto, GetClusterMetricsResponseProto};

    fn proxy(addresses: Vec<String>, max_attempts: u32) -> ServiceProxy {
        let config = Configuration::new(
            None,
            Some(HashMap::from([(
                "ipc.client.connect.timeout".to_string(),
                "200".to_string(),
            )])),
        )
        .unwrap();
        ServiceProxy::new(
            Protocol {
                name: "org.apache.hadoop.example.Protocol",
                token_kind: "EXAMPLE",
            },
            addresses,
            String::new(),
            None,
            FailoverSettings {
                max_attempts,
                sleep_base_millis: 1,
                sleep_max_millis: 1,
            },
            Arc::new(config),
            Handle::current(),
            None,
            None,
        )
        .unwrap()
    }

    /// Addresses that are very likely closed: bind and immediately drop listeners.
    fn closed_addresses(count: usize) -> Vec<String> {
        (0..count)
            .map(|_| {
                std::net::TcpListener::bind("127.0.0.1:0")
                    .unwrap()
                    .local_addr()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    #[test]
    fn test_yarn_exceptions_are_converted_after_a_request() {
        let not_found = ProxyCallError::after_request(YarnError::RPCError(
            APPLICATION_NOT_FOUND_EXCEPTION.to_string(),
            "Application with id 'application_1_0001' doesn't exist in RM.".to_string(),
        ));
        assert!(matches!(
            not_found.into_error(),
            YarnError::ApplicationNotFound(_)
        ));

        let not_registered = ProxyCallError::after_request(YarnError::RPCError(
            APPLICATION_MASTER_NOT_REGISTERED_EXCEPTION.to_string(),
            "AM is not registered".to_string(),
        ));
        assert!(matches!(
            not_registered.into_error(),
            YarnError::ApplicationMasterNotRegistered(_)
        ));

        let rejected = ProxyCallError::before_request(YarnError::RPCError(
            ACCESS_CONTROL_EXCEPTION.to_string(),
            "denied".to_string(),
        ));
        assert!(matches!(rejected.into_error(), YarnError::RPCError(..)));
    }

    #[test]
    fn test_authentication_errors_are_not_failed_over() {
        let authentication_errors = [
            YarnError::SASLError("Kerberos authentication failed".to_string()),
            YarnError::NoSASLMechanism,
            YarnError::FatalRPCError(
                INVALID_TOKEN_EXCEPTION.to_string(),
                "Invalid AMRMToken".to_string(),
            ),
        ];
        assert!(
            authentication_errors
                .iter()
                .all(ServiceProxy::is_authentication_error)
        );
        assert!(!ServiceProxy::is_authentication_error(
            &YarnError::RPCError(STANDBY_EXCEPTION.to_string(), "standby".to_string())
        ));
    }

    #[test]
    fn test_writes_only_fail_over_on_safe_errors() {
        assert!(ServiceProxy::is_safe_write_failover_error(
            &ProxyCallError::after_request(YarnError::RPCError(
                STANDBY_EXCEPTION.to_string(),
                "standby".to_string(),
            ))
        ));
        assert!(ServiceProxy::is_safe_write_failover_error(
            &ProxyCallError::before_request(YarnError::IOError(std::io::Error::other(
                "connection refused"
            )))
        ));
        assert!(!ServiceProxy::is_safe_write_failover_error(
            &ProxyCallError::after_request(YarnError::IOError(ErrorKind::ConnectionReset.into()))
        ));
    }

    #[tokio::test]
    async fn test_connection_errors_fail_over_through_every_server() {
        let proxy = proxy(closed_addresses(2), 2);

        let error = proxy
            .call::<_, GetClusterMetricsResponseProto>(
                "getClusterMetrics",
                GetClusterMetricsRequestProto {},
                true,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, YarnError::IOError(_)), "{error:?}");
        // Neither server answered, so the active index is unchanged
        assert_eq!(proxy.current_active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_every_server_is_tried_at_least_once() {
        // Retry settings that allow no failovers, such as a max wait shorter than the retry
        // interval, still let an HA client reach the second ResourceManager
        assert_eq!(proxy(closed_addresses(2), 0).failover.max_attempts, 1);
        assert_eq!(proxy(closed_addresses(3), 0).failover.max_attempts, 2);
        assert_eq!(proxy(closed_addresses(1), 0).failover.max_attempts, 0);
        assert_eq!(proxy(closed_addresses(2), 5).failover.max_attempts, 5);
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
