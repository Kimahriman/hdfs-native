use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use bytes::Bytes;
use log::{debug, warn};
use prost::Message;
use tokio::runtime::Handle;
use url::Url;

use crate::{
    common::config::Configuration,
    hdfs::connection::{AlignmentContext, RpcConnection},
    proto::{common::HaServiceStateProto, hdfs},
    HdfsError, Result,
};

// RPC exceptions that should be tried
const STANDBY_EXCEPTION: &str = "org.apache.hadoop.ipc.StandbyException";
const OBSERVER_RETRY_EXCEPTION: &str = "org.apache.hadoop.ipc.ObserverRetryOnActiveException";

/// Lazily creates a connection to a host, and recreates the connection
/// on fatal errors.
#[derive(Debug)]
struct ProxyConnection {
    url: String,
    inner: Arc<tokio::sync::Mutex<Option<RpcConnection>>>,
    alignment_context: Option<Arc<Mutex<AlignmentContext>>>,
    nameservice: Option<String>,
    config: Arc<Configuration>,
    handle: Handle,
}

impl ProxyConnection {
    fn new(
        url: String,
        alignment_context: Option<Arc<Mutex<AlignmentContext>>>,
        nameservice: Option<String>,
        config: Arc<Configuration>,
        handle: Handle,
    ) -> Self {
        ProxyConnection {
            url,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
            alignment_context,
            nameservice,
            config,
            handle,
        }
    }

    async fn call(&self, method_name: &str, message: &[u8]) -> Result<Bytes> {
        let receiver = {
            let mut connection = self.inner.lock().await;
            match &mut *connection {
                Some(c) if c.is_alive() => (),
                c => {
                    *c = Some(
                        RpcConnection::connect(
                            &self.url,
                            self.alignment_context.clone(),
                            self.nameservice.as_deref(),
                            &self.config,
                            &self.handle,
                        )
                        .await?,
                    );
                }
            }

            connection
                .as_ref()
                .unwrap()
                .call(method_name, message)
                .await?
        };
        receiver.await.unwrap()
    }
}

#[derive(Debug)]
pub(crate) struct NameServiceProxy {
    proxy_connections: Vec<ProxyConnection>,
    current_active: AtomicUsize,
    find_observer: bool,
    current_observer: tokio::sync::Mutex<Option<usize>>,
    msynced: Option<tokio::sync::Mutex<bool>>,
}

impl NameServiceProxy {
    /// Creates a new proxy for a name service. If the URL contains a port,
    /// it is assumed to be for a single NameNode.
    pub(crate) fn new(
        nameservice: &Url,
        config: Arc<Configuration>,
        handle: Handle,
    ) -> Result<Self> {
        let host = nameservice.host_str().ok_or(HdfsError::InvalidArgument(
            "No host for name service".to_string(),
        ))?;

        let (context_enabled, find_observer) = match config.get_proxy_for_nameservice(host)
        {
            Some("org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider") => {
                (true, true)
            }
            Some("org.apache.hadoop.hdfs.server.namenode.ha.RouterObserverReadConfiguredFailoverProxyProvider") => {
                (true, false)
            }
            Some("org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider") | None => (false, false),
            Some(provider) => {
                warn!("Unsupported proxy provider {provider}, falling back to ConfiguredFailoverProxyProvider behavior");
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

    async fn find_observer(&self) -> Option<usize> {
        for i in 0..self.proxy_connections.len() {
            let ha_state_msg = hdfs::HaServiceStateRequestProto::default();
            let response = self.proxy_connections[i]
                .call(
                    "getHAServiceState",
                    &ha_state_msg.encode_length_delimited_to_vec(),
                )
                .await;

            match response {
                Ok(response) => {
                    if let Ok(ha_state) =
                        hdfs::HaServiceStateResponseProto::decode_length_delimited(response)
                    {
                        if matches!(ha_state.state(), HaServiceStateProto::Observer) {
                            return Some(i);
                        }
                    }
                }
                Err(e) => {
                    debug!("Couldn't get HA service status: {e:?}");
                    continue;
                }
            }
        }
        None
    }

    async fn call_observer(&self, method_name: &'static str, message: &[u8]) -> Result<Bytes> {
        let observer_index = {
            let mut observer = self.current_observer.lock().await;
            if let Some(index) = *observer {
                index
            } else if let Some(index) = self.find_observer().await {
                *observer = Some(index);
                index
            } else {
                return Err(HdfsError::InternalError(
                    "Unable to find observer node".to_string(),
                ));
            }
        };
        let result = self.proxy_connections[observer_index]
            .call(method_name, message)
            .await;

        #[cfg(feature = "integration-test")]
        if result.is_ok() {
            if let Some(v) = crate::test::PROXY_CALLS.lock().unwrap().as_mut() {
                v.push((method_name, true));
            }
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
                Err(e) => warn!("Failed to call observer node, falling back to the active: {e:?}"),
            }
        }
        let current_active = self.current_active.load(Ordering::SeqCst);
        let rest = (0..self.proxy_connections.len()).filter(|i| *i != current_active);
        let proxy_indices = [current_active].into_iter().chain(rest).collect::<Vec<_>>();

        let mut attempts = 0;
        loop {
            let proxy_index = proxy_indices[attempts];
            let result = self.proxy_connections[proxy_index]
                .call(method_name, message)
                .await;

            match result {
                Ok(bytes) => {
                    // We may have gotten a result from an observer node for a read, so only remember
                    // the active if this is a write method
                    if write {
                        self.current_active.store(proxy_index, Ordering::SeqCst);
                    }

                    #[cfg(feature = "integration-test")]
                    if let Some(v) = crate::test::PROXY_CALLS.lock().unwrap().as_mut() {
                        v.push((method_name, false));
                    }

                    return Ok(bytes);
                }
                // RPCError indicates the call was successfully attempted but had an error, so should be returned immediately
                Err(HdfsError::RPCError(exception, msg)) if !Self::is_retriable(&exception) => {
                    return Err(Self::convert_rpc_error(exception, msg));
                }
                Err(_) if attempts >= self.proxy_connections.len() - 1 => return result,
                // Retriable error, do nothing and try the next connection
                Err(HdfsError::RPCError(exception, _))
                | Err(HdfsError::FatalRPCError(exception, _))
                    if Self::is_retriable(&exception) => {}
                Err(e) => {
                    // Some other error, we will retry but log the error
                    warn!("{:?}", e);
                }
            }

            attempts += 1;
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
