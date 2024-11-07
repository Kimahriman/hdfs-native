use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use bytes::Bytes;
use log::warn;
use prost::Message;
use url::Url;

use crate::{
    common::config::Configuration,
    hdfs::connection::{AlignmentContext, RpcConnection},
    proto::hdfs,
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
    alignment_context: Arc<Mutex<AlignmentContext>>,
    nameservice: Option<String>,
}

impl ProxyConnection {
    fn new(
        url: String,
        alignment_context: Arc<Mutex<AlignmentContext>>,
        nameservice: Option<String>,
    ) -> Self {
        ProxyConnection {
            url,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
            alignment_context,
            nameservice,
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
    current_observers: Arc<Mutex<HashSet<usize>>>,
    msycned: AtomicBool,
}

impl NameServiceProxy {
    /// Creates a new proxy for a name service. If the URL contains a port,
    /// it is assumed to be for a single NameNode.
    pub(crate) fn new(nameservice: &Url, config: &Configuration) -> Result<Self> {
        let alignment_context = Arc::new(Mutex::new(AlignmentContext::default()));

        let proxy_connections = if let Some(port) = nameservice.port() {
            let url = format!("{}:{}", nameservice.host_str().unwrap(), port);
            vec![ProxyConnection::new(url, alignment_context.clone(), None)]
        } else if let Some(host) = nameservice.host_str() {
            // TODO: Add check for no configured namenodes
            config
                .get_urls_for_nameservice(host)?
                .into_iter()
                .map(|url| {
                    ProxyConnection::new(url, alignment_context.clone(), Some(host.to_string()))
                })
                .collect()
        } else {
            todo!()
        };

        if proxy_connections.is_empty() {
            Err(HdfsError::InvalidArgument(
                "No NameNode hosts found".to_string(),
            ))
        } else {
            Ok(NameServiceProxy {
                proxy_connections,
                current_active: AtomicUsize::new(0),
                current_observers: Arc::new(Mutex::new(HashSet::new())),
                msycned: AtomicBool::new(false),
            })
        }
    }

    async fn msync_if_needed(&self, write: bool) -> Result<()> {
        if !self.msycned.fetch_or(true, Ordering::SeqCst) && !write {
            let msync_msg = hdfs::MsyncRequestProto::default();
            self.call_inner("msync", msync_msg.encode_length_delimited_to_vec(), true)
                .await
                .map(|_| ())
                .or_else(|err| match err {
                    HdfsError::RPCError(class, _)
                        if class == "java.lang.UnsupportedOperationException"
                            || class == "org.apache.hadoop.ipc.RpcNoSuchMethodException" =>
                    {
                        Ok(())
                    }
                    _ => Err(err),
                })?;
        }
        Ok(())
    }

    pub(crate) async fn call(
        &self,
        method_name: &'static str,
        message: Vec<u8>,
        write: bool,
    ) -> Result<Bytes> {
        self.msync_if_needed(write).await?;
        self.call_inner(method_name, message, write).await
    }

    fn is_retriable(exception: &str) -> bool {
        exception == STANDBY_EXCEPTION || exception == OBSERVER_RETRY_EXCEPTION
    }

    async fn call_inner(
        &self,
        method_name: &'static str,
        message: Vec<u8>,
        write: bool,
    ) -> Result<Bytes> {
        let current_active = self.current_active.load(Ordering::SeqCst);
        let proxy_indices = if write {
            // If we're writing, try the current known active and then loop
            // through the rest if that fails
            let first = current_active;
            let rest = (0..self.proxy_connections.len()).filter(|i| *i != first);
            [first].into_iter().chain(rest).collect::<Vec<_>>()
        } else {
            // If we're reading, try all known observers, then the active, then
            // any remaining
            let mut first = self
                .current_observers
                .lock()
                .unwrap()
                .iter()
                .copied()
                .collect::<Vec<_>>();
            if !first.contains(&current_active) {
                first.push(current_active);
            }
            let rest = (0..self.proxy_connections.len()).filter(|i| !first.contains(i));
            first.iter().copied().chain(rest).collect()
        };

        let mut attempts = 0;
        loop {
            let proxy_index = proxy_indices[attempts];
            let result = self.proxy_connections[proxy_index]
                .call(method_name, &message)
                .await;

            match result {
                Ok(bytes) => {
                    if write {
                        self.current_active.store(proxy_index, Ordering::SeqCst);
                    } else {
                        self.current_observers.lock().unwrap().insert(proxy_index);
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
                    if Self::is_retriable(&exception) =>
                {
                    match exception.as_ref() {
                        OBSERVER_RETRY_EXCEPTION => {
                            self.current_observers.lock().unwrap().insert(proxy_index);
                        }
                        STANDBY_EXCEPTION => {
                            self.current_observers.lock().unwrap().remove(&proxy_index);
                        }
                        _ => (),
                    }
                }
                Err(e) => {
                    // Some other error, we will retry but log the error
                    self.current_observers.lock().unwrap().remove(&proxy_index);
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
