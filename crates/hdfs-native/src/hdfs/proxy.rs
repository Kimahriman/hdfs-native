use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
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
    inner: Option<RpcConnection>,
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
            inner: None,
            alignment_context,
            nameservice,
        }
    }

    async fn get_connection(&mut self) -> Result<&RpcConnection> {
        if self.inner.is_none() || !self.inner.as_ref().unwrap().is_alive() {
            self.inner = Some(
                RpcConnection::connect(
                    &self.url,
                    self.alignment_context.clone(),
                    self.nameservice.as_deref(),
                )
                .await?,
            );
        }
        Ok(self.inner.as_ref().unwrap())
    }

    async fn call(&mut self, method_name: &str, message: &[u8]) -> Result<Bytes> {
        self.get_connection()
            .await?
            .call(method_name, message)
            .await
    }
}

#[derive(Debug)]
pub(crate) struct NameServiceProxy {
    proxy_connections: Vec<Arc<tokio::sync::Mutex<ProxyConnection>>>,
    current_index: AtomicUsize,
    msycned: AtomicBool,
}

impl NameServiceProxy {
    /// Creates a new proxy for a name service. If the URL contains a port,
    /// it is assumed to be for a single NameNode.
    pub(crate) fn new(nameservice: &Url, config: &Configuration) -> Self {
        let alignment_context = Arc::new(Mutex::new(AlignmentContext::default()));

        let proxy_connections = if let Some(port) = nameservice.port() {
            let url = format!("{}:{}", nameservice.host_str().unwrap(), port);
            vec![Arc::new(tokio::sync::Mutex::new(ProxyConnection::new(
                url,
                alignment_context.clone(),
                None,
            )))]
        } else if let Some(host) = nameservice.host_str() {
            // TODO: Add check for no configured namenodes
            config
                .get_urls_for_nameservice(host)
                .into_iter()
                .map(|url| {
                    Arc::new(tokio::sync::Mutex::new(ProxyConnection::new(
                        url,
                        alignment_context.clone(),
                        Some(host.to_string()),
                    )))
                })
                .collect()
        } else {
            todo!()
        };

        NameServiceProxy {
            proxy_connections,
            current_index: AtomicUsize::new(0),
            msycned: AtomicBool::new(false),
        }
    }

    async fn msync_if_needed(&self) -> Result<()> {
        if !self.msycned.fetch_or(true, Ordering::SeqCst) {
            let msync_msg = hdfs::MsyncRequestProto::default();
            self.call_inner("msync", msync_msg.encode_length_delimited_to_vec())
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

    pub(crate) async fn call(&self, method_name: &'static str, message: Vec<u8>) -> Result<Bytes> {
        self.msync_if_needed().await?;
        self.call_inner(method_name, message).await
    }

    fn is_retriable(exception: &str) -> bool {
        exception == STANDBY_EXCEPTION || exception == OBSERVER_RETRY_EXCEPTION
    }

    async fn call_inner(&self, method_name: &'static str, message: Vec<u8>) -> Result<Bytes> {
        let mut proxy_index = self.current_index.load(Ordering::SeqCst);
        let mut attempts = 0;
        loop {
            let result = self.proxy_connections[proxy_index]
                .lock()
                .await
                .call(method_name, &message)
                .await;

            match result {
                Ok(bytes) => {
                    self.current_index.store(proxy_index, Ordering::SeqCst);
                    return Ok(bytes);
                }
                // RPCError indicates the call was successfully attempted but had an error, so should be returned immediately
                Err(HdfsError::RPCError(exception, msg)) if !Self::is_retriable(&exception) => {
                    warn!("{}: {}", exception, msg);
                    return Err(Self::convert_rpc_error(exception, msg));
                }
                Err(_) if attempts >= self.proxy_connections.len() - 1 => return result,
                Err(e) => {
                    warn!("{:?}", e);
                }
            }

            proxy_index = (proxy_index + 1) % self.proxy_connections.len();
            attempts += 1;
        }
    }

    fn convert_rpc_error(exception: String, msg: String) -> HdfsError {
        match exception.as_ref() {
            "org.apache.hadoop.fs.FileAlreadyExistsException" => HdfsError::AlreadyExists(msg),
            _ => HdfsError::RPCError(exception, msg),
        }
    }
}
