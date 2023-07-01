use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use bytes::Bytes;
use log::warn;
use prost::Message;
use tokio::sync::Mutex;
use url::Url;

use crate::{
    common::config::Configuration,
    connection::{AlignmentContext, RpcConnection},
    proto::hdfs,
    Result,
};

/// Lazily creates a connection to a host, and recreates the connection
/// on fatal errors.
#[derive(Debug)]
struct ProxyConnection {
    url: String,
    inner: Option<RpcConnection>,
    alignment_context: Arc<AlignmentContext>,
}

impl ProxyConnection {
    fn new(url: String, alignment_context: Arc<AlignmentContext>) -> Self {
        ProxyConnection {
            url,
            inner: None,
            alignment_context,
        }
    }

    async fn get_connection(&mut self) -> Result<&RpcConnection> {
        if self.inner.is_none() || !self.inner.as_ref().unwrap().is_alive() {
            self.inner =
                Some(RpcConnection::connect(&self.url, self.alignment_context.clone()).await?);
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
    proxy_connections: Vec<Arc<Mutex<ProxyConnection>>>,
    current_index: AtomicUsize,
    msycned: AtomicBool,
}

impl NameServiceProxy {
    /// Creates a new proxy for a name service. If the URL contains a port,
    /// it is assumed to be for a single NameNode.
    pub(crate) fn new(nameservice: &Url, config: &Configuration) -> Self {
        let alignment_context = Arc::new(AlignmentContext::default());

        let proxy_connections = if let Some(port) = nameservice.port() {
            let url = format!("{}:{}", nameservice.host_str().unwrap(), port);
            vec![Arc::new(Mutex::new(ProxyConnection::new(
                url,
                alignment_context.clone(),
            )))]
        } else {
            config
                .get_urls_for_nameservice(nameservice.host_str().unwrap())
                .into_iter()
                .map(|url| {
                    Arc::new(Mutex::new(ProxyConnection::new(
                        url,
                        alignment_context.clone(),
                    )))
                })
                .collect()
        };

        NameServiceProxy {
            proxy_connections,
            current_index: AtomicUsize::new(0),
            msycned: AtomicBool::new(false),
        }
    }

    async fn msync_if_needed(&self) {
        if !self.msycned.fetch_or(true, Ordering::SeqCst) {
            let msync_msg = hdfs::MsyncRequestProto::default();
            let _ = self
                .call_inner("msync", msync_msg.encode_length_delimited_to_vec())
                .await;
        }
    }

    pub(crate) async fn call(&self, method_name: &'static str, message: Vec<u8>) -> Result<Bytes> {
        self.msync_if_needed().await;
        self.call_inner(method_name, message).await
    }

    async fn call_inner(&self, method_name: &'static str, message: Vec<u8>) -> Result<Bytes> {
        let mut proxy_index = self.current_index.load(Ordering::SeqCst);
        let mut attempts = 0;
        loop {
            let result = self.proxy_connections[proxy_index]
                .lock()
                .await
                .call(&method_name, &message)
                .await;

            match result {
                Ok(bytes) => {
                    self.current_index.store(proxy_index, Ordering::SeqCst);
                    return Ok(bytes);
                }
                Err(_) if attempts >= self.proxy_connections.len() - 1 => return result,
                Err(e) => {
                    warn!("{}", e);
                }
            }

            proxy_index = (proxy_index + 1) % self.proxy_connections.len();
            attempts += 1;
        }
    }
}
