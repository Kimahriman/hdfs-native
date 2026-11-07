use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use log::{debug, warn};
use prost::Message;
use socket2::SockRef;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::{io::AsyncWriteExt, net::TcpStream, task::JoinHandle};
use uuid::Uuid;

use crate::config::Configuration;
use crate::proto::common;
use crate::proto::common::rpc_response_header_proto::RpcStatusProto;
use crate::security::sasl::{SaslReader, SaslWriter, negotiate_sasl_session};
use crate::security::user::UserInfo;
use crate::{HadoopError, Result};

// Connect to a remote host and return a TcpStream with standard options we want
async fn connect_with_timeout<T, F>(
    addr: &str,
    timeout: Option<Duration>,
    connect: F,
) -> std::io::Result<T>
where
    F: Future<Output = std::io::Result<T>>,
{
    let Some(timeout) = timeout else {
        return connect.await;
    };

    tokio::time::timeout(timeout, connect).await.map_err(|_| {
        std::io::Error::new(
            ErrorKind::TimedOut,
            format!(
                "Timed out connecting to {addr} after {}ms",
                timeout.as_millis()
            ),
        )
    })?
}

async fn connect(addr: &str, config: &Configuration, handle: &Handle) -> Result<TcpStream> {
    let addr = addr.to_string();
    let timeout = config.ipc_client_connect_timeout();
    // Spawn a task to create the TcpStream so it captures the tokio runtime in case we
    // are not called from one
    let stream = handle
        .spawn(async move {
            let connect = TcpStream::connect(addr.as_str());
            connect_with_timeout(&addr, timeout, connect).await
        })
        .await
        .unwrap()?;
    stream.set_nodelay(true)?;

    let sf = SockRef::from(&stream);
    sf.set_keepalive(true)?;

    Ok(stream)
}

type CallResult = oneshot::Sender<Result<Bytes>>;

/// Service-specific state carried in Hadoop RPC request and response headers.
pub trait RpcAlignmentContext: Debug + Send + 'static {
    fn request_state(&self) -> (Option<i64>, Option<Vec<u8>>);
    fn update_response(
        &mut self,
        state_id: Option<i64>,
        federated_state: Option<Vec<u8>>,
    ) -> Result<()>;
}

impl RpcAlignmentContext for () {
    fn request_state(&self) -> (Option<i64>, Option<Vec<u8>>) {
        (None, None)
    }

    fn update_response(&mut self, _: Option<i64>, _: Option<Vec<u8>>) -> Result<()> {
        Ok(())
    }
}

/// Service-specific settings used to establish a Hadoop RPC connection.
pub struct RpcConnectionOptions<'a> {
    pub protocol: &'a str,
    pub token_kind: &'a str,
    pub token_service: &'a str,
    pub effective_user: Option<String>,
    pub auth: Option<Arc<crate::security::ClientAuth>>,
}

#[derive(Debug)]
pub struct RpcConnection<C: RpcAlignmentContext> {
    protocol: String,
    client_id: Vec<u8>,
    user_info: UserInfo,
    next_call_id: AtomicI32,
    alignment_context: Option<Arc<Mutex<C>>>,
    call_map: Arc<Mutex<Option<HashMap<i32, CallResult>>>>,
    rpc_timeout: Option<Duration>,
    handle: Handle,
    sender: mpsc::Sender<Vec<u8>>,
    listener: Option<JoinHandle<()>>,
}

/// A pending RPC response that uses the configured Hadoop RPC timeout.
struct PendingRpcCall {
    call_id: i32,
    method_name: String,
    receiver: Option<oneshot::Receiver<Result<Bytes>>>,
    call_map: Arc<Mutex<Option<HashMap<i32, CallResult>>>>,
    timeout: Option<Duration>,
    handle: Handle,
}

impl PendingRpcCall {
    async fn wait(mut self) -> Result<Bytes> {
        let receiver = self
            .receiver
            .take()
            .expect("RPC response receiver should only be consumed once");

        let response = match self.timeout {
            Some(timeout) => {
                let timed_response = {
                    let _runtime = self.handle.enter();
                    tokio::time::timeout(timeout, receiver)
                };
                timed_response.await.map_err(|_| {
                    HadoopError::IOError(std::io::Error::new(
                        ErrorKind::TimedOut,
                        format!(
                            "Timed out waiting for RPC response to {} after {}ms",
                            self.method_name,
                            timeout.as_millis()
                        ),
                    ))
                })?
            }
            None => receiver.await,
        };

        response.map_err(|_| {
            HadoopError::IOError(std::io::Error::new(
                ErrorKind::ConnectionAborted,
                "RPC listener disconnected",
            ))
        })?
    }
}

impl Drop for PendingRpcCall {
    fn drop(&mut self) {
        if let Some(call_map) = self.call_map.lock().unwrap().as_mut() {
            call_map.remove(&self.call_id);
        }
    }
}

impl<C: RpcAlignmentContext> RpcConnection<C> {
    pub async fn connect(
        url: &str,
        options: RpcConnectionOptions<'_>,
        alignment_context: Option<Arc<Mutex<C>>>,
        config: &Configuration,
        handle: &Handle,
    ) -> Result<Self> {
        let client_id = Uuid::new_v4().to_bytes_le().to_vec();
        let next_call_id = AtomicI32::new(0);
        let call_map = Arc::new(Mutex::new(Some(HashMap::new())));

        let mut stream = connect(url, config, handle).await?;
        stream.write_all("hrpc".as_bytes()).await?;
        // Current version
        stream.write_all(&[9u8]).await?;
        // Service class
        stream.write_all(&[0u8]).await?;
        // Auth protocol
        if config.security_enabled() {
            stream.write_all(&(-33i8).to_be_bytes()).await?;
        } else {
            stream.write_all(&(0i8).to_be_bytes()).await?;
        }

        let (user_info, reader, writer) = negotiate_sasl_session(
            stream,
            options.token_kind,
            options.token_service,
            config,
            options.effective_user,
            options.auth,
        )
        .await?;
        let (sender, receiver) = mpsc::channel::<Vec<u8>>(1000);

        let mut conn = RpcConnection {
            protocol: options.protocol.to_owned(),
            client_id,
            user_info,
            next_call_id,
            alignment_context,
            call_map,
            rpc_timeout: config.ipc_client_rpc_timeout(),
            handle: handle.clone(),
            listener: None,
            sender,
        };

        conn.start_sender(receiver, writer, handle);

        let context_header = conn
            .get_connection_header(-3, -1)
            .encode_length_delimited_to_vec();
        let context_msg = conn
            .get_connection_context()
            .encode_length_delimited_to_vec();
        conn.write_messages(&[&context_header, &context_msg])
            .await?;
        let listener = conn.start_listener(reader, handle)?;
        conn.listener = Some(listener);

        Ok(conn)
    }

    fn start_sender(
        &mut self,
        mut rx: mpsc::Receiver<Vec<u8>>,
        mut writer: SaslWriter,
        handle: &Handle,
    ) {
        handle.spawn(async move {
            while let Some(msg) = rx.recv().await {
                match writer.write_all(&msg).await {
                    Ok(_) => (),
                    Err(_) => break,
                }
            }
        });
    }

    fn start_listener(&mut self, reader: SaslReader, handle: &Handle) -> Result<JoinHandle<()>> {
        let call_map = Arc::clone(&self.call_map);
        let alignment_context = self.alignment_context.clone();
        let listener = handle.spawn(async move {
            RpcListener::new(call_map, reader, alignment_context)
                .start()
                .await;
        });
        Ok(listener)
    }

    fn get_next_call_id(&self) -> i32 {
        self.next_call_id.fetch_add(1, Ordering::SeqCst)
    }

    fn get_connection_header(
        &self,
        call_id: i32,
        retry_count: i32,
    ) -> common::RpcRequestHeaderProto {
        let (state_id, router_federated_state) =
            if let Some(context) = self.alignment_context.as_ref() {
                let context = context.lock().unwrap();
                context.request_state()
            } else {
                (None, None)
            };

        common::RpcRequestHeaderProto {
            rpc_kind: Some(common::RpcKindProto::RpcProtocolBuffer as i32),
            // RPC_FINAL_PACKET
            rpc_op: Some(0),
            call_id,
            client_id: self.client_id.clone(),
            retry_count: Some(retry_count),
            state_id,
            router_federated_state,
            ..Default::default()
        }
    }

    fn get_connection_context(&self) -> common::IpcConnectionContextProto {
        let user_info = common::UserInformationProto {
            effective_user: self.user_info.effective_user.clone(),
            real_user: self.user_info.real_user.clone(),
        };

        let context = common::IpcConnectionContextProto {
            protocol: Some(self.protocol.clone()),
            user_info: Some(user_info),
        };

        debug!("Connection context: {:?}", context);
        context
    }

    pub fn is_alive(&self) -> bool {
        self.listener
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    async fn write_messages(&self, messages: &[&[u8]]) -> Result<()> {
        let _ = self.sender.send(encode_messages(messages)).await;

        Ok(())
    }

    /// Send an RPC and await its response, applying `ipc.client.rpc-timeout.ms`.
    pub fn call(
        &self,
        method_name: &str,
        message: &[u8],
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let call_id = self.get_next_call_id();
        let conn_header = self.get_connection_header(call_id, 0);

        debug!("RPC connection header: {:?}", conn_header);

        let conn_header_buf = conn_header.encode_length_delimited_to_vec();

        let msg_header = common::RequestHeaderProto {
            method_name: method_name.to_string(),
            declaring_class_protocol_name: self.protocol.clone(),
            client_protocol_version: 1,
        };
        debug!("RPC request header: {:?}", msg_header);

        let header_buf = msg_header.encode_length_delimited_to_vec();
        let request = encode_messages(&[&conn_header_buf, &header_buf, message]);
        let method_name = method_name.to_owned();
        let call_map = Arc::clone(&self.call_map);
        let sender = self.sender.clone();
        let timeout = self.rpc_timeout;
        let handle = self.handle.clone();

        async move {
            let (response_sender, receiver) = oneshot::channel::<Result<Bytes>>();
            {
                let mut map = call_map.lock().unwrap();
                match map.as_mut() {
                    Some(m) => {
                        m.insert(call_id, response_sender);
                    }
                    None => {
                        return Err(HadoopError::IOError(std::io::Error::new(
                            ErrorKind::ConnectionAborted,
                            "RPC listener disconnected",
                        )));
                    }
                }
            }

            let response = PendingRpcCall {
                call_id,
                method_name,
                receiver: Some(receiver),
                call_map,
                timeout,
                handle,
            };

            if sender.send(request).await.is_err() {
                return Err(HadoopError::IOError(std::io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "RPC sender disconnected",
                )));
            }

            response.wait().await
        }
    }
}

fn encode_messages(messages: &[&[u8]]) -> Vec<u8> {
    let size: u32 = messages.iter().map(|message| message.len() as u32).sum();
    let mut buf = Vec::with_capacity(size as usize + 4);
    buf.extend(size.to_be_bytes());
    for message in messages {
        buf.extend(*message);
    }
    buf
}

struct RpcListener<C: RpcAlignmentContext> {
    call_map: Arc<Mutex<Option<HashMap<i32, CallResult>>>>,
    reader: SaslReader,
    alive: bool,
    alignment_context: Option<Arc<Mutex<C>>>,
}

impl<C: RpcAlignmentContext> RpcListener<C> {
    fn new(
        call_map: Arc<Mutex<Option<HashMap<i32, CallResult>>>>,
        reader: SaslReader,
        alignment_context: Option<Arc<Mutex<C>>>,
    ) -> Self {
        RpcListener {
            call_map,
            reader,
            alive: true,
            alignment_context,
        }
    }

    async fn start(&mut self) {
        loop {
            if let Err(error) = self.read_response().await {
                match &error {
                    HadoopError::IOError(e) if e.kind() == ErrorKind::UnexpectedEof => {}
                    e => {
                        warn!("RPC listener error: {:?}", e);
                    }
                }
                break;
            }
        }
        self.alive = false;

        if let Some(map) = self.call_map.lock().unwrap().take() {
            for (_, call) in map {
                let _ = call.send(Err(HadoopError::IOError(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "RPC listener disconnected",
                ))));
            }
        }
    }

    async fn read_response(&mut self) -> Result<()> {
        // Read the size of the message
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf).await?;
        let msg_length = u32::from_be_bytes(buf);

        // Read the whole message
        let mut buf = BytesMut::zeroed(msg_length as usize);
        self.reader.read_exact(&mut buf).await?;

        let mut bytes = buf.freeze();
        let rpc_response = common::RpcResponseHeaderProto::decode_length_delimited(&mut bytes)?;

        debug!("RPC header response: {:?}", rpc_response);

        let call_id = rpc_response.call_id as i32;

        let call = self
            .call_map
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|m| m.remove(&call_id));

        if let Some(call) = call {
            match rpc_response.status() {
                RpcStatusProto::Success => {
                    self.alignment_context
                        .as_ref()
                        .map(|alignment_context| {
                            alignment_context.lock().unwrap().update_response(
                                rpc_response.state_id,
                                rpc_response.router_federated_state,
                            )
                        })
                        .transpose()?;

                    let _ = call.send(Ok(bytes));
                }
                RpcStatusProto::Error => {
                    let _ = call.send(Err(HadoopError::RPCError(
                        rpc_response.exception_class_name().to_string(),
                        rpc_response.error_msg().to_string(),
                    )));
                }
                RpcStatusProto::Fatal => {
                    warn!(
                        "RPC fatal error: {}: {}",
                        rpc_response.exception_class_name(),
                        rpc_response.error_msg()
                    );
                    return Err(HadoopError::FatalRPCError(
                        rpc_response.exception_class_name().to_string(),
                        rpc_response.error_msg().to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::ErrorKind;
    use std::sync::atomic::AtomicI32;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::mpsc;

    use tokio::sync::oneshot;

    use super::{PendingRpcCall, RpcConnection, connect_with_timeout};
    use crate::security::user::UserInfo;

    #[test]
    fn connection_context_contains_protocol_and_user() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let connection = RpcConnection::<()> {
            protocol: "org.apache.hadoop.example.Protocol".to_owned(),
            client_id: Vec::new(),
            user_info: UserInfo {
                real_user: Some("real-user".to_owned()),
                effective_user: Some("alice".to_owned()),
            },
            next_call_id: AtomicI32::new(0),
            alignment_context: None,
            call_map: Arc::new(Mutex::new(Some(HashMap::new()))),
            rpc_timeout: None,
            handle: runtime.handle().clone(),
            sender,
            listener: None,
        };

        let context = connection.get_connection_context();
        assert_eq!(
            context.protocol.as_deref(),
            Some("org.apache.hadoop.example.Protocol")
        );
        let user = context.user_info.unwrap();
        assert_eq!(user.real_user.as_deref(), Some("real-user"));
        assert_eq!(user.effective_user.as_deref(), Some("alice"));
    }

    #[test]
    fn connect_timeout_returns_timed_out_io_error() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let error = runtime
            .block_on(connect_with_timeout(
                "namenode.example.com:8020",
                Some(Duration::from_millis(1)),
                std::future::pending::<std::io::Result<()>>(),
            ))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::TimedOut);
        assert_eq!(
            error.to_string(),
            "Timed out connecting to namenode.example.com:8020 after 1ms"
        );
    }

    #[test]
    fn rpc_timeout_returns_timed_out_error_and_removes_pending_call() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let call_map = Arc::new(Mutex::new(Some(HashMap::new())));
        let (sender, receiver) = oneshot::channel();
        call_map.lock().unwrap().as_mut().unwrap().insert(1, sender);

        let response = PendingRpcCall {
            call_id: 1,
            method_name: "getFileInfo".to_owned(),
            receiver: Some(receiver),
            call_map: Arc::clone(&call_map),
            timeout: Some(Duration::from_millis(1)),
            handle: runtime.handle().clone(),
        };
        let error = runtime.block_on(response.wait()).unwrap_err();

        assert!(
            matches!(error, crate::HadoopError::IOError(ref e) if e.kind() == ErrorKind::TimedOut)
        );
        assert!(call_map.lock().unwrap().as_ref().unwrap().is_empty());
    }
}
