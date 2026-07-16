use std::collections::HashMap;
use std::fmt::Debug;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, Ordering};

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
async fn connect(addr: &str, handle: &Handle) -> Result<TcpStream> {
    let addr = addr.to_string();
    // Spawn a task to create the TcpStream so it captures the tokio runtime in case we
    // are not called from one
    let stream = handle.spawn(TcpStream::connect(addr)).await.unwrap()?;
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
}

#[derive(Debug)]
pub struct RpcConnection<C: RpcAlignmentContext> {
    protocol: String,
    client_id: Vec<u8>,
    user_info: UserInfo,
    next_call_id: AtomicI32,
    alignment_context: Option<Arc<Mutex<C>>>,
    call_map: Arc<Mutex<Option<HashMap<i32, CallResult>>>>,
    sender: mpsc::Sender<Vec<u8>>,
    listener: Option<JoinHandle<()>>,
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

        let mut stream = connect(url, handle).await?;
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

    pub async fn write_messages(&self, messages: &[&[u8]]) -> Result<()> {
        let mut size = 0u32;
        for msg in messages.iter() {
            size += msg.len() as u32;
        }

        let mut buf: Vec<u8> = Vec::with_capacity(size as usize + 4);

        buf.extend(size.to_be_bytes());
        for msg in messages.iter() {
            buf.extend(*msg);
        }

        let _ = self.sender.send(buf).await;

        Ok(())
    }

    pub async fn call(
        &self,
        method_name: &str,
        message: &[u8],
    ) -> Result<oneshot::Receiver<Result<Bytes>>> {
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

        let (sender, receiver) = oneshot::channel::<Result<Bytes>>();

        {
            let mut map = self.call_map.lock().unwrap();
            match map.as_mut() {
                Some(m) => {
                    m.insert(call_id, sender);
                }
                None => {
                    return Err(HadoopError::IOError(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "RPC listener disconnected",
                    )));
                }
            }
        }

        self.write_messages(&[&conn_header_buf, &header_buf, message])
            .await?;

        Ok(receiver)
    }
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
    use std::sync::atomic::AtomicI32;
    use std::sync::{Arc, Mutex};

    use tokio::sync::mpsc;

    use super::RpcConnection;
    use crate::security::user::UserInfo;

    #[test]
    fn connection_context_contains_protocol_and_user() {
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
}
