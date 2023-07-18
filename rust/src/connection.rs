use std::collections::HashMap;
use std::default::Default;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::{mpsc, oneshot};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    task::{self, JoinHandle},
};

use bytes::Bytes;
use bytes::BytesMut;
use log::{debug, warn};
use prost::Message;
use uuid::Uuid;

use crate::proto::common::rpc_response_header_proto::RpcStatusProto;
use crate::proto::{common, hdfs};
use crate::security::sasl::{AuthMethod, SaslReader, SaslRpcClient, SaslWriter};
use crate::security::user::User;
use crate::{HdfsError, Result};

const PROTOCOL: &'static str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
const DATA_TRANSFER_VERSION: u16 = 28;

#[derive(Debug)]
pub(crate) struct AlignmentContext {
    state_id: AtomicI64,
    router_federated_state: Option<Arc<Mutex<Vec<u8>>>>,
}

impl Default for AlignmentContext {
    fn default() -> Self {
        Self {
            state_id: AtomicI64::new(i64::MIN),
            router_federated_state: None,
        }
    }
}

type CallResult = oneshot::Sender<Result<Bytes>>;

#[derive(Debug)]
pub(crate) struct RpcConnection {
    client_id: Vec<u8>,
    next_call_id: AtomicI32,
    alignment_context: Arc<AlignmentContext>,
    call_map: Arc<Mutex<HashMap<i32, CallResult>>>,
    sender: mpsc::Sender<Vec<u8>>,
    listener: Option<JoinHandle<()>>,
}

impl RpcConnection {
    pub(crate) async fn connect(
        url: &str,
        alignment_context: Arc<AlignmentContext>,
        nameservice: Option<&str>,
    ) -> Result<Self> {
        let client_id = Uuid::new_v4().to_bytes_le().to_vec();
        let next_call_id = AtomicI32::new(0);
        let call_map = Arc::new(Mutex::new(HashMap::new()));

        let mut stream = TcpStream::connect(url).await?;
        stream.write("hrpc".as_bytes()).await?;
        // Current version
        stream.write(&[9u8]).await?;
        // Service class
        stream.write(&[0u8]).await?;
        // Auth protocol
        stream.write(&(-33i8).to_be_bytes()).await?;

        let mut client = SaslRpcClient::create(stream);

        let service = nameservice
            .map(|ns| format!("ha-hdfs:{ns}"))
            .unwrap_or(url.to_string());
        let auth_method = client.negotiate(service.as_str()).await?;
        let (reader, writer) = client.split();
        let (sender, receiver) = mpsc::channel::<Vec<u8>>(1000);

        let mut conn = RpcConnection {
            client_id,
            next_call_id,
            alignment_context,
            call_map,
            listener: None,
            sender,
        };

        conn.start_sender(receiver, writer);

        let context_header = conn
            .get_connection_header(-3, -1)
            .encode_length_delimited_to_vec();
        let context_msg = conn
            .get_connection_context(auth_method)
            .encode_length_delimited_to_vec();
        conn.write_messages(&[&context_header, &context_msg])
            .await?;
        let listener = conn.start_listener(reader)?;
        conn.listener = Some(listener);

        Ok(conn)
    }

    fn start_sender(&mut self, mut rx: mpsc::Receiver<Vec<u8>>, mut writer: SaslWriter) {
        task::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match writer.write(&msg).await {
                    Ok(_) => (),
                    Err(_) => break,
                }
            }
        });
    }

    fn start_listener(&mut self, reader: SaslReader) -> Result<JoinHandle<()>> {
        let call_map = Arc::clone(&self.call_map);
        let alignment_context = self.alignment_context.clone();
        let listener = task::spawn(async move {
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
        let mut request_header = common::RpcRequestHeaderProto::default();
        request_header.rpc_kind = Some(common::RpcKindProto::RpcProtocolBuffer as i32);
        // RPC_FINAL_PACKET
        request_header.rpc_op = Some(0);
        request_header.call_id = call_id;
        request_header.client_id = self.client_id.clone();
        request_header.retry_count = Some(retry_count);
        request_header.state_id = Some(self.alignment_context.state_id.load(Ordering::SeqCst));
        request_header.router_federated_state = self
            .alignment_context
            .router_federated_state
            .as_ref()
            .map(|state| state.lock().unwrap().clone());
        request_header
    }

    fn get_connection_context(&self, auth_method: AuthMethod) -> common::IpcConnectionContextProto {
        let mut context = common::IpcConnectionContextProto::default();
        context.protocol = Some(PROTOCOL.to_string());

        let user = User::get()
            .get_user_info(auth_method)
            .expect("Unable to get user for auth method");

        let mut user_info = common::UserInformationProto::default();
        user_info.effective_user = user.effective_user;
        user_info.real_user = user.real_user;
        context.user_info = Some(user_info);
        debug!("Connection context: {:?}", context);
        context
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.listener
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    pub(crate) async fn write_messages(&self, messages: &[&[u8]]) -> Result<()> {
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

    pub(crate) async fn call(&self, method_name: &str, message: &[u8]) -> Result<Bytes> {
        let call_id = self.get_next_call_id();
        let conn_header = self.get_connection_header(call_id, 0);

        debug!("RPC connection header: {:?}", conn_header);

        let conn_header_buf = conn_header.encode_length_delimited_to_vec();

        let mut msg_header = common::RequestHeaderProto::default();
        msg_header.method_name = method_name.to_string();
        msg_header.declaring_class_protocol_name = PROTOCOL.to_string();
        msg_header.client_protocol_version = 1;

        debug!("RPC request header: {:?}", msg_header);

        let header_buf = msg_header.encode_length_delimited_to_vec();

        let (sender, receiver) = oneshot::channel::<Result<Bytes>>();

        self.call_map.lock().unwrap().insert(call_id, sender);

        self.write_messages(&[&conn_header_buf, &header_buf, message])
            .await?;

        receiver.await.unwrap()
    }
}

struct RpcListener {
    call_map: Arc<Mutex<HashMap<i32, CallResult>>>,
    reader: SaslReader,
    alive: bool,
    alignment_context: Arc<AlignmentContext>,
}

impl RpcListener {
    fn new(
        call_map: Arc<Mutex<HashMap<i32, CallResult>>>,
        reader: SaslReader,
        alignment_context: Arc<AlignmentContext>,
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
                match error {
                    HdfsError::IOError(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                    _ => panic!("{:?}", error),
                }
            }
        }
        self.alive = false;
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

        if let Some(RpcStatusProto::Fatal) = RpcStatusProto::from_i32(rpc_response.status) {
            warn!("RPC fatal error: {:?}", rpc_response.error_msg);
            return Err(HdfsError::FatalRPCError(
                rpc_response.error_msg().to_string(),
            ));
        }

        let call_id = rpc_response.call_id as i32;

        let call = self.call_map.lock().unwrap().remove(&call_id);

        if let Some(call) = call {
            match RpcStatusProto::from_i32(rpc_response.status) {
                Some(RpcStatusProto::Error) => {
                    let _ = call.send(Err(HdfsError::RPCError(
                        rpc_response.error_msg().to_string(),
                    )));
                }
                Some(RpcStatusProto::Success) => {
                    if let Some(state_id) = rpc_response.state_id {
                        self.alignment_context
                            .state_id
                            .fetch_max(state_id, Ordering::SeqCst);
                    }
                    if let Some(_router_federation_state) = rpc_response.router_federated_state {
                        todo!();
                    }
                    let _ = call.send(Ok(bytes));
                }
                _ => {
                    return Err(HdfsError::RPCError(
                        "Unknown RPC response status".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

pub(crate) enum Op {
    ReadBlock,
}

impl Op {
    fn value(&self) -> u8 {
        match self {
            Self::ReadBlock => 81,
        }
    }
}

pub(crate) struct Packet {
    pub header: hdfs::PacketHeaderProto,
    #[allow(dead_code)]
    pub checksum: Bytes,
    pub data: Bytes,
}

impl Packet {
    fn new(header: hdfs::PacketHeaderProto, checksum: Bytes, data: Bytes) -> Self {
        Packet {
            header,
            checksum,
            data,
        }
    }
}

#[derive(Debug)]
pub struct DatanodeConnection {
    client_name: String,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl DatanodeConnection {
    pub(crate) async fn connect(url: String) -> Result<Self> {
        let stream = TcpStream::connect(url).await?;

        let (reader, writer) = stream.into_split();

        let conn = DatanodeConnection {
            client_name: Uuid::new_v4().to_string(),
            reader: BufReader::new(reader),
            writer,
        };
        Ok(conn)
    }

    pub(crate) async fn send(&mut self, op: Op, message: &impl Message) -> Result<()> {
        self.writer
            .write(&DATA_TRANSFER_VERSION.to_be_bytes())
            .await?;
        self.writer.write(&[op.value()]).await?;
        self.writer
            .write(&message.encode_length_delimited_to_vec())
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub(crate) fn build_header(
        &self,
        block: &hdfs::ExtendedBlockProto,
        token: Option<common::TokenProto>,
    ) -> hdfs::ClientOperationHeaderProto {
        let mut base_header = hdfs::BaseHeaderProto::default();
        base_header.block = block.clone();
        base_header.token = token;

        let mut header = hdfs::ClientOperationHeaderProto::default();
        header.base_header = base_header;
        header.client_name = self.client_name.clone();
        header
    }

    pub(crate) async fn read_block_op_response(&mut self) -> Result<hdfs::BlockOpResponseProto> {
        let buf = self.reader.fill_buf().await?;
        let msg_length = prost::decode_length_delimiter(buf)?;
        let total_size = msg_length + prost::length_delimiter_len(msg_length);

        let mut response_buf = BytesMut::zeroed(total_size);
        self.reader.read_exact(&mut response_buf).await?;

        let response = hdfs::BlockOpResponseProto::decode_length_delimited(response_buf.freeze())?;
        Ok(response)
    }

    pub(crate) async fn read_packet(&mut self) -> Result<Packet> {
        let mut payload_len_buf = [0u8; 4];
        let mut header_len_buf = [0u8; 2];
        self.reader.read_exact(&mut payload_len_buf).await?;
        self.reader.read_exact(&mut header_len_buf).await?;

        let payload_length = u32::from_be_bytes(payload_len_buf) as usize;
        let header_length = u16::from_be_bytes(header_len_buf) as usize;

        let mut remaining_buf = BytesMut::zeroed(payload_length - 4 + header_length);
        self.reader.read_exact(&mut remaining_buf).await?;

        let bytes = remaining_buf.freeze();

        let header = hdfs::PacketHeaderProto::decode(bytes.slice(0..header_length))?;

        let checksum_length = payload_length - 4 - header.data_len as usize;
        let checksum = bytes.slice(header_length..(header_length + checksum_length));
        let data = bytes.slice(
            (header_length + checksum_length)
                ..(header_length + checksum_length + header.data_len as usize),
        );

        Ok(Packet::new(header, checksum, data))
    }
}
