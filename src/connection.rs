use std::collections::HashMap;
use std::default::Default;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Error, ErrorKind, Result};
use std::net::TcpStream;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;

use bytes::Bytes;
use bytes::BytesMut;
use log::{debug, warn};
use prost::Message;
use uuid::Uuid;

use crate::proto::common::rpc_response_header_proto::RpcStatusProto;
use crate::proto::{common, hdfs};
use crate::security::sasl::{AuthMethod, SaslRpcClient};
use crate::security::user::User;

const PROTOCOL: &'static str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
const DATA_TRANSFER_VERSION: u16 = 28;

// #[derive(Debug)]
// struct RemoteError {
//     error_code: Option<RpcErrorCodeProto>,
//     error_msg: Option<String>,
//     exception_class_name: Option<String>,
// }

// impl From<RpcResponseHeaderProto> for RemoteError {
//     fn from(value: RpcResponseHeaderProto) -> Self {
//         RemoteError {
//             error_code: RpcErrorCodeProto::from_i32(value.error_detail.unwrap_or(0)),
//             error_msg: value.error_msg,
//             exception_class_name: value.exception_class_name,
//         }
//     }
// }

// impl Display for RemoteError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         todo!()
//     }
// }

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

pub(crate) trait RpcEngine {
    fn call(&self, method_name: &str, message: &[u8]) -> Result<CallResult>;
}

struct CallData {
    finished: bool,
    data: Option<Bytes>,
    error: Option<Error>,
}

impl Default for CallData {
    fn default() -> Self {
        CallData {
            finished: false,
            data: None,
            error: None,
        }
    }
}

struct Call {
    result: Mutex<CallData>,
    cond: Condvar,
}

impl Call {
    fn new() -> Self {
        Call {
            result: Mutex::new(CallData::default()),
            cond: Condvar::new(),
        }
    }
}

pub struct CallResult {
    call: Arc<Call>,
}

impl CallResult {
    fn new(call: Arc<Call>) -> Self {
        CallResult { call }
    }

    pub fn get(&mut self) -> Result<Bytes> {
        let call = &*self.call;
        let mut result = call
            .cond
            .wait_while(call.result.lock().unwrap(), |result| !result.finished)
            .unwrap();

        if let Some(error) = result.error.take() {
            Err(error)
        } else {
            Ok(result.data.take().unwrap())
        }
    }
}

pub(crate) struct RpcConnection {
    client_id: Vec<u8>,
    next_call_id: AtomicI32,
    client: Mutex<SaslRpcClient>,
    alignment_context: Arc<AlignmentContext>,
    call_map: Arc<Mutex<HashMap<i32, Arc<Call>>>>,
    listener: Option<JoinHandle<()>>,
}

impl RpcConnection {
    pub fn connect(url: &str, alignment_context: Arc<AlignmentContext>) -> Result<Self> {
        let client_id = Uuid::new_v4().to_bytes_le().to_vec();
        let next_call_id = AtomicI32::new(0);
        let call_map = Arc::new(Mutex::new(HashMap::new()));

        let mut stream = TcpStream::connect(url)?;
        stream.write("hrpc".as_bytes())?;
        // Current version
        stream.write(&[9u8])?;
        // Service class
        stream.write(&[0u8])?;
        // Auth protocol
        stream.write(&(-33i8).to_be_bytes())?;

        let mut client = SaslRpcClient::create(stream);
        let auth_method = client.negotiate()?;

        let mut conn = RpcConnection {
            client_id,
            next_call_id,
            client: Mutex::new(client),
            alignment_context,
            call_map,
            listener: None,
        };

        let context_header = conn
            .get_connection_header(-3, -1)
            .encode_length_delimited_to_vec();
        let context_msg = conn
            .get_connection_context(auth_method)
            .encode_length_delimited_to_vec();
        conn.write_messages(&[&context_header, &context_msg])?;
        let listener = conn.start_listener()?;
        conn.listener = Some(listener);

        Ok(conn)
    }

    fn start_listener(&mut self) -> std::io::Result<JoinHandle<()>> {
        let call_map = Arc::clone(&self.call_map);
        let stream = self.client.lock().unwrap().try_clone()?;
        let alignment_context = self.alignment_context.clone();
        let listener = thread::spawn(move || {
            RpcListener::new(call_map, stream, alignment_context).start();
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

    pub fn write_messages(&self, messages: &[&Vec<u8>]) -> Result<()> {
        let mut size = 0u32;
        for msg in messages.iter() {
            size += msg.len() as u32;
        }

        let mut client = self.client.lock().unwrap();

        client.write(&size.to_be_bytes())?;
        for msg in messages.iter() {
            client.write(&msg)?;
        }

        Ok(())
    }
}

impl RpcEngine for RpcConnection {
    fn call(&self, method_name: &str, message: &[u8]) -> Result<CallResult> {
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

        let size = (conn_header_buf.len() + header_buf.len() + message.len()) as u32;

        let call = Arc::new(Call::new());

        self.call_map
            .lock()
            .unwrap()
            .insert(call_id, Arc::clone(&call));

        let mut stream = self.client.lock().unwrap();

        stream.write(&size.to_be_bytes())?;
        stream.write(&conn_header_buf)?;
        stream.write(&header_buf)?;
        stream.write(&message)?;
        stream.flush()?;

        Ok(CallResult::new(call))
    }
}

struct RpcListener {
    call_map: Arc<Mutex<HashMap<i32, Arc<Call>>>>,
    stream: SaslRpcClient,
    alive: bool,
    alignment_context: Arc<AlignmentContext>,
}

impl RpcListener {
    fn new(
        call_map: Arc<Mutex<HashMap<i32, Arc<Call>>>>,
        stream: SaslRpcClient,
        alignment_context: Arc<AlignmentContext>,
    ) -> Self {
        RpcListener {
            call_map,
            stream,
            alive: true,
            alignment_context,
        }
    }

    fn start(&mut self) {
        loop {
            if let Err(error) = self.read_response() {
                match error.kind() {
                    ErrorKind::UnexpectedEof => break,
                    _ => panic!("{:?}", error),
                }
            }
        }
        self.alive = false;
    }

    fn read_response(&mut self) -> Result<()> {
        // Read the size of the message
        let mut buf = [0u8; 4];
        self.stream.read_exact(&mut buf)?;
        let msg_length = u32::from_be_bytes(buf);

        // Read the whole message
        let mut buf = BytesMut::zeroed(msg_length as usize);
        self.stream.read_exact(&mut buf)?;

        let mut bytes = buf.freeze();
        let rpc_response =
            common::RpcResponseHeaderProto::decode_length_delimited(&mut bytes).unwrap();

        debug!("RPC header response: {:?}", rpc_response);

        if let Some(RpcStatusProto::Fatal) = RpcStatusProto::from_i32(rpc_response.status) {
            warn!("RPC fatal error: {:?}", rpc_response.error_msg);
            return Err(Error::new(
                ErrorKind::ConnectionAborted,
                rpc_response.error_msg(),
            ));
        }

        let call_id = rpc_response.call_id as i32;

        let call = self.call_map.lock().unwrap().remove(&call_id);

        if let Some(call) = call {
            let mut result = call.result.lock().unwrap();
            match RpcStatusProto::from_i32(rpc_response.status) {
                Some(RpcStatusProto::Error) => {
                    result.error = Some(Error::new(ErrorKind::Other, rpc_response.error_msg()));
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
                    result.data = Some(bytes);
                }
                _ => todo!(),
            }
            result.finished = true;
            call.cond.notify_one();
        }
        Ok(())
    }
}

pub enum Op {
    ReadBlock,
}

impl Op {
    fn value(&self) -> u8 {
        match self {
            Self::ReadBlock => 81,
        }
    }
}

pub struct Packet {
    pub header: hdfs::PacketHeaderProto,
    pub checksum: Bytes,
    pub data: Bytes,
}

impl Packet {
    pub fn new(header: hdfs::PacketHeaderProto, checksum: Bytes, data: Bytes) -> Self {
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
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl DatanodeConnection {
    pub fn connect(url: String) -> Result<Self> {
        let stream = TcpStream::connect(url)?;

        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);

        let conn = DatanodeConnection {
            client_name: Uuid::new_v4().to_string(),
            reader,
            writer,
        };
        Ok(conn)
    }

    pub fn send(&mut self, op: Op, message: &impl Message) -> Result<()> {
        self.writer.write(&DATA_TRANSFER_VERSION.to_be_bytes())?;
        self.writer.write(&[op.value()])?;
        self.writer
            .write(&message.encode_length_delimited_to_vec())?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn build_header(
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

    pub fn read_block_op_response(&mut self) -> Result<hdfs::BlockOpResponseProto> {
        let buf = self.reader.fill_buf().unwrap();
        let msg_length = prost::decode_length_delimiter(buf)?;
        let total_size = msg_length + prost::length_delimiter_len(msg_length);

        let mut response_buf = BytesMut::zeroed(total_size);
        self.reader.read_exact(&mut response_buf)?;

        let response = hdfs::BlockOpResponseProto::decode_length_delimited(response_buf.freeze())?;
        Ok(response)
    }

    pub fn read_packet(&mut self) -> Result<Packet> {
        let mut payload_len_buf = [0u8; 4];
        let mut header_len_buf = [0u8; 2];
        self.reader.read_exact(&mut payload_len_buf)?;
        self.reader.read_exact(&mut header_len_buf)?;

        let payload_length = u32::from_be_bytes(payload_len_buf) as usize;
        let header_length = u16::from_be_bytes(header_len_buf) as usize;

        let mut remaining_buf = BytesMut::zeroed(payload_length - 4 + header_length);
        self.reader.read_exact(&mut remaining_buf)?;

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
