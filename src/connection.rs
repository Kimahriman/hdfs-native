use std::collections::HashMap;
use std::default::Default;
use std::io::prelude::*;
use std::io::Result;
use std::io::{BufReader, BufWriter};
use std::marker::PhantomData;
use std::net::TcpStream;
use std::result;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::thread;

use bytes::Bytes;
use bytes::BytesMut;
use prost::DecodeError;
use prost::Message;
use uuid::Uuid;

use crate::proto::{common, hdfs};
use crate::security::sasl::SaslClient;

const PROTOCOL: &'static str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
const DATA_TRANSFER_VERSION: u16 = 28;

pub trait RpcEngine {
    fn call<T: Message + Default>(
        &self,
        method_name: &str,
        message: &impl Message,
    ) -> Result<CallResult<T>>;
}

struct Call {
    result: Mutex<Option<Bytes>>,
    cond: Condvar,
}

impl Call {
    fn new() -> Self {
        Call {
            result: Mutex::new(None),
            cond: Condvar::new(),
        }
    }
}

pub struct CallResult<T: Message + Default> {
    call: Arc<Call>,
    phantom: PhantomData<T>,
}

impl<T: Message + Default> CallResult<T> {
    fn new(call: Arc<Call>) -> Self {
        CallResult {
            call,
            phantom: PhantomData,
        }
    }

    pub fn get(&mut self) -> result::Result<T, DecodeError> {
        let call = &*self.call;
        let mut result = call
            .cond
            .wait_while(call.result.lock().unwrap(), |result| result.is_none())
            .unwrap();
        let buf = result.take().unwrap();
        T::decode_length_delimited(buf)
    }
}

struct RpcConnection {
    client: SaslClient,
}

impl RpcConnection {
    pub fn connect(url: String) -> Result<Self> {
        let mut stream = TcpStream::connect(url)?;
        stream.write("hrpc".as_bytes())?;
        // Current version
        stream.write(&[9u8])?;
        // Service class
        stream.write(&[0u8])?;
        // Auth protocol
        stream.write(&(-33i8).to_be_bytes())?;

        let client = SaslClient::create(stream)?;
        Ok(RpcConnection { client })
    }

    pub fn try_clone(&self) -> Result<Self> {
        Ok(RpcConnection {
            client: self.client.try_clone()?,
        })
    }
}

impl Read for RpcConnection {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.client.read(buf)
    }
}

impl Write for RpcConnection {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.client.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.client.flush()
    }
}

pub struct NamenodeConnection {
    client_id: Vec<u8>,
    next_call_id: Mutex<AtomicI32>,
    stream: Mutex<RpcConnection>,
    call_map: Arc<Mutex<HashMap<i32, Arc<Call>>>>,
}

impl NamenodeConnection {
    pub fn connect(url: String) -> Result<Self> {
        let client_id = Uuid::new_v4().to_bytes_le().to_vec();
        let next_call_id = Mutex::new(AtomicI32::new(0));
        let call_map = Arc::new(Mutex::new(HashMap::new()));
        let stream = Mutex::new(RpcConnection::connect(url)?);

        let mut conn = NamenodeConnection {
            client_id,
            next_call_id,
            stream,
            call_map,
        };
        conn.initialize()?;
        Ok(conn)
    }

    fn initialize(&mut self) -> std::io::Result<()> {
        let context_header = self
            .get_connection_header(-3, -1)
            .encode_length_delimited_to_vec();
        let context_msg = self
            .get_connection_context()
            .encode_length_delimited_to_vec();
        self.write_messages(&[&context_header, &context_msg])?;
        self.start_listener()?;

        Ok(())
    }

    fn start_listener(&mut self) -> std::io::Result<()> {
        let call_map = Arc::clone(&self.call_map);
        let mut stream = self.stream.lock().unwrap().try_clone()?;
        thread::spawn(move || loop {
            let mut buf = [0u8; 4];
            let size = stream.read(&mut buf).unwrap();

            let msg_length = u32::from_be_bytes(buf);

            let mut buf: Vec<u8> = Vec::new();
            buf.resize(msg_length as usize, 0);

            let size = stream.read(&mut buf).unwrap();

            let mut bytes = Bytes::from(buf);
            let rpc_response =
                common::RpcResponseHeaderProto::decode_length_delimited(&mut bytes).unwrap();

            let call_id = rpc_response.call_id as i32;

            let mut call_map_data = call_map.lock().unwrap();
            let call = call_map_data.remove(&call_id);
            drop(call_map_data);

            match call {
                Some(call) => {
                    let mut call_result = call.result.lock().unwrap();
                    *call_result = Some(bytes);
                    call.cond.notify_one();
                }
                None => {
                    println!("Call not found in map, ignoring");
                }
            }
        });
        Ok(())
    }

    fn get_next_call_id(&self) -> i32 {
        self.next_call_id
            .lock()
            .unwrap()
            .fetch_add(1, Ordering::SeqCst)
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
        request_header
    }

    fn get_connection_context(&self) -> common::IpcConnectionContextProto {
        let mut context = common::IpcConnectionContextProto::default();
        context.protocol = Some(PROTOCOL.to_string());

        let mut user_info = common::UserInformationProto::default();
        user_info.effective_user = Some("hadoop".to_string());
        // user_info.real_user = Some("hadoop".to_string());
        context.user_info = Some(user_info);
        context
    }

    pub fn write_messages(&self, messages: &[&Vec<u8>]) -> Result<()> {
        let mut size = 0u32;
        for msg in messages.iter() {
            size += msg.len() as u32;
        }

        let mut stream = self.stream.lock().unwrap();

        stream.write(&size.to_be_bytes())?;
        for msg in messages.iter() {
            stream.write(&msg)?;
        }

        Ok(())
    }
}

impl RpcEngine for NamenodeConnection {
    fn call<T: Message + std::default::Default>(
        &self,
        method_name: &str,
        message: &impl Message,
    ) -> Result<CallResult<T>> {
        let call_id = self.get_next_call_id();
        let conn_header = self
            .get_connection_header(call_id, 0)
            .encode_length_delimited_to_vec();

        let mut msg_header = common::RequestHeaderProto::default();
        msg_header.method_name = method_name.to_string();
        msg_header.declaring_class_protocol_name = PROTOCOL.to_string();
        msg_header.client_protocol_version = 1;

        let header_buf = msg_header.encode_length_delimited_to_vec();
        let msg_buf = message.encode_length_delimited_to_vec();

        let size = (conn_header.len() + header_buf.len() + msg_buf.len()) as u32;

        let call = Arc::new(Call::new());

        self.call_map
            .lock()
            .unwrap()
            .insert(call_id, Arc::clone(&call));

        let mut stream = self.stream.lock().unwrap();

        stream.write(&size.to_be_bytes())?;
        stream.write(&conn_header)?;
        stream.write(&header_buf)?;
        stream.write(&msg_buf)?;
        stream.flush()?;

        Ok(CallResult::new(call))
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
