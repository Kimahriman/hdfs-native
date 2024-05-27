use std::collections::{HashMap, VecDeque};
use std::default::Default;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::{prelude::*, TimeDelta};
use crc::{Crc, Table, CRC_32_CKSUM, CRC_32_ISCSI};
use log::{debug, warn};
use once_cell::sync::Lazy;
use prost::Message;
use socket2::SockRef;
use tokio::sync::{mpsc, oneshot};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    task::{self, JoinHandle},
};
use uuid::Uuid;

use crate::proto::common::rpc_response_header_proto::RpcStatusProto;
use crate::proto::common::TokenProto;
use crate::proto::hdfs::{DataEncryptionKeyProto, DatanodeIdProto};
use crate::proto::{common, hdfs};
use crate::security::sasl::{SaslDatanodeConnection, SaslDatanodeReader, SaslDatanodeWriter};
use crate::security::sasl::{SaslReader, SaslRpcClient, SaslWriter};
use crate::security::user::UserInfo;
use crate::{HdfsError, Result};

const PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
const DATA_TRANSFER_VERSION: u16 = 28;
const MAX_PACKET_HEADER_SIZE: usize = 33;
const DATANODE_CACHE_EXPIRY: TimeDelta = TimeDelta::seconds(3);

const CRC32: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_CKSUM);
const CRC32C: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISCSI);

pub(crate) static DATANODE_CACHE: Lazy<DatanodeConnectionCache> =
    Lazy::new(DatanodeConnectionCache::new);

// Connect to a remote host and return a TcpStream with standard options we want
async fn connect(addr: &str) -> Result<TcpStream> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    let sf = SockRef::from(&stream);
    sf.set_keepalive(true)?;

    Ok(stream)
}

#[derive(Debug)]
pub(crate) struct AlignmentContext {
    state_id: i64,
    router_federated_state: Option<HashMap<String, i64>>,
}

impl AlignmentContext {
    fn update(
        &mut self,
        state_id: Option<i64>,
        router_federated_state: Option<Vec<u8>>,
    ) -> Result<()> {
        if let Some(new_state_id) = state_id {
            self.state_id = new_state_id
        }

        if let Some(new_router_state) = router_federated_state {
            let new_map = hdfs::RouterFederatedStateProto::decode(Bytes::from(new_router_state))?
                .namespace_state_ids;

            let current_map = if let Some(cur) = self.router_federated_state.as_mut() {
                cur
            } else {
                self.router_federated_state = Some(HashMap::new());
                self.router_federated_state.as_mut().unwrap()
            };

            for (key, value) in new_map.into_iter() {
                current_map.insert(
                    key.clone(),
                    i64::max(value, *current_map.get(&key).unwrap_or(&i64::MIN)),
                );
            }
        }

        Ok(())
    }

    fn encode_router_state(&self) -> Option<Vec<u8>> {
        self.router_federated_state.as_ref().map(|state| {
            hdfs::RouterFederatedStateProto {
                namespace_state_ids: state.clone(),
            }
            .encode_to_vec()
        })
    }
}

impl Default for AlignmentContext {
    fn default() -> Self {
        Self {
            state_id: i64::MIN,
            router_federated_state: None,
        }
    }
}

type CallResult = oneshot::Sender<Result<Bytes>>;

#[derive(Debug)]
pub(crate) struct RpcConnection {
    client_id: Vec<u8>,
    user_info: UserInfo,
    next_call_id: AtomicI32,
    alignment_context: Arc<Mutex<AlignmentContext>>,
    call_map: Arc<Mutex<HashMap<i32, CallResult>>>,
    sender: mpsc::Sender<Vec<u8>>,
    listener: Option<JoinHandle<()>>,
}

impl RpcConnection {
    pub(crate) async fn connect(
        url: &str,
        alignment_context: Arc<Mutex<AlignmentContext>>,
        nameservice: Option<&str>,
    ) -> Result<Self> {
        let client_id = Uuid::new_v4().to_bytes_le().to_vec();
        let next_call_id = AtomicI32::new(0);
        let call_map = Arc::new(Mutex::new(HashMap::new()));

        let mut stream = connect(url).await?;
        stream.write_all("hrpc".as_bytes()).await?;
        // Current version
        stream.write_all(&[9u8]).await?;
        // Service class
        stream.write_all(&[0u8]).await?;
        // Auth protocol
        stream.write_all(&(-33i8).to_be_bytes()).await?;

        let mut client = SaslRpcClient::create(stream);

        let service = nameservice
            .map(|ns| format!("ha-hdfs:{ns}"))
            .unwrap_or(url.to_string());
        let user_info = client.negotiate(service.as_str()).await?;
        let (reader, writer) = client.split();
        let (sender, receiver) = mpsc::channel::<Vec<u8>>(1000);

        let mut conn = RpcConnection {
            client_id,
            user_info,
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
            .get_connection_context()
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
                match writer.write_all(&msg).await {
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
        let context = self.alignment_context.lock().unwrap();

        common::RpcRequestHeaderProto {
            rpc_kind: Some(common::RpcKindProto::RpcProtocolBuffer as i32),
            // RPC_FINAL_PACKET
            rpc_op: Some(0),
            call_id,
            client_id: self.client_id.clone(),
            retry_count: Some(retry_count),
            state_id: Some(context.state_id),
            router_federated_state: context.encode_router_state(),
            ..Default::default()
        }
    }

    fn get_connection_context(&self) -> common::IpcConnectionContextProto {
        let user_info = common::UserInformationProto {
            effective_user: self.user_info.effective_user.clone(),
            real_user: self.user_info.real_user.clone(),
        };

        let context = common::IpcConnectionContextProto {
            protocol: Some(PROTOCOL.to_string()),
            user_info: Some(user_info),
        };

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

        let msg_header = common::RequestHeaderProto {
            method_name: method_name.to_string(),
            declaring_class_protocol_name: PROTOCOL.to_string(),
            client_protocol_version: 1,
        };
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
    alignment_context: Arc<Mutex<AlignmentContext>>,
}

impl RpcListener {
    fn new(
        call_map: Arc<Mutex<HashMap<i32, CallResult>>>,
        reader: SaslReader,
        alignment_context: Arc<Mutex<AlignmentContext>>,
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

        let call_id = rpc_response.call_id as i32;

        let call = self.call_map.lock().unwrap().remove(&call_id);

        if let Some(call) = call {
            match rpc_response.status() {
                RpcStatusProto::Success => {
                    self.alignment_context
                        .lock()
                        .unwrap()
                        .update(rpc_response.state_id, rpc_response.router_federated_state)?;
                    let _ = call.send(Ok(bytes));
                }
                RpcStatusProto::Error => {
                    let _ = call.send(Err(HdfsError::RPCError(
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
                    return Err(HdfsError::FatalRPCError(
                        rpc_response.exception_class_name().to_string(),
                        rpc_response.error_msg().to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

pub(crate) enum Op {
    WriteBlock,
    ReadBlock,
}

impl Op {
    fn value(&self) -> u8 {
        match self {
            Self::WriteBlock => 80,
            Self::ReadBlock => 81,
        }
    }
}

const CHECKSUM_BYTES: usize = 4;

pub(crate) struct Packet {
    pub header: hdfs::PacketHeaderProto,
    checksum: BytesMut,
    data: BytesMut,
    bytes_per_checksum: usize,
    max_data_size: usize,
}

impl Packet {
    fn new(header: hdfs::PacketHeaderProto, checksum: BytesMut, data: BytesMut) -> Self {
        Self {
            header,
            checksum,
            data,
            bytes_per_checksum: 0,
            max_data_size: 0,
        }
    }

    pub(crate) fn empty(
        offset: i64,
        seqno: i64,
        bytes_per_checksum: u32,
        max_packet_size: u32,
    ) -> Self {
        let header = hdfs::PacketHeaderProto {
            offset_in_block: offset,
            seqno,
            ..Default::default()
        };

        let num_chunks = Self::max_packet_chunks(bytes_per_checksum, max_packet_size);

        Self {
            header,
            checksum: BytesMut::with_capacity(num_chunks * CHECKSUM_BYTES),
            data: BytesMut::with_capacity(num_chunks * bytes_per_checksum as usize),
            bytes_per_checksum: bytes_per_checksum as usize,
            max_data_size: num_chunks * bytes_per_checksum as usize,
        }
    }

    pub(crate) fn set_last_packet(&mut self) {
        self.header.last_packet_in_block = true;
        // Opinionated: always sync block for safety
        self.header.sync_block = Some(true);
    }

    fn max_packet_chunks(bytes_per_checksum: u32, max_packet_size: u32) -> usize {
        if max_packet_size > 0 {
            let data_size = max_packet_size as usize - MAX_PACKET_HEADER_SIZE;
            let chunk_size = bytes_per_checksum as usize + CHECKSUM_BYTES;
            data_size / chunk_size
        } else {
            // Create a packet with a single chunk for appending to a file
            1
        }
    }

    pub(crate) fn write(&mut self, buf: &mut Bytes) {
        self.data
            .put(buf.split_to(usize::min(self.max_data_size - self.data.len(), buf.len())));
    }

    pub(crate) fn is_full(&self) -> bool {
        self.data.len() == self.max_data_size
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn finalize(&mut self) -> (hdfs::PacketHeaderProto, Bytes, Bytes) {
        let data = self.data.split().freeze();

        let mut chunk_start = 0;
        while chunk_start < data.len() {
            let chunk_end = usize::min(chunk_start + self.bytes_per_checksum, data.len());
            let chunk_checksum = CRC32C.checksum(&data[chunk_start..chunk_end]);
            self.checksum.put_u32(chunk_checksum);
            chunk_start += self.bytes_per_checksum;
        }

        let checksum = self.checksum.split().freeze();

        self.header.data_len = data.len() as i32;

        (self.header.clone(), checksum, data)
    }

    pub(crate) fn get_data(
        self,
        checksum_info: &Option<hdfs::ReadOpChecksumInfoProto>,
    ) -> Result<Bytes> {
        // Verify the checksums if they were requested
        let mut checksums = self.checksum.freeze();
        let data = self.data.freeze();
        if let Some(info) = checksum_info {
            let algorithm = match info.checksum.r#type() {
                hdfs::ChecksumTypeProto::ChecksumCrc32 => Some(&CRC32),
                hdfs::ChecksumTypeProto::ChecksumCrc32c => Some(&CRC32C),
                hdfs::ChecksumTypeProto::ChecksumNull => None,
            };

            if let Some(algorithm) = algorithm {
                // Create a new Bytes view over the data that we can consume
                let mut checksum_data = data.clone();
                while !checksum_data.is_empty() {
                    let chunk_checksum = algorithm.checksum(&checksum_data.split_to(usize::min(
                        info.checksum.bytes_per_checksum as usize,
                        checksum_data.len(),
                    )));
                    if chunk_checksum != checksums.get_u32() {
                        return Err(HdfsError::ChecksumError);
                    }
                }
            }
        }
        Ok(data)
    }
}

pub(crate) struct DatanodeConnection {
    client_name: String,
    reader: SaslDatanodeReader,
    writer: SaslDatanodeWriter,
    url: String,
}

impl DatanodeConnection {
    pub(crate) async fn connect(
        datanode_id: &DatanodeIdProto,
        token: &TokenProto,
        encryption_key: Option<DataEncryptionKeyProto>,
    ) -> Result<Self> {
        let url = format!("{}:{}", datanode_id.ip_addr, datanode_id.xfer_port);
        let stream = connect(&url).await?;

        let sasl_connection = SaslDatanodeConnection::create(stream);
        let (reader, writer) = sasl_connection
            .negotiate(datanode_id, token, encryption_key.as_ref())
            .await?;

        let conn = DatanodeConnection {
            client_name: Uuid::new_v4().to_string(),
            reader,
            writer,
            url: url.to_string(),
        };
        Ok(conn)
    }

    pub(crate) async fn send(
        &mut self,
        op: Op,
        message: &impl Message,
    ) -> Result<hdfs::BlockOpResponseProto> {
        self.writer
            .write_all(&DATA_TRANSFER_VERSION.to_be_bytes())
            .await?;
        self.writer.write_all(&[op.value()]).await?;
        self.writer
            .write_all(&message.encode_length_delimited_to_vec())
            .await?;
        self.writer.flush().await?;

        let message = self.reader.read_proto().await?;

        let response = hdfs::BlockOpResponseProto::decode(message)?;
        Ok(response)
    }

    pub(crate) fn build_header(
        &self,
        block: &hdfs::ExtendedBlockProto,
        token: Option<common::TokenProto>,
    ) -> hdfs::ClientOperationHeaderProto {
        let base_header = hdfs::BaseHeaderProto {
            block: block.clone(),
            token,
            ..Default::default()
        };

        hdfs::ClientOperationHeaderProto {
            base_header,
            client_name: self.client_name.clone(),
        }
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

        let header =
            hdfs::PacketHeaderProto::decode(remaining_buf.split_to(header_length).freeze())?;

        let checksum_length = payload_length - 4 - header.data_len as usize;
        let checksum = remaining_buf.split_to(checksum_length);
        let data = remaining_buf;

        Ok(Packet::new(header, checksum, data))
    }

    pub(crate) async fn send_read_success(&mut self) -> Result<()> {
        let client_read_status = hdfs::ClientReadStatusProto {
            status: hdfs::Status::ChecksumOk as i32,
        };

        self.writer
            .write_all(&client_read_status.encode_length_delimited_to_vec())
            .await?;
        self.writer.flush().await?;

        Ok(())
    }

    pub(crate) fn split(self) -> (DatanodeReader, DatanodeWriter) {
        let reader = DatanodeReader {
            reader: self.reader,
        };
        let writer = DatanodeWriter {
            writer: self.writer,
        };
        (reader, writer)
    }
}

/// A reader half of a Datanode connection used for reading acks during
/// write operations.
pub(crate) struct DatanodeReader {
    reader: SaslDatanodeReader,
}

impl DatanodeReader {
    pub(crate) async fn read_ack(&mut self) -> Result<hdfs::PipelineAckProto> {
        let message = self.reader.read_proto().await?;

        let response = hdfs::PipelineAckProto::decode(message)?;
        Ok(response)
    }
}

/// A write half of a Datanode connection used for writing packets.
pub(crate) struct DatanodeWriter {
    writer: SaslDatanodeWriter,
}

impl DatanodeWriter {
    /// Create a buffer to send to the datanode
    pub(crate) async fn write_packet(&mut self, packet: &mut Packet) -> Result<()> {
        let (header, checksum, data) = packet.finalize();

        let payload_len = (checksum.len() + data.len() + 4) as u32;
        let header_encoded = header.encode_to_vec();

        self.writer.write_all(&payload_len.to_be_bytes()).await?;
        self.writer
            .write_all(&(header_encoded.len() as u16).to_be_bytes())
            .await?;
        self.writer.write_all(&header.encode_to_vec()).await?;
        self.writer.write_all(&checksum).await?;
        self.writer.write_all(&data).await?;
        self.writer.flush().await?;

        Ok(())
    }
}

type DatanodeConnectionCacheEntry = VecDeque<(DateTime<Utc>, DatanodeConnection)>;

pub(crate) struct DatanodeConnectionCache {
    cache: Mutex<HashMap<String, DatanodeConnectionCacheEntry>>,
}

impl DatanodeConnectionCache {
    fn new() -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn get(&self, datanode_id: &hdfs::DatanodeIdProto) -> Option<DatanodeConnection> {
        // Keep things simply and just expire cache entries when checking the cache. We could
        // move this to its own task but that will add a little more complexity.
        self.remove_expired();

        let url = format!("{}:{}", datanode_id.ip_addr, datanode_id.xfer_port);
        let mut cache = self.cache.lock().unwrap();

        cache
            .get_mut(&url)
            .iter_mut()
            .flat_map(|conns| conns.pop_front())
            .map(|(_, conn)| conn)
            .next()
    }

    pub(crate) fn release(&self, conn: DatanodeConnection) {
        let expire_at = Utc::now() + DATANODE_CACHE_EXPIRY;
        let mut cache = self.cache.lock().unwrap();
        cache
            .entry(conn.url.clone())
            .or_default()
            .push_back((expire_at, conn));
    }

    fn remove_expired(&self) {
        let mut cache = self.cache.lock().unwrap();
        let now = Utc::now();
        for (_, values) in cache.iter_mut() {
            values.retain(|(expire_at, _)| expire_at > &now)
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use prost::Message;

    use crate::{hdfs::connection::MAX_PACKET_HEADER_SIZE, proto::hdfs};

    use super::AlignmentContext;

    #[test]
    fn test_max_packet_header_size() {
        // Create a dummy header to get its size
        let header = hdfs::PacketHeaderProto {
            sync_block: Some(false),
            ..Default::default()
        };
        // Add 4 bytes for size of whole packet and 2 bytes for size of header
        assert_eq!(MAX_PACKET_HEADER_SIZE, header.encoded_len() + 4 + 2);
    }

    fn encode_router_state(map: &HashMap<String, i64>) -> Vec<u8> {
        hdfs::RouterFederatedStateProto {
            namespace_state_ids: map.clone(),
        }
        .encode_to_vec()
    }

    #[test]
    fn test_router_federated_state() {
        let mut alignment_context = AlignmentContext::default();

        assert!(alignment_context.router_federated_state.is_none());

        let mut state_map = HashMap::<String, i64>::new();
        state_map.insert("ns-1".to_string(), 3);

        alignment_context
            .update(None, Some(encode_router_state(&state_map)))
            .unwrap();

        assert!(alignment_context.router_federated_state.is_some());

        let router_state = alignment_context.router_federated_state.as_ref().unwrap();

        assert_eq!(router_state.len(), 1);
        assert_eq!(*router_state.get("ns-1").unwrap(), 3);

        state_map.insert("ns-1".to_string(), 5);
        state_map.insert("ns-2".to_string(), 7);

        alignment_context
            .update(None, Some(encode_router_state(&state_map)))
            .unwrap();

        let router_state = alignment_context.router_federated_state.as_ref().unwrap();

        assert_eq!(router_state.len(), 2);
        assert_eq!(*router_state.get("ns-1").unwrap(), 5);
        assert_eq!(*router_state.get("ns-2").unwrap(), 7);
    }
}
