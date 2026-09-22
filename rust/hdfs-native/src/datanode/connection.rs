use std::collections::{HashMap, VecDeque};
use std::default::Default;
use std::sync::Mutex;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::{TimeDelta, prelude::*};
use crc::{CRC_32_ISCSI, CRC_32_ISO_HDLC, Crc, Table};
use once_cell::sync::Lazy;
use prost::Message;
use socket2::SockRef;
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::config::Configuration;
use crate::datanode::sasl::{SaslDatanodeConnection, SaslDatanodeReader, SaslDatanodeWriter};
use crate::proto::common::TokenProto;
use crate::proto::hdfs::{DataEncryptionKeyProto, DatanodeIdProto};
use crate::proto::{common, hdfs};
use crate::{HdfsError, Result};

const DATA_TRANSFER_VERSION: u16 = 28;
const MAX_PACKET_HEADER_SIZE: usize = 33;
const DATANODE_CACHE_EXPIRY: TimeDelta = TimeDelta::seconds(3);

const CRC32: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISO_HDLC);
const CRC32C: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISCSI);

pub(crate) static DATANODE_CACHE: Lazy<DatanodeConnectionCache> =
    Lazy::new(DatanodeConnectionCache::new);

fn datanode_url(datanode_id: &DatanodeIdProto, config: &Configuration) -> String {
    let host = if config.use_datanode_hostname() {
        &datanode_id.host_name
    } else {
        &datanode_id.ip_addr
    };
    format!("{}:{}", host, datanode_id.xfer_port)
}

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

#[allow(clippy::enum_variant_names)]
pub(crate) enum Op {
    WriteBlock,
    ReadBlock,
    TransferBlock,
}

impl Op {
    fn value(&self) -> u8 {
        match self {
            Self::WriteBlock => 80,
            Self::ReadBlock => 81,
            Self::TransferBlock => 86,
        }
    }
}

const CHECKSUM_BYTES: usize = 4;

pub(crate) struct ReadPacket {
    pub header: hdfs::PacketHeaderProto,
    checksum: Bytes,
    data: Bytes,
}

impl ReadPacket {
    fn new(header: hdfs::PacketHeaderProto, checksum: Bytes, data: Bytes) -> Self {
        Self {
            header,
            checksum,
            data,
        }
    }

    pub(crate) fn get_data(
        mut self,
        checksum_info: &Option<hdfs::ReadOpChecksumInfoProto>,
    ) -> Result<Bytes> {
        // Verify the checksums if they were requested
        if let Some(info) = checksum_info {
            let algorithm = match info.checksum.r#type() {
                hdfs::ChecksumTypeProto::ChecksumCrc32 => Some(&CRC32),
                hdfs::ChecksumTypeProto::ChecksumCrc32c => Some(&CRC32C),
                hdfs::ChecksumTypeProto::ChecksumNull => None,
            };

            if let Some(algorithm) = algorithm {
                // Create a new Bytes view over the data that we can consume
                let mut checksum_data = self.data.clone();
                while !checksum_data.is_empty() {
                    let chunk_checksum = algorithm.checksum(&checksum_data.split_to(usize::min(
                        info.checksum.bytes_per_checksum as usize,
                        checksum_data.len(),
                    )));
                    if chunk_checksum != self.checksum.get_u32() {
                        return Err(HdfsError::ChecksumError);
                    }
                }
            }
        }
        Ok(self.data)
    }
}

pub(crate) struct WritePacket {
    pub header: hdfs::PacketHeaderProto,
    pub data: PacketData,
    bytes_per_checksum: usize,
    max_data_size: usize,
    acknowledgement: Option<oneshot::Sender<()>>,
}

/// Payload retained by a write packet until the DataNode acknowledges it.
///
/// Keeping the caller's reference-counted `Bytes` slices avoids copying every
/// write into an intermediate packet buffer. The slices also remain available
/// unchanged if the pipeline has to replay an unacknowledged packet.
pub(crate) struct PacketData {
    segments: Vec<Bytes>,
    len: usize,
}

impl PacketData {
    fn new() -> Self {
        Self {
            // Most packets are a slice of one application write. Reserve a
            // little room for packets completed by a subsequent small write.
            segments: Vec::with_capacity(2),
            len: 0,
        }
    }

    fn push(&mut self, data: Bytes) {
        if !data.is_empty() {
            self.len += data.len();
            self.segments.push(data);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.segments.iter()
    }
}

impl WritePacket {
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
            data: PacketData::new(),
            bytes_per_checksum: bytes_per_checksum as usize,
            max_data_size: num_chunks * bytes_per_checksum as usize,
            acknowledgement: None,
        }
    }

    pub(crate) fn request_acknowledgement(&mut self) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        self.acknowledgement = Some(sender);
        receiver
    }

    pub(crate) fn acknowledge(&mut self) {
        if let Some(sender) = self.acknowledgement.take() {
            let _ = sender.send(());
        }
    }

    pub(crate) fn set_sync_block(&mut self) {
        self.header.sync_block = Some(true);
    }

    pub(crate) fn set_last_packet(&mut self) {
        self.header.last_packet_in_block = true;
        // Opinionated: always sync block for safety
        self.set_sync_block();
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
            .push(buf.split_to(usize::min(self.max_data_size - self.data.len(), buf.len())));
        self.header.data_len = self.data.len() as i32;
    }

    pub(crate) fn is_full(&self) -> bool {
        self.data.len() == self.max_data_size
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn calculate_checksum(&self) -> Bytes {
        if self.data.is_empty() || self.bytes_per_checksum == 0 {
            return Bytes::new();
        }

        let checksum_count = self.data.len().div_ceil(self.bytes_per_checksum);
        let mut checksum = BytesMut::with_capacity(checksum_count * CHECKSUM_BYTES);
        let mut digest = CRC32C.digest();
        let mut bytes_in_checksum_chunk = 0;

        for segment in self.data.iter() {
            let mut remaining = segment.as_ref();
            while !remaining.is_empty() {
                let bytes_to_hash = usize::min(
                    self.bytes_per_checksum - bytes_in_checksum_chunk,
                    remaining.len(),
                );
                digest.update(&remaining[..bytes_to_hash]);
                remaining = &remaining[bytes_to_hash..];
                bytes_in_checksum_chunk += bytes_to_hash;

                if bytes_in_checksum_chunk == self.bytes_per_checksum {
                    checksum.put_u32(digest.finalize());
                    digest = CRC32C.digest();
                    bytes_in_checksum_chunk = 0;
                }
            }
        }

        if bytes_in_checksum_chunk != 0 {
            checksum.put_u32(digest.finalize());
        }

        checksum.freeze()
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
        config: &Configuration,
        handle: &Handle,
    ) -> Result<Self> {
        let url = datanode_url(datanode_id, config);
        let stream = connect(&url, handle).await?;

        let sasl_connection = SaslDatanodeConnection::create(stream);
        let (reader, writer) = sasl_connection
            .negotiate(datanode_id, token, encryption_key.as_ref(), config)
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

    pub(crate) async fn read_packet(&mut self) -> Result<ReadPacket> {
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
        let checksum = remaining_buf.split_to(checksum_length).freeze();
        let data = remaining_buf.freeze();

        Ok(ReadPacket::new(header, checksum, data))
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
    pub(crate) async fn write_packet(&mut self, packet: &mut WritePacket) -> Result<()> {
        let checksum = packet.calculate_checksum();

        let payload_len = (checksum.len() + packet.data.len() + 4) as u32;
        let header_encoded = packet.header.encode_to_vec();

        self.writer.write_all(&payload_len.to_be_bytes()).await?;
        self.writer
            .write_all(&(header_encoded.len() as u16).to_be_bytes())
            .await?;
        self.writer.write_all(&header_encoded).await?;
        self.writer.write_all(&checksum).await?;
        self.writer
            .write_all_vectored(&packet.data.segments)
            .await?;
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

    pub(crate) fn get(
        &self,
        datanode_id: &hdfs::DatanodeIdProto,
        config: &Configuration,
    ) -> Option<DatanodeConnection> {
        // Keep things simple and just expire cache entries when checking the cache. We could
        // move this to its own task but that will add a little more complexity.
        self.remove_expired();

        let url = datanode_url(datanode_id, config);
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
        for values in cache.values_mut() {
            values.retain(|(expire_at, _)| expire_at > &now)
        }
    }
}

#[cfg(test)]
mod test {
    use prost::Message;

    use crate::{config::Configuration, proto::hdfs};

    use super::{CRC32C, MAX_PACKET_HEADER_SIZE, WritePacket, datanode_url};
    use bytes::{BufMut, Bytes, BytesMut};

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

    #[tokio::test]
    async fn sync_packet_is_acknowledged_without_ending_the_block() {
        let mut packet = WritePacket::empty(512, 7, 512, 64 * 1024);
        packet.set_sync_block();
        let acknowledgement = packet.request_acknowledgement();

        assert_eq!(packet.header.sync_block, Some(true));
        assert!(!packet.header.last_packet_in_block);
        packet.acknowledge();
        assert!(acknowledgement.await.is_ok());
    }

    #[test]
    fn write_packet_retains_shared_payload_slices() {
        let original = Bytes::from_static(b"abcdefgh");
        let original_ptr = original.as_ptr();
        let mut input = original.clone();
        let mut packet = WritePacket::empty(0, 0, 4, 49);

        packet.write(&mut input);

        assert!(input.is_empty());
        assert_eq!(packet.data.len(), original.len());
        assert_eq!(packet.data.segments.len(), 1);
        assert_eq!(packet.data.segments[0].as_ptr(), original_ptr);
    }

    #[test]
    fn write_packet_checksums_span_payload_segments() {
        let mut packet = WritePacket::empty(0, 0, 4, 49);
        let mut first = Bytes::from_static(b"abc");
        let mut second = Bytes::from_static(b"defgh");
        packet.write(&mut first);
        packet.write(&mut second);

        let mut expected = BytesMut::new();
        expected.put_u32(CRC32C.checksum(b"abcd"));
        expected.put_u32(CRC32C.checksum(b"efgh"));

        assert_eq!(packet.calculate_checksum(), expected.freeze());
    }

    #[test]
    fn test_datanode_url_uses_ip_address_by_default() {
        let datanode_id = hdfs::DatanodeIdProto {
            ip_addr: "127.0.0.1".to_string(),
            host_name: "hdfs-datanode".to_string(),
            xfer_port: 9866,
            ..Default::default()
        };
        let config =
            Configuration::new(Some("/tmp/hdfs-native-missing-conf".to_string()), None).unwrap();

        assert_eq!(datanode_url(&datanode_id, &config), "127.0.0.1:9866");
    }

    #[test]
    fn test_datanode_url_uses_hostname_when_configured() {
        let datanode_id = hdfs::DatanodeIdProto {
            ip_addr: "127.0.0.1".to_string(),
            host_name: "hdfs-datanode".to_string(),
            xfer_port: 9866,
            ..Default::default()
        };
        let config = Configuration::new(
            Some("/tmp/hdfs-native-missing-conf".to_string()),
            Some(
                [(
                    "dfs.client.use.datanode.hostname".to_string(),
                    "true".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
        )
        .unwrap();

        assert_eq!(datanode_url(&datanode_id, &config), "hdfs-datanode:9866");
    }
}
