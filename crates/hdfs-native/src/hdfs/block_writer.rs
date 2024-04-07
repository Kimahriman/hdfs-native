use std::{sync::Arc, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use futures::future::join_all;
use log::debug;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    ec::{gf256::Coder, EcSchema},
    hdfs::connection::{DatanodeConnection, DatanodeReader, DatanodeWriter, Op, Packet},
    proto::hdfs,
    HdfsError, Result,
};

use super::protocol::NamenodeProtocol;

const HEART_BEAT_SEQNO: i64 = -1;
const UNKNOWN_SEQNO: i64 = -2;

const HEARTBEAT_INTERVAL_SECONDS: u64 = 30;

/// Wrapper around both types of block writers. This was simpler than trying to
/// do dynamic dispatch with a BlockWriter trait.
pub(crate) enum BlockWriter {
    Replicated(ReplicatedBlockWriter),
    Striped(StripedBlockWriter),
}

impl BlockWriter {
    pub(crate) async fn new(
        protocol: Arc<NamenodeProtocol>,
        block: hdfs::LocatedBlockProto,
        block_size: usize,
        server_defaults: hdfs::FsServerDefaultsProto,
        ec_schema: Option<&EcSchema>,
    ) -> Result<Self> {
        let block_writer = if let Some(ec_schema) = ec_schema {
            Self::Striped(StripedBlockWriter::new(
                protocol,
                block,
                ec_schema,
                block_size,
                server_defaults,
            ))
        } else {
            Self::Replicated(
                ReplicatedBlockWriter::new(&protocol, block, block_size, server_defaults).await?,
            )
        };
        Ok(block_writer)
    }

    pub(crate) async fn write(&mut self, buf: &mut Bytes) -> Result<()> {
        match self {
            Self::Replicated(writer) => writer.write(buf).await,
            Self::Striped(writer) => writer.write(buf).await,
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        match self {
            Self::Replicated(writer) => writer.is_full(),
            Self::Striped(writer) => writer.is_full(),
        }
    }

    pub(crate) fn get_extended_block(&self) -> hdfs::ExtendedBlockProto {
        match self {
            Self::Replicated(writer) => writer.get_extended_block(),
            Self::Striped(writer) => writer.get_extended_block(),
        }
    }

    pub(crate) async fn close(self) -> Result<()> {
        match self {
            Self::Replicated(writer) => writer.close().await,
            Self::Striped(writer) => writer.close().await,
        }
    }
}

pub(crate) struct ReplicatedBlockWriter {
    block: hdfs::LocatedBlockProto,
    block_size: usize,
    server_defaults: hdfs::FsServerDefaultsProto,

    next_seqno: i64,
    current_packet: Packet,

    // Tracks the state of acknowledgements. Set to an Err if any error occurs doing receiving
    // acknowledgements. Set to Ok(()) when the last acknowledgement is received.
    ack_listener_handle: JoinHandle<Result<()>>,
    // Tracks the state of packet sender. Set to Err if any error occurs during writing packets,
    packet_sender_handle: JoinHandle<Result<()>>,
    // Tracks the heartbeat task so we can abort it when we close
    heartbeat_handle: JoinHandle<()>,

    ack_queue: mpsc::Sender<(i64, bool)>,
    packet_sender: mpsc::Sender<Packet>,
}

impl ReplicatedBlockWriter {
    async fn new(
        protocol: &Arc<NamenodeProtocol>,
        block: hdfs::LocatedBlockProto,
        block_size: usize,
        server_defaults: hdfs::FsServerDefaultsProto,
    ) -> Result<Self> {
        let datanode = &block.locs[0].id;
        let mut connection = DatanodeConnection::connect(
            datanode,
            &block.block_token,
            protocol.get_cached_data_encryption_key().await?,
        )
        .await?;

        let checksum = hdfs::ChecksumProto {
            r#type: hdfs::ChecksumTypeProto::ChecksumCrc32c as i32,
            bytes_per_checksum: server_defaults.bytes_per_checksum,
        };

        let append = block.b.num_bytes() > 0;

        let stage = if append {
            hdfs::op_write_block_proto::BlockConstructionStage::PipelineSetupAppend as i32
        } else {
            hdfs::op_write_block_proto::BlockConstructionStage::PipelineSetupCreate as i32
        };

        let message = hdfs::OpWriteBlockProto {
            header: connection.build_header(&block.b, Some(block.block_token.clone())),
            stage,
            targets: block.locs[1..].to_vec(),
            pipeline_size: block.locs.len() as u32,
            latest_generation_stamp: block.b.generation_stamp,
            min_bytes_rcvd: block.b.num_bytes(),
            max_bytes_rcvd: block.b.num_bytes(),
            requested_checksum: checksum,
            storage_type: Some(block.storage_types[0]),
            target_storage_types: block.storage_types[1..].to_vec(),
            storage_id: Some(block.storage_i_ds[0].clone()),
            target_storage_ids: block.storage_i_ds[1..].to_vec(),
            ..Default::default()
        };

        debug!("Block write request: {:?}", &message);
        let response = connection.send(Op::WriteBlock, &message).await?;
        debug!("Block write response: {:?}", response);

        let (reader, writer) = connection.split();

        // Channel for tracking packets that need to be acked
        let (ack_queue_sender, ack_queue_receiever) = mpsc::channel::<(i64, bool)>(100);
        let (packet_sender, packet_receiver) = mpsc::channel::<Packet>(100);

        let ack_listener_handle = Self::listen_for_acks(reader, ack_queue_receiever);
        let packet_sender_handle = Self::start_packet_sender(writer, packet_receiver);
        let heartbeat_handle = Self::start_heartbeat_sender(packet_sender.clone());

        let bytes_per_checksum = server_defaults.bytes_per_checksum;
        let write_packet_size = server_defaults.write_packet_size;

        let bytes_left_in_chunk = server_defaults.bytes_per_checksum
            - (block.b.num_bytes() % server_defaults.bytes_per_checksum as u64) as u32;
        let current_packet = if append && bytes_left_in_chunk > 0 {
            // When appending, we want to first send a packet with a single chunk of the data required
            // to get the block to a multiple of bytes_per_checksum. After that, things work the same
            // as create.
            Packet::empty(block.b.num_bytes() as i64, 0, bytes_left_in_chunk, 0)
        } else {
            Packet::empty(
                block.b.num_bytes() as i64,
                0,
                bytes_per_checksum,
                write_packet_size,
            )
        };

        let this = Self {
            block,
            block_size,
            server_defaults,
            next_seqno: 1,
            current_packet,

            ack_listener_handle,
            packet_sender_handle,
            heartbeat_handle,

            ack_queue: ack_queue_sender,
            packet_sender,
        };

        Ok(this)
    }

    // Create the next packet and return the current packet
    fn create_next_packet(&mut self) -> Packet {
        let next_packet = Packet::empty(
            self.block.b.num_bytes() as i64,
            self.next_seqno,
            self.server_defaults.bytes_per_checksum,
            self.server_defaults.write_packet_size,
        );
        self.next_seqno += 1;
        std::mem::replace(&mut self.current_packet, next_packet)
    }

    async fn queue_ack(&self) -> Result<()> {
        self.ack_queue
            .send((
                self.current_packet.header.seqno,
                self.current_packet.header.last_packet_in_block,
            ))
            .await
            .map_err(|_| HdfsError::DataTransferError("Failed to send to ack queue".to_string()))
    }

    async fn send_current_packet(&mut self) -> Result<()> {
        // Queue up the sequence number for acknowledgement
        self.queue_ack().await?;

        // Create a fresh packet
        let current_packet = self.create_next_packet();

        // Send the packet
        // TODO: handler error
        let _ = self.packet_sender.send(current_packet).await;

        Ok(())
    }

    fn check_error(&mut self) -> Result<()> {
        // If either task is finished, something went wrong
        if self.ack_listener_handle.is_finished() {
            return Err(HdfsError::DataTransferError(
                "Ack listener finished prematurely".to_string(),
            ));
        }

        if self.packet_sender_handle.is_finished() {
            return Err(HdfsError::DataTransferError(
                "Packet sender finished prematurely".to_string(),
            ));
        }

        Ok(())
    }

    fn is_full(&self) -> bool {
        self.block.b.num_bytes() == self.block_size as u64
    }

    fn get_extended_block(&self) -> hdfs::ExtendedBlockProto {
        self.block.b.clone()
    }

    async fn write(&mut self, buf: &mut Bytes) -> Result<()> {
        self.check_error()?;

        // Only write up to what's left in this block
        let bytes_to_write = usize::min(
            buf.len(),
            self.block_size - self.block.b.num_bytes() as usize,
        );
        let mut buf_to_write = buf.split_to(bytes_to_write);

        while !buf_to_write.is_empty() {
            let initial_buf_len = buf_to_write.len();
            self.current_packet.write(&mut buf_to_write);

            // Track how many bytes are written to this block
            *self.block.b.num_bytes.as_mut().unwrap() +=
                (initial_buf_len - buf_to_write.len()) as u64;

            if self.current_packet.is_full() {
                self.send_current_packet().await?;
            }
        }
        Ok(())
    }

    /// Send a packet with any remaining data and then send a last packet
    async fn close(mut self) -> Result<()> {
        self.check_error()?;

        // Send a packet with any remaining data
        if !self.current_packet.is_empty() {
            self.send_current_packet().await?;
        }

        // Send an empty last packet
        self.current_packet.set_last_packet();
        self.send_current_packet().await?;

        self.heartbeat_handle.abort();

        // Wait for all packets to be sent
        self.packet_sender_handle.await.map_err(|_| {
            HdfsError::DataTransferError(
                "Packet sender task err while waiting for packets to send".to_string(),
            )
        })??;

        // Wait for the channel to close, meaning all acks have been received or an error occured
        self.ack_listener_handle.await.map_err(|_| {
            HdfsError::DataTransferError(
                "Ack status channel closed while waiting for final ack".to_string(),
            )
        })??;

        Ok(())
    }

    fn listen_for_acks(
        mut reader: DatanodeReader,
        mut ack_queue: mpsc::Receiver<(i64, bool)>,
    ) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            loop {
                let next_ack = reader.read_ack().await?;

                for reply in next_ack.reply.iter() {
                    if *reply != hdfs::Status::Success as i32 {
                        return Err(HdfsError::DataTransferError(format!(
                            "Received non-success status in datanode ack: {:?}",
                            hdfs::Status::try_from(*reply)
                        )));
                    }
                }

                if next_ack.seqno == HEART_BEAT_SEQNO {
                    continue;
                }
                if next_ack.seqno == UNKNOWN_SEQNO {
                    return Err(HdfsError::DataTransferError(
                        "Received unknown seqno for successful ack".to_string(),
                    ));
                }

                if let Some((seqno, last_packet)) = ack_queue.recv().await {
                    if next_ack.seqno != seqno {
                        return Err(HdfsError::DataTransferError(
                            "Received acknowledgement does not match expected sequence number"
                                .to_string(),
                        ));
                    }

                    if last_packet {
                        return Ok(());
                    }
                } else {
                    return Err(HdfsError::DataTransferError(
                        "Channel closed while getting next seqno to acknowledge".to_string(),
                    ));
                }
            }
        })
    }

    fn start_packet_sender(
        mut writer: DatanodeWriter,
        mut packet_receiver: mpsc::Receiver<Packet>,
    ) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            while let Some(mut packet) = packet_receiver.recv().await {
                writer.write_packet(&mut packet).await?;

                if packet.header.last_packet_in_block {
                    break;
                }
            }
            Ok(())
        })
    }

    fn start_heartbeat_sender(packet_sender: mpsc::Sender<Packet>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
                let heartbeat_packet = Packet::empty(0, HEART_BEAT_SEQNO, 0, 0);
                // If this fails, sending anymore data packets will generate an error as well
                if packet_sender.send(heartbeat_packet).await.is_err() {
                    break;
                }
            }
        })
    }
}

// Holds data for the current slice being written.
struct CellBuffer {
    buffers: Vec<BytesMut>,
    cell_size: usize,
    current_index: usize,
    coder: Coder,
}

impl CellBuffer {
    fn new(ec_schema: &EcSchema) -> Self {
        let buffers = (0..ec_schema.data_units)
            .map(|_| BytesMut::with_capacity(ec_schema.cell_size))
            .collect();
        Self {
            buffers,
            cell_size: ec_schema.cell_size,
            current_index: 0,
            coder: Coder::new(ec_schema.data_units, ec_schema.parity_units),
        }
    }

    fn write(&mut self, buf: &mut Bytes) {
        while !buf.is_empty() && self.current_index < self.buffers.len() {
            let current_buffer = &mut self.buffers[self.current_index];
            let remaining = self.cell_size - current_buffer.len();

            let split_at = usize::min(remaining, buf.len());

            let bytes_to_write = buf.split_to(split_at);
            current_buffer.put(bytes_to_write);

            if current_buffer.len() == self.cell_size {
                self.current_index += 1;
            }
        }
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.current_index == self.buffers.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buffers[0].is_empty()
    }

    fn encode(&mut self) -> Vec<Bytes> {
        // This is kinda dumb how many copies are being made. Figure out how to do this without
        // cloning the buffers at all.

        // Pad any buffers with 0 so they are all the same length. The first buffer will always be
        // the largest since we write data there first.
        let slice_size = self.buffers[0].len();

        // Remember the original sizes so we can resize after encoding
        let original_sizes: Vec<_> = self.buffers.iter().map(|buf| buf.len()).collect();

        let mut data_slices: Vec<_> = self
            .buffers
            .iter()
            .cloned()
            .map(|mut buf| {
                buf.resize(slice_size, 0);
                buf.freeze()
            })
            .collect();

        let parity_slices = self.coder.encode(&data_slices[..]);

        for (slice, size) in data_slices.iter_mut().zip(original_sizes.into_iter()) {
            let _ = slice.split_off(size);
        }

        for buf in self.buffers.iter_mut() {
            buf.clear();
        }
        self.current_index = 0;

        data_slices.extend(parity_slices);
        data_slices
    }
}

// Writer for erasure coded blocks.
pub(crate) struct StripedBlockWriter {
    protocol: Arc<NamenodeProtocol>,
    block: hdfs::LocatedBlockProto,
    server_defaults: hdfs::FsServerDefaultsProto,
    block_size: usize,
    block_writers: Vec<Option<ReplicatedBlockWriter>>,
    cell_buffer: CellBuffer,
    bytes_written: usize,
    capacity: usize,
}

impl StripedBlockWriter {
    fn new(
        protocol: Arc<NamenodeProtocol>,
        block: hdfs::LocatedBlockProto,
        ec_schema: &EcSchema,
        block_size: usize,
        server_defaults: hdfs::FsServerDefaultsProto,
    ) -> Self {
        let block_writers = (0..block.block_indices().len()).map(|_| None).collect();

        Self {
            protocol,
            block,
            block_size,
            server_defaults,
            block_writers,
            cell_buffer: CellBuffer::new(ec_schema),
            bytes_written: 0,
            capacity: ec_schema.data_units * block_size,
        }
    }

    fn bytes_remaining(&self) -> usize {
        self.capacity - self.bytes_written
    }

    async fn write_cells(&mut self) -> Result<()> {
        let mut write_futures = vec![];
        for (index, (data, writer)) in self
            .cell_buffer
            .encode()
            .into_iter()
            .zip(self.block_writers.iter_mut())
            .enumerate()
        {
            // Don't create the blocks on the data nodes until there's actually data for it
            if data.is_empty() {
                continue;
            }

            if writer.is_none() {
                let mut cloned = self.block.clone();
                cloned.b.block_id += index as u64;
                cloned.locs = vec![cloned.locs[index].clone()];
                cloned.block_token = cloned.block_tokens[index].clone();
                cloned.storage_i_ds = vec![cloned.storage_i_ds[index].clone()];
                cloned.storage_types = vec![cloned.storage_types[index]];

                *writer = Some(
                    ReplicatedBlockWriter::new(
                        &self.protocol,
                        cloned,
                        self.block_size,
                        self.server_defaults.clone(),
                    )
                    .await?,
                )
            }

            let mut data = data.clone();
            write_futures.push(async move { writer.as_mut().unwrap().write(&mut data).await })
        }

        for write in join_all(write_futures).await {
            write?;
        }

        Ok(())
    }

    async fn write(&mut self, buf: &mut Bytes) -> Result<()> {
        let bytes_to_write = usize::min(buf.len(), self.bytes_remaining());

        let mut buf_to_write = buf.split_to(bytes_to_write);

        while !buf_to_write.is_empty() {
            self.cell_buffer.write(&mut buf_to_write);
            if self.cell_buffer.is_full() {
                self.write_cells().await?;
            }
        }

        self.bytes_written += bytes_to_write;

        Ok(())
    }

    async fn close(mut self) -> Result<()> {
        if !self.cell_buffer.is_empty() {
            self.write_cells().await?;
        }

        let close_futures = self
            .block_writers
            .into_iter()
            .filter_map(|mut writer| writer.take())
            .map(|writer| async move { writer.close().await });

        for close_result in join_all(close_futures).await {
            close_result?;
        }

        Ok(())
    }

    fn is_full(&self) -> bool {
        self.block_writers
            .iter()
            .all(|writer| writer.as_ref().is_some_and(|w| w.is_full()))
    }

    fn get_extended_block(&self) -> hdfs::ExtendedBlockProto {
        let mut extended_block = self.block.b.clone();

        extended_block.num_bytes = Some(self.bytes_written as u64);
        extended_block
    }
}
