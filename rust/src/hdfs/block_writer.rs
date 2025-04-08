use std::{sync::Arc, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use futures::future::join_all;
use log::{debug, warn};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    common::config::Configuration,
    ec::{gf256::Coder, EcSchema},
    hdfs::{
        connection::{DatanodeConnection, DatanodeReader, DatanodeWriter, Op, WritePacket},
        protocol::NamenodeProtocol,
        replace_datanode::ReplaceDatanodeOnFailure,
    },
    proto::{common, hdfs},
    HdfsError, Result,
};

const HEART_BEAT_SEQNO: i64 = -1;
const UNKNOWN_SEQNO: i64 = -2;

const HEARTBEAT_INTERVAL_SECONDS: u64 = 30;

// The number of packets and acks to queue up on writes
const WRITE_PACKET_BUFFER_LEN: usize = 100;

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
        server_defaults: hdfs::FsServerDefaultsProto,
        config: Arc<Configuration>,
        ec_schema: Option<&EcSchema>,
        src: &str,
        status: &hdfs::HdfsFileStatusProto,
    ) -> Result<Self> {
        let block_writer = if let Some(ec_schema) = ec_schema {
            Self::Striped(StripedBlockWriter::new(
                protocol,
                block,
                ec_schema,
                server_defaults,
                config,
                status,
            ))
        } else {
            Self::Replicated(
                ReplicatedBlockWriter::new(src, protocol, block, server_defaults, config, status)
                    .await?,
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

    pub(crate) async fn close(self) -> Result<hdfs::ExtendedBlockProto> {
        match self {
            Self::Replicated(writer) => writer.close().await,
            Self::Striped(writer) => writer.close().await,
        }
    }
}

enum WriteStatus {
    Success,
    Recover(Vec<usize>, Vec<WritePacket>),
}

struct Pipeline {
    packet_sender: mpsc::Sender<WritePacket>,
    // Tracks the state of acknowledgements. Set to an Err if any error occurs doing receiving
    // acknowledgements. Set to Ok(WriteStatus::Success) when the last acknowledgement is received.
    ack_listener_handle: JoinHandle<Result<WriteStatus>>,
    // Tracks the state of packet sender. Set to Err if any error occurs during writing packets,
    packet_sender_handle: JoinHandle<Vec<WritePacket>>,
    // Tracks the heartbeat task so we can abort it when we close
    heartbeat_handle: JoinHandle<()>,
}

impl Pipeline {
    fn new(connection: DatanodeConnection) -> Self {
        let (reader, writer) = connection.split();

        // Channel for tracking packets that need to be acked
        let (ack_queue_sender, ack_queue_receiever) =
            mpsc::channel::<WritePacket>(WRITE_PACKET_BUFFER_LEN);
        let (packet_sender, packet_receiver) =
            mpsc::channel::<WritePacket>(WRITE_PACKET_BUFFER_LEN);

        let ack_listener_handle = Self::listen_for_acks(reader, ack_queue_receiever);
        let packet_sender_handle =
            Self::start_packet_sender(writer, packet_receiver, ack_queue_sender);
        let heartbeat_handle = Self::start_heartbeat_sender(packet_sender.clone());

        Self {
            packet_sender,
            ack_listener_handle,
            packet_sender_handle,
            heartbeat_handle,
        }
    }

    async fn send_packet(&self, packet: WritePacket) -> std::result::Result<(), WritePacket> {
        self.packet_sender.send(packet).await.map_err(|e| e.0)
    }

    async fn shutdown(self) -> Result<WriteStatus> {
        self.heartbeat_handle.abort();

        let (failed_nodes, unacked_packets) = match self.ack_listener_handle.await.unwrap()? {
            WriteStatus::Success => {
                if !self.packet_sender_handle.await.unwrap().is_empty() {
                    return Err(HdfsError::DataTransferError(
                        "Failed to send all packets to DataNode".to_string(),
                    ));
                }
                return Ok(WriteStatus::Success);
            }
            WriteStatus::Recover(failed_nodes, unacked_packets) => (failed_nodes, unacked_packets),
        };

        let packets_to_send = self.packet_sender_handle.await.unwrap();

        let packets_to_replay = unacked_packets.into_iter().chain(packets_to_send).collect();

        Ok(WriteStatus::Recover(failed_nodes, packets_to_replay))
    }

    async fn drain_queue(mut queue: mpsc::Receiver<WritePacket>) -> Vec<WritePacket> {
        queue.close();

        let mut packets = Vec::with_capacity(queue.len());
        while let Some(packet) = queue.recv().await {
            packets.push(packet);
        }
        packets
    }

    fn start_packet_sender(
        mut writer: DatanodeWriter,
        mut packet_receiver: mpsc::Receiver<WritePacket>,
        ack_queue: mpsc::Sender<WritePacket>,
    ) -> JoinHandle<Vec<WritePacket>> {
        tokio::spawn(async move {
            while let Some(mut packet) = packet_receiver.recv().await {
                // Simulate node we are writing to failing
                #[cfg(feature = "integration-test")]
                if crate::test::WRITE_CONNECTION_FAULT_INJECTOR
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    debug!("Failing write to active node");
                    return [packet]
                        .into_iter()
                        .chain(Self::drain_queue(packet_receiver).await)
                        .collect();
                }

                if let Err(e) = writer.write_packet(&mut packet).await {
                    warn!("Failed to send packet to DataNode: {:?}", e);
                    return [packet]
                        .into_iter()
                        .chain(Self::drain_queue(packet_receiver).await)
                        .collect();
                }

                if packet.header.seqno == HEART_BEAT_SEQNO {
                    continue;
                }

                let last_packet = packet.header.last_packet_in_block;

                if let Err(err) = ack_queue.send(packet).await {
                    // Ack listener failed, so it will have a failed node
                    return [err.0]
                        .into_iter()
                        .chain(Self::drain_queue(packet_receiver).await)
                        .collect();
                };

                if last_packet {
                    break;
                }
            }
            vec![]
        })
    }

    fn start_heartbeat_sender(packet_sender: mpsc::Sender<WritePacket>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
                let heartbeat_packet = WritePacket::empty(0, HEART_BEAT_SEQNO, 0, 0);
                // If this fails, sending anymore data packets will generate an error as well
                if packet_sender.send(heartbeat_packet).await.is_err() {
                    break;
                }
            }
        })
    }

    fn listen_for_acks(
        mut reader: DatanodeReader,
        mut ack_queue: mpsc::Receiver<WritePacket>,
    ) -> JoinHandle<Result<WriteStatus>> {
        tokio::spawn(async move {
            loop {
                let next_ack = match reader.read_ack().await {
                    Ok(next_ack) => next_ack,
                    Err(e) => {
                        warn!("Failed to read ack from DataNode: {}", e);
                        return Ok(WriteStatus::Recover(
                            vec![0],
                            Self::drain_queue(ack_queue).await,
                        ));
                    }
                };

                let mut failed_nodes: Vec<usize> = vec![];

                for (i, reply) in next_ack.reply().enumerate() {
                    // Simulate node we are replicating to failing
                    #[cfg(feature = "integration-test")]
                    if crate::test::WRITE_REPLY_FAULT_INJECTOR
                        .lock()
                        .unwrap()
                        .take()
                        .is_some_and(|j| i == j)
                    {
                        debug!("Failing write to replica node");
                        failed_nodes.push(i);
                    }

                    if reply != hdfs::Status::Success {
                        failed_nodes.push(i);
                    }
                }

                if !failed_nodes.is_empty() {
                    // I need a way to make sure the packet sender thread dies here
                    return Ok(WriteStatus::Recover(
                        failed_nodes,
                        Self::drain_queue(ack_queue).await,
                    ));
                }

                if next_ack.seqno == HEART_BEAT_SEQNO {
                    continue;
                }
                if next_ack.seqno == UNKNOWN_SEQNO {
                    return Err(HdfsError::DataTransferError(
                        "Received unknown seqno for successful ack".to_string(),
                    ));
                }

                if let Some(packet) = ack_queue.recv().await {
                    debug!("Next: {}, packet: {}", next_ack.seqno, packet.header.seqno);
                    if next_ack.seqno != packet.header.seqno {
                        return Err(HdfsError::DataTransferError(
                            "Received acknowledgement does not match expected sequence number"
                                .to_string(),
                        ));
                    }

                    if packet.header.last_packet_in_block {
                        return Ok(WriteStatus::Success);
                    }
                } else {
                    // Error occurred in the packet sender, which would only happen on errors
                    // communicating with the DataNode
                    return Ok(WriteStatus::Recover(
                        vec![0],
                        Self::drain_queue(ack_queue).await,
                    ));
                }
            }
        })
    }
}

pub(crate) struct ReplicatedBlockWriter {
    src: String,
    protocol: Arc<NamenodeProtocol>,
    block: hdfs::LocatedBlockProto,
    block_size: usize,
    server_defaults: hdfs::FsServerDefaultsProto,

    current_packet: WritePacket,
    pipeline: Option<Pipeline>,
    status: hdfs::HdfsFileStatusProto,
    replace_datanode: ReplaceDatanodeOnFailure,
}

impl ReplicatedBlockWriter {
    async fn new(
        src: &str,
        protocol: Arc<NamenodeProtocol>,
        block: hdfs::LocatedBlockProto,
        server_defaults: hdfs::FsServerDefaultsProto,
        config: Arc<Configuration>,
        status: &hdfs::HdfsFileStatusProto,
    ) -> Result<Self> {
        let pipeline =
            Self::setup_pipeline(&protocol, &block, &server_defaults, None, None).await?;

        let bytes_in_last_chunk = block.b.num_bytes() % server_defaults.bytes_per_checksum as u64;
        let current_packet = if bytes_in_last_chunk > 0 {
            // When appending, we want to first send a packet with a single chunk of the data required
            // to get the block to a multiple of bytes_per_checksum. After that, things work the same
            // as create.
            WritePacket::empty(
                block.b.num_bytes() as i64,
                0,
                server_defaults.bytes_per_checksum - bytes_in_last_chunk as u32,
                0,
            )
        } else {
            WritePacket::empty(
                block.b.num_bytes() as i64,
                0,
                server_defaults.bytes_per_checksum,
                server_defaults.write_packet_size,
            )
        };

        let this = Self {
            src: src.to_string(),
            protocol,
            block,
            block_size: status.blocksize() as usize,
            server_defaults,
            current_packet,
            pipeline: Some(pipeline),
            status: status.clone(),
            replace_datanode: config.get_replace_datanode_on_failure_policy(),
        };

        Ok(this)
    }

    async fn recover(
        &mut self,
        failed_nodes: Vec<usize>,
        packets_to_replay: Vec<WritePacket>,
        next_packet: Option<&WritePacket>,
    ) -> Result<()> {
        debug!(
            "Failed nodes: {:?}, block locs: {:?}",
            failed_nodes, self.block.locs
        );
        if failed_nodes.len() >= self.block.locs.len() {
            return Err(HdfsError::DataTransferError(
                "All nodes failed for write".to_string(),
            ));
        }

        debug!("Recovering block writer");

        let mut existing_block = self.block.clone();
        let mut exclude_nodes = vec![];
        for failed_node in &failed_nodes {
            exclude_nodes.push(existing_block.locs[*failed_node].clone());

            existing_block.locs.remove(*failed_node);
            existing_block.storage_i_ds.remove(*failed_node);
            existing_block.storage_types.remove(*failed_node);
        }
        let should_replace = self.replace_datanode.should_replace(
            self.status.block_replication.unwrap(),
            &existing_block.locs,
            self.block.b.num_bytes() > 0,
            false,
        );

        let mut new_block = self.block.clone();

        if should_replace {
            match self
                .add_datanode_to_pipeline(existing_block, &exclude_nodes)
                .await
            {
                Ok(located_block) => {
                    new_block.locs = located_block.locs;
                    new_block.storage_i_ds = located_block.storage_i_ds;
                    new_block.storage_types = located_block.storage_types;
                }
                Err(e) => {
                    if !self.replace_datanode.is_best_effort() {
                        return Err(e);
                    }
                    warn!("Failed to add replacement datanode: {}", e);
                    for failed_node in &failed_nodes {
                        new_block.locs.remove(*failed_node);
                        new_block.storage_i_ds.remove(*failed_node);
                        new_block.storage_types.remove(*failed_node);
                    }
                }
            }
        } else {
            for failed_node in failed_nodes {
                new_block.locs.remove(failed_node);
                new_block.storage_i_ds.remove(failed_node);
                new_block.storage_types.remove(failed_node);
            }
        }

        let mut bytes_acked = new_block.b.num_bytes();
        for packet in packets_to_replay.iter() {
            bytes_acked -= packet.data.len() as u64;
        }

        if let Some(next_packet) = next_packet {
            bytes_acked -= next_packet.data.len() as u64;
        }

        let old_block = std::mem::replace(&mut self.block, new_block);

        let updated_block = self
            .protocol
            .update_block_for_pipeline(self.block.b.clone())
            .await?;

        // self.block.b.generation_stamp = updated_block.block.b.generation_stamp;
        self.block.block_token = updated_block.block.block_token;

        let pipeline = Self::setup_pipeline(
            &self.protocol,
            &self.block,
            &self.server_defaults,
            Some(updated_block.block.b.generation_stamp),
            Some(bytes_acked),
        )
        .await?;

        self.block.b.generation_stamp = updated_block.block.b.generation_stamp;

        for packet in packets_to_replay {
            pipeline.send_packet(packet).await.map_err(|_| {
                HdfsError::DataTransferError("Failed to replay packets during recovery".to_string())
            })?;
        }

        self.protocol
            .update_pipeline(
                old_block.b,
                self.block.b.clone(),
                self.block.locs.iter().map(|l| l.id.clone()).collect(),
                self.block.storage_i_ds.clone(),
            )
            .await?;

        self.pipeline = Some(pipeline);

        Ok(())
    }

    async fn setup_pipeline(
        protocol: &Arc<NamenodeProtocol>,
        block: &hdfs::LocatedBlockProto,
        server_defaults: &hdfs::FsServerDefaultsProto,
        new_gs: Option<u64>,
        bytes_acked: Option<u64>,
    ) -> Result<Pipeline> {
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

        let stage = if new_gs.is_some() {
            hdfs::op_write_block_proto::BlockConstructionStage::PipelineSetupStreamingRecovery
                as i32
        } else if block.b.num_bytes() > 0 {
            hdfs::op_write_block_proto::BlockConstructionStage::PipelineSetupAppend as i32
        } else {
            hdfs::op_write_block_proto::BlockConstructionStage::PipelineSetupCreate as i32
        };

        let message = hdfs::OpWriteBlockProto {
            header: connection.build_header(&block.b, Some(block.block_token.clone())),
            stage,
            targets: block.locs[1..].to_vec(),
            pipeline_size: block.locs.len() as u32,
            latest_generation_stamp: new_gs.unwrap_or(block.b.generation_stamp),
            min_bytes_rcvd: bytes_acked.unwrap_or(block.b.num_bytes()),
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

        Ok(Pipeline::new(connection))
    }

    // Create the next packet and return the current packet
    fn create_next_packet(&mut self) -> WritePacket {
        let next_packet = WritePacket::empty(
            self.block.b.num_bytes() as i64,
            self.current_packet.header.seqno + 1,
            self.server_defaults.bytes_per_checksum,
            self.server_defaults.write_packet_size,
        );
        std::mem::replace(&mut self.current_packet, next_packet)
    }

    async fn send_current_packet(&mut self) -> Result<()> {
        // Create a fresh packet
        let mut current_packet = self.create_next_packet();

        loop {
            let pipeline = self.pipeline.take().ok_or(HdfsError::DataTransferError(
                "Block writer is closed".to_string(),
            ))?;

            if let Err(packet) = pipeline.send_packet(current_packet).await {
                // Shutdown the pipeline and try to recover
                current_packet = packet;
                match pipeline.shutdown().await? {
                    WriteStatus::Success => {
                        return Err(HdfsError::DataTransferError(
                            "Pipeline succeeded but failure was expected".to_string(),
                        ))
                    }
                    WriteStatus::Recover(failed_nodes, packets_to_replay) => {
                        self.recover(failed_nodes, packets_to_replay, Some(&current_packet))
                            .await?;
                    }
                }
            } else {
                self.pipeline = Some(pipeline);
                return Ok(());
            }
            // Send the packet
        }
    }

    fn is_full(&self) -> bool {
        self.block.b.num_bytes() == self.block_size as u64
    }

    async fn write(&mut self, buf: &mut Bytes) -> Result<()> {
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
    async fn close(mut self) -> Result<hdfs::ExtendedBlockProto> {
        // Send a packet with any remaining data
        if !self.current_packet.is_empty() {
            self.send_current_packet().await?;
        }

        // Send an empty last packet
        self.current_packet.set_last_packet();
        self.send_current_packet().await?;

        loop {
            match self
                .pipeline
                .take()
                .ok_or(HdfsError::DataTransferError(
                    "Block writer closed prematurely".to_string(),
                ))?
                .shutdown()
                .await?
            {
                WriteStatus::Success => return Ok(self.block.b),
                WriteStatus::Recover(failed_nodes, packets_to_replay) => {
                    self.recover(failed_nodes, packets_to_replay, None).await?
                }
            }
        }
    }

    async fn transfer_block(
        &self,
        src_node: &hdfs::DatanodeInfoProto,
        target_nodes: &[hdfs::DatanodeInfoProto],
        target_storage_types: &[i32],
        block_token: &common::TokenProto,
    ) -> Result<()> {
        let mut connection = DatanodeConnection::connect(
            &src_node.id,
            block_token,
            self.protocol.get_cached_data_encryption_key().await?,
        )
        .await?;

        let message = hdfs::OpTransferBlockProto {
            header: connection.build_header(&self.block.b, Some(block_token.clone())),
            targets: target_nodes.to_vec(),
            target_storage_types: target_storage_types.to_vec(),
            target_storage_ids: vec![],
        };

        debug!("Transfer block request: {:?}", &message);

        let response = connection.send(Op::TransferBlock, &message).await?;

        debug!("Transfer block response: {:?}", response);

        if response.status != hdfs::Status::Success as i32 {
            return Err(HdfsError::DataTransferError(
                "Failed to add a datanode".to_string(),
            ));
        }

        Ok(())
    }

    async fn add_datanode_to_pipeline(
        &mut self,
        block: hdfs::LocatedBlockProto,
        exclude_nodes: &[hdfs::DatanodeInfoProto],
    ) -> Result<hdfs::LocatedBlockProto> {
        let original_nodes = self.block.locs.clone();
        let located_block = self
            .protocol
            .get_additional_datanode(
                &self.src,
                &block.b,
                &block.locs,
                exclude_nodes,
                &block.storage_i_ds,
                1,
            )
            .await?;

        let new_nodes = &located_block.locs;
        let new_node_idx = new_nodes
            .iter()
            .position(|node| {
                !original_nodes.iter().any(|orig| {
                    orig.id.ip_addr == node.id.ip_addr && orig.id.xfer_port == node.id.xfer_port
                })
            })
            .ok_or_else(|| {
                HdfsError::DataTransferError(
                    "No new datanode found in updated block locations".to_string(),
                )
            })?;

        let src_node = if new_node_idx == 0 {
            new_nodes[1].clone()
        } else {
            new_nodes[new_node_idx - 1].clone()
        };

        debug!(
            "Start to transfer block. src_node: {:?} target_node: {:?}",
            src_node, new_nodes[new_node_idx]
        );
        self.transfer_block(
            &src_node,
            &[new_nodes[new_node_idx].clone()],
            &[self.block.storage_types[0]],
            &located_block.block_token,
        )
        .await?;
        debug!(
            "Finished to transfer block. src_node: {:?} target_node: {:?}",
            src_node, new_nodes[new_node_idx]
        );

        Ok(located_block)
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
    config: Arc<Configuration>,
    block_writers: Vec<Option<ReplicatedBlockWriter>>,
    cell_buffer: CellBuffer,
    bytes_written: usize,
    capacity: usize,
    status: hdfs::HdfsFileStatusProto,
}

impl StripedBlockWriter {
    fn new(
        protocol: Arc<NamenodeProtocol>,
        block: hdfs::LocatedBlockProto,
        ec_schema: &EcSchema,
        server_defaults: hdfs::FsServerDefaultsProto,
        config: Arc<Configuration>,
        status: &hdfs::HdfsFileStatusProto,
    ) -> Self {
        let block_writers = (0..block.block_indices().len()).map(|_| None).collect();

        Self {
            protocol,
            block,
            server_defaults,
            config,
            block_writers,
            cell_buffer: CellBuffer::new(ec_schema),
            bytes_written: 0,
            capacity: ec_schema.data_units * status.blocksize() as usize,
            status: status.clone(),
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
                        "",
                        Arc::clone(&self.protocol),
                        cloned,
                        self.server_defaults.clone(),
                        Arc::clone(&self.config),
                        &self.status,
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

    async fn close(mut self) -> Result<hdfs::ExtendedBlockProto> {
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

        let mut extended_block = self.block.b;

        extended_block.num_bytes = Some(self.bytes_written as u64);

        Ok(extended_block)
    }

    fn is_full(&self) -> bool {
        self.block_writers
            .iter()
            .all(|writer| writer.as_ref().is_some_and(|w| w.is_full()))
    }
}
