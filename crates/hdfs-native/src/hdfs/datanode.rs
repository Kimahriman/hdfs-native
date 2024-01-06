use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{
    future::join_all,
    stream::{self, BoxStream},
    Stream, StreamExt,
};
use log::{debug, error};
use tokio::sync::{mpsc, oneshot};

use crate::{
    ec::{gf256::Coder, EcSchema},
    hdfs::connection::{DatanodeConnection, Op},
    proto::{common, hdfs},
    HdfsError, Result,
};

use super::connection::Packet;

const HEART_BEAT_SEQNO: i64 = -1;
const UNKNOWN_SEQNO: i64 = -1;

pub(crate) fn get_block_stream(
    block: hdfs::LocatedBlockProto,
    offset: usize,
    len: usize,
    ec_schema: Option<EcSchema>,
) -> BoxStream<'static, Result<Bytes>> {
    if let Some(ec_schema) = ec_schema {
        StripedBlockStream::new(block, offset, len, ec_schema)
            .into_stream()
            .boxed()
    } else {
        ReplicatedBlockStream::new(block, offset, len)
            .into_stream()
            .boxed()
    }
}

struct ReplicatedBlockStream {
    block: hdfs::LocatedBlockProto,
    offset: usize,
    len: usize,

    connection: Option<DatanodeConnection>,
    checksum_info: Option<hdfs::ReadOpChecksumInfoProto>,
    current_replica: usize,
}

impl ReplicatedBlockStream {
    fn new(block: hdfs::LocatedBlockProto, offset: usize, len: usize) -> Self {
        Self {
            block,
            offset,
            len,
            connection: None,
            checksum_info: None,
            current_replica: 0,
        }
    }

    async fn select_next_datanode(&mut self) -> Result<()> {
        if self.connection.is_some() {
            self.current_replica += 1;
            if self.current_replica >= self.block.locs.len() {
                return Err(HdfsError::DataTransferError(
                    "All DataNodes failed".to_string(),
                ));
            }
        }
        let datanode = &self.block.locs[self.current_replica].id;
        let mut connection =
            DatanodeConnection::connect(&format!("{}:{}", datanode.ip_addr, datanode.xfer_port))
                .await?;

        let message = hdfs::OpReadBlockProto {
            header: connection.build_header(&self.block.b, Some(self.block.block_token.clone())),
            offset: self.offset as u64,
            len: self.len as u64,
            send_checksums: Some(true),
            ..Default::default()
        };

        debug!("Block read op request {:?}", &message);

        connection.send(Op::ReadBlock, &message).await?;
        let response = connection.read_block_op_response().await?;
        debug!("Block read op response {:?}", response);

        if response.status() != hdfs::Status::Success {
            return Err(HdfsError::DataTransferError(response.message().to_string()));
        }

        self.connection = Some(connection);
        self.checksum_info = response.read_op_checksum_info;

        Ok(())
    }

    async fn next_packet(&mut self) -> Result<Option<Bytes>> {
        if self.len == 0 {
            return Ok(None);
        }
        if self.connection.is_none() {
            self.select_next_datanode().await?;
        }
        let conn = self.connection.as_mut().unwrap();
        let packet = conn.read_packet().await?;

        let packet_offset = if self.offset > packet.header.offset_in_block as usize {
            self.offset - packet.header.offset_in_block as usize
        } else {
            0
        };
        let packet_len = usize::min(packet.header.data_len as usize - packet_offset, self.len);
        let packet_data = packet.get_data(&self.checksum_info)?;

        self.offset += packet_len;
        self.len -= packet_len;

        Ok(Some(
            packet_data.slice(packet_offset..(packet_offset + packet_len)),
        ))
    }

    fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        stream::unfold(self, |mut state| async move {
            let next = state.next_packet().await.transpose();
            next.map(|n| (n, state))
        })
    }
}

struct StripedBlockStream {
    block: hdfs::LocatedBlockProto,
    offset: usize,
    len: usize,
    ec_schema: EcSchema,
}

impl StripedBlockStream {
    fn new(block: hdfs::LocatedBlockProto, offset: usize, len: usize, ec_schema: EcSchema) -> Self {
        Self {
            block,
            offset,
            len,
            ec_schema,
        }
    }

    /// Hacky "stream" of a single value to match replicated behavior
    /// TODO: Stream the results based on rows of cells?
    fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        stream::once(async move { self.read_striped().await })
    }

    /// Erasure coded data is stored in "cells" that are striped across Data Nodes.
    /// An example of what 3-2-1024k cells would look like:
    /// ----------------------------------------------
    /// | blk_0  | blk_1  | blk_2  | blk_3  | blk_4  |
    /// |--------|--------|--------|--------|--------|
    /// | cell_0 | cell_1 | cell_2 | parity | parity |
    /// | cell_3 | cell_4 | cell_5 | parity | parity |
    /// ----------------------------------------------
    ///
    /// Where cell_0 contains the first 1024k bytes, cell_1 contains the next 1024k bytes, and so on.
    ///
    /// For an initial, simple implementation, determine the cells containing the start and end
    /// of the range being requested, and request all "rows" or horizontal stripes of data containing
    /// and between the start and end cell. So if the read range starts in cell_1 and ends in cell_4,
    /// simply read all data blocks for cell_0 through cell_5.
    ///
    /// We then convert these logical horizontal stripes into vertical stripes to read from each block/DataNode.
    /// In this case, we will have one read for cell_0 and cell_3 from blk_0, one for cell_1 and cell_4 from blk_1,
    /// and one for cell_2 and cell_5 from blk_2. If all of these reads succeed, we know we have everything we need
    /// to reconstruct the data being requested. If any read fails, we will then request the parity cells for the same
    /// vertical range of cells. If more data block reads fail then parity blocks exist, the read will fail.
    ///
    /// Once we have enough of the vertical stripes, we can then convert those back into horizontal stripes to
    /// re-create each "row" of data. Then we simply need to take the range being requested out of the range
    /// we reconstructed.
    ///
    /// In the future we can look at making this more efficient by not reading as many extra cells that aren't
    /// part of the range being requested at all. Currently the overhead of not doing this would be up to
    /// `data_units * cell_size * 2` of extra data being read from disk (basically two extra "rows" of data).
    async fn read_striped(&self) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(self.len);

        // Cell IDs for the range we are reading, inclusive
        let starting_cell = self.ec_schema.cell_for_offset(self.offset);
        let ending_cell = self.ec_schema.cell_for_offset(self.offset + self.len - 1);

        // Logical rows or horizontal stripes we need to read, tail-exclusive
        let starting_row = self.ec_schema.row_for_cell(starting_cell);
        let ending_row = self.ec_schema.row_for_cell(ending_cell) + 1;

        // Block start/end within each vertical stripe, tail-exclusive
        let block_start = self.ec_schema.offset_for_row(starting_row);
        let block_end = self.ec_schema.offset_for_row(ending_row);
        let block_read_len = block_end - block_start;

        assert_eq!(self.block.block_indices().len(), self.block.locs.len());
        let block_map: HashMap<u8, &hdfs::DatanodeInfoProto> = self
            .block
            .block_indices()
            .iter()
            .copied()
            .zip(self.block.locs.iter())
            .collect();

        let mut stripe_results: Vec<Option<Bytes>> =
            vec![None; self.ec_schema.data_units + self.ec_schema.parity_units];

        let mut futures = Vec::new();

        for index in 0..self.ec_schema.data_units as u8 {
            futures.push(self.read_vertical_stripe(
                &self.ec_schema,
                index,
                block_map.get(&index),
                block_start,
                block_read_len,
            ));
        }

        // Do the actual reads and count how many data blocks failed
        let mut failed_data_blocks = 0usize;
        for (index, result) in join_all(futures).await.into_iter().enumerate() {
            if let Ok(bytes) = result {
                stripe_results[index] = Some(bytes);
            } else {
                failed_data_blocks += 1;
            }
        }

        let mut blocks_needed = failed_data_blocks;
        let mut parity_unit = 0usize;
        while blocks_needed > 0 && parity_unit < self.ec_schema.parity_units {
            let block_index = (self.ec_schema.data_units + parity_unit) as u8;
            let datanode_info = block_map.get(&block_index).unwrap();
            let result = self
                .read_vertical_stripe(
                    &self.ec_schema,
                    block_index,
                    Some(datanode_info),
                    block_start,
                    block_read_len,
                )
                .await;

            if let Ok(bytes) = result {
                stripe_results[block_index as usize] = Some(bytes);
                blocks_needed -= 1;
            }
            parity_unit += 1;
        }

        let decoded_bufs = self.ec_schema.ec_decode(stripe_results)?;
        let mut bytes_to_skip =
            self.offset - starting_row * self.ec_schema.data_units * self.ec_schema.cell_size;
        let mut bytes_to_write = self.len;
        for mut cell in decoded_bufs.into_iter() {
            if bytes_to_skip > 0 {
                if cell.len() > bytes_to_skip {
                    bytes_to_skip -= cell.len();
                    continue;
                } else {
                    cell.advance(bytes_to_skip);
                    bytes_to_skip = 0;
                }
            }

            if cell.len() >= bytes_to_write {
                buf.put(cell.split_to(bytes_to_write));
                break;
            } else {
                bytes_to_write -= cell.len();
                buf.put(cell);
            }
        }

        Ok(buf.freeze())
    }

    async fn read_vertical_stripe(
        &self,
        ec_schema: &EcSchema,
        index: u8,
        datanode: Option<&&hdfs::DatanodeInfoProto>,
        offset: usize,
        len: usize,
    ) -> Result<Bytes> {
        #[cfg(feature = "integration-test")]
        if let Some(fault_injection) = crate::test::EC_FAULT_INJECTOR.lock().unwrap().as_ref() {
            if fault_injection.fail_blocks.contains(&(index as usize)) {
                debug!("Failing block read for {}", index);
                return Err(HdfsError::InternalError("Testing error".to_string()));
            }
        }

        let mut buf = BytesMut::zeroed(len);
        if let Some(datanode_info) = datanode {
            let max_block_offset =
                ec_schema.max_offset(index as usize, self.block.b.num_bytes() as usize);

            let read_len = usize::min(len, max_block_offset - offset);

            // Each vertical stripe has a block ID of the original located block ID + block index
            // That was fun to figure out
            let mut block = self.block.b.clone();
            block.block_id += index as u64;

            // The token of the first block is the main one, then all the rest are in the `block_tokens` list
            let token = &self.block.block_tokens[self
                .block
                .block_indices()
                .iter()
                .position(|x| *x == index)
                .unwrap()];

            self.read_from_datanode(&datanode_info.id, &block, token, offset, read_len, &mut buf)
                .await?;
        }

        Ok(buf.freeze())
    }

    async fn read_from_datanode(
        &self,
        datanode: &hdfs::DatanodeIdProto,
        block: &hdfs::ExtendedBlockProto,
        token: &common::TokenProto,
        offset: usize,
        len: usize,
        mut buf: &mut [u8],
    ) -> Result<()> {
        if len == 0 {
            return Ok(());
        }

        let mut conn =
            DatanodeConnection::connect(&format!("{}:{}", datanode.ip_addr, datanode.xfer_port))
                .await?;

        let message = hdfs::OpReadBlockProto {
            header: conn.build_header(block, Some(token.clone())),
            offset: offset as u64,
            len: len as u64,
            send_checksums: Some(true),
            ..Default::default()
        };
        debug!("Block read op request {:?}", &message);

        conn.send(Op::ReadBlock, &message).await?;
        let response = conn.read_block_op_response().await?;
        debug!("Block read op response {:?}", response);

        if response.status() != hdfs::Status::Success {
            return Err(HdfsError::DataTransferError(response.message().to_string()));
        }

        // First handle the offset into the first packet
        let mut packet = conn.read_packet().await?;
        let packet_offset = offset - packet.header.offset_in_block as usize;
        let data_len = packet.header.data_len as usize - packet_offset;
        let data_to_read = usize::min(data_len, len);
        let mut data_left = len - data_to_read;

        let packet_data = packet.get_data(&response.read_op_checksum_info)?;
        buf.put(packet_data.slice(packet_offset..(packet_offset + data_to_read)));

        while data_left > 0 {
            packet = conn.read_packet().await?;
            // TODO: Error checking
            let data_to_read = usize::min(data_left, packet.header.data_len as usize);
            buf.put(
                packet
                    .get_data(&response.read_op_checksum_info)?
                    .slice(0..data_to_read),
            );
            data_left -= data_to_read;
        }

        // There should be one last empty packet after we are done
        conn.read_packet().await?;

        Ok(())
    }
}

/// Wrapper around both types of block writers. This was simpler than trying to
/// do dynamic dispatch with a BlockWriter trait.
pub(crate) enum BlockWriter {
    Replicated(ReplicatedBlockWriter),
    Striped(StripedBlockWriter),
}

impl BlockWriter {
    pub(crate) async fn new(
        block: hdfs::LocatedBlockProto,
        block_size: usize,
        server_defaults: hdfs::FsServerDefaultsProto,
        ec_schema: Option<&EcSchema>,
    ) -> Result<Self> {
        let block_writer = if let Some(ec_schema) = ec_schema {
            Self::Striped(StripedBlockWriter::new(
                block,
                ec_schema,
                block_size,
                server_defaults,
            ))
        } else {
            Self::Replicated(ReplicatedBlockWriter::new(block, block_size, server_defaults).await?)
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

    pub(crate) async fn close(&mut self) -> Result<()> {
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
    connection: DatanodeConnection,
    current_packet: Packet,

    // Tracks the state of acknowledgements. Set to an Err if any error occurs doing receiving
    // acknowledgements. Set to Ok(()) when the last acknowledgement is received.
    status: Option<oneshot::Receiver<Result<()>>>,
    ack_queue: mpsc::Sender<(i64, bool)>,
}

impl ReplicatedBlockWriter {
    async fn new(
        block: hdfs::LocatedBlockProto,
        block_size: usize,
        server_defaults: hdfs::FsServerDefaultsProto,
    ) -> Result<Self> {
        let datanode = &block.locs[0].id;
        let mut connection =
            DatanodeConnection::connect(&format!("{}:{}", datanode.ip_addr, datanode.xfer_port))
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

        connection.send(Op::WriteBlock, &message).await?;
        let response = connection.read_block_op_response().await?;
        debug!("Block write response: {:?}", response);

        // Channel for receiving acks from the datanode
        let (ack_response_sender, ack_response_receiver) =
            mpsc::channel::<hdfs::PipelineAckProto>(100);
        // Channel for tracking packets that need to be acked
        let (ack_queue_sender, ack_queue_receiever) = mpsc::channel::<(i64, bool)>(100);
        // Channel for tracking errors that occur listening for acks or successful ack of the last packet
        let (status_sender, status_receiver) = oneshot::channel::<Result<()>>();

        connection.read_acks(ack_response_sender)?;

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
            connection,
            current_packet,
            status: Some(status_receiver),
            ack_queue: ack_queue_sender,
        };
        this.listen_for_acks(ack_response_receiver, ack_queue_receiever, status_sender);

        Ok(this)
    }

    fn create_next_packet(&mut self) {
        self.current_packet = Packet::empty(
            self.block.b.num_bytes() as i64,
            self.next_seqno,
            self.server_defaults.bytes_per_checksum,
            self.server_defaults.write_packet_size,
        );
        self.next_seqno += 1;
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

        // Send the packet
        self.connection
            .write_packet(&mut self.current_packet)
            .await?;

        // Create a fresh packet
        self.create_next_packet();

        Ok(())
    }

    fn check_error(&mut self) -> Result<()> {
        if let Some(status) = self.status.as_mut() {
            match status.try_recv() {
                Ok(result) => result?,
                Err(oneshot::error::TryRecvError::Empty) => (),
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(HdfsError::DataTransferError(
                        "Status channel closed prematurely".to_string(),
                    ))
                }
            }
        }

        Ok(())
    }

    fn listen_for_acks(
        &self,
        mut ack_receiver: mpsc::Receiver<hdfs::PipelineAckProto>,
        mut ack_queue: mpsc::Receiver<(i64, bool)>,
        status: oneshot::Sender<Result<()>>,
    ) {
        tokio::spawn(async move {
            loop {
                let next_ack_opt = ack_receiver.recv().await;
                if next_ack_opt.is_none() {
                    error!("Channel closed while waiting for next ack");
                    break;
                }
                let next_ack = next_ack_opt.unwrap();
                for reply in next_ack.reply.iter() {
                    if *reply != hdfs::Status::Success as i32 {
                        let _ = status.send(Err(HdfsError::DataTransferError(format!(
                            "Received non-success status in datanode ack: {:?}",
                            hdfs::Status::from_i32(*reply)
                        ))));
                        return;
                    }
                }

                if next_ack.seqno == HEART_BEAT_SEQNO {
                    continue;
                }
                if next_ack.seqno == UNKNOWN_SEQNO {
                    let _ = status.send(Err(HdfsError::DataTransferError(
                        "Received unknown seqno for successful ack".to_string(),
                    )));
                    return;
                }

                let next_seqno = ack_queue.recv().await;
                if next_seqno.is_none() {
                    let _ = status.send(Err(HdfsError::DataTransferError(
                        "Channel closed while getting next seqno to acknowledge".to_string(),
                    )));
                    return;
                }

                let (seqno, last_packet) = next_seqno.unwrap();

                if next_ack.seqno != seqno {
                    let _ = status.send(Err(HdfsError::DataTransferError(
                        "Received acknowledgement does not match expected sequence number"
                            .to_string(),
                    )));
                    return;
                }

                if last_packet {
                    let _ = status.send(Ok(()));
                    return;
                }
            }
        });
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
    async fn close(&mut self) -> Result<()> {
        self.check_error()?;

        // Send a packet with any remaining data
        if !self.current_packet.is_empty() {
            self.send_current_packet().await?;
        }

        // Send an empty last packet
        self.current_packet.set_last_packet();
        self.send_current_packet().await?;

        // Wait for the channel to close, meaning all acks have been received or an error occured
        if let Some(status) = self.status.take() {
            let result = status.await.map_err(|_| {
                HdfsError::DataTransferError(
                    "Status channel closed while waiting for final ack".to_string(),
                )
            })?;
            result?;
        } else {
            return Err(HdfsError::DataTransferError(
                "Block already closed".to_string(),
            ));
        }

        Ok(())
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
        block: hdfs::LocatedBlockProto,
        ec_schema: &EcSchema,
        block_size: usize,
        server_defaults: hdfs::FsServerDefaultsProto,
    ) -> Self {
        let block_writers = (0..block.block_indices().len()).map(|_| None).collect();

        Self {
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
            if writer.is_none() {
                let mut cloned = self.block.clone();
                cloned.b.block_id += index as u64;
                cloned.locs = vec![cloned.locs[index].clone()];
                cloned.block_token = cloned.block_tokens[index].clone();
                cloned.storage_i_ds = vec![cloned.storage_i_ds[index].clone()];
                cloned.storage_types = vec![cloned.storage_types[index]];

                *writer = Some(
                    ReplicatedBlockWriter::new(
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

    async fn close(&mut self) -> Result<()> {
        if !self.cell_buffer.is_empty() {
            self.write_cells().await?;
        }

        let close_futures = self
            .block_writers
            .iter_mut()
            .filter_map(|writer| writer.as_mut())
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
