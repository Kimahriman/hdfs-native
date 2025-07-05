use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt,
};
use log::{debug, warn};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    ec::EcSchema,
    hdfs::connection::{DatanodeConnection, Op, DATANODE_CACHE},
    proto::{
        common,
        hdfs::{
            self, BlockOpResponseProto, LocatedBlockProto, PacketHeaderProto,
            ReadOpChecksumInfoProto,
        },
    },
    HdfsError, Result,
};

use super::protocol::NamenodeProtocol;

// The number of packets to queue up on reads
const READ_PACKET_BUFFER_LEN: usize = 100;

pub(crate) fn get_block_stream(
    protocol: Arc<NamenodeProtocol>,
    block: hdfs::LocatedBlockProto,
    offset: usize,
    len: usize,
    ec_schema: Option<EcSchema>,
) -> BoxStream<'static, Result<Bytes>> {
    if let Some(ec_schema) = ec_schema {
        StripedBlockStream::new(protocol, block, offset, len, ec_schema)
            .into_stream()
            .boxed()
    } else {
        ReplicatedBlockStream::new(protocol, block, offset, len)
            .into_stream()
            .boxed()
    }
}

/// Connects to a DataNode to do a read, attempting to used cached connections.
async fn connect_and_send(
    protocol: &Arc<NamenodeProtocol>,
    datanode_id: &hdfs::DatanodeIdProto,
    block: &hdfs::ExtendedBlockProto,
    token: common::TokenProto,
    offset: u64,
    len: u64,
) -> Result<(DatanodeConnection, BlockOpResponseProto)> {
    #[cfg(feature = "integration-test")]
    if crate::test::DATANODE_CONNECT_FAULT_INJECTOR.swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(HdfsError::DataTransferError(
            "DataNode connect fault injection".to_string(),
        ));
    }

    let mut remaining_attempts = 2;
    while remaining_attempts > 0 {
        if let Some(mut conn) = DATANODE_CACHE.get(datanode_id) {
            let message = hdfs::OpReadBlockProto {
                header: conn.build_header(block, Some(token.clone())),
                offset,
                len,
                send_checksums: Some(true),
                ..Default::default()
            };
            debug!("Block read op request {:?}", &message);
            match conn.send(Op::ReadBlock, &message).await {
                Ok(response) => {
                    debug!("Block read op response {:?}", response);
                    return Ok((conn, response));
                }
                Err(e) => {
                    warn!("Failed to use cached connection: {:?}", e);
                }
            }
        } else {
            break;
        }
        remaining_attempts -= 1;
    }
    let mut conn = DatanodeConnection::connect(
        datanode_id,
        &token,
        protocol.get_cached_data_encryption_key().await?,
    )
    .await?;

    let message = hdfs::OpReadBlockProto {
        header: conn.build_header(block, Some(token)),
        offset,
        len,
        send_checksums: Some(true),
        ..Default::default()
    };

    debug!("Block read op request {:?}", &message);
    let response = conn.send(Op::ReadBlock, &message).await?;
    debug!("Block read op response {:?}", response);
    Ok((conn, response))
}

struct ReplicatedBlockStream {
    protocol: Arc<NamenodeProtocol>,
    block: hdfs::LocatedBlockProto,
    offset: usize,
    len: usize,

    listener: Option<JoinHandle<Result<DatanodeConnection>>>,
    sender: Sender<Result<(PacketHeaderProto, Bytes)>>,
    receiver: Receiver<Result<(PacketHeaderProto, Bytes)>>,
    next_replica: usize,
}

impl ReplicatedBlockStream {
    fn new(
        protocol: Arc<NamenodeProtocol>,
        block: hdfs::LocatedBlockProto,
        offset: usize,
        len: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(READ_PACKET_BUFFER_LEN);

        Self {
            protocol,
            block,
            offset,
            len,
            listener: None,
            sender,
            receiver,
            next_replica: 0,
        }
    }

    async fn select_next_datanode(
        &mut self,
    ) -> Result<(DatanodeConnection, Option<ReadOpChecksumInfoProto>)> {
        loop {
            if self.next_replica >= self.block.locs.len() {
                return Err(HdfsError::DataTransferError(format!(
                    "All DataNodes failed: {:?}",
                    self.block.locs
                )));
            }

            let datanode = &self.block.locs[self.next_replica].id;

            self.next_replica += 1;

            match connect_and_send(
                &self.protocol,
                datanode,
                &self.block.b,
                self.block.block_token.clone(),
                self.offset as u64,
                self.len as u64,
            )
            .await
            {
                Ok((connection, response)) => {
                    if response.status() != hdfs::Status::Success {
                        warn!(
                            "Read operation did not succeed for DataNode {:?}: {}",
                            datanode,
                            response.message().to_string()
                        );
                    } else {
                        return Ok((connection, response.read_op_checksum_info));
                    }
                }
                Err(e) => warn!("Failed to connect to DataNode {:?}: {:?}", datanode, e),
            }
        }
    }

    async fn next_packet_from_receiver(&mut self) -> Option<Result<(PacketHeaderProto, Bytes)>> {
        #[cfg(feature = "integration-test")]
        if crate::test::DATANODE_READ_FAULT_INJECTOR
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Some(Err(HdfsError::IOError(std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof,
            ))));
        }

        self.receiver.recv().await
    }

    async fn next_packet(&mut self) -> Result<Option<Bytes>> {
        // We've finished this read, just return None
        if self.len == 0 {
            return Ok(None);
        }

        let (header, data) = loop {
            // If we are using an existing listener, we should retry on the same DataNode in case of
            // transient IO errors due to socket timeouts. If this is a new listener, there should be no socket
            // timeout so we should move directly to the next node.
            let retry_connection = if self.listener.is_none() {
                let (connection, checksum_info) = self.select_next_datanode().await?;
                self.listener = Some(Self::start_packet_listener(
                    connection,
                    checksum_info,
                    self.sender.clone(),
                ));
                false
            } else {
                true
            };

            match self.next_packet_from_receiver().await {
                Some(Ok(data)) => break data,
                Some(Err(e)) => {
                    // Some error communicating with datanode, log a warning and then retry on a different Datanode
                    warn!("Error occurred while reading from DataNode: {:?}", e);
                    if retry_connection && matches!(e, HdfsError::IOError(_)) {
                        // Retry transient IO errors on the same DataNode
                        self.next_replica -= 1;
                    }
                    self.listener = None;
                }
                None => {
                    // This means there's a disconnect between the data we are getting back and what we asked for,
                    // so just raise an error
                    return Err(HdfsError::DataTransferError(
                        "Not enough data returned from DataNode".to_string(),
                    ));
                }
            }
        };

        let packet_offset = self.offset.saturating_sub(header.offset_in_block as usize);
        let packet_len = usize::min(header.data_len as usize - packet_offset, self.len);

        self.offset += packet_len;
        self.len -= packet_len;

        // We've consumed the whole read, there should be no more packets and the listener should complete
        if self.len == 0 {
            let conn = self.listener.take().unwrap().await.unwrap()?;
            DATANODE_CACHE.release(conn);
        }

        Ok(Some(
            data.slice(packet_offset..(packet_offset + packet_len)),
        ))
    }

    async fn get_next_packet(
        connection: &mut DatanodeConnection,
        checksum_info: Option<ReadOpChecksumInfoProto>,
    ) -> Result<(PacketHeaderProto, Bytes)> {
        let packet = connection.read_packet().await?;
        let header = packet.header;
        Ok((header, packet.get_data(&checksum_info)?))
    }

    fn start_packet_listener(
        mut connection: DatanodeConnection,
        checksum_info: Option<ReadOpChecksumInfoProto>,
        sender: Sender<Result<(PacketHeaderProto, Bytes)>>,
    ) -> JoinHandle<Result<DatanodeConnection>> {
        tokio::spawn(async move {
            loop {
                let next_packet = Self::get_next_packet(&mut connection, checksum_info).await;
                if next_packet.as_ref().is_ok_and(|(_, data)| data.is_empty()) {
                    // If the packet is empty it means it's the last packet
                    // so tell the DataNode the read was a success and finish this task
                    connection.send_read_success().await?;
                    break;
                }

                if sender.send(next_packet).await.is_err() {
                    // The block reader was dropped, so just kill the listener
                    return Err(HdfsError::DataTransferError(
                        "Reader was dropped without consuming all data".to_string(),
                    ));
                }
            }
            Ok(connection)
        })
    }

    fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        stream::unfold(self, |mut state| async move {
            let next = state.next_packet().await.transpose();
            next.map(|n| (n, state))
        })
    }
}

// Reads cells of data from a DataNode connection
struct CellReader {
    cell_size: usize,
    cell_buffer: BytesMut,
    current_packet: Bytes,
    block_stream: Option<ReplicatedBlockStream>,
}

impl CellReader {
    fn new(cell_size: usize, block_stream: Option<ReplicatedBlockStream>) -> Self {
        Self {
            cell_size,
            cell_buffer: BytesMut::with_capacity(cell_size),
            current_packet: Bytes::new(),
            block_stream,
        }
    }

    async fn next_cell(&mut self) -> Result<Bytes> {
        // We always should be reading a full cell, no current optimizations for a partial cell
        // Only exception is the final cell may be partial
        while self.cell_buffer.len() < self.cell_size {
            if !self.current_packet.has_remaining() {
                if let Some(block_stream) = self.block_stream.as_mut() {
                    match block_stream.next_packet().await? {
                        Some(next_packet) => self.current_packet = next_packet,
                        None => {
                            break;
                        }
                    }
                } else {
                    // At the end of a block, just return all 0s
                    break;
                }
            }

            let bytes_to_copy = usize::min(
                self.cell_size - self.cell_buffer.len(),
                self.current_packet.remaining(),
            );

            self.cell_buffer
                .put(self.current_packet.split_to(bytes_to_copy));
        }

        // Pad a partial final cell with zeros
        self.cell_buffer.resize(self.cell_size, 0);

        Ok(std::mem::replace(
            &mut self.cell_buffer,
            BytesMut::with_capacity(self.cell_size),
        )
        .freeze())
    }
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
/// and one for cell_2 and cell_5 from blk_2. We read each block a cell at a time, and return the horizontal
/// stripe of cells when we have all cells in a row. So first the first call to read_slice returns cells
/// cell_0, cell_1, and cell_2, and the second returns cell_3, cell_4, and cell_5. If any cell fails to read
/// we construct a read of a parity block for the remaining horizontal slices we still need to process. If
/// more reads fail than we have parity rows, the entire read fails.
///
/// In the future we can look at making this more efficient by not reading as many extra cells that aren't
/// part of the range being requested at all. Currently the overhead of not doing this would be up to
/// `data_units * cell_size * 2` of extra data being read from disk (basically two extra "rows" of data).
struct StripedBlockStream {
    protocol: Arc<NamenodeProtocol>,
    block: LocatedBlockProto,
    block_map: HashMap<usize, (hdfs::DatanodeInfoProto, common::TokenProto)>,
    remaining: usize,
    bytes_to_skip: usize,
    ec_schema: EcSchema,
    // Position of the start of the current cell in a single block
    current_block_start: usize,
    // End location in a single block we need to read
    block_end: usize,
    cell_readers: Vec<Option<CellReader>>,
}

impl StripedBlockStream {
    fn new(
        protocol: Arc<NamenodeProtocol>,
        block: hdfs::LocatedBlockProto,
        offset: usize,
        len: usize,
        ec_schema: EcSchema,
    ) -> Self {
        assert_eq!(block.block_indices().len(), block.locs.len());

        // Cell IDs for the range we are reading, inclusive
        let starting_cell = ec_schema.cell_for_offset(offset);
        let ending_cell = ec_schema.cell_for_offset(offset + len - 1);

        // Logical rows or horizontal stripes we need to read, tail-exclusive
        let starting_row = ec_schema.row_for_cell(starting_cell);
        let ending_row = ec_schema.row_for_cell(ending_cell) + 1;
        let block_end = ec_schema.offset_for_row(ending_row);

        let current_block_start = ec_schema.offset_for_row(starting_row);

        let bytes_to_skip = offset - starting_row * ec_schema.data_units * ec_schema.cell_size;

        let datanode_infos: Vec<(hdfs::DatanodeInfoProto, common::TokenProto)> = block
            .locs
            .iter()
            .cloned()
            .zip(block.block_tokens.iter().cloned())
            .collect();

        let block_map: HashMap<usize, (hdfs::DatanodeInfoProto, common::TokenProto)> = block
            .block_indices()
            .iter()
            .copied()
            .map(|i| i as usize)
            .zip(datanode_infos)
            .collect();

        Self {
            protocol,
            block,
            block_map,
            remaining: len,
            bytes_to_skip,
            ec_schema,
            current_block_start,
            block_end,
            cell_readers: vec![],
        }
    }

    // Reads the next slice of cells and decodes if necessary
    async fn read_slice(&mut self) -> Result<Option<VecDeque<Bytes>>> {
        if self.remaining == 0 {
            return Ok(None);
        }

        // Check if we need to start any new reads
        let mut good_blocks = self.cell_readers.iter().filter(|r| r.is_some()).count();
        while good_blocks < self.ec_schema.data_units {
            if self.start_next_reader().await? {
                good_blocks += 1;
            }
        }

        let mut slice = vec![None; self.ec_schema.data_units + self.ec_schema.parity_units];
        let mut good_cells = 0;
        let mut block_index = 0;
        while good_cells < self.ec_schema.data_units {
            if block_index >= self.cell_readers.len() {
                // Need to start reading from the next parity
                if !self.start_next_reader().await? {
                    block_index += 1;
                    continue;
                }
            }
            if let Some(reader) = self.cell_readers[block_index].as_mut() {
                match reader.next_cell().await {
                    Ok(bytes) => {
                        slice[block_index] = Some(bytes);
                        good_cells += 1;
                    }
                    Err(e) => {
                        warn!(
                            "Error reading erasure coded block, trying next replica: {:?}",
                            e
                        );
                        self.cell_readers[block_index] = None;
                    }
                }
            }
            block_index += 1;
        }

        let mut decoded = VecDeque::from(self.ec_schema.ec_decode(slice)?);

        // Skip any bytes at the beginning
        while self.bytes_to_skip > 0 {
            if decoded[0].len() < self.bytes_to_skip {
                self.bytes_to_skip -= decoded.pop_front().unwrap().len();
            } else {
                let _ = decoded[0].split_to(self.bytes_to_skip);
                self.bytes_to_skip = 0;
            }
        }

        let total_size: usize = decoded.iter().map(|bytes| bytes.len()).sum();
        if total_size > self.remaining {
            let mut bytes_to_trim = total_size - self.remaining;
            while bytes_to_trim > 0 {
                if decoded.back().unwrap().len() <= bytes_to_trim {
                    bytes_to_trim -= decoded.pop_back().unwrap().len();
                } else {
                    let last_cell = decoded.back_mut().unwrap();
                    let _ = last_cell.split_off(last_cell.len() - bytes_to_trim);
                    bytes_to_trim = 0;
                }
            }
            self.remaining = 0;
        } else {
            self.remaining -= total_size;
        }

        self.current_block_start += self.ec_schema.cell_size;

        Ok(Some(decoded))
    }

    async fn start_next_reader(&mut self) -> Result<bool> {
        if self.cell_readers.len() >= self.ec_schema.data_units + self.ec_schema.parity_units {
            return Err(HdfsError::ErasureCodingError(
                "Not enough valid shards".to_string(),
            ));
        }

        let index = self.cell_readers.len();

        #[cfg(feature = "integration-test")]
        if let Some(fault_injection) = crate::test::EC_FAULT_INJECTOR.lock().unwrap().as_ref() {
            if fault_injection.fail_blocks.contains(&index) {
                debug!("Failing block read for {}", index);
                self.cell_readers.push(None);
                return Ok(false);
            }
        }

        let max_block_offset = self
            .ec_schema
            .max_offset(index, self.block.b.num_bytes() as usize);

        let end = usize::min(self.block_end, max_block_offset);
        let len = end - self.current_block_start;

        // Three cases we need to worry about
        // 1. The length to read is 0, which means we are reading the last slice and this index
        //    is past the end of the data. Just create a reader that returns all 0s, regardless
        //    if the block exists or not.
        // 2. We have the DataNode info, so start reading from it
        // 3. We don't have the DataNode info, so this shard isn't valid
        if len == 0 {
            self.cell_readers
                .push(Some(CellReader::new(self.ec_schema.cell_size, None)));
            Ok(true)
        } else if let Some((datanode_info, token)) = self.block_map.get(&index) {
            let mut block = self.block.clone();

            // Each vertical stripe has a block ID of the original located block ID + block index
            // That was fun to figure out
            block.b.block_id += index as u64;
            block.locs = vec![datanode_info.clone()];
            block.block_token = token.clone();

            let block_stream = ReplicatedBlockStream::new(
                Arc::clone(&self.protocol),
                block,
                self.current_block_start,
                len,
            );

            self.cell_readers.push(Some(CellReader::new(
                self.ec_schema.cell_size,
                Some(block_stream),
            )));

            Ok(true)
        } else {
            self.cell_readers.push(None);
            Ok(false)
        }
    }

    fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        stream::unfold(
            (self, VecDeque::new()),
            |(mut stream, mut buffers)| async move {
                if buffers.is_empty() {
                    match stream.read_slice().await {
                        Ok(Some(next_buffers)) => {
                            buffers = next_buffers;
                        }
                        Ok(None) => {
                            return None;
                        }
                        Err(e) => {
                            return Some((Err(e), (stream, buffers)));
                        }
                    }
                }

                buffers.pop_front().map(|b| (Ok(b), (stream, buffers)))
            },
        )
    }
}
