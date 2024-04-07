use std::{collections::HashMap, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{
    future::join_all,
    stream::{self, BoxStream},
    Stream, StreamExt,
};
use log::{debug, warn};

use crate::{
    ec::EcSchema,
    hdfs::connection::{DatanodeConnection, Op, DATANODE_CACHE},
    proto::{
        common,
        hdfs::{self, BlockOpResponseProto},
    },
    HdfsError, Result,
};

use super::protocol::NamenodeProtocol;

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

    connection: Option<DatanodeConnection>,
    checksum_info: Option<hdfs::ReadOpChecksumInfoProto>,
    current_replica: usize,
}

impl ReplicatedBlockStream {
    fn new(
        protocol: Arc<NamenodeProtocol>,
        block: hdfs::LocatedBlockProto,
        offset: usize,
        len: usize,
    ) -> Self {
        Self {
            protocol,
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

        let (connection, response) = connect_and_send(
            &self.protocol,
            datanode,
            &self.block.b,
            self.block.block_token.clone(),
            self.offset as u64,
            self.len as u64,
        )
        .await?;

        if response.status() != hdfs::Status::Success {
            return Err(HdfsError::DataTransferError(response.message().to_string()));
        }

        self.connection = Some(connection);
        self.checksum_info = response.read_op_checksum_info;

        Ok(())
    }

    async fn next_packet(&mut self) -> Result<Option<Bytes>> {
        if self.connection.is_none() {
            self.select_next_datanode().await?;
        }

        if self.len == 0 {
            let mut conn = self.connection.take().unwrap();

            // Read the final empty packet
            conn.read_packet().await?;

            conn.send_read_success().await?;
            DATANODE_CACHE.release(conn);
            return Ok(None);
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
    protocol: Arc<NamenodeProtocol>,
    block: hdfs::LocatedBlockProto,
    offset: usize,
    len: usize,
    ec_schema: EcSchema,
}

impl StripedBlockStream {
    fn new(
        protocol: Arc<NamenodeProtocol>,
        block: hdfs::LocatedBlockProto,
        offset: usize,
        len: usize,
        ec_schema: EcSchema,
    ) -> Self {
        Self {
            protocol,
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
        let datanode_infos: Vec<(&hdfs::DatanodeInfoProto, &common::TokenProto)> = self
            .block
            .locs
            .iter()
            .zip(self.block.block_tokens.iter())
            .collect();

        let block_map: HashMap<u8, (&hdfs::DatanodeInfoProto, &common::TokenProto)> = self
            .block
            .block_indices()
            .iter()
            .copied()
            .zip(datanode_infos.into_iter())
            .collect();

        let mut stripe_results: Vec<Option<Bytes>> =
            vec![None; self.ec_schema.data_units + self.ec_schema.parity_units];

        let mut futures = Vec::new();

        for index in 0..self.ec_schema.data_units as u8 {
            let datanode_info = block_map.get(&index);
            futures.push(self.read_vertical_stripe(
                &self.ec_schema,
                index,
                datanode_info.map(|(datanode, _)| datanode),
                datanode_info.map(|(_, token)| token),
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
            let datanode_info = block_map.get(&block_index);
            let result = self
                .read_vertical_stripe(
                    &self.ec_schema,
                    block_index,
                    datanode_info.map(|(datanode, _)| datanode),
                    datanode_info.map(|(_, token)| token),
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
        token: Option<&&common::TokenProto>,
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
        let max_block_offset =
            ec_schema.max_offset(index as usize, self.block.b.num_bytes() as usize);

        let read_len = usize::min(len, max_block_offset - offset);

        if read_len == 0 {
            // We're past the end of the file so there's nothign to read, just return a zeroed buffer
            Ok(BytesMut::zeroed(len).freeze())
        } else if let Some((datanode_info, token)) = datanode.zip(token) {
            let mut buf = BytesMut::zeroed(len);

            // Each vertical stripe has a block ID of the original located block ID + block index
            // That was fun to figure out
            let mut block = self.block.b.clone();
            block.block_id += index as u64;

            self.read_from_datanode(&datanode_info.id, &block, token, offset, read_len, &mut buf)
                .await?;

            Ok(buf.freeze())
        } else {
            // There should be data to read but we didn't get block info for this index, so this shard is missing
            Err(HdfsError::ErasureCodingError(
                "Shard is missing".to_string(),
            ))
        }
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

        let (mut connection, response) = connect_and_send(
            &self.protocol,
            datanode,
            block,
            token.clone(),
            offset as u64,
            len as u64,
        )
        .await?;

        if response.status() != hdfs::Status::Success {
            return Err(HdfsError::DataTransferError(response.message().to_string()));
        }

        // First handle the offset into the first packet
        let mut packet = connection.read_packet().await?;
        let packet_offset = offset - packet.header.offset_in_block as usize;
        let data_len = packet.header.data_len as usize - packet_offset;
        let data_to_read = usize::min(data_len, len);
        let mut data_left = len - data_to_read;

        let packet_data = packet.get_data(&response.read_op_checksum_info)?;
        buf.put(packet_data.slice(packet_offset..(packet_offset + data_to_read)));

        while data_left > 0 {
            packet = connection.read_packet().await?;
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
        connection.read_packet().await?;
        connection.send_read_success().await?;
        DATANODE_CACHE.release(connection);

        Ok(())
    }
}
