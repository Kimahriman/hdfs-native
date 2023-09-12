use bytes::{BufMut, Bytes};
use log::{debug, error, warn};
use tokio::sync::{mpsc, oneshot};

use crate::{
    hdfs::connection::{DatanodeConnection, Op},
    proto::hdfs,
    HdfsError, Result,
};

use super::connection::Packet;

const HEART_BEAT_SEQNO: i64 = -1;
const UNKNOWN_SEQNO: i64 = -1;

const RS_CODEC_NAME: &str = "rs";
const RS_LEGACY_CODEC_NAME: &str = "rs-legacy";
const XOR_CODEC_NAME: &str = "xor";
const DEFAULT_EC_CELL_SIZE: u32 = 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct EcSchema {
    pub codec_name: String,
    pub data_units: u32,
    pub parity_units: u32,
    pub cell_size: u32,
}

impl EcSchema {
    /// Returns the cell number (0-based) containing the offset
    fn cell_for_offset(&self, offset: usize) -> usize {
        offset / self.cell_size as usize
    }

    fn row_for_cell(&self, cell_id: usize) -> usize {
        cell_id / self.data_units as usize
    }

    fn offset_for_row(&self, row_id: usize) -> usize {
        row_id * self.cell_size as usize
    }
}

/// Hadoop hard codes a default list of EC policies by ID
pub(crate) fn resolve_ec_policy(policy: &hdfs::ErasureCodingPolicyProto) -> Result<EcSchema> {
    if let Some(schema) = policy.schema.as_ref() {
        return Ok(EcSchema {
            codec_name: schema.codec_name.clone(),
            data_units: schema.data_units,
            parity_units: schema.parity_units,
            cell_size: policy.cell_size(),
        });
    }

    match policy.id {
        // RS-6-3-1024k
        1 => Ok(EcSchema {
            codec_name: RS_CODEC_NAME.to_string(),
            data_units: 6,
            parity_units: 3,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        // RS-3-2-1024k
        2 => Ok(EcSchema {
            codec_name: RS_CODEC_NAME.to_string(),
            data_units: 3,
            parity_units: 2,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        // RS-6-3-1024k
        3 => Ok(EcSchema {
            codec_name: RS_LEGACY_CODEC_NAME.to_string(),
            data_units: 6,
            parity_units: 3,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        // XOR-2-1-1024k
        4 => Ok(EcSchema {
            codec_name: XOR_CODEC_NAME.to_string(),
            data_units: 2,
            parity_units: 1,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        // RS-10-4-1024k
        5 => Ok(EcSchema {
            codec_name: RS_CODEC_NAME.to_string(),
            data_units: 10,
            parity_units: 4,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        _ => Err(HdfsError::UnsupportedErasureCodingPolicy(format!(
            "ID: {}",
            policy.id
        ))),
    }
}

#[derive(Debug)]
pub(crate) struct BlockReader {
    block: hdfs::LocatedBlockProto,
    ec_schema: Option<EcSchema>,
    offset: usize,
    pub(crate) len: usize,
}

impl BlockReader {
    pub fn new(
        block: hdfs::LocatedBlockProto,
        ec_schema: Option<EcSchema>,
        offset: usize,
        len: usize,
    ) -> Self {
        assert!(len > 0);
        BlockReader {
            block,
            ec_schema,
            offset,
            len,
        }
    }

    /// Select a best order to try the datanodes in. For now just use the order we
    /// got them in. In the future we could consider things like locality, storage type, etc.
    fn choose_datanodes(&self) -> Vec<&hdfs::DatanodeIdProto> {
        self.block.locs.iter().map(|l| &l.id).collect()
    }

    pub(crate) async fn read(&self, buf: &mut [u8]) -> Result<()> {
        assert!(buf.len() == self.len);
        if let Some(ec_schema) = self.ec_schema.as_ref() {
            self.read_striped(ec_schema, buf).await
        } else {
            self.read_replicated(buf).await
        }
    }

    async fn read_replicated(&self, buf: &mut [u8]) -> Result<()> {
        let datanodes = self.choose_datanodes();
        let mut index = 0;
        loop {
            let result = self
                .read_from_datanode(datanodes[index], self.offset, self.len, buf)
                .await;
            if result.is_ok() || index >= datanodes.len() - 1 {
                return Ok(result?);
            } else {
                warn!("{:?}", result.unwrap_err());
            }
            index += 1;
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
    ///
    /// When calculating cells from offsets, we use inclusive values to make sure we don't request extra data.
    async fn read_striped(&self, ec_schema: &EcSchema, buf: &mut [u8]) -> Result<()> {
        // Cell IDs for the range we are reading
        let starting_cell = ec_schema.cell_for_offset(self.offset);
        let ending_cell = ec_schema.cell_for_offset(self.offset + self.len - 1);

        // Logical rows or horizontal stripes we need to read
        let starting_row = ec_schema.row_for_cell(starting_cell);
        let ending_row = ec_schema.row_for_cell(ending_cell);

        let block_start = ec_schema.offset_for_row(starting_row);
        let block_end = ec_schema.offset_for_row(ending_row);

        Ok(())
    }

    async fn read_from_datanode(
        &self,
        datanode: &hdfs::DatanodeIdProto,
        offset: usize,
        len: usize,
        mut buf: &mut [u8],
    ) -> Result<()> {
        let mut conn =
            DatanodeConnection::connect(&format!("{}:{}", datanode.ip_addr, datanode.xfer_port))
                .await?;

        let mut message = hdfs::OpReadBlockProto::default();
        message.header = conn.build_header(&self.block.b, Some(self.block.block_token.clone()));
        message.offset = offset as u64;
        message.len = len as u64;
        message.send_checksums = Some(false);

        conn.send(Op::ReadBlock, &message).await?;
        let response = conn.read_block_op_response().await?;
        debug!("Block read op response {:?}", response);

        // First handle the offset into the first packet
        let mut packet = conn.read_packet().await?;
        let packet_offset = offset - packet.header.offset_in_block as usize;
        let data_len = packet.header.data_len as usize - packet_offset;
        let data_to_read = usize::min(data_len, len);
        let mut data_left = len - data_to_read;
        buf.put(
            packet
                .get_data()
                .slice(packet_offset..(packet_offset + data_to_read)),
        );

        while data_left > 0 {
            packet = conn.read_packet().await?;
            // TODO: Error checking
            let data_to_read = usize::min(data_left, packet.header.data_len as usize);
            buf.put(packet.get_data().slice(0..data_to_read));
            data_left -= data_to_read;
        }

        // There should be one last empty packet after we are done
        conn.read_packet().await?;

        Ok(())
    }
}

pub(crate) struct BlockWriter {
    block: hdfs::LocatedBlockProto,
    block_size: usize,
    server_defaults: hdfs::FsServerDefaultsProto,

    bytes_written: usize,
    next_seqno: i64,
    connection: DatanodeConnection,
    current_packet: Packet,

    // Tracks the state of acknowledgements. Set to an Err if any error occurs doing receiving
    // acknowledgements. Set to Ok(()) when the last acknowledgement is received.
    status: Option<oneshot::Receiver<Result<()>>>,
    ack_queue: mpsc::Sender<(i64, bool)>,
}

impl BlockWriter {
    pub(crate) async fn new(
        block: hdfs::LocatedBlockProto,
        block_size: usize,
        server_defaults: hdfs::FsServerDefaultsProto,
    ) -> Result<Self> {
        let datanode = &block.locs[0].id;
        let mut connection =
            DatanodeConnection::connect(&format!("{}:{}", datanode.ip_addr, datanode.xfer_port))
                .await?;

        let mut message = hdfs::OpWriteBlockProto::default();
        message.header = connection.build_header(&block.b, Some(block.block_token.clone()));
        message.stage =
            hdfs::op_write_block_proto::BlockConstructionStage::PipelineSetupCreate as i32;
        message.targets = block.locs[1..].to_vec();
        message.pipeline_size = block.locs.len() as u32;
        message.latest_generation_stamp = 0; //block.b.generation_stamp;

        let mut checksum = hdfs::ChecksumProto::default();
        checksum.r#type = hdfs::ChecksumTypeProto::ChecksumCrc32c as i32;
        checksum.bytes_per_checksum = server_defaults.bytes_per_checksum;
        message.requested_checksum = checksum;

        message.storage_type = Some(block.storage_types[0].clone());
        message.target_storage_types = block.storage_types[1..].to_vec();
        message.storage_id = Some(block.storage_i_ds[0].clone());
        message.target_storage_ids = block.storage_i_ds[1..].to_vec();

        debug!("sending write block message: {:?}", &message);

        connection.send(Op::WriteBlock, &message).await?;
        let response = connection.read_block_op_response().await?;
        debug!("block write response: {:?}", response);

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

        let this = Self {
            block,
            block_size,
            server_defaults,
            bytes_written: 0,
            next_seqno: 1,
            connection,
            current_packet: Packet::empty(0, 0, bytes_per_checksum, write_packet_size),
            status: Some(status_receiver),
            ack_queue: ack_queue_sender,
        };
        this.listen_for_acks(ack_response_receiver, ack_queue_receiever, status_sender);

        Ok(this)
    }

    pub(crate) fn get_extended_block(&self) -> hdfs::ExtendedBlockProto {
        self.block.b.clone()
    }

    pub(crate) fn is_full(&self) -> bool {
        self.bytes_written == self.block_size
    }

    fn create_next_packet(&mut self) {
        self.current_packet = Packet::empty(
            self.bytes_written as i64,
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

    pub(crate) async fn write(&mut self, buf: &mut Bytes) -> Result<()> {
        self.check_error()?;

        // Only write up to what's left in this block
        let bytes_to_write = usize::min(buf.len(), self.block_size - self.bytes_written);
        let mut buf_to_write = buf.split_to(bytes_to_write);

        while !buf_to_write.is_empty() {
            let initial_buf_len = buf_to_write.len();
            self.current_packet.write(&mut buf_to_write);

            // Track how many bytes are written to this block
            self.bytes_written += initial_buf_len - buf_to_write.len();

            if self.current_packet.is_full() {
                self.send_current_packet().await?;
            }
        }
        Ok(())
    }

    /// Send a packet with any remaining data and then send a last packet
    pub(crate) async fn close(&mut self) -> Result<()> {
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
            let _ = result?;
        } else {
            return Err(HdfsError::DataTransferError(
                "Block already closed".to_string(),
            ));
        }

        // Update the block size in the ExtendedBlockProto used for communicate the status with the namenode
        self.block.b.num_bytes = Some(self.bytes_written as u64);

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
}
