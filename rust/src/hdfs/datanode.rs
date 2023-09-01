use bytes::BufMut;
use log::{debug, warn};

use crate::{
    hdfs::connection::{DatanodeConnection, Op},
    proto::hdfs,
    HdfsError, Result,
};

#[derive(Debug)]
pub(crate) struct BlockReader {
    block: hdfs::LocatedBlockProto,
    offset: usize,
    pub(crate) len: usize,
}

impl BlockReader {
    pub fn new(block: hdfs::LocatedBlockProto, offset: usize, len: usize) -> Self {
        assert!(len > 0);
        BlockReader { block, offset, len }
    }

    /// Select a best order to try the datanodes in. For now just use the order we
    /// got them in. In the future we could consider things like locality, storage type, etc.
    fn choose_datanodes(&self) -> Vec<&hdfs::DatanodeIdProto> {
        self.block.locs.iter().map(|l| &l.id).collect()
    }

    pub(crate) async fn read(&self, buf: &mut [u8]) -> Result<()> {
        assert!(buf.len() == self.len);
        let datanodes = self.choose_datanodes();
        let mut index = 0;
        loop {
            let result = self.read_from_datanode(datanodes[index], buf).await;
            if result.is_ok() || index >= datanodes.len() - 1 {
                return Ok(result?);
            } else {
                warn!("{:?}", result.unwrap_err());
            }
            index += 1;
        }
    }

    async fn read_from_datanode(
        &self,
        datanode: &hdfs::DatanodeIdProto,
        mut buf: &mut [u8],
    ) -> Result<()> {
        let mut conn =
            DatanodeConnection::connect(format!("{}:{}", datanode.ip_addr, datanode.xfer_port))
                .await?;

        let mut message = hdfs::OpReadBlockProto::default();
        message.header = conn.build_header(&self.block.b, Some(self.block.block_token.clone()));
        message.offset = self.offset as u64;
        message.len = self.len as u64;
        message.send_checksums = Some(false);

        conn.send(Op::ReadBlock, &message).await?;
        let response = conn.read_block_op_response().await?;
        debug!("Block read op response {:?}", response);

        // First handle the offset into the first packet
        let mut packet = conn.read_packet().await?;
        let packet_offset = self.offset - packet.header.offset_in_block as usize;
        let data_len = packet.header.data_len as usize - packet_offset;
        let data_to_read = usize::min(data_len, self.len);
        let mut data_left = self.len - data_to_read;
        buf.put(
            packet
                .data
                .slice(packet_offset..(packet_offset + data_to_read)),
        );

        while data_left > 0 {
            packet = conn.read_packet().await?;
            // TODO: Error checking
            let data_to_read = usize::min(data_left, packet.header.data_len as usize);
            buf.put(packet.data.slice(0..data_to_read));
            data_left -= data_to_read;
        }

        // There should be one last empty packet after we are done
        conn.read_packet().await?;

        Ok(())
    }
}

pub(crate) struct BlockWriter {
    block: hdfs::LocatedBlockProto,
    pub(crate) bytes_remaining: usize,
    connection: DatanodeConnection,
}

impl BlockWriter {
    pub(crate) async fn new(block: hdfs::LocatedBlockProto, block_size: usize) -> Result<Self> {
        let datanode = &block.locs[0].id;
        let mut connection =
            DatanodeConnection::connect(format!("{}:{}", datanode.ip_addr, datanode.xfer_port))
                .await?;

        let mut message = hdfs::OpWriteBlockProto::default();
        message.header = connection.build_header(&block.b, Some(block.block_token.clone()));
        message.stage =
            hdfs::op_write_block_proto::BlockConstructionStage::PipelineSetupCreate as i32;
        message.targets = block.locs.clone();
        message.pipeline_size = block.locs.len() as u32;
        message.latest_generation_stamp = block.b.generation_stamp;
        message.storage_type = Some(block.storage_types[0].clone());
        message.target_storage_types = block.storage_types.clone();
        message.storage_id = Some(block.storage_i_ds[0].clone());
        message.target_storage_ids = block.storage_i_ds.clone();

        connection.send(Op::WriteBlock, &message).await?;
        let response = connection.read_block_op_response().await?;
        debug!("{:?}", response);

        Ok(Self {
            block,
            bytes_remaining: block_size,
            connection,
        })
    }

    pub(crate) fn get_extended_block(&self) -> hdfs::ExtendedBlockProto {
        self.block.b.clone()
    }

    pub(crate) async fn write(&mut self, buf: &[u8]) -> Result<()> {
        if buf.len() > self.bytes_remaining {
            return Err(HdfsError::InternalError(
                "Tried to write more bytes to block than the block size".to_string(),
            ));
        }

        Ok(())
    }
}
