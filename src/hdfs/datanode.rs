use bytes::BufMut;
use log::{debug, warn};

use crate::{
    connection::{DatanodeConnection, Op},
    proto::hdfs,
    Result,
};

#[derive(Debug)]
pub(crate) struct BlockReader {
    block: hdfs::LocatedBlockProto,
    offset: usize,
    len: usize,
}

impl BlockReader {
    pub fn new(block: hdfs::LocatedBlockProto, offset: usize, len: usize) -> Self {
        assert!(len > 0);
        BlockReader { block, offset, len }
    }

    /// Select a best order to try the datanodes in
    fn choose_datanodes(&self) -> Vec<&hdfs::DatanodeIdProto> {
        self.block.locs.iter().map(|l| &l.id).collect()
    }

    pub(crate) fn read(&self, buf: &mut impl BufMut) -> Result<()> {
        let datanodes = self.choose_datanodes();
        let mut index = 0;
        loop {
            let result = self.read_from_datanode(datanodes[index], buf);
            if result.is_ok() || index >= datanodes.len() - 1 {
                return Ok(result?);
            } else {
                warn!("{}", result.unwrap_err());
            }
            index += 1;
        }
    }

    fn read_from_datanode(
        &self,
        datanode: &hdfs::DatanodeIdProto,
        buf: &mut impl BufMut,
    ) -> Result<()> {
        let mut conn =
            DatanodeConnection::connect(format!("{}:{}", datanode.ip_addr, datanode.xfer_port))?;

        let mut message = hdfs::OpReadBlockProto::default();
        message.header = conn.build_header(&self.block.b, Some(self.block.block_token.clone()));
        message.offset = self.offset as u64;
        message.len = self.len as u64;
        message.send_checksums = Some(false);

        conn.send(Op::ReadBlock, &message)?;
        let response = conn.read_block_op_response()?;
        debug!("{:?}", response);

        // First handle the offset into the first packet
        let mut packet = conn.read_packet()?;
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
            packet = conn.read_packet()?;
            // TODO: Error checking
            let data_to_read = usize::min(data_left, packet.header.data_len as usize);
            buf.put(packet.data.slice(0..data_to_read));
            data_left -= data_to_read;
        }

        // There should be one last empty packet after we are done
        conn.read_packet()?;

        Ok(())
    }
}
