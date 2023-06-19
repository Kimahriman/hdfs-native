use std::io::Result;

use bytes::BufMut;

use crate::{
    connection::{DatanodeConnection, Op},
    proto::hdfs,
};

#[derive(Debug)]
pub struct BlockReader {
    block: hdfs::LocatedBlockProto,
    offset: usize,
    len: usize,
    current_datanode: Option<DatanodeConnection>,
}

impl BlockReader {
    pub fn new(block: hdfs::LocatedBlockProto, offset: usize, len: usize) -> Self {
        assert!(len > 0);
        BlockReader {
            block,
            offset,
            len,
            current_datanode: None,
        }
    }

    fn choose_datanode(&mut self) -> Result<()> {
        let datanode = &self.block.locs.first().unwrap().id;
        let conn =
            DatanodeConnection::connect(format!("{}:{}", datanode.ip_addr, datanode.xfer_port))?;
        self.current_datanode = Some(conn);
        Ok(())
    }

    pub fn read(&mut self, buf: &mut impl BufMut) -> Result<()> {
        if self.current_datanode.is_none() {
            self.choose_datanode()?;
        }

        let conn = self.current_datanode.as_mut().unwrap();

        let mut message = hdfs::OpReadBlockProto::default();
        message.header = conn.build_header(&self.block.b, Some(self.block.block_token.clone()));
        message.offset = self.offset as u64;
        message.len = self.len as u64;
        message.send_checksums = Some(false);

        conn.send(Op::ReadBlock, &message)?;
        let response = conn.read_block_op_response()?;
        println!("{:?}", response);

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
