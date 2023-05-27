use std::io::Result;

use bytes::{BufMut, Bytes, BytesMut};

use crate::proto::hdfs;

use super::datanode::BlockReader;

#[derive(Debug)]
pub struct HdfsFileReader {
    located_blocks: hdfs::LocatedBlocksProto,
}

impl HdfsFileReader {
    pub fn new(located_blocks: hdfs::LocatedBlocksProto) -> Self {
        HdfsFileReader { located_blocks }
    }

    pub fn read(&self, offset: usize, len: usize) -> std::io::Result<Bytes> {
        let mut buf = BytesMut::with_capacity(len);
        self.read_buf(&mut buf, offset, len)?;
        Ok(buf.freeze())
    }

    /// Read file data into an existing buffer. Buffer will be extended by the length of the file.
    pub fn read_buf(&self, buf: &mut impl BufMut, offset: usize, len: usize) -> Result<()> {
        let mut block_readers = self.create_block_readers(offset, len);
        for reader in block_readers.iter_mut() {
            reader.read(buf)?;
        }

        Ok(())
    }

    fn create_block_readers(&self, offset: usize, len: usize) -> Vec<BlockReader> {
        self.located_blocks
            .blocks
            .iter()
            .flat_map(|block| {
                let block_file_start = block.offset as usize;
                let block_file_end = block_file_start + block.b.num_bytes.unwrap() as usize;

                if block_file_start <= (offset + len) && block_file_end >= offset {
                    // We need to read this block
                    let block_start = offset - block_file_start;
                    let block_end = usize::min(offset + len, block_file_end) - block_file_start;
                    Some(BlockReader::new(
                        block.clone(),
                        block_start,
                        block_end - block_start,
                    ))
                } else {
                    // No data is needed from this block
                    None
                }
            })
            .collect()
    }
}
