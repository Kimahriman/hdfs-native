use bytes::{Bytes, BytesMut};
use futures::future::join_all;
use log::debug;

use crate::proto::hdfs;
use crate::Result;

use super::datanode::BlockReader;

#[derive(Debug)]
pub struct HdfsFileReader {
    located_blocks: hdfs::LocatedBlocksProto,
}

impl HdfsFileReader {
    pub(crate) fn new(located_blocks: hdfs::LocatedBlocksProto) -> Self {
        HdfsFileReader { located_blocks }
    }

    pub async fn read(&self) -> Result<Bytes> {
        self.read_range(0, self.located_blocks.file_length as usize)
            .await
    }

    pub async fn read_range(&self, offset: usize, len: usize) -> Result<Bytes> {
        let mut buf = BytesMut::zeroed(len);
        self.read_buf(&mut buf, offset, len).await?;
        Ok(buf.freeze())
    }

    /// Read file data into an existing buffer. Buffer will be extended by the length of the file.
    pub async fn read_buf(&self, buf: &mut [u8], offset: usize, len: usize) -> Result<()> {
        assert!(buf.len() >= len);
        let block_readers = self.create_block_readers(offset, len);

        let mut futures = Vec::new();

        let mut remaining = buf;

        for reader in block_readers.iter() {
            debug!("Block reader: {:?}", reader);
            let (left, right) = remaining.split_at_mut(reader.len);
            futures.push(reader.read(left));
            remaining = right;
        }

        for future in join_all(futures).await.into_iter() {
            future?;
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

                if block_file_start <= (offset + len) && block_file_end > offset {
                    // We need to read this block
                    let block_start = offset - usize::min(offset, block_file_start);
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
