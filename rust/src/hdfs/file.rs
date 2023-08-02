use bytes::{Bytes, BytesMut};
use futures::future::join_all;
use log::debug;

use crate::proto::hdfs;
use crate::Result;

use super::datanode::BlockReader;

pub struct HdfsFileReader {
    located_blocks: hdfs::LocatedBlocksProto,
    position: usize,
}

impl HdfsFileReader {
    pub(crate) fn new(located_blocks: hdfs::LocatedBlocksProto) -> Self {
        HdfsFileReader {
            located_blocks,
            position: 0,
        }
    }

    pub fn file_length(&self) -> usize {
        self.located_blocks.file_length as usize
    }

    /// Read up to `len` bytes into a new [Bytes] object, advancing the internal position in the file.
    /// An empty [Bytes] object will be returned if the end of the file has been reached.
    pub async fn read(&mut self, len: usize) -> Result<Bytes> {
        if self.position >= self.file_length() {
            Ok(Bytes::new())
        } else {
            let offset = self.position;
            self.position = usize::min(self.position + len, self.file_length());
            self.read_range(offset, self.position - offset as usize)
                .await
        }
    }

    /// Read up to `buf.len()` bytes into the provided slice, advancing the internal position in the file.
    /// Returns the number of bytes that were read, or 0 if the end of the file has been reached.
    pub async fn read_buf(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.position >= self.file_length() {
            Ok(0)
        } else {
            let offset = self.position;
            self.position = usize::min(self.position + buf.len(), self.file_length());
            let read_bytes = self.position - offset;
            self.read_range_buf(buf, offset).await?;
            Ok(read_bytes)
        }
    }

    /// Read up to `len` bytes starting at `offset` into a new [Bytes] object. The returned buffer
    /// could be smaller than `len` if `offset + len` extends beyond the end of the file.
    pub async fn read_range(&self, offset: usize, len: usize) -> Result<Bytes> {
        let end = usize::min(self.file_length(), offset + len);
        assert!(offset <= end);
        let buf_size = end - offset;
        let mut buf = BytesMut::zeroed(buf_size);
        self.read_range_buf(&mut buf, offset).await?;
        Ok(buf.freeze())
    }

    /// Read file data into an existing buffer. Buffer will be extended by the length of the file.
    pub async fn read_range_buf(&self, buf: &mut [u8], offset: usize) -> Result<()> {
        let block_readers = self.create_block_readers(offset, buf.len());

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
