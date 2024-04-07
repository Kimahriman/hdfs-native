use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{stream, Stream, StreamExt};
use log::warn;

use crate::ec::{resolve_ec_policy, EcSchema};
use crate::hdfs::block_reader::get_block_stream;
use crate::hdfs::block_writer::BlockWriter;
use crate::hdfs::protocol::{LeaseTracker, NamenodeProtocol};
use crate::proto::hdfs;
use crate::{HdfsError, Result};

const COMPLETE_RETRY_DELAY_MS: u64 = 500;
const COMPLETE_RETRIES: u32 = 5;

pub struct FileReader {
    protocol: Arc<NamenodeProtocol>,
    status: hdfs::HdfsFileStatusProto,
    located_blocks: hdfs::LocatedBlocksProto,
    ec_schema: Option<EcSchema>,
    position: usize,
}

impl FileReader {
    pub(crate) fn new(
        protocol: Arc<NamenodeProtocol>,
        status: hdfs::HdfsFileStatusProto,
        located_blocks: hdfs::LocatedBlocksProto,
        ec_schema: Option<EcSchema>,
    ) -> Self {
        Self {
            protocol,
            status,
            located_blocks,
            ec_schema,
            position: 0,
        }
    }

    pub fn file_length(&self) -> usize {
        self.status.length as usize
    }

    pub fn remaining(&self) -> usize {
        if self.position > self.status.length as usize {
            0
        } else {
            self.status.length as usize - self.position
        }
    }

    /// Read up to `len` bytes into a new [Bytes] object, advancing the internal position in the file.
    /// An empty [Bytes] object will be returned if the end of the file has been reached.
    pub async fn read(&mut self, len: usize) -> Result<Bytes> {
        if self.position >= self.file_length() {
            Ok(Bytes::new())
        } else {
            let offset = self.position;
            self.position = usize::min(self.position + len, self.file_length());
            self.read_range(offset, self.position - offset).await
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
    ///
    /// Panics if the requested range is outside of the file
    pub async fn read_range(&self, offset: usize, len: usize) -> Result<Bytes> {
        let mut stream = self.read_range_stream(offset, len).boxed();
        let mut buf = BytesMut::with_capacity(len);
        while let Some(bytes) = stream.next().await.transpose()? {
            buf.put(bytes);
        }
        Ok(buf.freeze())
    }

    /// Read file data into an existing buffer
    ///
    /// Panics if the requested range is outside of the file
    pub async fn read_range_buf(&self, mut buf: &mut [u8], offset: usize) -> Result<()> {
        let mut stream = self.read_range_stream(offset, buf.len()).boxed();
        while let Some(bytes) = stream.next().await.transpose()? {
            buf.put(bytes);
        }

        Ok(())
    }

    /// Return a stream of `Bytes` objects containing the content of the file
    ///
    /// Panics if the requested range is outside of the file
    pub fn read_range_stream(
        &self,
        offset: usize,
        len: usize,
    ) -> impl Stream<Item = Result<Bytes>> {
        if offset + len > self.file_length() {
            panic!("Cannot read past end of the file");
        }

        let block_streams: Vec<BoxStream<Result<Bytes>>> = self
            .located_blocks
            .blocks
            .iter()
            .flat_map(move |block| {
                let block_file_start = block.offset as usize;
                let block_file_end = block_file_start + block.b.num_bytes() as usize;

                if block_file_start < (offset + len) && block_file_end > offset {
                    // We need to read this block
                    let block_start = offset - usize::min(offset, block_file_start);
                    let block_end = usize::min(offset + len, block_file_end) - block_file_start;
                    Some(get_block_stream(
                        Arc::clone(&self.protocol),
                        block.clone(),
                        block_start,
                        block_end - block_start,
                        self.ec_schema.clone(),
                    ))
                } else {
                    // No data is needed from this block
                    None
                }
            })
            .collect();

        stream::iter(block_streams).flatten()
    }
}

pub struct FileWriter {
    src: String,
    protocol: Arc<NamenodeProtocol>,
    status: hdfs::HdfsFileStatusProto,
    block_writer: Option<BlockWriter>,
    last_block: Option<hdfs::LocatedBlockProto>,
    closed: bool,
    bytes_written: usize,
}

impl FileWriter {
    pub(crate) fn new(
        protocol: Arc<NamenodeProtocol>,
        src: String,
        status: hdfs::HdfsFileStatusProto,
        // Some for append, None for create
        last_block: Option<hdfs::LocatedBlockProto>,
    ) -> Self {
        protocol.add_file_lease(status.file_id(), status.namespace.clone());
        Self {
            protocol,
            src,
            status,
            block_writer: None,
            last_block,
            closed: false,
            bytes_written: 0,
        }
    }

    async fn create_block_writer(&mut self) -> Result<()> {
        let new_block = if let Some(last_block) = self.last_block.take() {
            // Append operation on first write. Erasure code appends always just create a new block.
            if last_block.b.num_bytes() < self.status.blocksize() && self.status.ec_policy.is_none()
            {
                // The last block isn't full, just write data to it
                last_block
            } else {
                // The last block is full, so create a new block to write to
                self.protocol
                    .add_block(&self.src, Some(last_block.b), self.status.file_id)
                    .await?
                    .block
            }
        } else {
            // Not appending to an existing block, just create a new one
            // If there's an existing block writer, close it first
            let extended_block = if let Some(block_writer) = self.block_writer.take() {
                let extended_block = block_writer.get_extended_block();
                block_writer.close().await?;
                Some(extended_block)
            } else {
                None
            };

            self.protocol
                .add_block(&self.src, extended_block, self.status.file_id)
                .await?
                .block
        };

        let block_writer = BlockWriter::new(
            Arc::clone(&self.protocol),
            new_block,
            self.status.blocksize() as usize,
            self.protocol.get_cached_server_defaults().await?,
            self.status
                .ec_policy
                .as_ref()
                .map(resolve_ec_policy)
                .transpose()?
                .as_ref(),
        )
        .await?;

        self.block_writer = Some(block_writer);
        Ok(())
    }

    async fn get_block_writer(&mut self) -> Result<&mut BlockWriter> {
        // If the current writer is full, or hasn't been created, create one
        if self.block_writer.as_ref().is_some_and(|b| b.is_full()) || self.block_writer.is_none() {
            self.create_block_writer().await?;
        }

        Ok(self.block_writer.as_mut().unwrap())
    }

    pub async fn write(&mut self, mut buf: Bytes) -> Result<usize> {
        let bytes_to_write = buf.len();
        // Create a shallow copy of the bytes instance to mutate and track what's been read
        while !buf.is_empty() {
            let block_writer = self.get_block_writer().await?;

            block_writer.write(&mut buf).await?;
        }

        self.bytes_written += bytes_to_write;

        Ok(bytes_to_write)
    }

    pub async fn close(&mut self) -> Result<()> {
        if !self.closed {
            let extended_block = if let Some(block_writer) = self.block_writer.take() {
                let extended_block = block_writer.get_extended_block();
                block_writer.close().await?;
                Some(extended_block)
            } else {
                None
            };

            let mut retry_delay = COMPLETE_RETRY_DELAY_MS;
            let mut retries = 0;
            while retries < COMPLETE_RETRIES {
                let successful = self
                    .protocol
                    .complete(&self.src, extended_block.clone(), self.status.file_id)
                    .await?
                    .result;

                if successful {
                    self.closed = true;
                    return Ok(());
                }

                tokio::time::sleep(Duration::from_millis(retry_delay)).await;

                retry_delay *= 2;
                retries += 1;
            }
            Err(HdfsError::OperationFailed(
                "Failed to complete file in time".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        if !self.closed {
            warn!("FileWriter dropped without being closed. File content may not have saved or may not be complete");
        }

        self.protocol
            .remove_file_lease(self.status.file_id(), self.status.namespace.clone());
    }
}
