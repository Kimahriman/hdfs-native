use base64::{Engine as _, engine::general_purpose};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use cipher::{KeyIvInit, StreamCipher};
use log::debug;
use prost::Message;
use std::sync::{Arc, Mutex};
use tokio::io::BufReader;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream},
    net::TcpStream,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use super::user::BlockTokenIdentifier;
use crate::common::config::Configuration;
use crate::proto::hdfs::{CipherOptionProto, CipherSuiteProto, DataEncryptionKeyProto};
use crate::proto::{
    common::TokenProto,
    hdfs::{
        DataTransferEncryptorMessageProto, DatanodeIdProto, HandshakeSecretProto,
        data_transfer_encryptor_message_proto::DataTransferEncryptorStatus,
    },
};
use crate::{HdfsError, Result};

use hadoop_native::security::{DigestSaslSession, sasl::SaslSession};

type Aes128Ctr = ctr::Ctr128BE<aes::Aes128>;
type Aes192Ctr = ctr::Ctr128BE<aes::Aes192>;
type Aes256Ctr = ctr::Ctr128BE<aes::Aes256>;

const SASL_TRANSFER_MAGIC_NUMBER: i32 = 0xDEADBEEFu32 as i32;

struct SaslDecryptor {
    session: Arc<Mutex<DigestSaslSession>>,
    size_buffer: [u8; 4],
    response_buffer: Vec<u8>,
    data_buffer: Bytes,
}

impl SaslDecryptor {
    async fn read_more_data(&mut self, stream: &mut BufReader<OwnedReadHalf>) -> Result<()> {
        stream.read_exact(&mut self.size_buffer).await?;
        let msg_length = u32::from_be_bytes(self.size_buffer) as usize;

        // Resize our internal buffer if the message is larger
        if msg_length > self.response_buffer.len() {
            self.response_buffer.resize(msg_length, 0);
        }

        stream
            .read_exact(&mut self.response_buffer[..msg_length])
            .await?;

        self.data_buffer = self
            .session
            .lock()
            .unwrap()
            .decode(&self.response_buffer[..msg_length])?
            .into();

        Ok(())
    }
}

enum DatanodeDecryptor {
    Sasl(SaslDecryptor),
    Cipher(Box<dyn StreamCipher + Send>),
}

pub(crate) struct SaslDatanodeReader {
    stream: BufReader<OwnedReadHalf>,
    decryptor: Option<DatanodeDecryptor>,
}

impl SaslDatanodeReader {
    fn unencrypted(stream: OwnedReadHalf) -> Self {
        Self {
            stream: BufReader::new(stream),
            decryptor: None,
        }
    }

    fn sasl(stream: OwnedReadHalf, session: Arc<Mutex<DigestSaslSession>>) -> Self {
        let decryptor = SaslDecryptor {
            session,
            size_buffer: [0u8; 4],
            response_buffer: Vec::with_capacity(65536),
            data_buffer: Bytes::new(),
        };
        Self {
            stream: BufReader::new(stream),
            decryptor: Some(DatanodeDecryptor::Sasl(decryptor)),
        }
    }

    fn cipher(stream: OwnedReadHalf, cipher: Box<dyn StreamCipher + Send>) -> Self {
        Self {
            stream: BufReader::new(stream),
            decryptor: Some(DatanodeDecryptor::Cipher(cipher)),
        }
    }

    pub(crate) async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize> {
        match &mut self.decryptor {
            Some(DatanodeDecryptor::Sasl(sasl)) => {
                let read_len = buf.len();
                let mut bytes_remaining = read_len;
                while bytes_remaining > 0 {
                    if !sasl.data_buffer.has_remaining() {
                        sasl.read_more_data(&mut self.stream).await?;
                    }
                    let copy_len = usize::min(bytes_remaining, sasl.data_buffer.remaining());
                    let copy_start = read_len - bytes_remaining;
                    sasl.data_buffer
                        .copy_to_slice(&mut buf[copy_start..(copy_start + copy_len)]);
                    bytes_remaining -= copy_len;
                }

                Ok(read_len)
            }
            Some(DatanodeDecryptor::Cipher(cipher)) => {
                let read_len = self.stream.read_exact(buf).await?;
                cipher.apply_keystream(buf);
                Ok(read_len)
            }
            None => Ok(self.stream.read_exact(buf).await?),
        }
    }

    /// Reads a length delimiter from the stream and then reads that many bytes for a full proto message
    pub(crate) async fn read_proto(&mut self) -> Result<Bytes> {
        match &mut self.decryptor {
            Some(DatanodeDecryptor::Sasl(sasl)) => {
                // assumption is we'll have the whole length in a single message
                if !sasl.data_buffer.has_remaining() {
                    sasl.read_more_data(&mut self.stream).await?;
                }
                let decoded_len = prost::decode_length_delimiter(&mut sasl.data_buffer)?;

                let mut buf = BytesMut::zeroed(decoded_len);
                self.read_exact(&mut buf).await?;
                Ok(buf.freeze())
            }
            Some(DatanodeDecryptor::Cipher(cipher)) => {
                let mut msg_len = BytesMut::with_capacity(10);
                // Known from varint parsing, once we either get 10 bytes or a byte less than 0x80
                // we have enough to decode the length
                while msg_len.len() < 10 {
                    let mut byte = [self.stream.read_u8().await?];
                    cipher.apply_keystream(&mut byte);
                    msg_len.put(&byte[..]);
                    if byte[0] < 0x80 {
                        break;
                    }
                }

                let decoded_len = prost::decode_length_delimiter(&mut msg_len.freeze())?;

                let mut msg_buf = BytesMut::zeroed(decoded_len);
                self.stream.read_exact(&mut msg_buf).await?;
                cipher.apply_keystream(&mut msg_buf);

                Ok(msg_buf.freeze())
            }
            None => {
                let mut buf = self.stream.fill_buf().await?;
                if buf.is_empty() {
                    // The stream has been closed
                    return Err(HdfsError::DataTransferError(
                        "Datanode connection closed while waiting for ack".to_string(),
                    ));
                }

                let decoded_len = prost::decode_length_delimiter(&mut buf)?;
                self.stream
                    .consume(prost::length_delimiter_len(decoded_len));

                let mut msg_buf = BytesMut::zeroed(decoded_len);
                self.stream.read_exact(&mut msg_buf).await?;

                Ok(msg_buf.freeze())
            }
        }
    }
}

enum DatanodeEncryptor {
    Sasl(Arc<Mutex<DigestSaslSession>>),
    Cipher(Box<dyn StreamCipher + Send>),
}

pub(crate) struct SaslDatanodeWriter {
    stream: OwnedWriteHalf,
    encryptor: Option<DatanodeEncryptor>,
}

impl SaslDatanodeWriter {
    fn unencrypted(stream: OwnedWriteHalf) -> Self {
        Self {
            stream,
            encryptor: None,
        }
    }

    fn sasl(stream: OwnedWriteHalf, session: Arc<Mutex<DigestSaslSession>>) -> Self {
        Self {
            stream,
            encryptor: Some(DatanodeEncryptor::Sasl(session)),
        }
    }

    fn cipher(stream: OwnedWriteHalf, cipher: Box<dyn StreamCipher + Send>) -> Self {
        Self {
            stream,
            encryptor: Some(DatanodeEncryptor::Cipher(cipher)),
        }
    }

    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        match &mut self.encryptor {
            Some(DatanodeEncryptor::Sasl(sasl)) => {
                let wrapped = sasl.lock().unwrap().encode(buf)?;
                self.stream.write_u32(wrapped.len() as u32).await?;
                self.stream.write_all(&wrapped).await?;
            }
            Some(DatanodeEncryptor::Cipher(cipher)) => {
                let mut encrypted = vec![0u8; buf.len()];
                cipher.apply_keystream_b2b(buf, &mut encrypted);
                self.stream.write_all(&encrypted).await?;
            }
            None => {
                self.stream.write_all(buf).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        Ok(self.stream.flush().await?)
    }
}

pub(crate) struct SaslDatanodeConnection {
    stream: BufStream<TcpStream>,
}

impl SaslDatanodeConnection {
    pub fn create(stream: TcpStream) -> Self {
        Self {
            stream: BufStream::new(stream),
        }
    }

    /// There are a few different paths for negotiating a connection with a DataNode:
    ///
    /// 1. If `dfs.encrypt.data.transfer` is set on the NameNode, always encrypt the session
    ///    and use an encryption key from the NameNode for the negotiation. This will happen
    ///    if `encryption_key` is defined.
    /// 2. If there is no block token
    ///    or the DataNode transfer port is privileged (<= 1024)
    ///    or `dfs.data.transfer.protection` not set,
    ///    we skip the SASL handshake and assume it is trusted.
    /// 3. Otherwise, we do a SAL handshake using the provided block token.
    ///
    /// For cases 1 and 3, we optionally negotiate a cipher to use for encryption instead of
    /// SASL protection mechanisms.
    pub(crate) async fn negotiate(
        mut self,
        datanode_id: &DatanodeIdProto,
        token: &TokenProto,
        encryption_key: Option<&DataEncryptionKeyProto>,
        config: &Configuration,
    ) -> Result<(SaslDatanodeReader, SaslDatanodeWriter)> {
        let mut session = if let Some(key) = encryption_key {
            DigestSaslSession::new(
                "hdfs".to_string(),
                "0".to_string(),
                format!(
                    "{} {} {}",
                    key.key_id,
                    key.block_pool_id,
                    general_purpose::STANDARD.encode(&key.nonce)
                ),
                general_purpose::STANDARD.encode(&key.encryption_key),
            )
        } else if !config.security_enabled()
            || token.identifier.is_empty()
            || datanode_id.xfer_port <= 1024
            || !config.data_transfer_protection_enabled()
        {
            return self.split(None, None);
        } else {
            DigestSaslSession::from_token(
                "hdfs".to_string(),
                "0".to_string(),
                &token.clone().into(),
            )
        };

        self.stream.write_i32(SASL_TRANSFER_MAGIC_NUMBER).await?;
        self.stream.flush().await?;

        let token_identifier = BlockTokenIdentifier::from_identifier(&token.identifier)?;

        let handshake_secret = if !token_identifier.handshake_secret.is_empty() {
            Some(HandshakeSecretProto {
                bpid: token_identifier.block_pool_id.clone(),
                secret: token_identifier.handshake_secret.clone(),
            })
        } else {
            None
        };

        let message = DataTransferEncryptorMessageProto {
            handshake_secret,
            status: DataTransferEncryptorStatus::Success as i32,
            ..Default::default()
        };

        debug!("Sending data transfer encryptor message: {:?}", message);

        self.stream
            .write_all(&message.encode_length_delimited_to_vec())
            .await?;
        self.stream.flush().await?;

        let response = self.read_sasl_response().await?;
        debug!("Data transfer encryptor response: {:?}", response);

        let (payload, finished) = session.step(response.payload.as_ref().map(|p| &p[..]))?;
        assert!(!finished);

        let cipher_option = if session.supports_encryption() {
            vec![CipherOptionProto {
                suite: CipherSuiteProto::AesCtrNopadding as i32,
                ..Default::default()
            }]
        } else {
            vec![]
        };

        let message = DataTransferEncryptorMessageProto {
            status: DataTransferEncryptorStatus::Success as i32,
            payload: Some(payload),
            cipher_option,
            ..Default::default()
        };

        debug!("Sending data transfer encryptor message: {:?}", message);

        self.stream
            .write_all(&message.encode_length_delimited_to_vec())
            .await?;
        self.stream.flush().await?;

        let response = self.read_sasl_response().await?;
        debug!("Data transfer encryptor response: {:?}", response);

        let (_, finished) = session.step(response.payload.as_ref().map(|p| &p[..]))?;

        assert!(finished);

        if session.has_security_layer() {
            self.split(Some(session), response.cipher_option.first())
        } else {
            self.split(None, None)
        }
    }

    async fn read_sasl_response(&mut self) -> Result<DataTransferEncryptorMessageProto> {
        self.stream.fill_buf().await?;

        let buf = self.stream.fill_buf().await?;
        if buf.is_empty() {
            Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        }
        let msg_length = prost::decode_length_delimiter(buf)?;
        let total_size = msg_length + prost::length_delimiter_len(msg_length);

        let mut response_buf = BytesMut::zeroed(total_size);
        self.stream.read_exact(&mut response_buf).await?;

        Ok(DataTransferEncryptorMessageProto::decode_length_delimited(
            response_buf.freeze(),
        )?)
    }

    fn split(
        self,
        session: Option<DigestSaslSession>,
        cipher_option: Option<&CipherOptionProto>,
    ) -> Result<(SaslDatanodeReader, SaslDatanodeWriter)> {
        let (stream_reader, stream_writer) = self.stream.into_inner().into_split();
        if let Some(cipher) = cipher_option {
            let mut session = session.unwrap();
            match cipher.suite() {
                CipherSuiteProto::AesCtrNopadding => {
                    let in_key = session.decode(cipher.in_key())?;
                    let out_key = session.decode(cipher.out_key())?;

                    // For the client, the in_key is used to encrypt data to send and the out_key is for decrypting incoming data
                    let encryptor = Self::create_aes_cipher(&in_key, cipher.in_iv())?;
                    let decryptor = Self::create_aes_cipher(&out_key, cipher.out_iv())?;

                    let reader = SaslDatanodeReader::cipher(stream_reader, decryptor);
                    let writer = SaslDatanodeWriter::cipher(stream_writer, encryptor);
                    Ok((reader, writer))
                }
                c => Err(HdfsError::SASLError(format!("Unsupported cipher {c:?}"))),
            }
        } else if let Some(session) = session {
            let reader_session = Arc::new(Mutex::new(session));
            let writer_session = Arc::clone(&reader_session);
            let reader = SaslDatanodeReader::sasl(stream_reader, reader_session);
            let writer = SaslDatanodeWriter::sasl(stream_writer, writer_session);
            Ok((reader, writer))
        } else {
            Ok((
                SaslDatanodeReader::unencrypted(stream_reader),
                SaslDatanodeWriter::unencrypted(stream_writer),
            ))
        }
    }

    fn create_aes_cipher(key: &[u8], iv: &[u8]) -> Result<Box<dyn StreamCipher + Send>> {
        match key.len() * 8 {
            128 => Aes128Ctr::new_from_slices(key, iv)
                .map(|cipher| Box::new(cipher) as Box<dyn StreamCipher + Send>)
                .map_err(|_| HdfsError::SASLError("Invalid AES-128 key or IV length".to_string())),
            192 => Aes192Ctr::new_from_slices(key, iv)
                .map(|cipher| Box::new(cipher) as Box<dyn StreamCipher + Send>)
                .map_err(|_| HdfsError::SASLError("Invalid AES-192 key or IV length".to_string())),
            256 => Aes256Ctr::new_from_slices(key, iv)
                .map(|cipher| Box::new(cipher) as Box<dyn StreamCipher + Send>)
                .map_err(|_| HdfsError::SASLError("Invalid AES-256 key or IV length".to_string())),
            x => Err(HdfsError::SASLError(format!(
                "Unsupported AES bit length {x}"
            ))),
        }
    }
}
