use bytes::{Buf, BufMut, Bytes, BytesMut};
use cipher::{KeyIvInit, StreamCipher};
use log::{debug, warn};
use prost::Message;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::io::BufReader;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    net::TcpStream,
};

use super::user::BlockTokenIdentifier;
use crate::proto::hdfs::{CipherOptionProto, CipherSuiteProto, DataEncryptionKeyProto};
use crate::proto::{
    common::{
        rpc_response_header_proto::RpcStatusProto,
        rpc_sasl_proto::{SaslAuth, SaslState},
        RpcKindProto, RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto, TokenProto,
    },
    hdfs::{
        data_transfer_encryptor_message_proto::DataTransferEncryptorStatus,
        DataTransferEncryptorMessageProto, DatanodeIdProto, HandshakeSecretProto,
    },
};
use crate::security::digest::DigestSaslSession;
use crate::{HdfsError, Result};

#[cfg(feature = "kerberos")]
use super::gssapi::GssapiSession;
use super::user::{User, UserInfo};

type Aes128Ctr = ctr::Ctr128BE<aes::Aes128>;
type Aes192Ctr = ctr::Ctr128BE<aes::Aes192>;
type Aes256Ctr = ctr::Ctr128BE<aes::Aes256>;

const SASL_CALL_ID: i32 = -33;
const SASL_TRANSFER_MAGIC_NUMBER: i32 = 0xDEADBEEFu32 as i32;
const HDFS_DELEGATION_TOKEN: &str = "HDFS_DELEGATION_TOKEN";

pub(crate) enum AuthMethod {
    Simple,
    Kerberos,
    Token,
}
impl AuthMethod {
    fn parse(method: &str) -> Option<Self> {
        match method {
            "SIMPLE" => Some(Self::Simple),
            "KERBEROS" => Some(Self::Kerberos),
            "TOKEN" => Some(Self::Token),
            _ => None,
        }
    }
}

pub(crate) trait SaslSession: Send + Sync {
    fn step(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)>;

    fn has_security_layer(&self) -> bool;

    fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>>;

    fn decode(&mut self, buf: &[u8]) -> Result<Vec<u8>>;

    fn get_user_info(&self) -> Result<UserInfo>;
}

pub struct SaslRpcClient {
    reader: SaslReader,
    writer: SaslWriter,
    session: Option<Arc<Mutex<Box<dyn SaslSession>>>>,
}

impl SaslRpcClient {
    pub fn create(stream: TcpStream) -> SaslRpcClient {
        let (reader, writer) = stream.into_split();
        SaslRpcClient {
            reader: SaslReader::new(reader),
            writer: SaslWriter::new(writer),
            session: None,
        }
    }

    /// Service should be the connection host:port for a single NameNode connection, or the
    /// name service name when connecting to HA NameNodes.
    pub(crate) async fn negotiate(&mut self, service: &str) -> Result<UserInfo> {
        let rpc_sasl = RpcSaslProto {
            state: SaslState::Negotiate as i32,
            ..Default::default()
        };

        self.writer.send_sasl_message(&rpc_sasl).await?;

        let mut done = false;
        let mut session: Option<Box<dyn SaslSession>> = None;
        while !done {
            let mut response: Option<RpcSaslProto> = None;
            let message = self.reader.read_response().await?;
            debug!("Handling SASL message: {:?}", message);
            match SaslState::try_from(message.state).unwrap() {
                SaslState::Negotiate => {
                    let (mut selected_auth, selected_session) =
                        self.select_method(&message.auths, service)?;
                    session = selected_session;

                    let token = if let Some(session) = session.as_mut() {
                        let (token, finished) =
                            session.step(selected_auth.challenge.as_ref().map(|c| &c[..]))?;
                        if finished {
                            return Err(HdfsError::SASLError(
                                "SASL negotiation finished too soon".to_string(),
                            ));
                        }
                        Some(token)
                    } else {
                        done = true;
                        None
                    };

                    // Response shouldn't contain the challenge
                    selected_auth.challenge = None;

                    let r = RpcSaslProto {
                        state: SaslState::Initiate as i32,
                        auths: Vec::from([selected_auth]),
                        token: token.or(Some(Vec::new())),
                        ..Default::default()
                    };
                    response = Some(r);
                }
                SaslState::Challenge => {
                    let (token, _) = session
                        .as_mut()
                        .unwrap()
                        .step(message.token.as_ref().map(|t| &t[..]))?;

                    let r = RpcSaslProto {
                        state: SaslState::Response as i32,
                        token: Some(token),
                        ..Default::default()
                    };
                    response = Some(r);
                }
                SaslState::Success => {
                    if let Some(token) = message.token.as_ref() {
                        let (_, finished) = session.as_mut().unwrap().step(Some(&token[..]))?;
                        if !finished {
                            return Err(HdfsError::SASLError(
                                "Client not finished after server success".to_string(),
                            ));
                        }
                    }
                    done = true;
                }
                _ => todo!(),
            }

            if let Some(r) = response {
                debug!("Sending SASL response {:?}", r);
                self.writer.send_sasl_message(&r).await?;
            }
        }

        let user_info = if let Some(s) = session.as_ref() {
            s.get_user_info()?
        } else {
            User::get_simpler_user()
        };
        self.session = session
            .filter(|x| {
                debug!("Has security layer: {:?}", x.has_security_layer());
                x.has_security_layer()
            })
            .map(|s| Arc::new(Mutex::new(s)));

        Ok(user_info)
    }

    fn select_method(
        &mut self,
        auths: &[SaslAuth],
        service: &str,
    ) -> Result<(SaslAuth, Option<Box<dyn SaslSession>>)> {
        let user = User::get();
        for auth in auths.iter() {
            match (
                AuthMethod::parse(&auth.method),
                user.get_token(HDFS_DELEGATION_TOKEN, service),
            ) {
                (Some(AuthMethod::Simple), _) => {
                    return Ok((auth.clone(), None));
                }
                #[cfg(feature = "kerberos")]
                (Some(AuthMethod::Kerberos), _) => {
                    let session = GssapiSession::new(auth.protocol(), auth.server_id())?;
                    return Ok((auth.clone(), Some(Box::new(session))));
                }
                (Some(AuthMethod::Token), Some(token)) => {
                    let session = DigestSaslSession::from_token(
                        auth.protocol().to_string(),
                        auth.server_id().to_string(),
                        token,
                    );
                    // let session = GSASLSession::new(auth.protocol(), auth.server_id(), token)?;

                    return Ok((auth.clone(), Some(Box::new(session))));
                }
                _ => (),
            }
        }
        Err(HdfsError::NoSASLMechanism)
    }

    pub(crate) fn split(self) -> (SaslReader, SaslWriter) {
        let mut reader = self.reader;
        let mut writer = self.writer;
        if let Some(session) = self.session {
            reader.set_session(Arc::clone(&session));
            writer.set_session(session);
        }
        (reader, writer)
    }
}

pub(crate) struct SaslReader {
    stream: OwnedReadHalf,
    session: Option<Arc<Mutex<Box<dyn SaslSession>>>>,
    buffer: Bytes,
}

impl SaslReader {
    fn new(stream: OwnedReadHalf) -> Self {
        SaslReader {
            stream,
            session: None,
            buffer: Bytes::new(),
        }
    }

    fn set_session(&mut self, session: Arc<Mutex<Box<dyn SaslSession>>>) {
        self.session = Some(session);
    }

    async fn read_response(&mut self) -> Result<RpcSaslProto> {
        let mut buf = [0u8; 4];
        self.stream.read_exact(&mut buf).await?;

        let msg_length = u32::from_be_bytes(buf);

        let mut buf = BytesMut::zeroed(msg_length as usize);
        self.stream.read_exact(&mut buf).await?;

        let mut bytes = buf.freeze();
        let rpc_response = RpcResponseHeaderProto::decode_length_delimited(&mut bytes)?;
        debug!("{:?}", rpc_response);

        match RpcStatusProto::try_from(rpc_response.status).unwrap() {
            RpcStatusProto::Error => {
                return Err(HdfsError::RPCError(
                    rpc_response.exception_class_name().to_string(),
                    rpc_response.error_msg().to_string(),
                ));
            }
            RpcStatusProto::Fatal => {
                warn!(
                    "RPC fatal error: {}: {}",
                    rpc_response.exception_class_name(),
                    rpc_response.error_msg()
                );
                return Err(HdfsError::FatalRPCError(
                    rpc_response.exception_class_name().to_string(),
                    rpc_response.error_msg().to_string(),
                ));
            }
            _ => (),
        }

        let sasl_response = RpcSaslProto::decode_length_delimited(&mut bytes)?;
        Ok(sasl_response)
    }

    pub(crate) async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.session.is_some() {
            let read_len = buf.len();
            let mut bytes_remaining = read_len;
            while bytes_remaining > 0 {
                if !self.buffer.has_remaining() {
                    let response = self.read_response().await?;
                    if response.state() != SaslState::Wrap {
                        todo!();
                    }

                    // let mut writer = BytesMut::with_capacity(response.token().len()).writer();
                    let decoded = self
                        .session
                        .as_ref()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .decode(response.token())?;
                    self.buffer = Bytes::from(decoded)
                }
                let copy_len = usize::min(bytes_remaining, self.buffer.remaining());
                let copy_start = read_len - bytes_remaining;
                self.buffer
                    .copy_to_slice(&mut buf[copy_start..(copy_start + copy_len)]);
                bytes_remaining -= copy_len;
            }

            Ok(read_len)
        } else {
            Ok(self.stream.read_exact(buf).await?)
        }
    }
}

pub(crate) struct SaslWriter {
    stream: OwnedWriteHalf,
    session: Option<Arc<Mutex<Box<dyn SaslSession>>>>,
}

impl SaslWriter {
    fn new(stream: OwnedWriteHalf) -> Self {
        SaslWriter {
            stream,
            session: None,
        }
    }

    fn set_session(&mut self, session: Arc<Mutex<Box<dyn SaslSession>>>) {
        self.session = Some(session);
    }

    fn create_request_header() -> RpcRequestHeaderProto {
        RpcRequestHeaderProto {
            rpc_kind: Some(RpcKindProto::RpcProtocolBuffer as i32),
            // RPC_FINAL_PACKET
            rpc_op: Some(0),
            call_id: SASL_CALL_ID,
            client_id: Vec::new(),
            retry_count: Some(-1),
            ..Default::default()
        }
    }

    async fn send_sasl_message(&mut self, message: &RpcSaslProto) -> io::Result<()> {
        let header_buf = Self::create_request_header().encode_length_delimited_to_vec();
        let message_buf = message.encode_length_delimited_to_vec();
        let size = (header_buf.len() + message_buf.len()) as u32;

        self.stream.write_all(&size.to_be_bytes()).await?;
        self.stream.write_all(&header_buf).await?;
        self.stream.write_all(&message_buf).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        if self.session.is_some() {
            let mut rpc_sasl = RpcSaslProto {
                state: SaslState::Wrap as i32,
                ..Default::default()
            };

            // let mut writer = Vec::with_capacity(buf.len()).writer();
            let encoded = self
                .session
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .encode(buf)
                .unwrap_or_else(|_| todo!());

            rpc_sasl.token = Some(encoded);

            self.send_sasl_message(&rpc_sasl).await?;
        } else {
            self.stream.write_all(buf).await?
        }
        Ok(())
    }
}

impl std::fmt::Debug for SaslWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslWriter")
            .field("stream", &self.stream)
            .finish()
    }
}

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
                cipher.apply_keystream_b2b(buf, &mut encrypted).unwrap();
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
    /// 2. If there is no block token or the DataNode transfer port is privileged (<= 1024), we
    ///    skip the SASL handshake and assume it is trusted.
    /// 3. Otherwise, we do a SAL handshake using the provided block token.
    ///
    /// For cases 1 and 3, we optionally negotiate a cipher to use for encryption instead of
    /// SASL protection mechanisms.
    pub(crate) async fn negotiate(
        mut self,
        datanode_id: &DatanodeIdProto,
        token: &TokenProto,
        encryption_key: Option<&DataEncryptionKeyProto>,
    ) -> Result<(SaslDatanodeReader, SaslDatanodeWriter)> {
        let mut session = if let Some(key) = encryption_key {
            DigestSaslSession::from_encryption_key("hdfs".to_string(), "0".to_string(), key)
        } else if token.identifier.is_empty() || datanode_id.xfer_port <= 1024 {
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
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
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
                    let encryptor = Self::create_aes_cipher(&in_key, cipher.in_iv());
                    let decryptor = Self::create_aes_cipher(&out_key, cipher.out_iv());

                    let reader = SaslDatanodeReader::cipher(stream_reader, decryptor);
                    let writer = SaslDatanodeWriter::cipher(stream_writer, encryptor);
                    Ok((reader, writer))
                }
                c => Err(HdfsError::SASLError(format!("Unsupported cipher {:?}", c))),
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

    fn create_aes_cipher(key: &[u8], iv: &[u8]) -> Box<dyn StreamCipher + Send> {
        match key.len() * 8 {
            128 => Box::new(Aes128Ctr::new(key.into(), iv.into())),
            192 => Box::new(Aes192Ctr::new(key.into(), iv.into())),
            256 => Box::new(Aes256Ctr::new(key.into(), iv.into())),
            x => panic!("Unsupported AES bit length {}", x),
        }
    }
}
