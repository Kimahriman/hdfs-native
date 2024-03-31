use bytes::{Buf, Bytes, BytesMut};
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
// use crate::proto::hdfs::{CipherOptionProto, CipherSuiteProto};
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
                    let session = DigestSaslSession::new(
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

pub(crate) struct SaslDatanodeReader {
    stream: BufReader<OwnedReadHalf>,
    session: Option<Arc<Mutex<DigestSaslSession>>>,
    size_buffer: [u8; 4],
    response_buffer: Vec<u8>,
    data_buffer: Bytes,
}

impl SaslDatanodeReader {
    fn new(stream: OwnedReadHalf, session: Option<Arc<Mutex<DigestSaslSession>>>) -> Self {
        Self {
            stream: BufReader::new(stream),
            session,
            size_buffer: [0u8; 4],
            response_buffer: Vec::with_capacity(65536),
            data_buffer: Bytes::new(),
        }
    }

    async fn read_more_data(&mut self) -> Result<()> {
        self.stream.read_exact(&mut self.size_buffer).await?;
        let msg_length = u32::from_be_bytes(self.size_buffer) as usize;

        // Resize our internal buffer if the message is larger
        if msg_length > self.response_buffer.len() {
            self.response_buffer.resize(msg_length, 0);
        }

        self.stream
            .read_exact(&mut self.response_buffer[..msg_length])
            .await?;

        self.data_buffer = self
            .session
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .decode(&self.response_buffer[..msg_length])?
            .into();

        Ok(())
    }

    pub(crate) async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.session.is_some() {
            let read_len = buf.len();
            let mut bytes_remaining = read_len;
            while bytes_remaining > 0 {
                if !self.data_buffer.has_remaining() {
                    self.read_more_data().await?;
                }
                let copy_len = usize::min(bytes_remaining, self.data_buffer.remaining());
                let copy_start = read_len - bytes_remaining;
                self.data_buffer
                    .copy_to_slice(&mut buf[copy_start..(copy_start + copy_len)]);
                bytes_remaining -= copy_len;
            }

            Ok(read_len)
        } else {
            Ok(self.stream.read_exact(buf).await?)
        }
    }

    /// Reads a length delimiter from the stream without advancing the position of the stream
    pub(crate) async fn read_length_delimiter(&mut self) -> Result<usize> {
        if self.session.is_some() {
            // assumption is we'll have the whole length in a single message
            if !self.data_buffer.has_remaining() {
                self.read_more_data().await?;
            }
            let decoded_len = prost::decode_length_delimiter(&mut self.data_buffer)?;
            Ok(decoded_len)
        } else {
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

            Ok(decoded_len)
        }
    }
}

pub(crate) struct SaslDatanodeWriter {
    stream: OwnedWriteHalf,
    session: Option<Arc<Mutex<DigestSaslSession>>>,
}

impl SaslDatanodeWriter {
    fn new(stream: OwnedWriteHalf, session: Option<Arc<Mutex<DigestSaslSession>>>) -> Self {
        Self { stream, session }
    }

    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        if let Some(session) = self.session.as_ref() {
            let wrapped = session.lock().unwrap().encode(buf)?;
            self.stream.write_u32(wrapped.len() as u32).await?;
            self.stream.write_all(&wrapped).await?;
        } else {
            self.stream.write_all(buf).await?;
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

    pub(crate) async fn negotiate(
        mut self,
        datanode_id: &DatanodeIdProto,
        token: &TokenProto,
    ) -> Result<(SaslDatanodeReader, SaslDatanodeWriter)> {
        // If there's no token identifier or it's a privileged port, don't do SASL negotation
        if token.identifier.is_empty() || datanode_id.xfer_port <= 1024 {
            return Ok(self.split(None));
        }

        self.stream.write_i32(SASL_TRANSFER_MAGIC_NUMBER).await?;
        self.stream.flush().await?;

        let mut session =
            DigestSaslSession::new("hdfs".to_string(), "0".to_string(), &token.clone().into());

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

        // let cipher_option = if session.supports_encryption() {
        //     vec![CipherOptionProto {
        //         suite: CipherSuiteProto::AesCtrNopadding as i32,
        //         ..Default::default()
        //     }]
        // } else {
        //     vec![]
        // };

        let message = DataTransferEncryptorMessageProto {
            status: DataTransferEncryptorStatus::Success as i32,
            payload: Some(payload),
            // cipher_option,
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
            Ok(self.split(Some(session)))
        } else {
            Ok(self.split(None))
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

    fn split(self, session: Option<DigestSaslSession>) -> (SaslDatanodeReader, SaslDatanodeWriter) {
        let reader_session = session.map(|s| Arc::new(Mutex::new(s)));
        let writer_session = reader_session.clone();

        let (stream_reader, stream_writer) = self.stream.into_inner().into_split();

        let reader = SaslDatanodeReader::new(stream_reader, reader_session);
        let writer = SaslDatanodeWriter::new(stream_writer, writer_session);
        (reader, writer)
    }
}
