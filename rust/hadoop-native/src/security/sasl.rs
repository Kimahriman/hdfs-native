use bytes::{Buf, Bytes, BytesMut};
use log::debug;
use prost::Message;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::config::Configuration;
use crate::proto::common::{
    RpcKindProto, RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto,
    rpc_response_header_proto::RpcStatusProto,
    rpc_sasl_proto::{SaslAuth, SaslState},
};
use crate::security::DigestSaslSession;
use crate::{HadoopError as HdfsError, Result};

use super::gssapi::GssapiSession;
use super::user::{User, UserInfo};

const SASL_CALL_ID: i32 = -33;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

pub trait SaslSession: Send + Sync {
    fn step(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)>;

    fn has_security_layer(&self) -> bool;

    fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>>;

    fn decode(&mut self, buf: &[u8]) -> Result<Vec<u8>>;

    fn get_user_info(&self) -> Result<UserInfo>;
}

pub(crate) async fn negotiate_sasl_session(
    stream: TcpStream,
    token_kind: &str,
    service: &str,
    config: &Configuration,
    effective_user: Option<String>,
) -> Result<(UserInfo, SaslReader, SaslWriter)> {
    let (reader, writer) = stream.into_split();
    let mut reader = SaslReader::new(reader);
    let mut writer = SaslWriter::new(writer);

    if !config.security_enabled() {
        return Ok((User::get_simple_user(effective_user), reader, writer));
    }

    let rpc_sasl = RpcSaslProto {
        state: SaslState::Negotiate as i32,
        ..Default::default()
    };

    writer.send_sasl_message(&rpc_sasl).await?;

    let mut done = false;
    let mut session: Option<Box<dyn SaslSession>> = None;
    while !done {
        let mut response: Option<RpcSaslProto> = None;
        let message = reader.read_response().await?;
        debug!("Handling SASL message: {:?}", message);
        match SaslState::try_from(message.state).unwrap() {
            SaslState::Negotiate => {
                let (mut selected_auth, selected_session) =
                    select_method(&message.auths, token_kind, service, effective_user.clone())?;
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
            writer.send_sasl_message(&r).await?;
        }
    }

    let user_info = if let Some(session) = session.as_ref() {
        session.get_user_info()?
    } else {
        User::get_simple_user(effective_user)
    };
    let session = session
        .filter(|x| {
            debug!("Has security layer: {:?}", x.has_security_layer());
            x.has_security_layer()
        })
        .map(|s| Arc::new(Mutex::new(s)));

    if let Some(session) = session {
        reader.set_session(Arc::clone(&session));
        writer.set_session(session);
    }
    Ok((user_info, reader, writer))
}

fn select_method(
    auths: &[SaslAuth],
    token_kind: &str,
    service: &str,
    effective_user: Option<String>,
) -> Result<(SaslAuth, Option<Box<dyn SaslSession>>)> {
    let user = User::get();
    for auth in auths.iter() {
        match (
            AuthMethod::parse(&auth.method),
            user.get_token(token_kind, service),
        ) {
            (Some(AuthMethod::Simple), _) => {
                return Ok((auth.clone(), None));
            }
            (Some(AuthMethod::Kerberos), _) => {
                let session =
                    GssapiSession::new(auth.protocol(), auth.server_id(), effective_user)?;
                return Ok((auth.clone(), Some(Box::new(session))));
            }
            (Some(AuthMethod::Token), Some(token)) => {
                let session = DigestSaslSession::from_token(
                    auth.protocol().to_string(),
                    auth.server_id().to_string(),
                    token,
                );

                return Ok((auth.clone(), Some(Box::new(session))));
            }
            _ => (),
        }
    }
    Err(HdfsError::NoSASLMechanism)
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
        debug!("RPC response: {:?}", rpc_response);

        match RpcStatusProto::try_from(rpc_response.status).unwrap() {
            RpcStatusProto::Error => {
                return Err(HdfsError::RPCError(
                    rpc_response.exception_class_name().to_string(),
                    rpc_response.error_msg().to_string(),
                ));
            }
            RpcStatusProto::Fatal => {
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
        if let Some(session) = self.session.clone() {
            let read_len = buf.len();
            let mut bytes_remaining = read_len;
            while bytes_remaining > 0 {
                if !self.buffer.has_remaining() {
                    let response = self.read_response().await?;
                    if response.state() != SaslState::Wrap {
                        todo!();
                    }

                    let decoded = session.lock().unwrap().decode(response.token())?;
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
        debug!("Sending SASL message {:?}", message);

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
        if let Some(session) = &self.session {
            let mut rpc_sasl = RpcSaslProto {
                state: SaslState::Wrap as i32,
                ..Default::default()
            };

            let encoded = session
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
