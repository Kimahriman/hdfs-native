use bytes::{Buf, Bytes, BytesMut};
use log::{debug, warn};
use prost::Message;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::OwnedReadHalf,
    net::TcpStream,
};

use crate::proto::common::rpc_response_header_proto::RpcStatusProto;
use crate::proto::common::rpc_sasl_proto::{SaslAuth, SaslState};
use crate::proto::common::{
    RpcKindProto, RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto,
};
use crate::{HdfsError, Result};
#[cfg(feature = "token")]
use {
    super::user::Token,
    base64::{engine::general_purpose, Engine as _},
    gsasl_sys as gsasl,
    libc::{c_char, c_void, memcpy},
    std::ffi::CString,
    std::ptr,
    std::sync::atomic::AtomicPtr,
};

#[cfg(feature = "kerberos")]
use {
    rsasl::callback::{Context, Request, SessionCallback, SessionData},
    rsasl::mechanisms::gssapi::properties::{GssSecurityLayer, GssService, SecurityLayer},
    rsasl::mechanisms::gssapi::GSSAPI,
    rsasl::prelude::*,
    rsasl::property::{Hostname, Password},
    // rsasl::registry::{Matches, Named, Side},
    std::io::Cursor,
};

use super::user::User;

const SASL_CALL_ID: i32 = -33;
const HDFS_DELEGATION_TOKEN: &str = "HDFS_DELEGATION_TOKEN";

#[cfg(feature = "kerberos")]
static KERBEROS_MECHANISMS: &[Mechanism] = &[GSSAPI];
// #[cfg(feature = "kerberos")]
// static TOKEN_MECHANISMS: &[Mechanism] = &[DIGEST_MD5];

pub(crate) enum AuthMethod {
    SIMPLE,
    KERBEROS,
    TOKEN,
}
impl AuthMethod {
    fn parse(method: &str) -> Option<Self> {
        match method {
            "SIMPLE" => Some(Self::SIMPLE),
            "KERBEROS" => Some(Self::KERBEROS),
            "TOKEN" => Some(Self::TOKEN),
            _ => None,
        }
    }
}
#[cfg(feature = "kerberos")]
struct KerberosCallback {
    service: String,
    hostname: String,
}

#[cfg(feature = "kerberos")]
impl SessionCallback for KerberosCallback {
    fn callback(
        &self,
        _session_data: &SessionData,
        _context: &Context,
        request: &mut Request,
    ) -> std::result::Result<(), SessionError> {
        request
            .satisfy::<GssService>(self.service.as_str())?
            .satisfy::<Hostname>(self.hostname.as_str())?
            .satisfy::<GssSecurityLayer>(
                &(SecurityLayer::NO_SECURITY_LAYER
                    | SecurityLayer::INTEGRITY
                    | SecurityLayer::CONFIDENTIALITY),
            )?;

        Ok(())
    }
}

#[cfg(feature = "kerberos")]
struct TokenCallback {}

#[cfg(feature = "kerberos")]
impl SessionCallback for TokenCallback {
    fn callback(
        &self,
        _session_data: &SessionData,
        _context: &Context,
        request: &mut Request,
    ) -> std::result::Result<(), SessionError> {
        request
            // satisfy calls can be chained, making use of short-circuiting
            .satisfy::<Password>(b"")?
            .satisfy::<GssSecurityLayer>(&SecurityLayer::NO_SECURITY_LAYER)?;

        Ok(())
    }
}

trait SaslSession: Send + Sync {
    fn step(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)>;

    fn has_security_layer(&self) -> bool {
        false
    }

    fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>>;

    fn decode(&mut self, buf: &[u8]) -> Result<Vec<u8>>;
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
    pub(crate) async fn negotiate(&mut self, service: &str) -> Result<AuthMethod> {
        let mut rpc_sasl = RpcSaslProto::default();
        rpc_sasl.state = SaslState::Negotiate as i32;

        self.writer.send_sasl_message(&rpc_sasl).await?;

        let mut done = false;
        let mut selected_method: Option<AuthMethod> = None;
        let mut session: Option<Box<dyn SaslSession>> = None;
        while !done {
            let mut response: Option<RpcSaslProto> = None;
            let message = self.reader.read_response().await?;
            debug!("Handling SASL message: {:?}", message);
            match SaslState::from_i32(message.state).unwrap() {
                SaslState::Negotiate => {
                    let (mut selected_auth, selected_session) =
                        self.select_method(&message.auths, service)?;
                    session = selected_session;
                    selected_method = AuthMethod::parse(&selected_auth.method);

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

                    let mut r = RpcSaslProto::default();
                    r.state = SaslState::Initiate as i32;
                    r.auths = Vec::from([selected_auth]);
                    r.token = token.or(Some(Vec::new()));
                    response = Some(r);
                }
                SaslState::Challenge => {
                    let (token, _) = session
                        .as_mut()
                        .unwrap()
                        .step(message.token.as_ref().map(|t| &t[..]))?;

                    let mut r = RpcSaslProto::default();
                    r.state = SaslState::Response as i32;
                    r.token = Some(token);
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

        self.session = session
            .filter(|x| {
                debug!("Has security layer: {:?}", x.has_security_layer());
                x.has_security_layer()
            })
            .map(|s| Arc::new(Mutex::new(s)));

        Ok(selected_method.expect("SASL finished but no method selected"))
    }

    fn select_method(
        &mut self,
        auths: &Vec<SaslAuth>,
        service: &str,
    ) -> Result<(SaslAuth, Option<Box<dyn SaslSession>>)> {
        let user = User::get();
        for auth in auths.iter() {
            match (
                AuthMethod::parse(&auth.method),
                user.get_token(HDFS_DELEGATION_TOKEN, service),
            ) {
                (Some(AuthMethod::SIMPLE), _) => {
                    return Ok((auth.clone(), None));
                }
                #[cfg(feature = "kerberos")]
                (Some(AuthMethod::KERBEROS), _) if User::get_kerberos_user().is_some() => {
                    let session = RSaslSession::new(auth.protocol(), auth.server_id())?;

                    return Ok((auth.clone(), Some(Box::new(session))));
                }
                #[cfg(feature = "token")]
                (Some(AuthMethod::TOKEN), Some(token)) => {
                    debug!("Using token {:?}", token);
                    let session = GSASLSession::new(auth.protocol(), auth.server_id(), token)?;

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

        match RpcStatusProto::from_i32(rpc_response.status).unwrap() {
            RpcStatusProto::Error => {
                return Err(HdfsError::RPCError(
                    rpc_response.exception_class_name().to_string(),
                    rpc_response.error_msg().to_string(),
                ));
            }
            RpcStatusProto::Fatal => {
                warn!("RPC fatal error: {:?}", rpc_response.error_msg);
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
                        .decode(response.token())
                        .unwrap_or_else(|_| todo!());
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

// TODO: Can we implement this?
// impl AsyncRead for SaslReader {
//     fn poll_read(
//         self: Pin<&mut Self>,
//         cx: &mut task::Context<'_>,
//         buf: &mut ReadBuf<'_>,
//     ) -> Poll<io::Result<()>> {
//         todo!()
//     }
// }

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
        let mut request_header = RpcRequestHeaderProto::default();
        request_header.rpc_kind = Some(RpcKindProto::RpcProtocolBuffer as i32);
        // RPC_FINAL_PACKET
        request_header.rpc_op = Some(0);
        request_header.call_id = SASL_CALL_ID;
        request_header.client_id = Vec::new();
        request_header.retry_count = Some(-1);
        request_header
    }

    async fn send_sasl_message(&mut self, message: &RpcSaslProto) -> io::Result<()> {
        let header_buf = Self::create_request_header().encode_length_delimited_to_vec();
        let message_buf = message.encode_length_delimited_to_vec();
        let size = (header_buf.len() + message_buf.len()) as u32;

        self.stream.write(&size.to_be_bytes()).await?;
        self.stream.write(&header_buf).await?;
        self.stream.write(&message_buf).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub(crate) async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.session.is_some() {
            let mut rpc_sasl = RpcSaslProto::default();
            rpc_sasl.state = SaslState::Wrap as i32;

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
            Ok(buf.len())
        } else {
            self.stream.write(buf).await
        }
    }
}

impl std::fmt::Debug for SaslWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslWriter")
            .field("stream", &self.stream)
            .finish()
    }
}

// TODO: Can we implement this?
// impl AsyncWrite for SaslWriter {
//     fn poll_write(
//         self: Pin<&mut Self>,
//         cx: &mut task::Context<'_>,
//         buf: &[u8],
//     ) -> Poll<result::Result<usize, io::Error>> {
//         let mut stream = self.stream;
//         Pin::new(&mut stream).poll_write(cx, buf)
//     }

//     fn poll_flush(
//         self: Pin<&mut Self>,
//         cx: &mut task::Context<'_>,
//     ) -> Poll<result::Result<(), io::Error>> {
//         let mut stream = self.stream;
//         Pin::new(&mut stream).poll_flush(cx)
//     }

//     fn poll_shutdown(
//         self: Pin<&mut Self>,
//         cx: &mut task::Context<'_>,
//     ) -> Poll<result::Result<(), io::Error>> {
//         let mut stream = self.stream;
//         Pin::new(&mut stream).poll_shutdown(cx)
//     }
// }

// #[cfg(feature = "kerberos")]
// #[cfg_attr(feature = "registry_static", distributed_slice(MECHANISMS))]
// pub static DIGEST_MD5: Mechanism = Mechanism::build(
//     Mechname::const_new(b"DIGEST-MD5"),
//     300,
//     Some(|| Ok(Box::new(DigestMD5::default()))),
//     None,
//     Side::Server,
//     |_| Some(Matches::<Select>::name()),
//     |_| true,
// );

// #[cfg(feature = "kerberos")]
// struct Select;
// #[cfg(feature = "kerberos")]
// impl Named for Select {
//     fn mech() -> &'static Mechanism {
//         &DIGEST_MD5
//     }
// }

// struct DigestMD5 {}

// impl Default for DigestMD5 {
//     fn default() -> Self {
//         Self {}
//     }
// }

// #[cfg(feature = "kerberos")]
// impl Authentication for DigestMD5 {
//     fn step(
//         &mut self,
//         session: &mut rsasl::mechanism::MechanismData,
//         input: Option<&[u8]>,
//         _writer: &mut dyn Write,
//     ) -> std::result::Result<State, SessionError> {
//         println!("{:?}", session);
//         println!("{:?}", input);
//         println!("{:?}", std::str::from_utf8(input.unwrap()).unwrap());
//         todo!()
//     }
// }

#[cfg(feature = "kerberos")]
struct RSaslSession {
    inner: Session,
}

#[cfg(feature = "kerberos")]
impl RSaslSession {
    fn new(service: &str, hostname: &str) -> Result<Self> {
        let config = SASLConfig::builder()
            .with_registry(Registry::with_mechanisms(KERBEROS_MECHANISMS))
            .with_callback(KerberosCallback {
                service: service.to_string(),
                hostname: hostname.to_string(),
            })
            .unwrap();

        let sasl = SASLClient::new(config);
        let offered_mechs = [Mechname::parse(b"GSSAPI").unwrap()];

        let inner = sasl.start_suggested_iter(offered_mechs)?;
        Ok(Self { inner })
    }
}

#[cfg(feature = "kerberos")]
impl SaslSession for RSaslSession {
    fn step(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)> {
        let mut cursor = Cursor::new(Vec::new());
        let state = self.inner.step(token, &mut cursor)?;

        Ok((cursor.into_inner(), state != State::Running))
    }

    fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        let mut writer = Cursor::new(Vec::with_capacity(buf.len()));
        self.inner.encode(buf, &mut writer)?;
        Ok(writer.into_inner())
    }

    fn decode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        let mut writer = Cursor::new(Vec::with_capacity(buf.len()));
        self.inner.decode(buf, &mut writer)?;
        Ok(writer.into_inner())
    }

    fn has_security_layer(&self) -> bool {
        self.inner.has_security_layer()
    }
}

#[cfg(feature = "token")]
struct GSASLSession {
    ctx: AtomicPtr<gsasl::Gsasl>,
    conn: AtomicPtr<gsasl::Gsasl_session>,
}

#[cfg(feature = "token")]
impl GSASLSession {
    fn new(service: &str, hostname: &str, token: &Token) -> Result<Self> {
        let mut ctx = ptr::null_mut::<gsasl::Gsasl>();

        let ret = unsafe { gsasl::gsasl_init(&mut ctx) };
        if ret != gsasl::Gsasl_rc::GSASL_OK as i32 {
            return Err(HdfsError::SASLError(
                "Failed to initialize SASL".to_string(),
            ));
        }

        let mut conn = ptr::null_mut::<gsasl::Gsasl_session>();
        let mechanism = CString::new("DIGEST-MD5").unwrap();

        let ret = unsafe { gsasl::gsasl_client_start(ctx, mechanism.as_ptr(), &mut conn) };
        if ret != gsasl::Gsasl_rc::GSASL_OK as i32 {
            return Err(HdfsError::SASLError(
                "Failed to create new SASL client".to_string(),
            ));
        }

        debug!("Started SASL: {:?}, {:?}", conn, mechanism);

        if ret != gsasl::Gsasl_rc::GSASL_OK as i32 {
            return Err(HdfsError::SASLError(format!(
                "Failed to start SASL client: {}",
                ret
            )));
        }

        let service_c = CString::new(service).unwrap();
        let hostname_c = CString::new(hostname).unwrap();

        unsafe {
            gsasl::gsasl_property_set(
                conn,
                gsasl::Gsasl_property::GSASL_SERVICE,
                service_c.as_ptr(),
            );
            gsasl::gsasl_property_set(
                conn,
                gsasl::Gsasl_property::GSASL_HOSTNAME,
                hostname_c.as_ptr(),
            );
            let identifier =
                CString::new(general_purpose::STANDARD.encode(&token.identifier)).unwrap();
            let password = CString::new(general_purpose::STANDARD.encode(&token.password)).unwrap();

            gsasl::gsasl_property_set(
                conn,
                gsasl::Gsasl_property::GSASL_AUTHID,
                identifier.as_ptr(),
            );
            gsasl::gsasl_property_set(
                conn,
                gsasl::Gsasl_property::GSASL_PASSWORD,
                password.as_ptr(),
            );
        }

        Ok(Self {
            ctx: AtomicPtr::new(ctx),
            conn: AtomicPtr::new(conn),
        })
    }
}

#[cfg(feature = "token")]
impl SaslSession for GSASLSession {
    fn step(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)> {
        let mut clientout = ptr::null_mut::<c_char>();
        let mut clientoutlen: u64 = 0;
        let ret = unsafe {
            gsasl::gsasl_step(
                self.conn.load(std::sync::atomic::Ordering::SeqCst),
                token.map(|t| t.as_ptr()).unwrap_or(ptr::null_mut()) as *const i8,
                token.map(|t| t.len()).unwrap_or(0) as u64,
                &mut clientout,
                &mut clientoutlen,
            )
        };

        debug!("SASL step response: {}", ret);

        if ret != gsasl::Gsasl_rc::GSASL_OK as i32
            && ret != gsasl::Gsasl_rc::GSASL_NEEDS_MORE as i32
        {
            return Err(HdfsError::SASLError(format!(
                "Failed to make SASL client step: {}",
                ret
            )));
        }

        let vec = unsafe {
            let mut vec = vec![0u8; clientoutlen as usize];
            memcpy(
                vec.as_mut_ptr() as *mut c_void,
                clientout as *const c_void,
                vec.len(),
            );
            vec
        };

        Ok((vec, ret == gsasl::Gsasl_rc::GSASL_OK as i32))
    }

    fn encode(&mut self, _buf: &[u8]) -> Result<Vec<u8>> {
        todo!()
    }

    fn decode(&mut self, _buf: &[u8]) -> Result<Vec<u8>> {
        todo!()
    }
}

#[cfg(feature = "token")]
impl Drop for GSASLSession {
    fn drop(&mut self) {
        unsafe {
            gsasl::gsasl_finish(self.conn.load(std::sync::atomic::Ordering::SeqCst));
            gsasl::gsasl_done(self.ctx.load(std::sync::atomic::Ordering::SeqCst))
        }
    }
}
