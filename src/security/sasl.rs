use std::io::{Cursor, Error, ErrorKind, Read, Result, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;

use crate::proto::common::rpc_response_header_proto::RpcStatusProto;
use crate::proto::common::rpc_sasl_proto::{SaslAuth, SaslState};
use crate::proto::common::{
    RpcKindProto, RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto,
};
#[cfg(feature = "rsasl1")]
use rsasl::{
    DiscardOnDrop, Property, Session,
    Step::{Done, NeedsMore},
    SASL,
};
#[cfg(feature = "rsasl1")]
use std::ffi::CString;
#[cfg(feature = "rsasl2")]
use {
    rsasl2::callback::{Context, Request, SessionCallback, SessionData},
    rsasl2::mechanism::Authentication,
    rsasl2::mechanisms::gssapi::properties::{GssSecurityLayer, GssService, SecurityLayer},
    rsasl2::mechanisms::gssapi::GSSAPI,
    rsasl2::prelude::*,
    rsasl2::property::{DigestMD5HashedPassword, Hostname, Password, Realm},
    rsasl2::registry::{distributed_slice, Matches, Named, Side, MECHANISMS},
};

use super::user::User;

const SASL_CALL_ID: i32 = -33;

#[cfg(feature = "rsasl2")]
static KERBEROS_MECHANISMS: &[Mechanism] = &[GSSAPI];
#[cfg(feature = "rsasl2")]
static TOKEN_MECHANISMS: &[Mechanism] = &[DIGEST_MD5];

static EMPTY: &[u8] = &[0u8; 0];

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

struct KerberosCallback {
    service: String,
    hostname: String,
}

#[cfg(feature = "rsasl2")]
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

struct TokenCallback {}

#[cfg(feature = "rsasl2")]
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

#[cfg(feature = "rsasl1")]
type SaslSession = DiscardOnDrop<Session<()>>;
#[cfg(feature = "rsasl2")]
type SaslSession = Session;

pub struct SaslRpcClient {
    stream: TcpStream,
    #[cfg(feature = "rsasl2")]
    session: Option<Arc<Mutex<SaslSession>>>,
    buffer: Bytes,
}

impl SaslRpcClient {
    pub fn create(stream: TcpStream) -> SaslRpcClient {
        SaslRpcClient {
            stream,
            #[cfg(feature = "rsasl2")]
            session: None,
            buffer: Bytes::new(),
        }
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

    fn send_sasl_message(&mut self, message: &RpcSaslProto) -> Result<()> {
        let header_buf = Self::create_request_header().encode_length_delimited_to_vec();
        let message_buf = message.encode_length_delimited_to_vec();
        let size = (header_buf.len() + message_buf.len()) as u32;

        self.stream.write(&size.to_be_bytes())?;
        self.stream.write(&header_buf)?;
        self.stream.write(&message_buf)?;
        self.stream.flush()?;

        Ok(())
    }

    fn read_response(&mut self) -> Result<RpcSaslProto> {
        let mut buf = [0u8; 4];
        self.stream.read_exact(&mut buf)?;

        let msg_length = u32::from_be_bytes(buf);

        let mut buf = BytesMut::zeroed(msg_length as usize);
        self.stream.read_exact(&mut buf)?;

        let mut bytes = buf.freeze();
        let rpc_response = RpcResponseHeaderProto::decode_length_delimited(&mut bytes).unwrap();
        println!("{:?}", rpc_response);

        match RpcStatusProto::from_i32(rpc_response.status).unwrap() {
            RpcStatusProto::Error => {
                todo!()
            }
            RpcStatusProto::Fatal => {
                todo!()
            }
            _ => (),
        }

        let sasl_response = RpcSaslProto::decode_length_delimited(&mut bytes).unwrap();
        Ok(sasl_response)
    }

    pub(crate) fn negotiate(&mut self) -> Result<AuthMethod> {
        let mut rpc_sasl = RpcSaslProto::default();
        rpc_sasl.state = SaslState::Negotiate as i32;

        self.send_sasl_message(&rpc_sasl)?;

        let mut done = false;
        let mut selected_method: Option<AuthMethod> = None;
        let mut session: Option<SaslSession> = None;
        while !done {
            let mut response: Option<RpcSaslProto> = None;
            let message = self.read_response()?;
            println!("{:?}", message);
            match SaslState::from_i32(message.state).unwrap() {
                SaslState::Negotiate => {
                    let (selected_auth, selected_session) = self.select_method(&message.auths)?;
                    session = selected_session;
                    selected_method = AuthMethod::parse(&selected_auth.method);

                    let token = if let Some(session) = session.as_mut() {
                        #[cfg(feature = "rsasl1")]
                        {
                            let challenge =
                                selected_auth.challenge.as_ref().map_or(EMPTY, |c| &c[..]);
                            println!("Stepping! {:?}", selected_auth);
                            let result = session.step(challenge).unwrap();
                            println!("Stepped! {:?}", result);
                            match result {
                                Done(buffer) => Some(buffer.to_vec()),
                                NeedsMore(buffer) => Some(buffer.to_vec()),
                            }
                        }
                        #[cfg(feature = "rsasl2")]
                        {
                            let mut cursor = Cursor::new(Vec::new());
                            session
                                .step(
                                    selected_auth.challenge.as_ref().map(|x| &x[..]),
                                    &mut cursor,
                                )
                                .unwrap();
                            Some(cursor.into_inner())
                        }
                    } else {
                        done = true;
                        None
                    };

                    let mut r = RpcSaslProto::default();
                    r.state = SaslState::Initiate as i32;
                    r.auths = Vec::from([selected_auth]);
                    r.token = token.or(Some(Vec::new()));
                    response = Some(r);
                }
                SaslState::Challenge => {
                    println!("Starting challenge: {:?}", message);
                    #[cfg(feature = "rsasl1")]
                    let token = {
                        let challenge =
                            Box::into_raw(Box::new(message.token.unwrap_or(Vec::new())));
                        println!("Passing challenge: {:?}", challenge);
                        let result = unsafe {
                            session
                                .as_mut()
                                .unwrap()
                                .step(challenge.as_ref().unwrap().as_slice())
                        };
                        println!("Got result");
                        println!("Got result {:?}", result);
                        match result.unwrap() {
                            Done(buffer) => Some(buffer.to_vec()),
                            NeedsMore(buffer) => Some(buffer.to_vec()),
                        }
                    };
                    #[cfg(feature = "rsasl2")]
                    let token = {
                        let mut cursor = Cursor::new(Vec::new());
                        session
                            .as_mut()
                            .unwrap()
                            .step(message.token.as_ref().map(|t| &t[..]), &mut cursor)
                            .unwrap();

                        Some(cursor.into_inner())
                    };

                    let mut r = RpcSaslProto::default();
                    r.state = SaslState::Response as i32;
                    r.token = token;
                    response = Some(r);
                }
                SaslState::Success => {
                    // let mut token = Cursor::new(Vec::new());
                    // self.session
                    //     .as_ref()
                    //     .unwrap()
                    //     .lock()
                    //     .unwrap()
                    //     .step(message.token.as_ref().map(|t| &t[..]), &mut token)
                    //     .unwrap();

                    // assert!(token.into_inner().is_empty());
                    done = true;
                }
                _ => todo!(),
            }

            if let Some(r) = response {
                println!("Sending SASL response {:?}", r);
                self.send_sasl_message(&r)?;
            }
        }

        #[cfg(feature = "rsasl2")]
        {
            self.session = session
                .filter(|x| x.has_security_layer())
                .map(|s| Arc::new(Mutex::new(s)));
        }

        println!(
            "Has security layer: {:?}",
            self.session
                .as_ref()
                .map(|s| s.lock().unwrap().has_security_layer())
        );

        Ok(selected_method.expect("SASL finished but no method selected"))
    }

    fn select_method(&mut self, auths: &Vec<SaslAuth>) -> Result<(SaslAuth, Option<SaslSession>)> {
        for auth in auths.iter() {
            match AuthMethod::parse(&auth.method) {
                Some(AuthMethod::SIMPLE) => {
                    return Ok((auth.clone(), None));
                }
                #[cfg(feature = "rsasl1")]
                Some(AuthMethod::KERBEROS) if User::get_kerberos_user().is_some() => {
                    let mut sasl = SASL::new_untyped().unwrap();
                    let mut session = sasl.client_start("GSSAPI").unwrap();

                    unsafe {
                        session.set_property(
                            Property::GSASL_HOSTNAME,
                            Box::into_raw(Box::new(
                                CString::new(auth.server_id())
                                    .unwrap()
                                    .as_bytes_with_nul()
                                    .to_vec(),
                            ))
                            .as_ref()
                            .unwrap(),
                        );
                        session.set_property(
                            Property::GSASL_SERVICE,
                            Box::into_raw(Box::new(
                                CString::new(auth.protocol())
                                    .unwrap()
                                    .as_bytes_with_nul()
                                    .to_vec(),
                            ))
                            .as_ref()
                            .unwrap(),
                        );
                    }
                    // self.session = Some(Arc::new(Mutex::new(session)));
                    return Ok((auth.clone(), Some(session)));
                }
                #[cfg(feature = "rsasl1")]
                Some(AuthMethod::TOKEN) if false => {
                    let mut sasl = SASL::new_untyped().unwrap();
                    let mut session = sasl.client_start("DIGEST-MD5").unwrap();
                    session.set_property(
                        Property::GSASL_HOSTNAME,
                        auth.server_id().to_string().as_bytes(),
                    );
                    session.set_property(
                        Property::GSASL_SERVICE,
                        auth.protocol().to_string().as_bytes(),
                    );
                    // self.session = Some(Arc::new(Mutex::new(session)));
                    return Ok((auth.clone(), Some(session)));
                }
                #[cfg(feature = "rsasl2")]
                Some(AuthMethod::KERBEROS) if User::get_kerberos_user().is_some() => {
                    let config = SASLConfig::builder()
                        .with_registry(Registry::with_mechanisms(KERBEROS_MECHANISMS))
                        .with_callback(KerberosCallback {
                            service: auth.protocol().to_string(),
                            hostname: auth.server_id().to_string(),
                        })
                        .unwrap();

                    let sasl = SASLClient::new(config);
                    let offered_mechs = [Mechname::parse(b"GSSAPI").unwrap()];

                    let session = sasl
                        .start_suggested_iter(offered_mechs)
                        .expect("no shared mechanism");

                    // self.session = Some(Arc::new(Mutex::new(session)));

                    return Ok((auth.clone(), Some(session)));
                }
                #[cfg(feature = "rsasl2")]
                Some(AuthMethod::TOKEN) if false => {
                    let config = SASLConfig::builder()
                        .with_registry(Registry::with_mechanisms(TOKEN_MECHANISMS))
                        .with_callback(TokenCallback {})
                        .unwrap();

                    let sasl = SASLClient::new(config);
                    let offered_mechs = [Mechname::parse(b"DIGEST-MD5").unwrap()];

                    let session = sasl
                        .start_suggested_iter(offered_mechs)
                        .expect("no shared mechanism");

                    return Ok((auth.clone(), Some(session)));
                }
                _ => (),
            }
        }
        Err(Error::new(
            ErrorKind::Unsupported,
            "No valid authentication method found.",
        ))
    }

    pub fn try_clone(&self) -> Result<SaslRpcClient> {
        let stream = self.stream.try_clone()?;
        let client = SaslRpcClient {
            stream,
            #[cfg(feature = "rsasl2")]
            session: self.session.as_ref().map(Arc::clone),
            buffer: Bytes::new(),
        };
        Ok(client)
    }
}

impl Read for SaslRpcClient {
    /// If the session uses security, we load the next SASL message if our buffer is empty,
    /// and then return the entire buffer up to the amount the client expects. We rely on
    /// read_exact to call this multiple times as needed to get data from multiple SASL messages.
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.session.is_some() {
            if !self.buffer.has_remaining() {
                let response = self.read_response()?;
                if response.state() != SaslState::Wrap {
                    todo!();
                }

                let mut writer = BytesMut::with_capacity(response.token().len()).writer();
                self.session
                    .as_ref()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .decode(response.token(), &mut writer)
                    .unwrap_or_else(|_| todo!());
                self.buffer = writer.into_inner().freeze();
            }
            let read_len = usize::min(buf.len(), self.buffer.remaining());
            self.buffer.copy_to_slice(&mut buf[0..read_len]);

            Ok(read_len)
        } else {
            self.stream.read(buf)
        }
    }
}

impl Write for SaslRpcClient {
    /// If we are using security, encodes the provided buffer and sends the SASL message.
    /// TODO: Respect max size for a SASL message
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.session.is_some() {
            let mut rpc_sasl = RpcSaslProto::default();
            rpc_sasl.state = SaslState::Wrap as i32;

            let mut writer = Vec::with_capacity(buf.len()).writer();
            self.session
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .encode(buf, &mut writer)
                .unwrap_or_else(|_| todo!());

            rpc_sasl.token = Some(writer.into_inner());

            self.send_sasl_message(&rpc_sasl)?;
            Ok(buf.len())
        } else {
            self.stream.write(buf)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

#[cfg(feature = "rsasl2")]
#[cfg_attr(feature = "registry_static", distributed_slice(MECHANISMS))]
pub static DIGEST_MD5: Mechanism = Mechanism::build(
    Mechname::const_new(b"DIGEST-MD5"),
    300,
    Some(|| Ok(Box::new(DigestMD5::default()))),
    None,
    Side::Server,
    |_| Some(Matches::<Select>::name()),
    |_| true,
);

#[cfg(feature = "rsasl2")]
struct Select;
#[cfg(feature = "rsasl2")]
impl Named for Select {
    fn mech() -> &'static Mechanism {
        &DIGEST_MD5
    }
}

struct DigestMD5 {}

impl Default for DigestMD5 {
    fn default() -> Self {
        Self {}
    }
}

#[cfg(feature = "rsasl2")]
impl Authentication for DigestMD5 {
    fn step(
        &mut self,
        session: &mut rsasl2::mechanism::MechanismData,
        input: Option<&[u8]>,
        writer: &mut dyn Write,
    ) -> std::result::Result<State, SessionError> {
        println!("{:?}", session);
        println!("{:?}", input);
        println!("{:?}", std::str::from_utf8(input.unwrap()).unwrap());
        todo!()
    }
}
