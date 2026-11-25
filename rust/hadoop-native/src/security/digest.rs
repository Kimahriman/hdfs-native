//! Client side of the SASL DIGEST-MD5 mechanism ([RFC 2831]).
//!
//! Hadoop uses DIGEST-MD5 where the client and the server share a secret, such as the password
//! of a delegation token. [`DigestSaslSession`] does two things:
//!
//! - Authentication: [`SaslSession::step`] runs the exchange described below.
//! - Security layer: after authentication, [`SaslSession::encode`] and [`SaslSession::decode`]
//!   wrap and unwrap messages, if the negotiated QOP is `auth-int` or `auth-conf`.
//!
//! # Authentication
//!
//! Each side proves to the other that it knows the secret, without sending the secret:
//!
//! ```text
//! client                                                  server
//!   |                                                       |
//!   |<-------------------- 1. challenge --------------------|
//!   |      realm, nonce, offered QOPs, optional ciphers     |
//!   |                                                       |
//!   |--------------------- 2. response -------------------->|
//!   |      username, cnonce, chosen QOP, optional cipher,   |
//!   |      hash of the secret and both nonces               |
//!   |                                                       |
//!   |<--------------------- 3. rspauth ---------------------|
//!   |      another hash of the secret and both nonces       |
//! ```
//!
//! - nonce, cnonce: values that the server and the client choose anew for each exchange, so that a
//!   hash from an earlier exchange can't be reused.
//! - QOP (quality of protection): how messages are protected after authentication. `auth` adds no
//!   protection, `auth-int` adds an HMAC to each message, and `auth-conf` also encrypts each
//!   message. The client chooses the strongest QOP that the server offers.
//! - cipher: only with `auth-conf`. The server offers ciphers, and the client chooses `3des`.
//! - rspauth (response authentication): proves that the server knows the secret too.
//!
//! The first call to [`SaslSession::step`] takes the challenge and returns the response. The
//! second call checks the rspauth and completes the session.
//!
//! # Wrapped message format
//!
//! Sizes are in bytes. With `auth-int`:
//!
//! ```text
//! +---------+-----------+----------+-----------+
//! | message | HMAC (10) | type (2) | seqno (4) |
//! +---------+-----------+----------+-----------+
//! ```
//!
//! With `auth-conf`, the message, the padding and the HMAC are encrypted together with 3DES-CBC.
//! The cipher state carries over from one message to the next, so messages must be unwrapped in
//! the order the sender wrapped them:
//!
//! ```text
//! +---------+---------------+-----------+----------+-----------+
//! | message | padding (1-8) | HMAC (10) | type (2) | seqno (4) |
//! +---------+---------------+-----------+----------+-----------+
//! |<------------ encrypted ------------>|
//! ```
//!
//! - HMAC: the first 10 bytes of the HMAC-MD5 of the seqno and the message, keyed with the
//!   sender's integrity key. The padding is not part of the HMAC input.
//! - padding: only with `auth-conf`. Makes the encrypted part a multiple of 8 bytes. Each padding
//!   byte is the padding length.
//! - type: `[0, 1]`. [`SaslSession::decode`] does not check the type.
//! - seqno: the number of messages the sender has wrapped before this one, big-endian.
//!   [`SaslSession::decode`] does not check the seqno against the number of messages received.
//!
//! [RFC 2831]: https://www.rfc-editor.org/rfc/rfc2831

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use base64::{Engine as _, engine::general_purpose};
use cbc::cipher::{BlockModeDecrypt, BlockModeEncrypt, KeyIvInit};
use cipher::KeyInit;
use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use once_cell::sync::Lazy;
use rand::RngExt;
use regex::Regex;

use crate::{HadoopError as HdfsError, Result};

use super::{
    sasl::SaslSession,
    user::{Token, UserInfo},
};

type HmacMD5 = Hmac<Md5>;
type TdesCBCEnc = cbc::Encryptor<des::TdesEde2>;
type TdesCBCDec = cbc::Decryptor<des::TdesEde2>;

static CHALLENGE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#",?([a-zA-Z0-9]+)=("([^"]+)"|([^,]+)),?"#).unwrap());
static RESPONSE_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new("rspauth=([a-f0-9]{32})").unwrap());

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(u8)]
enum Qop {
    Auth = 0,
    AuthInt = 1,
    AuthConf = 2,
}

impl TryFrom<&str> for Qop {
    type Error = HdfsError;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "auth" => Ok(Self::Auth),
            "auth-int" => Ok(Self::AuthInt),
            "auth-conf" => Ok(Self::AuthConf),
            v => Err(HdfsError::SASLError(format!("Unknown qop value: {v}"))),
        }
    }
}

impl From<Qop> for String {
    fn from(value: Qop) -> Self {
        match value {
            Qop::Auth => "auth",
            Qop::AuthInt => "auth-int",
            Qop::AuthConf => "auth-conf",
        }
        .to_string()
    }
}

impl Display for Qop {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Qop::Auth => "auth",
            Qop::AuthInt => "auth-int",
            Qop::AuthConf => "auth-conf",
        };

        write!(f, "{s}")
    }
}

impl Qop {
    fn has_security_layer(&self) -> bool {
        !matches!(self, Qop::Auth)
    }
}

fn choose_qop(options: Vec<Qop>) -> Result<Qop> {
    options
        .into_iter()
        .max_by(|x, y| x.cmp(y))
        .ok_or(HdfsError::SASLError(
            "No valid QOP found for negotiation".to_string(),
        ))
}

fn choose_cipher(options: &[String]) -> Result<String> {
    // Only allow 3DES
    options
        .iter()
        .find(|o| *o == "3des")
        .cloned()
        .ok_or(HdfsError::SASLError(
            "No valid cipher found, only 3DES is supported".to_string(),
        ))
}

// We need to take 7 bytes of key and turn it into 8 odd-parity bytes
fn construct_des_key(key: &[u8]) -> Vec<u8> {
    assert_eq!(key.len(), 14);
    let mut output = Vec::with_capacity(16);

    let mut bytes = [0u8; 8];
    for byte_range in [0..7, 7..14] {
        key[byte_range]
            .iter()
            .zip(bytes.iter_mut())
            .for_each(|(k, b)| *b = *k);
        let bits = u64::from_be_bytes(bytes);

        for i in 0..8 {
            let mut byte = (bits >> ((8 - i) * 7)) as u8 & 0xFE;
            let ones = byte.count_ones();
            if ones % 2 == 1 {
                // Set odd parity bit
                byte |= 0x01;
            }
            output.push(byte);
        }
    }
    output
}

fn h(b: impl AsRef<[u8]>) -> Vec<u8> {
    let mut hasher = Md5::new();
    hasher.update(b.as_ref());
    hasher.finalize().to_vec()
}

fn kd(k: impl AsRef<[u8]>, v: impl AsRef<[u8]>) -> Vec<u8> {
    h([k.as_ref(), b":", v.as_ref()].concat())
}

fn gen_nonce() -> String {
    let mut rng = rand::rng();
    let cnonce_bytes: Vec<u8> = (0..12).map(|_| rng.random()).collect();
    general_purpose::STANDARD.encode(cnonce_bytes)
}

#[derive(Debug)]
#[allow(unused)]
pub(super) struct Challenge {
    realm: String,
    nonce: String,
    qop: Vec<Qop>,
    maxbuf: u32,
    cipher: Option<Vec<String>>,
}

impl TryFrom<&[u8]> for Challenge {
    type Error = HdfsError;

    fn try_from(value: &[u8]) -> std::prelude::v1::Result<Self, Self::Error> {
        value.to_vec().try_into()
    }
}

impl TryFrom<Vec<u8>> for Challenge {
    type Error = HdfsError;

    fn try_from(value: Vec<u8>) -> core::result::Result<Self, Self::Error> {
        let decoded = String::from_utf8(value).map_err(|_| {
            HdfsError::SASLError("DIGEST challenge is not a UTF-8 string".to_string())
        })?;

        let mut options: HashMap<String, String> = HashMap::new();
        for capture in CHALLENGE_PATTERN.captures_iter(&decoded) {
            let key = capture.get(1).unwrap().as_str().to_string();
            // Third group is quoted value, fourth is non-quoted value
            let value = capture
                .get(3)
                .or(capture.get(4))
                .unwrap()
                .as_str()
                .to_string();
            options.insert(key, value);
        }

        let realm = options.remove("realm").ok_or(HdfsError::SASLError(
            "No realm supplied in DIGEST challenge".to_string(),
        ))?;
        let nonce = options.remove("nonce").ok_or(HdfsError::SASLError(
            "No nonce supplied in DIGEST challenge".to_string(),
        ))?;
        let qop = options
            .remove("qop")
            .ok_or(HdfsError::SASLError(
                "No qop supplied in DIGEST challenge".to_string(),
            ))?
            .split(',')
            .map(|s| s.try_into())
            .collect::<Result<Vec<Qop>>>()?;
        let maxbuf: u32 = options
            .get("maxbuf")
            .map(|mb| mb.parse())
            .transpose()
            .map_err(|_| HdfsError::SASLError("Invalid maxbuf in DIGEST challenge".to_string()))?
            .unwrap_or(65536);
        let cipher = options
            .remove("cipher")
            .map(|c| c.split(',').map(|s| s.to_string()).collect());

        Ok(Self {
            realm,
            nonce,
            qop,
            maxbuf,
            cipher,
        })
    }
}

struct DigestContext {
    nonce: String,
    cnonce: String,
    realm: String,
    qop: Qop,
}

struct KeyPair {
    client: Vec<u8>,
    server: Vec<u8>,
}

struct SecurityContext {
    integrity_keys: KeyPair,
    encryptor: Option<TdesCBCEnc>,
    decryptor: Option<TdesCBCDec>,
    seq_num: u32,
}

impl SecurityContext {
    fn new(integrity_keys: KeyPair, encryption_keys: Option<KeyPair>) -> Self {
        let encryptor = encryption_keys.as_ref().map(|enc_keys| {
            TdesCBCEnc::new_from_slices(
                &construct_des_key(&enc_keys.client[..14]),
                &enc_keys.client[8..],
            )
            .unwrap()
        });
        let decryptor = encryption_keys.as_ref().map(|enc_keys| {
            TdesCBCDec::new_from_slices(
                &construct_des_key(&enc_keys.server[..14]),
                &enc_keys.server[8..],
            )
            .unwrap()
        });
        SecurityContext {
            integrity_keys,
            encryptor,
            decryptor,
            seq_num: 0,
        }
    }

    /// Wraps a message to the server.
    ///
    /// - `buf`: the `message` part.
    /// - Returns one message in the wrapped message format described in the module docs,
    ///   encrypted if the QOP is `auth-conf`.
    fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        // HMAC of the seqno and the message
        let mut mac = HmacMD5::new_from_slice(&self.integrity_keys.client).map_err(|_| {
            HdfsError::SASLError("Invalid DIGEST-MD5 client integrity key".to_string())
        })?;
        mac.update(&self.seq_num.to_be_bytes());
        mac.update(buf);
        let hmac = mac.finalize().into_bytes();

        // The body is [message][HMAC (10)], or with auth-conf, [message][padding (1-8)][HMAC (10)]
        let mut body = buf.to_vec();
        if self.encryptor.is_some() {
            // Each padding byte is the padding length
            let padding_len = 8 - (buf.len() + 10) % 8;
            body.resize(body.len() + padding_len, padding_len as u8);
        }
        body.extend(&hmac[..10]);

        // With auth-conf, the body is encrypted in blocks of 8 bytes
        if let Some(encryptor) = &mut self.encryptor {
            // The padding makes the body a multiple of 8 bytes, so there is no remainder
            let (blocks, _) = body.as_chunks_mut::<8>();
            for block in blocks {
                encryptor.encrypt_block(block.into());
            }
        }

        // The wrapped message is [body][type (2)][seqno (4)]
        body.extend([0, 1]);
        body.extend(self.seq_num.to_be_bytes());

        self.seq_num += 1;
        Ok(body)
    }

    /// Unwraps a message from the server.
    ///
    /// - `buf`: one message in the wrapped message format described in the module docs.
    /// - Returns the `message` part, decrypted if the QOP is `auth-conf`.
    /// - Fails if `buf` is malformed or the HMAC doesn't match.
    fn decode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        let malformed = || HdfsError::SASLError("Malformed DIGEST-MD5 message".to_string());

        // buf is [body][type (2)][seqno (4)]
        let (rest, seq_num) = buf.split_last_chunk::<4>().ok_or_else(malformed)?;
        let (body, _msg_type) = rest.split_last_chunk::<2>().ok_or_else(malformed)?;
        let mut body = body.to_vec();

        // With auth-conf, the body is encrypted in blocks of 8 bytes
        if let Some(decryptor) = &mut self.decryptor {
            let (blocks, remainder) = body.as_chunks_mut::<8>();
            if !remainder.is_empty() {
                return Err(HdfsError::SASLError(
                    "Invalid DIGEST-MD5 decryption block length".to_string(),
                ));
            }
            for block in blocks {
                decryptor.decrypt_block(block.into());
            }
        }

        // The body is [message][HMAC (10)], or with auth-conf, [message][padding (1-8)][HMAC (10)]
        let (mut message, hmac) = body.split_last_chunk::<10>().ok_or_else(malformed)?;
        if self.decryptor.is_some() {
            // The last byte of the padding is the padding length
            let padding = usize::from(*message.last().ok_or_else(malformed)?);
            let message_len = message.len().checked_sub(padding).ok_or_else(malformed)?;
            message = &message[..message_len];
        }

        let mut mac = HmacMD5::new_from_slice(&self.integrity_keys.server).map_err(|_| {
            HdfsError::SASLError("Invalid DIGEST-MD5 server integrity key".to_string())
        })?;
        mac.update(seq_num);
        mac.update(message);
        mac.verify_truncated_left(hmac)
            .map_err(|_| HdfsError::SASLError("Integrity HMAC check failed".to_string()))?;

        body.truncate(message.len());
        Ok(body)
    }
}

#[allow(clippy::large_enum_variant)]
enum DigestState {
    Pending,
    Stepped(DigestContext),
    Completed(Option<SecurityContext>),
    Errored,
}

pub struct DigestSaslSession {
    auth_id: String,
    password: String,
    service: String,
    hostname: String,
    state: DigestState,
}

impl DigestSaslSession {
    pub fn from_token(service: String, hostname: String, token: &Token) -> Self {
        Self {
            auth_id: general_purpose::STANDARD.encode(&token.identifier),
            password: general_purpose::STANDARD.encode(&token.password),
            service,
            hostname,
            state: DigestState::Pending,
        }
    }

    /// Create a DIGEST-MD5 session from already encoded credentials.
    pub fn new(service: String, hostname: String, auth_id: String, password: String) -> Self {
        Self {
            auth_id,
            password,
            service,
            hostname,
            state: DigestState::Pending,
        }
    }

    fn compute(&self, ctx: &DigestContext, initial: bool) -> String {
        let x = hex::encode(h(self.a1(ctx)));
        let y = format!(
            "{}:{:08x}:{}:{}:{}",
            ctx.nonce,
            1,
            ctx.cnonce,
            ctx.qop,
            hex::encode(h(self.a2(initial, &ctx.qop)))
        );

        hex::encode(kd(x, y))
    }

    fn a1(&self, ctx: &DigestContext) -> Vec<u8> {
        let hashed = h([
            self.auth_id.as_bytes(),
            b":",
            ctx.realm.as_bytes(),
            b":",
            self.password.as_bytes(),
        ]
        .concat());
        [
            hashed.as_slice(),
            b":",
            ctx.nonce.as_bytes(),
            b":",
            ctx.cnonce.as_bytes(),
        ]
        .concat()
    }

    fn a2(&self, initial: bool, qop: &Qop) -> String {
        let digest_uri = format!("{}/{}", self.service, self.hostname);
        let authenticate = if initial { "AUTHENTICATE" } else { "" };
        let tail = if qop.has_security_layer() {
            ":00000000000000000000000000000000"
        } else {
            ""
        };
        format!("{authenticate}:{digest_uri}{tail}")
    }

    fn integrity_keys(&self, ctx: &DigestContext) -> KeyPair {
        let kic = h([
            &h(self.a1(ctx))[..],
            b"Digest session key to client-to-server signing key magic constant",
        ]
        .concat());
        let kis = h([
            &h(self.a1(ctx))[..],
            b"Digest session key to server-to-client signing key magic constant",
        ]
        .concat());

        KeyPair {
            client: kic,
            server: kis,
        }
    }

    fn confidentiality_keys(&self, ctx: &DigestContext) -> KeyPair {
        let kic = h([
            &h(self.a1(ctx))[..],
            b"Digest H(A1) to client-to-server sealing key magic constant",
        ]
        .concat());
        let kis = h([
            &h(self.a1(ctx))[..],
            b"Digest H(A1) to server-to-client sealing key magic constant",
        ]
        .concat());

        KeyPair {
            client: kic,
            server: kis,
        }
    }

    pub fn supports_encryption(&self) -> bool {
        match &self.state {
            DigestState::Stepped(ctx) => matches!(ctx.qop, Qop::AuthConf),
            DigestState::Completed(ctx) => ctx.as_ref().is_some_and(|c| c.encryptor.is_some()),
            _ => false,
        }
    }
}

impl SaslSession for DigestSaslSession {
    fn step(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)> {
        match core::mem::replace(&mut self.state, DigestState::Errored) {
            DigestState::Pending => {
                // First step, token is a challenge
                let challenge: Challenge = token
                    .ok_or_else(|| {
                        HdfsError::SASLError("No DIGEST challenge from server".to_string())
                    })?
                    .try_into()?;

                let qop = choose_qop(challenge.qop)?;
                let cipher = match (qop, &challenge.cipher) {
                    (Qop::AuthConf, Some(cipher)) => Some(choose_cipher(cipher)?),
                    (Qop::AuthConf, None) => {
                        return Err(HdfsError::SASLError(
                            "Confidentiality was chosen, but no cipher was provided".to_string(),
                        ));
                    }
                    _ => None,
                };

                let ctx = DigestContext {
                    nonce: challenge.nonce.clone(),
                    cnonce: gen_nonce(),
                    realm: challenge.realm.clone(),
                    qop,
                };

                let response = self.compute(&ctx, true);

                let mut message = format!(
                    r#"username="{}",realm="{}",nonce="{}",cnonce="{}",nc={:08x},qop={},digest-uri="{}/{}",response={},charset=utf-8"#,
                    self.auth_id,
                    challenge.realm,
                    ctx.nonce,
                    ctx.cnonce,
                    1,
                    qop,
                    self.service,
                    self.hostname,
                    response
                );
                if let Some(c) = cipher {
                    message.push_str(&format!(r#",cipher="{c}""#));
                }

                self.state = DigestState::Stepped(ctx);
                Ok((message.as_bytes().to_vec(), false))
            }
            DigestState::Stepped(ctx) => {
                let token = token.ok_or_else(|| {
                    HdfsError::SASLError("No DIGEST response from server".to_string())
                })?;

                let token_str = String::from_utf8(token.to_vec()).map_err(|_| {
                    HdfsError::SASLError("Failed to parse token as UTF-8 string".to_string())
                })?;

                if let Some(captures) = RESPONSE_PATTERN.captures(&token_str) {
                    let rspauth = captures.get(1).unwrap();
                    if rspauth.as_str() != self.compute(&ctx, false) {
                        return Err(HdfsError::SASLError(
                            "rspauth from server did not match".to_string(),
                        ));
                    }
                } else {
                    return Err(HdfsError::SASLError(
                        "Message from server did not contain rspauth".to_string(),
                    ));
                }

                self.state = match ctx.qop {
                    Qop::Auth => DigestState::Completed(None),
                    Qop::AuthInt => DigestState::Completed(Some(SecurityContext::new(
                        self.integrity_keys(&ctx),
                        None,
                    ))),
                    Qop::AuthConf => DigestState::Completed(Some(SecurityContext::new(
                        self.integrity_keys(&ctx),
                        Some(self.confidentiality_keys(&ctx)),
                    ))),
                };
                Ok((Vec::new(), true))
            }
            DigestState::Completed(_) => Err(HdfsError::SASLError(
                "Failed to step, DIGEST-MD5 handshake already complete".to_string(),
            )),
            DigestState::Errored => Err(HdfsError::SASLError(
                "Failed to step, DIGEST-MD5 authentication failed".to_string(),
            )),
        }
    }

    fn has_security_layer(&self) -> bool {
        match &self.state {
            DigestState::Completed(ctx) => ctx.is_some(),
            _ => false,
        }
    }

    fn encode(&mut self, buf: &[u8]) -> crate::Result<Vec<u8>> {
        match &mut self.state {
            DigestState::Completed(Some(ctx)) => ctx.encode(buf),
            DigestState::Completed(None) => Err(HdfsError::SASLError(
                "QOP doesn't support security layer".to_string(),
            )),
            _ => Err(HdfsError::SASLError(
                "SASL negotiation not complete, can't encode message".to_string(),
            )),
        }
    }

    fn decode(&mut self, buf: &[u8]) -> crate::Result<Vec<u8>> {
        match &mut self.state {
            DigestState::Completed(Some(ctx)) => ctx.decode(buf),
            DigestState::Completed(None) => Err(HdfsError::SASLError(
                "QOP doesn't support security layer".to_string(),
            )),
            _ => Err(HdfsError::SASLError(
                "SASL negotiation not complete, can't decode message".to_string(),
            )),
        }
    }

    fn get_user_info(&self) -> crate::Result<super::user::UserInfo> {
        // The token has all the info
        Ok(UserInfo {
            real_user: None,
            effective_user: None,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::HadoopError;
    use crate::security::{
        digest::{Challenge, DigestContext, DigestState, KeyPair, Qop, SecurityContext},
        sasl::SaslSession,
        user::Token,
    };

    use super::DigestSaslSession;

    fn session(state: DigestState) -> DigestSaslSession {
        DigestSaslSession {
            state,
            ..DigestSaslSession::new(
                "hdfs".to_string(),
                "0".to_string(),
                "user".to_string(),
                "password".to_string(),
            )
        }
    }

    /// Client and server keys are equal, so the session can decode its own messages
    fn established_session(encrypted: bool) -> DigestSaslSession {
        let keys = || KeyPair {
            client: (0..16).collect(),
            server: (0..16).collect(),
        };
        session(DigestState::Completed(Some(SecurityContext::new(
            keys(),
            encrypted.then(keys),
        ))))
    }

    #[test]
    fn test_invalid_challenge() {
        let challenges: [Option<&[u8]>; 6] = [
            None,
            Some(&[0xff, 0xfe]),
            Some(br#"nonce="abc",qop="auth""#),
            Some(br#"realm="default",qop="auth""#),
            Some(br#"realm="default",nonce="abc""#),
            Some(br#"realm="default",nonce="abc",qop="auth",maxbuf=large"#),
        ];

        for challenge in challenges {
            let result = session(DigestState::Pending).step(challenge);
            assert!(
                matches!(result, Err(HadoopError::SASLError(_))),
                "{challenge:?}"
            );
        }
    }

    #[test]
    fn test_invalid_server_response() {
        for response in [None, Some(&[0xff, 0xfe][..])] {
            let mut session = session(DigestState::Stepped(DigestContext {
                nonce: "nonce".to_string(),
                cnonce: "cnonce".to_string(),
                realm: "default".to_string(),
                qop: Qop::Auth,
            }));
            assert!(
                matches!(session.step(response), Err(HadoopError::SASLError(_))),
                "{response:?}"
            );
        }
    }

    #[test]
    fn test_encode_decode_round_trip() {
        for encrypted in [false, true] {
            for len in 0..=17 {
                let message: Vec<u8> = (0..len).collect();
                let mut session = established_session(encrypted);

                let encoded = session.encode(&message).unwrap();
                assert_eq!(
                    session.decode(&encoded).unwrap(),
                    message,
                    "{encrypted} {len}"
                );
            }
        }
    }

    #[test]
    fn test_decode_truncated() {
        for encrypted in [false, true] {
            let encoded = established_session(encrypted).encode(b"message").unwrap();

            for end in 0..encoded.len() {
                let result = established_session(encrypted).decode(&encoded[..end]);
                assert!(
                    matches!(result, Err(HadoopError::SASLError(_))),
                    "{encrypted} {end}"
                );
            }
        }
    }

    #[test]
    fn test_decode_corrupted() {
        for encrypted in [false, true] {
            let encoded = established_session(encrypted).encode(b"message").unwrap();

            // Every byte except the 2 type bytes, which are not checked.
            // With encryption, flipping byte 5 sets the padding length (byte 13) to 0xf8 via CBC.
            let type_bytes = encoded.len() - 6..encoded.len() - 4;
            for index in (0..encoded.len()).filter(|i| !type_bytes.contains(i)) {
                let mut corrupted = encoded.clone();
                corrupted[index] ^= 0xff;
                let result = established_session(encrypted).decode(&corrupted);
                assert!(
                    matches!(result, Err(HadoopError::SASLError(_))),
                    "{encrypted} {index}"
                );
            }
        }
    }

    #[test]
    fn test_challenge_parse() {
        let token = vec![
            114u8, 101, 97, 108, 109, 61, 34, 100, 101, 102, 97, 117, 108, 116, 34, 44, 110, 111,
            110, 99, 101, 61, 34, 81, 117, 76, 51, 98, 121, 86, 111, 75, 83, 48, 99, 77, 47, 77,
            52, 98, 57, 47, 68, 110, 50, 80, 97, 48, 115, 47, 69, 56, 72, 83, 106, 88, 74, 87, 55,
            88, 80, 105, 109, 34, 44, 113, 111, 112, 61, 34, 97, 117, 116, 104, 45, 99, 111, 110,
            102, 44, 97, 117, 116, 104, 34, 44, 99, 104, 97, 114, 115, 101, 116, 61, 117, 116, 102,
            45, 56, 44, 99, 105, 112, 104, 101, 114, 61, 34, 51, 100, 101, 115, 44, 114, 99, 52,
            44, 100, 101, 115, 44, 114, 99, 52, 45, 53, 54, 44, 114, 99, 52, 45, 52, 48, 34, 44,
            97, 108, 103, 111, 114, 105, 116, 104, 109, 61, 109, 100, 53, 45, 115, 101, 115, 115,
        ];

        let challenge: Challenge = token.try_into().unwrap();
        assert_eq!(challenge.realm, "default");
        assert_eq!(challenge.qop, vec![Qop::AuthConf, Qop::Auth]);
        assert_eq!(
            challenge
                .cipher
                .as_ref()
                .map(|c| c.iter().map(String::as_ref).collect()),
            Some(vec!["3des", "rc4", "des", "rc4-56", "rc4-40"])
        );
    }

    #[test]
    fn test_digest_step_authentication() {
        let token = Token {
            alias: "127.0.0.1:9000".to_string(),
            identifier: vec![
                0, 26, 104, 100, 102, 115, 47, 108, 111, 99, 97, 108, 104, 111, 115, 116, 64, 69,
                88, 65, 77, 80, 76, 69, 46, 67, 79, 77, 0, 0, 138, 1, 142, 109, 254, 210, 147, 138,
                1, 142, 146, 11, 86, 147, 1, 2,
            ],
            password: vec![
                14, 15, 143, 95, 134, 65, 129, 9, 34, 197, 119, 3, 114, 115, 76, 203, 148, 2, 171,
                78,
            ],
            kind: "HDFS_DELEGATION_TOKEN".to_string(),
            service: "127.0.0.1:9000".to_string(),
        };

        let session = DigestSaslSession::from_token("".to_string(), "default".to_string(), &token);
        let ctx = DigestContext {
            nonce: "A+DoU3+eajz9Ei11Ib0S9CUKLyLPh0qFJbwn1/OZ".to_string(),
            cnonce: "UGP4ejVb7M54KO4yqDwCsA==".to_string(),
            realm: "default".to_string(),
            qop: Qop::Auth,
        };
        assert_eq!(
            &session.compute(&ctx, true),
            "6dca7e74e7ea760a91758106fa7c885c"
        );
    }

    #[test]
    fn test_digest_step_privacy() {
        let token = Token {
            alias: "127.0.0.1:9000".to_string(),
            identifier: vec![
                0, 26, 104, 100, 102, 115, 47, 108, 111, 99, 97, 108, 104, 111, 115, 116, 64, 69,
                88, 65, 77, 80, 76, 69, 46, 67, 79, 77, 0, 0, 138, 1, 142, 110, 11, 155, 153, 138,
                1, 142, 146, 24, 31, 153, 1, 2,
            ],
            password: vec![
                65, 93, 14, 143, 178, 108, 30, 148, 166, 96, 31, 211, 51, 100, 5, 190, 193, 17,
                230, 5,
            ],
            kind: "HDFS_DELEGATION_TOKEN".to_string(),
            service: "127.0.0.1:9000".to_string(),
        };

        let session = DigestSaslSession::from_token("".to_string(), "default".to_string(), &token);
        let ctx = DigestContext {
            nonce: "tm3kclm9F0JMECFNJi5xk/NaGgQ75ZOqb9/vCHt5".to_string(),
            cnonce: "kp49R9SjR4de6ynNgMwNwQ==".to_string(),
            realm: "default".to_string(),
            qop: Qop::Auth,
        };
        assert_eq!(
            &session.compute(&ctx, true),
            "260f34cc6e484ede6277cb9c37672d07"
        );
    }
}
