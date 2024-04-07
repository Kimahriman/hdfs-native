use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use base64::{engine::general_purpose, Engine as _};
use cbc::cipher::{BlockEncryptMut, KeyIvInit};
use cipher::BlockDecryptMut;
use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use once_cell::sync::Lazy;
use rand::Rng;
use regex::Regex;

use crate::{proto::hdfs::DataEncryptionKeyProto, HdfsError, Result};

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
            v => Err(HdfsError::SASLError(format!("Unknown qop value: {}", v))),
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

        write!(f, "{}", s)
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
    let mut gen = rand::thread_rng();
    let cnonce_bytes: Vec<u8> = (0..12).map(|_| gen.gen()).collect();
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
        let decoded = String::from_utf8(value).unwrap();
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
            .map(|mb| mb.parse().unwrap())
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
}

#[allow(clippy::large_enum_variant)]
enum DigestState {
    Pending,
    Stepped(DigestContext),
    Completed(Option<SecurityContext>),
    Errored,
}

pub(super) struct DigestSaslSession {
    auth_id: String,
    password: String,
    service: String,
    hostname: String,
    state: DigestState,
}

impl DigestSaslSession {
    pub(super) fn from_token(service: String, hostname: String, token: &Token) -> Self {
        Self {
            auth_id: general_purpose::STANDARD.encode(&token.identifier),
            password: general_purpose::STANDARD.encode(&token.password),
            service,
            hostname,
            state: DigestState::Pending,
        }
    }

    pub(super) fn from_encryption_key(
        service: String,
        hostname: String,
        encryption_key: &DataEncryptionKeyProto,
    ) -> Self {
        Self {
            auth_id: format!(
                "{} {} {}",
                encryption_key.key_id,
                encryption_key.block_pool_id,
                general_purpose::STANDARD.encode(&encryption_key.nonce)
            ),
            password: general_purpose::STANDARD.encode(&encryption_key.encryption_key),
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
        format!("{}:{}{}", authenticate, digest_uri, tail)
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

    pub(crate) fn supports_encryption(&self) -> bool {
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
                let challenge: Challenge = token.unwrap().try_into().unwrap();

                let qop = choose_qop(challenge.qop)?;
                let cipher = match (qop, &challenge.cipher) {
                    (Qop::AuthConf, Some(cipher)) => Some(choose_cipher(cipher)?),
                    (Qop::AuthConf, None) => {
                        return Err(HdfsError::SASLError(
                            "Confidentiality was chosen, but no cipher was provided".to_string(),
                        ))
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
                    message.push_str(&format!(r#",cipher="{}""#, c));
                }

                self.state = DigestState::Stepped(ctx);
                Ok((message.as_bytes().to_vec(), false))
            }
            DigestState::Stepped(ctx) => {
                let token_str = String::from_utf8(token.unwrap().to_vec()).map_err(|_| {
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
            DigestState::Completed(Some(ctx)) => {
                let mut mac = HmacMD5::new_from_slice(&ctx.integrity_keys.client).unwrap();
                mac.update(&ctx.seq_num.to_be_bytes());
                mac.update(buf);
                let hmac = mac.finalize().into_bytes();

                let mut message = if let Some(encryptor) = &mut ctx.encryptor {
                    // 10 bytes of HMAC, 8 byte block size
                    let padding_len = 8 - (buf.len() + 10) % 8;
                    let mut message =
                        [buf, &vec![padding_len as u8; padding_len], &hmac[..10]].concat();

                    let enc_block: &mut [u8] = message.as_mut();
                    let mut enc_bytes = 0;
                    while enc_bytes < enc_block.len() {
                        encryptor
                            .encrypt_block_mut((&mut enc_block[enc_bytes..enc_bytes + 8]).into());
                        enc_bytes += 8;
                    }
                    message
                } else {
                    [buf, &hmac[..10]].concat()
                };

                // let message = [&message, &[0, 1], &ctx.seq_num.to_be_bytes()].concat();
                message.extend(&[0, 1]);
                message.extend(ctx.seq_num.to_be_bytes());

                ctx.seq_num += 1;
                Ok(message)
            }
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
            DigestState::Completed(Some(ctx)) => {
                let (message, hmac) = if let Some(decryptor) = &mut ctx.decryptor {
                    // All but last 6 bytes are encrypted
                    let mut message = buf[..buf.len() - 6].to_vec();
                    let mut dec_bytes = 0;
                    while dec_bytes < message.len() {
                        decryptor
                            .decrypt_block_mut((&mut message[dec_bytes..dec_bytes + 8]).into());
                        dec_bytes += 8;
                    }

                    // Split HMAC off
                    let hmac = message.split_off(message.len() - 10);
                    // Remove padding at the end of the message
                    let _ = message.split_off(message.len() - *message.last().unwrap() as usize);

                    (message, hmac)
                } else {
                    // Not encrypted, last 16 bytes are 10 bytes of HMAC, 2 bytes of type, and 4 bytes of seqno
                    let mut message = buf[..buf.len() - 6].to_vec();
                    let hmac = message.split_off(message.len() - 10);
                    (message, hmac)
                };

                let mut mac = HmacMD5::new_from_slice(&ctx.integrity_keys.server).unwrap();
                mac.update(&buf[buf.len() - 4..]);
                mac.update(&message);

                mac.verify_truncated_left(&hmac)
                    .map_err(|_| HdfsError::SASLError("Integrity HMAC check failed".to_string()))?;

                Ok(message.to_vec())
            }
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
    use crate::security::{
        digest::{Challenge, DigestContext, Qop},
        user::Token,
    };

    use super::DigestSaslSession;

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
