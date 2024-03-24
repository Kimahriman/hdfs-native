use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use base64::{engine::general_purpose, Engine as _};
use md5::Digest;
use once_cell::sync::Lazy;
use rand::Rng;
use regex::Regex;

use crate::{HdfsError, Result};

use super::{
    sasl::SaslSession,
    user::{Token, UserInfo},
};

static CHALLENGE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#",?([a-zA-Z0-9]+)=("([^"]+)"|([^,]+)),?"#).unwrap());
static RESPONSE_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new("rspauth=([a-f0-9]{32})").unwrap());

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
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

static SUPPORTED_QOPS: [Qop; 1] = [Qop::Auth];

fn choose_qop(options: Vec<Qop>) -> Result<Qop> {
    options
        .into_iter()
        .filter(|qop| SUPPORTED_QOPS.contains(qop))
        .max_by(|x, y| x.cmp(y))
        .ok_or(HdfsError::SASLError(
            "No valid QOP found for negotiation".to_string(),
        ))
}

fn h(s: impl AsRef<[u8]>) -> Digest {
    md5::compute(s.as_ref())
}

fn kd(k: impl AsRef<[u8]>, v: impl AsRef<[u8]>) -> Digest {
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
        println!("Parsing {}", decoded);
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

enum DigestState {
    Pending,
    Stepped(DigestContext),
    Completed(DigestContext),
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
    pub(super) fn new(service: String, hostname: String, token: &Token) -> Self {
        Self {
            auth_id: general_purpose::STANDARD.encode(&token.identifier),
            password: general_purpose::STANDARD.encode(&token.password),
            service,
            hostname,
            state: DigestState::Pending,
        }
    }

    fn compute(&self, ctx: &DigestContext, initial: bool) -> String {
        let x = format!("{:x}", h(self.a1(ctx)));
        let y = format!(
            "{}:{:08x}:{}:{}:{:x}",
            ctx.nonce,
            1,
            ctx.cnonce,
            ctx.qop,
            h(self.a2(initial, &ctx.qop))
        );

        format!("{:x}", kd(x, y))
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
}

impl SaslSession for DigestSaslSession {
    fn step(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)> {
        match core::mem::replace(&mut self.state, DigestState::Errored) {
            DigestState::Pending => {
                // First step, token is a challenge
                let challenge: Challenge = token.unwrap().try_into().unwrap();

                let qop = choose_qop(challenge.qop)?;

                let ctx = DigestContext {
                    nonce: challenge.nonce.clone(),
                    cnonce: gen_nonce(),
                    realm: challenge.realm.clone(),
                    qop: qop.clone(),
                };

                let response = self.compute(&ctx, true);

                let message = format!(
                    r#"username="{}",realm="{}",nonce="{}",cnonce="{}",nc={:08x},qop={},digest-uri="{}/{}",response={},charset=utf-8,cipher="3des""#,
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

                self.state = DigestState::Completed(ctx);
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
            DigestState::Completed(ctx) => ctx.qop.has_security_layer(),
            _ => false,
        }
    }

    fn encode(&mut self, _buf: &[u8]) -> crate::Result<Vec<u8>> {
        todo!()
    }

    fn decode(&mut self, _buf: &[u8]) -> crate::Result<Vec<u8>> {
        todo!()
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

        let session = DigestSaslSession::new("".to_string(), "default".to_string(), &token);
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
    #[ignore]
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

        let session = DigestSaslSession::new("".to_string(), "default".to_string(), &token);
        let ctx = DigestContext {
            nonce: "tm3kclm9F0JMECFNJi5xk/NaGgQ75ZOqb9/vCHt5".to_string(),
            cnonce: "kp49R9SjR4de6ynNgMwNwQ==".to_string(),
            realm: "default".to_string(),
            qop: Qop::Auth,
        };
        assert_eq!(
            &session.compute(&ctx, true),
            "621c0b2210a1d648983abd6bb43ff507"
        );
    }
}
