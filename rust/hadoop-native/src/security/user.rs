use bytes::{Buf, Bytes};
use chrono::Utc;
use log::debug;
use prost::Message;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use whoami::username;

use crate::HadoopError as HdfsError;
use crate::proto::common::CredentialsProto;
use crate::proto::common::TokenProto;
use crate::security::gssapi::GssapiSession;

const HADOOP_USER_NAME: &str = "HADOOP_USER_NAME";
const HADOOP_PROXY_USER: &str = "HADOOP_PROXY_USER";
const HADOOP_TOKEN_FILE_LOCATION: &str = "HADOOP_TOKEN_FILE_LOCATION";
const TOKEN_STORAGE_MAGIC: &[u8] = "HDTS".as_bytes();

#[derive(Debug)]
#[allow(dead_code)]
struct TokenIdentifier {
    owner: String,
    renewer: String,
    real_user: String,
    issue_date: i64,
    max_date: i64,
    sequence_number: i32,
    master_key_id: i32,
}

impl TryFrom<Vec<u8>> for TokenIdentifier {
    type Error = HdfsError;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let mut buf = Bytes::from(value);
        let version = buf.get_u8();
        if version != 0 {
            panic!();
        }

        let owner = parse_vint_string(&mut buf)?;
        let renewer = parse_vint_string(&mut buf)?;
        let real_user = parse_vint_string(&mut buf)?;
        let issue_date = parse_vlong(&mut buf);
        let max_date = parse_vlong(&mut buf);
        let sequence_number = parse_vint(&mut buf);
        let master_key_id = parse_vint(&mut buf);

        Ok(TokenIdentifier {
            owner,
            renewer,
            real_user,
            issue_date,
            max_date,
            sequence_number,
            master_key_id,
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Token {
    pub(crate) alias: String,
    pub(crate) identifier: Vec<u8>,
    pub(crate) password: Vec<u8>,
    pub(crate) kind: String,
    pub(crate) service: String,
}

impl Token {
    fn load_tokens() -> Vec<Self> {
        match env::var(HADOOP_TOKEN_FILE_LOCATION).map(PathBuf::from) {
            Ok(path) if path.exists() => Self::read_token_file(&path).ok().unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn read_token_file(path: &Path) -> std::io::Result<Vec<Self>> {
        let mut content = Bytes::from(fs::read(path)?);

        let magic = content.copy_to_bytes(4);

        if magic != TOKEN_STORAGE_MAGIC {
            debug!("Invalid token in magic file");
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid token in magic file",
            ));
        }

        let format = content.copy_to_bytes(1);

        if format[0] == 0 {
            Self::parse_writable(&mut content)
        } else if format[0] == 1 {
            Self::parse_protobuf(content)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown token format",
            ))
        }
    }

    fn parse_writable(reader: &mut impl Buf) -> io::Result<Vec<Token>> {
        let token_count = parse_vlong(reader);
        let mut tokens = Vec::<Token>::with_capacity(token_count as usize);

        for _ in 0..token_count {
            let alias_length = parse_vlong(reader);
            let alias = String::from_utf8(reader.copy_to_bytes(alias_length as usize).to_vec())
                .map_err(|_| io::Error::other("Failed to parse token".to_string()))?;

            let identifier_length = parse_vlong(reader);
            let identifier = reader.copy_to_bytes(identifier_length as usize).to_vec();

            let password_length = parse_vlong(reader);
            let password = reader.copy_to_bytes(password_length as usize).to_vec();

            let kind = parse_vint_string(reader)?;
            let service = parse_vint_string(reader)?;

            tokens.push(Token {
                alias,
                identifier,
                password,
                kind,
                service,
            })
        }

        Ok(tokens)
    }

    fn parse_protobuf(reader: impl Buf) -> io::Result<Vec<Token>> {
        let storage = CredentialsProto::decode_length_delimited(reader)?;

        let tokens: Vec<Token> = storage
            .tokens
            .into_iter()
            .flat_map(|mut credential| {
                credential.token.take().into_iter().map(move |token| Token {
                    alias: credential.alias.clone(),
                    identifier: token.identifier,
                    password: token.password,
                    kind: token.kind,
                    service: token.service,
                })
            })
            .collect();

        Ok(tokens)
    }
}

impl From<TokenProto> for Token {
    fn from(value: TokenProto) -> Self {
        Self {
            alias: String::new(),
            identifier: value.identifier,
            password: value.password,
            kind: value.kind,
            service: value.service,
        }
    }
}

/// Adapted from WritableUtils class in Hadoop
fn parse_vlong(reader: &mut impl Buf) -> i64 {
    let first_byte = reader.get_i8();

    let length = if first_byte >= -112 {
        1
    } else if first_byte < -120 {
        -119 - first_byte
    } else {
        -111 - first_byte
    };

    if length == 1 {
        return first_byte as i64;
    }

    let mut i = 0i64;
    for _ in 0..length - 1 {
        let b = reader.get_u8();
        i <<= 8;
        i |= b as i64;
    }

    let is_negative = first_byte < -120 || (-112..0).contains(&first_byte);

    if is_negative { i ^ -1 } else { i }
}

fn parse_vint(reader: &mut impl Buf) -> i32 {
    // Same method as a long, but it should just be in the int range
    let n = parse_vlong(reader);
    assert!(n > i32::MIN as i64 && n < i32::MAX as i64);
    n as i32
}

fn parse_string(reader: &mut impl Buf, length: i32) -> io::Result<String> {
    String::from_utf8(reader.copy_to_bytes(length as usize).to_vec())
        .map_err(|_| io::Error::other("Failed to parse string from writable".to_string()))
}

/// Parse a string prefixed with the length as an int
#[allow(dead_code)]
fn parse_int_string(reader: &mut impl Buf) -> io::Result<Option<String>> {
    let length = reader.get_i32();
    let value = if length == -1 {
        None
    } else {
        Some(parse_string(reader, length)?)
    };
    Ok(value)
}

/// Parse a string prefixed with the length as a vint
fn parse_vint_string(reader: &mut impl Buf) -> io::Result<String> {
    let length = parse_vint(reader);
    parse_string(reader, length)
}

#[derive(Debug)]
pub struct UserInfo {
    pub real_user: Option<String>,
    pub effective_user: Option<String>,
}

#[derive(Debug)]
pub struct User {
    #[allow(dead_code)]
    tokens: Vec<Token>,
}

impl User {
    pub(crate) fn get() -> Self {
        let tokens = Token::load_tokens();
        User { tokens }
    }

    pub(crate) fn get_token(&self, kind: &str, service: &str) -> Option<&Token> {
        self.tokens
            .iter()
            .filter(|t| t.kind == kind && t.service == service)
            .find(|t| {
                // Ignore any tokens that are set to expire in the next 60 seconds
                let token_identifier: TokenIdentifier = t.identifier.clone().try_into().unwrap();
                debug!("Token Identifier: {:?}", token_identifier);
                token_identifier.max_date > Utc::now().timestamp_millis() + 60000
            })
    }

    pub fn get_user_info_from_principal(
        principal: &str,
        effective_user: Option<String>,
    ) -> UserInfo {
        UserInfo {
            real_user: Some(User::get_user_from_principal(principal)),
            effective_user: effective_user.or_else(|| env::var(HADOOP_PROXY_USER).ok()),
        }
    }

    pub(crate) fn get_simple_user(effective_user: Option<String>) -> UserInfo {
        UserInfo {
            real_user: None,
            effective_user: Some(
                effective_user
                    .or_else(|| env::var(HADOOP_USER_NAME).ok())
                    .unwrap_or_else(|| username().unwrap_or_else(|_| "unknown".to_string())),
            ),
        }
    }

    pub fn get_user_info(effective_user: Option<String>, security_enabled: bool) -> UserInfo {
        if security_enabled && let Ok(principal) = GssapiSession::get_default_principal() {
            let user_info = User::get_user_info_from_principal(&principal, effective_user);
            return user_info;
        }

        User::get_simple_user(effective_user)
    }

    fn get_user_from_principal(principal: &str) -> String {
        // If there's a /, take the part before it.
        if let Some(index) = principal.find('/') {
            principal[0..index].to_string()
        } else if let Some(index) = principal.find('@') {
            principal[0..index].to_string()
        } else {
            principal.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_get_user_info_uses_provided_effective_user_for_simple_auth() {
        let user_info = User::get_user_info(Some("alice".to_string()), false);

        assert_eq!(user_info.real_user, None);
        assert_eq!(user_info.effective_user.as_deref(), Some("alice"));
    }

    #[test]
    fn test_load_writable_token() {
        use base64::{Engine as _, engine::general_purpose};
        let b64_token = "SERUUwABDjEyNy4wLjAuMTo5MDAwLgAaaGRmcy9sb2NhbGhvc3RARVhBTVBMRS5DT00AAIoBiX/hghSKAYmj7gYUAQIUadF4ni3ObKqU8niv40WBFsGhFm4VSERGU19ERUxFR0FUSU9OX1RPS0VODjEyNy4wLjAuMTo5MDAwAA==";
        let mut token_file = NamedTempFile::new().unwrap();
        token_file
            .write_all(
                general_purpose::STANDARD
                    .decode(b64_token)
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
        token_file.flush().unwrap();

        let tokens = Token::read_token_file(token_file.path()).unwrap();

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].kind, "HDFS_DELEGATION_TOKEN");
        assert_eq!(tokens[0].service, "127.0.0.1:9000");

        let token_identifier: TokenIdentifier = tokens[0].identifier.clone().try_into().unwrap();
        assert_eq!(token_identifier.max_date, 1690672432660)
    }

    #[test]
    fn test_load_protobuf_token() {
        use base64::{Engine as _, engine::general_purpose};
        let b64_token = "SERUUwGBAQp/Cg5sb2NhbGhvc3Q6OTAwMBJtCi4AGmhkZnMvbG9jYWxob3N0QEVYQU1QTEUuQ09NAACKAYiiTtt9igGIxltffQECEhQoROcYNFMxMuoK9UHlAna6ZmhQSBoVSERGU19ERUxFR0FUSU9OX1RPS0VOIg4xMjcuMC4wLjE6OTAwMA==";
        let mut token_file = NamedTempFile::new().unwrap();
        token_file
            .write_all(
                general_purpose::STANDARD
                    .decode(b64_token)
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
        token_file.flush().unwrap();

        let tokens = Token::read_token_file(token_file.path()).unwrap();

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].kind, "HDFS_DELEGATION_TOKEN");
        assert_eq!(tokens[0].service, "127.0.0.1:9000");

        let token_identifier: TokenIdentifier = tokens[0].identifier.clone().try_into().unwrap();
        assert_eq!(token_identifier.max_date, 1686955057021)
    }
}
