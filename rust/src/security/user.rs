use bytes::{Buf, Bytes};
use log::debug;
use prost::Message;
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

use users::get_current_username;

use crate::proto::common::CredentialsProto;
use crate::proto::common::TokenProto;
use crate::proto::hdfs::AccessModeProto;
use crate::proto::hdfs::BlockTokenSecretProto;
use crate::proto::hdfs::StorageTypeProto;
use crate::Result;

const HADOOP_USER_NAME: &str = "HADOOP_USER_NAME";
#[cfg(feature = "kerberos")]
const HADOOP_PROXY_USER: &str = "HADOOP_PROXY_USER";
const HADOOP_TOKEN_FILE_LOCATION: &str = "HADOOP_TOKEN_FILE_LOCATION";
const TOKEN_STORAGE_MAGIC: &[u8] = "HDTS".as_bytes();

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct BlockTokenIdentifier {
    pub expiry_date: u64,
    pub key_id: u32,
    pub user_id: String,
    pub block_pool_id: String,
    pub block_id: u64,
    pub modes: Vec<i32>,
    pub storage_types: Vec<i32>,
    pub storage_ids: Vec<String>,
    pub handshake_secret: Vec<u8>,
}

#[allow(dead_code)]
impl BlockTokenIdentifier {
    fn parse_writable(reader: &mut impl Buf) -> Result<Self> {
        let expiry_date = parse_vlong(reader) as u64;
        let key_id = parse_vint(reader) as u32;
        let user_id = parse_int_string(reader)?.unwrap();
        let block_pool_id = parse_int_string(reader)?.unwrap();
        let block_id = parse_vlong(reader) as u64;

        let mut modes: Vec<i32> = Vec::new();
        let mut storage_types: Vec<i32> = Vec::new();
        let mut storage_ids: Vec<String> = Vec::new();

        // The rest of the fields may or may not be present depending on HDFS version
        if reader.has_remaining() {
            // Modes
            for _ in 0..parse_vint(reader) {
                if let Some(mode) = AccessModeProto::from_str_name(&parse_vint_string(reader)?) {
                    modes.push(mode as i32);
                }
            }
        }

        if reader.has_remaining() {
            // Storage Types
            for _ in 0..parse_vint(reader) {
                if let Some(storage_type) =
                    StorageTypeProto::from_str_name(&parse_vint_string(reader)?)
                {
                    storage_types.push(storage_type as i32);
                }
            }
        }

        if reader.has_remaining() {
            // Storage IDs
            for _ in 0..parse_vint(reader) {
                if let Some(storage_id) = parse_int_string(reader)? {
                    storage_ids.push(storage_id);
                }
            }
        }

        let handshake_secret = if reader.has_remaining() {
            let handshake_secret_len = parse_vint(reader) as usize;
            reader.copy_to_bytes(handshake_secret_len).to_vec()
        } else {
            vec![]
        };

        Ok(BlockTokenIdentifier {
            expiry_date,
            key_id,
            user_id,
            block_pool_id,
            block_id,
            modes,
            storage_types,
            storage_ids,
            handshake_secret,
        })
    }

    fn parse_protobuf(identifier: &[u8]) -> Result<Self> {
        let secret_proto = BlockTokenSecretProto::decode(identifier)?;

        Ok(BlockTokenIdentifier {
            expiry_date: secret_proto.expiry_date(),
            key_id: secret_proto.key_id(),
            user_id: secret_proto.user_id().to_string(),
            block_pool_id: secret_proto.block_pool_id().to_string(),
            block_id: secret_proto.block_id(),
            modes: secret_proto.modes.clone(),
            storage_types: secret_proto.storage_types.clone(),
            storage_ids: secret_proto.storage_ids.clone(),
            handshake_secret: secret_proto.handshake_secret().to_vec(),
        })
    }

    pub(crate) fn from_identifier(identifier: &[u8]) -> Result<Self> {
        if identifier[0] == 0 || identifier[0] > 127 {
            let mut content = Bytes::from(identifier.to_vec());
            Self::parse_writable(&mut content)
        } else {
            Self::parse_protobuf(identifier)
        }
    }
}

#[derive(Debug)]
pub struct Token {
    pub alias: String,
    pub identifier: Vec<u8>,
    pub password: Vec<u8>,
    pub kind: String,
    pub service: String,
}

impl Token {
    fn load_tokens() -> Vec<Self> {
        match env::var(HADOOP_TOKEN_FILE_LOCATION).map(PathBuf::from) {
            Ok(path) if path.exists() => Self::read_token_file(path).ok().unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn read_token_file(path: PathBuf) -> std::io::Result<Vec<Self>> {
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
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "Failed to parse token".to_string())
                })?;

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

    if is_negative {
        i ^ -1
    } else {
        i
    }
}

fn parse_vint(reader: &mut impl Buf) -> i32 {
    // Same method as a long, but it should just be in the int range
    let n = parse_vlong(reader);
    assert!(n > i32::MIN as i64 && n < i32::MAX as i64);
    n as i32
}

fn parse_string(reader: &mut impl Buf, length: i32) -> io::Result<String> {
    String::from_utf8(reader.copy_to_bytes(length as usize).to_vec()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::Other,
            "Failed to parse string from writable".to_string(),
        )
    })
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
pub(crate) struct UserInfo {
    pub(crate) real_user: Option<String>,
    pub(crate) effective_user: Option<String>,
}

#[derive(Debug)]
pub(crate) struct User {
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
            .find(|t| t.kind == kind && t.service == service)
    }

    #[cfg(feature = "kerberos")]
    pub(crate) fn get_user_info_from_principal(principal: &str) -> UserInfo {
        let username = User::get_user_from_principal(principal);
        let proxy_user = env::var(HADOOP_PROXY_USER).ok();
        UserInfo {
            real_user: Some(username),
            effective_user: proxy_user,
        }
    }

    pub(crate) fn get_simpler_user() -> UserInfo {
        let effective_user = env::var(HADOOP_USER_NAME).ok().unwrap_or_else(|| {
            get_current_username()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        });
        UserInfo {
            real_user: None,
            effective_user: Some(effective_user),
        }
    }

    #[cfg(feature = "kerberos")]
    pub(crate) fn get_user_from_principal(principal: &str) -> String {
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
    fn test_load_writable_token() {
        use base64::{engine::general_purpose, Engine as _};
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

        env::set_var(
            HADOOP_TOKEN_FILE_LOCATION,
            token_file.path().to_str().unwrap(),
        );

        let tokens = Token::load_tokens();

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].kind, "HDFS_DELEGATION_TOKEN");
        assert_eq!(tokens[0].service, "127.0.0.1:9000");
        tokens.iter().for_each(|t| println!("{:?}", t));
    }

    #[test]
    fn test_load_protobuf_token() {
        use base64::{engine::general_purpose, Engine as _};
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

        env::set_var(
            HADOOP_TOKEN_FILE_LOCATION,
            token_file.path().to_str().unwrap(),
        );

        let tokens = Token::load_tokens();

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].kind, "HDFS_DELEGATION_TOKEN");
        assert_eq!(tokens[0].service, "127.0.0.1:9000");
        tokens.iter().for_each(|t| println!("{:?}", t));
    }

    #[test]
    fn test_load_token_identifier() {
        let token = [
            138u8, 1, 142, 89, 190, 30, 189, 140, 100, 197, 210, 104, 0, 0, 0, 4, 104, 100, 102,
            115, 0, 0, 0, 40, 66, 80, 45, 57, 55, 51, 52, 55, 55, 51, 54, 48, 45, 49, 57, 50, 46,
            49, 54, 56, 46, 49, 46, 49, 56, 52, 45, 49, 55, 49, 48, 56, 54, 54, 54, 49, 49, 51, 50,
            53, 128, 127, 255, 255, 255, 255, 255, 255, 239, 1, 4, 82, 69, 65, 68, 3, 4, 68, 73,
            83, 75, 4, 68, 73, 83, 75, 4, 68, 73, 83, 75, 3, 0, 0, 0, 39, 68, 83, 45, 97, 50, 100,
            51, 55, 50, 98, 101, 45, 101, 99, 98, 55, 45, 52, 101, 101, 49, 45, 98, 48, 99, 51, 45,
            48, 57, 102, 49, 51, 100, 52, 49, 57, 101, 52, 102, 0, 0, 0, 39, 68, 83, 45, 53, 56,
            54, 55, 50, 99, 50, 50, 45, 51, 49, 57, 99, 45, 52, 99, 50, 53, 45, 56, 55, 50, 98, 45,
            97, 56, 48, 49, 98, 57, 99, 100, 53, 102, 51, 49, 0, 0, 0, 39, 68, 83, 45, 102, 49,
            102, 57, 57, 97, 52, 49, 45, 56, 54, 102, 51, 45, 52, 57, 102, 56, 45, 57, 48, 50, 55,
            45, 98, 101, 102, 102, 54, 100, 100, 52, 53, 54, 54, 100,
        ];

        let token_identifier = BlockTokenIdentifier::from_identifier(&token).unwrap();
        println!("{:?}", token_identifier);
        assert_eq!(token_identifier.user_id, "hdfs");
        assert_eq!(
            token_identifier.block_pool_id,
            "BP-973477360-192.168.1.184-1710866611325"
        );
        assert_eq!(token_identifier.block_id, 9223372036854775824);
        assert_eq!(token_identifier.key_id, 1690686056);
        assert!(token_identifier.handshake_secret.is_empty());
    }
}
