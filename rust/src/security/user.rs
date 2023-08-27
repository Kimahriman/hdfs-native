use bytes::{Buf, Bytes};
use log::debug;
use prost::Message;
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

use users::get_current_username;

use crate::proto::common::CredentialsProto;

const HADOOP_USER_NAME: &str = "HADOOP_USER_NAME";
#[cfg(feature = "kerberos")]
const HADOOP_PROXY_USER: &str = "HADOOP_PROXY_USER";
const HADOOP_TOKEN_FILE_LOCATION: &str = "HADOOP_TOKEN_FILE_LOCATION";
const TOKEN_STORAGE_MAGIC: &[u8] = "HDTS".as_bytes();

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) enum LoginMethod {
    SIMPLE,
    KERBEROS,
    TOKEN,
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
            Ok(path) if path.exists() => Self::read_token_file(path).ok().unwrap_or_else(Vec::new),
            _ => Vec::new(),
        }
    }

    fn read_token_file(path: PathBuf) -> std::io::Result<Vec<Self>> {
        let mut content = Bytes::from(fs::read(path)?);
        // let mut reader = content.reader();

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

            let kind_length = parse_vlong(reader);
            let kind = String::from_utf8(reader.copy_to_bytes(kind_length as usize).to_vec())
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "Failed to parse token".to_string())
                })?;

            let service_length = parse_vlong(reader);
            let service = String::from_utf8(reader.copy_to_bytes(service_length as usize).to_vec())
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "Failed to parse token".to_string())
                })?;

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

    // fn parse_writable
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
        i = i << 8;
        i = i | (b & 0xFF) as i64;
    }

    let is_negative = first_byte < -120 || (first_byte >= -112 && first_byte < 0);

    if is_negative {
        i ^ -1
    } else {
        i
    }
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
        if let Some(index) = principal.find("/") {
            principal[0..index].to_string()
        } else if let Some(index) = principal.find("@") {
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
            .write(
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
            .write(
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
}
