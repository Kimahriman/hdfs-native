use bytes::{Buf, Bytes};
use libgssapi::credential::{Cred, CredUsage};
use libgssapi::oid::{OidSet, GSS_MECH_KRB5};
use log::debug;
use prost::Message;
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

use users::get_current_username;

use crate::proto::common::CredentialsProto;

use super::sasl::AuthMethod;

const HADOOP_USER_NAME: &str = "HADOOP_USER_NAME";
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
#[allow(dead_code)]
pub(crate) struct Token {
    alias: String,
    identifier: Vec<u8>,
    password: Vec<u8>,
    kind: String,
    service: String,
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
            Self::parse_writable(content)
        } else if format[0] == 1 {
            Self::parse_protobuf(content)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown token format",
            ))
        }
    }

    fn parse_writable(_reader: impl Buf) -> io::Result<Vec<Token>> {
        // let token_count = prost::decode_length_delimiter(reader)?;
        // let mut tokens: Vec<Token> = Vec::new();

        // for _ in 0..token_count {

        // }

        todo!()
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

    pub(crate) fn get_user_info(&self, auth_method: AuthMethod) -> Option<UserInfo> {
        match auth_method {
            AuthMethod::SIMPLE => {
                let effective_user = env::var(HADOOP_USER_NAME).ok().unwrap_or_else(|| {
                    get_current_username()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string()
                });
                Some(UserInfo {
                    real_user: None,
                    effective_user: Some(effective_user),
                })
            }
            AuthMethod::TOKEN => {
                // The token has the user info
                Some(UserInfo {
                    real_user: None,
                    effective_user: None,
                })
            }
            AuthMethod::KERBEROS => Self::get_kerberos_user().map(|user| {
                let proxy_user = env::var(HADOOP_PROXY_USER).ok();
                UserInfo {
                    real_user: Some(user),
                    effective_user: proxy_user,
                }
            }),
        }
    }

    fn get_user_from_principal(principal: &str) -> String {
        // If there's a /, take the part before it.
        if let Some(index) = principal.find("/") {
            principal[0..index].to_string()
        } else if let Some(index) = principal.find("@") {
            principal[0..index].to_string()
        } else {
            principal.to_string()
        }
    }

    fn kerberos_user_from_creds() -> Result<String, libgssapi::error::Error> {
        let mut krb5 = OidSet::new()?;
        krb5.add(&GSS_MECH_KRB5)?;

        let cred = Cred::acquire(None, None, CredUsage::Initiate, Some(&krb5))?;
        Ok(Self::get_user_from_principal(
            cred.name()?.to_string().as_str(),
        ))
    }

    pub(crate) fn get_kerberos_user() -> Option<String> {
        Self::kerberos_user_from_creds().ok()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    #[ignore]
    fn test_load_writable_token() {
        use base64::{engine::general_purpose, Engine as _};
        let b64_token = "SERUUwABBWFsaWFzLgAaaGRmcy9sb2NhbGhvc3RARVhBTVBMRS5DT00AAIoBiJ0ri82KAYjBOA/NAQIUjt8w+3Jfh6hfQp0JEsC+VVKl7wIVSERGU19ERUxFR0FUSU9OX1RPS0VOAAA=";
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
