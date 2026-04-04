use std::marker::PhantomData;

use base64::{Engine as _, engine::general_purpose};
use hmac::{Hmac, Mac, digest::KeyInit};
use rand::Rng;
use sha2::{Digest, Sha256, Sha512};

use crate::{HdfsError, Result};

use super::{
    sasl::SaslSession,
    user::{Token, UserInfo},
};

const GS2_HEADER: &str = "n,,";

enum ScramState {
    Pending,
    ServerFirst {
        client_first_bare: String,
        client_nonce: String,
    },
    ServerFinal {
        expected_server_signature: Vec<u8>,
    },
    Completed,
    Errored,
}

trait ScramHash: Clone + Default + 'static {
    const MECHANISM: &'static str;
    type Digest: Digest + Clone + Default;
    type Hmac: Mac + KeyInit + Clone;
}

#[derive(Clone, Default)]
struct Sha256Variant;

impl ScramHash for Sha256Variant {
    const MECHANISM: &'static str = "SCRAM-SHA-256";
    type Digest = Sha256;
    type Hmac = Hmac<Sha256>;
}

#[derive(Clone, Default)]
struct Sha512Variant;

impl ScramHash for Sha512Variant {
    const MECHANISM: &'static str = "SCRAM-SHA-512";
    type Digest = Sha512;
    type Hmac = Hmac<Sha512>;
}

struct ScramSession<H: ScramHash> {
    auth_id: String,
    password: String,
    nonce: String,
    state: ScramState,
    _hash: PhantomData<H>,
}

pub(super) struct ScramSha256SaslSession(ScramSession<Sha256Variant>);
pub(super) struct ScramSha512SaslSession(ScramSession<Sha512Variant>);

impl ScramSha256SaslSession {
    pub(super) fn from_token(token: &Token) -> Self {
        Self(ScramSession::from_token(token))
    }

    #[cfg(test)]
    fn new_for_test(auth_id: &str, password: &str, nonce: &str) -> Self {
        Self(ScramSession::new_for_test(auth_id, password, nonce))
    }
}

impl ScramSha512SaslSession {
    pub(super) fn from_token(token: &Token) -> Self {
        Self(ScramSession::from_token(token))
    }

    #[cfg(test)]
    fn new_for_test(auth_id: &str, password: &str, nonce: &str) -> Self {
        Self(ScramSession::new_for_test(auth_id, password, nonce))
    }
}

impl<H: ScramHash> ScramSession<H> {
    fn from_token(token: &Token) -> Self {
        Self {
            auth_id: general_purpose::STANDARD.encode(&token.identifier),
            password: general_purpose::STANDARD.encode(&token.password),
            nonce: gen_nonce(),
            state: ScramState::Pending,
            _hash: PhantomData,
        }
    }

    #[cfg(test)]
    fn new_for_test(auth_id: &str, password: &str, nonce: &str) -> Self {
        Self {
            auth_id: auth_id.to_string(),
            password: password.to_string(),
            nonce: nonce.to_string(),
            state: ScramState::Pending,
            _hash: PhantomData,
        }
    }

    fn step_inner(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)> {
        match core::mem::replace(&mut self.state, ScramState::Errored) {
            ScramState::Pending => {
                if token.is_some_and(|token| !token.is_empty()) {
                    return Err(HdfsError::SASLError(format!(
                        "Unexpected {} challenge during initial client response",
                        H::MECHANISM
                    )));
                }

                let escaped_user = sasl_name_escape(&self.auth_id)?;
                let client_first_bare = format!("n={escaped_user},r={}", self.nonce);
                let client_first_message = format!("{GS2_HEADER}{client_first_bare}");

                self.state = ScramState::ServerFirst {
                    client_first_bare,
                    client_nonce: self.nonce.clone(),
                };
                Ok((client_first_message.into_bytes(), false))
            }
            ScramState::ServerFirst {
                client_first_bare,
                client_nonce,
            } => {
                let server_first = parse_utf8_token(token, "server-first")?;
                let attrs = parse_attributes(&server_first)?;
                let nonce = required_attr(&attrs, 'r')?;
                let salt = general_purpose::STANDARD
                    .decode(required_attr(&attrs, 's')?)
                    .map_err(|_| HdfsError::SASLError("Invalid SCRAM salt encoding".to_string()))?;
                let iterations: u32 = required_attr(&attrs, 'i')?.parse().map_err(|_| {
                    HdfsError::SASLError("Invalid SCRAM iteration count".to_string())
                })?;

                if attrs.iter().any(|(key, _)| *key == 'm') {
                    return Err(HdfsError::SASLError(
                        "Unsupported SCRAM mandatory extension".to_string(),
                    ));
                }
                if !nonce.starts_with(&client_nonce) {
                    return Err(HdfsError::SASLError(
                        "SCRAM server nonce did not include the client nonce".to_string(),
                    ));
                }
                if iterations == 0 {
                    return Err(HdfsError::SASLError(
                        "SCRAM iteration count must be positive".to_string(),
                    ));
                }

                let client_final_without_proof = format!(
                    "c={},r={nonce}",
                    general_purpose::STANDARD.encode(GS2_HEADER.as_bytes())
                );
                let auth_message =
                    format!("{client_first_bare},{server_first},{client_final_without_proof}");
                let salted_password =
                    hi::<H>(self.password.as_bytes(), &salt, iterations as usize)?;
                let client_key = hmac_hash::<H>(&salted_password, b"Client Key")?;
                let stored_key = H::Digest::digest(&client_key).to_vec();
                let client_signature = hmac_hash::<H>(&stored_key, auth_message.as_bytes())?;
                let client_proof = xor(&client_key, &client_signature)?;
                let server_key = hmac_hash::<H>(&salted_password, b"Server Key")?;
                let server_signature = hmac_hash::<H>(&server_key, auth_message.as_bytes())?;

                let client_final = format!(
                    "{client_final_without_proof},p={}",
                    general_purpose::STANDARD.encode(client_proof)
                );

                self.state = ScramState::ServerFinal {
                    expected_server_signature: server_signature,
                };
                Ok((client_final.into_bytes(), false))
            }
            ScramState::ServerFinal {
                expected_server_signature,
            } => {
                let server_final = parse_utf8_token(token, "server-final")?;
                let attrs = parse_attributes(&server_final)?;

                if attrs.iter().any(|(key, _)| *key == 'm') {
                    return Err(HdfsError::SASLError(
                        "Unsupported SCRAM mandatory extension".to_string(),
                    ));
                }
                if let Some(error) = attrs
                    .iter()
                    .find(|(key, _)| *key == 'e')
                    .map(|(_, value)| value)
                {
                    return Err(HdfsError::SASLError(format!(
                        "SCRAM server returned error: {error}"
                    )));
                }

                let verifier = general_purpose::STANDARD
                    .decode(required_attr(&attrs, 'v')?)
                    .map_err(|_| {
                        HdfsError::SASLError("Invalid SCRAM server signature".to_string())
                    })?;

                if verifier != expected_server_signature {
                    return Err(HdfsError::SASLError(
                        "SCRAM server signature did not match".to_string(),
                    ));
                }

                self.state = ScramState::Completed;
                Ok((Vec::new(), true))
            }
            ScramState::Completed => Err(HdfsError::SASLError(format!(
                "{} handshake already complete",
                H::MECHANISM
            ))),
            ScramState::Errored => Err(HdfsError::SASLError(format!(
                "{} authentication failed",
                H::MECHANISM
            ))),
        }
    }
}

impl SaslSession for ScramSha256SaslSession {
    fn step(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)> {
        self.0.step_inner(token)
    }

    fn has_security_layer(&self) -> bool {
        false
    }

    fn encode(&mut self, _buf: &[u8]) -> Result<Vec<u8>> {
        Err(HdfsError::SASLError(
            "SCRAM-SHA-256 does not provide a SASL security layer".to_string(),
        ))
    }

    fn decode(&mut self, _buf: &[u8]) -> Result<Vec<u8>> {
        Err(HdfsError::SASLError(
            "SCRAM-SHA-256 does not provide a SASL security layer".to_string(),
        ))
    }

    fn get_user_info(&self) -> Result<UserInfo> {
        Ok(UserInfo {
            real_user: None,
            effective_user: None,
        })
    }
}

impl SaslSession for ScramSha512SaslSession {
    fn step(&mut self, token: Option<&[u8]>) -> Result<(Vec<u8>, bool)> {
        self.0.step_inner(token)
    }

    fn has_security_layer(&self) -> bool {
        false
    }

    fn encode(&mut self, _buf: &[u8]) -> Result<Vec<u8>> {
        Err(HdfsError::SASLError(
            "SCRAM-SHA-512 does not provide a SASL security layer".to_string(),
        ))
    }

    fn decode(&mut self, _buf: &[u8]) -> Result<Vec<u8>> {
        Err(HdfsError::SASLError(
            "SCRAM-SHA-512 does not provide a SASL security layer".to_string(),
        ))
    }

    fn get_user_info(&self) -> Result<UserInfo> {
        Ok(UserInfo {
            real_user: None,
            effective_user: None,
        })
    }
}

fn parse_utf8_token(token: Option<&[u8]>, label: &str) -> Result<String> {
    let token = token.ok_or_else(|| {
        HdfsError::SASLError(format!("Missing SCRAM {label} message from server"))
    })?;
    String::from_utf8(token.to_vec())
        .map_err(|_| HdfsError::SASLError(format!("SCRAM {label} was not valid UTF-8")))
}

fn parse_attributes(message: &str) -> Result<Vec<(char, String)>> {
    let mut attrs = Vec::new();
    for part in message.split(',') {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| HdfsError::SASLError("Malformed SCRAM attribute".to_string()))?;
        let mut chars = key.chars();
        let key = chars
            .next()
            .ok_or_else(|| HdfsError::SASLError("Malformed SCRAM attribute key".to_string()))?;
        if chars.next().is_some() {
            return Err(HdfsError::SASLError(
                "Malformed SCRAM attribute key".to_string(),
            ));
        }
        attrs.push((key, value.to_string()));
    }
    Ok(attrs)
}

fn required_attr(attrs: &[(char, String)], key: char) -> Result<String> {
    attrs
        .iter()
        .find(|(attr_key, _)| *attr_key == key)
        .map(|(_, value)| value.clone())
        .ok_or_else(|| HdfsError::SASLError(format!("Missing SCRAM attribute {key}")))
}

fn sasl_name_escape(value: &str) -> Result<String> {
    if !value.is_ascii() {
        return Err(HdfsError::SASLError(
            "SCRAM currently only supports ASCII usernames".to_string(),
        ));
    }

    Ok(value.replace('=', "=3D").replace(',', "=2C"))
}

fn gen_nonce() -> String {
    let mut rng = rand::rng();
    let nonce_bytes: Vec<u8> = (0..18).map(|_| rng.random()).collect();
    general_purpose::STANDARD.encode(nonce_bytes)
}

fn hmac_hash<H: ScramHash>(key: &[u8], input: &[u8]) -> Result<Vec<u8>> {
    let mut mac = <<H as ScramHash>::Hmac as KeyInit>::new_from_slice(key)
        .map_err(|_| HdfsError::SASLError("Invalid SCRAM HMAC key".to_string()))?;
    mac.update(input);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hi<H: ScramHash>(password: &[u8], salt: &[u8], iterations: usize) -> Result<Vec<u8>> {
    let mut u = hmac_hash::<H>(password, &[salt, &[0, 0, 0, 1]].concat())?;
    let mut output = u.clone();

    for _ in 1..iterations {
        u = hmac_hash::<H>(password, &u)?;
        for (output_byte, u_byte) in output.iter_mut().zip(u.iter()) {
            *output_byte ^= *u_byte;
        }
    }

    Ok(output)
}

fn xor(left: &[u8], right: &[u8]) -> Result<Vec<u8>> {
    if left.len() != right.len() {
        return Err(HdfsError::SASLError(
            "SCRAM XOR inputs must have the same length".to_string(),
        ));
    }

    Ok(left.iter().zip(right).map(|(l, r)| l ^ r).collect())
}

#[cfg(test)]
mod test {
    use base64::{Engine as _, engine::general_purpose};

    use super::{ScramSha256SaslSession, ScramSha512SaslSession};
    use crate::security::sasl::SaslSession;

    #[test]
    fn test_scram_sha256_rfc7677_example() {
        let mut session =
            ScramSha256SaslSession::new_for_test("user", "pencil", "rOprNGfwEbeRWgbNEkqO");

        let client_first = String::from_utf8(session.step(None).unwrap().0).unwrap();
        assert_eq!(client_first, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");

        let server_first = concat!(
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,",
            "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        );
        let client_final =
            String::from_utf8(session.step(Some(server_first.as_bytes())).unwrap().0).unwrap();
        assert_eq!(
            client_final,
            concat!(
                "c=biws,",
                "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,",
                "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
            )
        );

        let server_final = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";
        let (response, complete) = session.step(Some(server_final.as_bytes())).unwrap();
        assert!(response.is_empty());
        assert!(complete);
    }

    #[test]
    fn test_scram_sha512_rfc5802_style_example() {
        let mut session =
            ScramSha512SaslSession::new_for_test("user", "pencil", "fyko+d2lbbFgONRv9qkxdawL");

        let client_first = String::from_utf8(session.step(None).unwrap().0).unwrap();
        assert_eq!(client_first, "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL");

        let server_first = concat!(
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,",
            "s=QSXCR+Q6sek8bf92,i=4096"
        );
        let client_final =
            String::from_utf8(session.step(Some(server_first.as_bytes())).unwrap().0).unwrap();
        let expected_prefix = concat!(
            "c=biws,",
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,",
            "p="
        );
        assert!(client_final.starts_with(expected_prefix));
        let proof = client_final.strip_prefix(expected_prefix).unwrap();
        assert_eq!(general_purpose::STANDARD.decode(proof).unwrap().len(), 64);
    }

    #[test]
    fn test_scram_rejects_missing_nonce_prefix() {
        let mut session = ScramSha256SaslSession::new_for_test("user", "pencil", "clientnonce");
        let _ = session.step(None).unwrap();

        let err = session
            .step(Some(b"r=servernonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "SASL error: SCRAM server nonce did not include the client nonce"
        );
    }
}
