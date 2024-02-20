use core::fmt;
use libgssapi::context::{ClientCtx, CtxFlags, SecurityContext};
use libgssapi::credential::{Cred, CredUsage};
use libgssapi::name::Name;
use libgssapi::oid::{OidSet, GSS_MECH_KRB5, GSS_NT_HOSTBASED_SERVICE};

use crate::{HdfsError, Result};

use super::sasl::SaslSession;
use super::user::User;

#[repr(u8)]
enum SecurityLayer {
    None = 1,
    Integrity = 2,
    Confidentiality = 4,
}

#[derive(Debug)]
pub struct GssapiSession {
    principal: String,
    state: GssapiState,
}

enum GssapiState {
    Pending(ClientCtx),
    Last(ClientCtx),
    Completed(Option<(ClientCtx, bool)>),
    Errored,
}

impl fmt::Debug for GssapiState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending(..) => f.write_str("Pending"),
            Self::Last(..) => f.write_str("Last"),
            Self::Completed(..) => f.write_str("Completed"),
            Self::Errored => f.write_str("Errored"),
        }
    }
}

impl GssapiSession {
    pub(crate) fn new(service: &str, hostname: &str) -> Result<Self> {
        let targ_name = format!("{}@{}", service, hostname);

        let target = Name::new(targ_name.as_bytes(), Some(&GSS_NT_HOSTBASED_SERVICE))?;

        let mut krb5 = OidSet::new()?;
        krb5.add(&GSS_MECH_KRB5)?;

        let cred = Cred::acquire(None, None, CredUsage::Initiate, Some(&krb5))?;
        let principal = cred.name()?.to_string();

        let state = GssapiState::Pending(ClientCtx::new(
            Some(cred),
            target,
            // Allow all flags. Setting them does not mean the final context will provide
            // them, so this should not be an issue.
            CtxFlags::all(),
            Some(&GSS_MECH_KRB5),
        ));
        Ok(Self { state, principal })
    }
}

impl SaslSession for GssapiSession {
    fn step(&mut self, token: Option<&[u8]>) -> crate::Result<(Vec<u8>, bool)> {
        match core::mem::replace(&mut self.state, GssapiState::Errored) {
            GssapiState::Pending(mut ctx) => {
                let mut ret = Vec::<u8>::new();
                if let Some(token) = ctx.step(token, None)? {
                    if !token.is_empty() {
                        ret = token.to_vec();
                    }
                }
                if !ctx.is_complete() {
                    self.state = GssapiState::Pending(ctx);
                    return Ok((ret, false));
                }

                self.state = GssapiState::Last(ctx);
                Ok((ret, false))
            }
            GssapiState::Last(mut ctx) => {
                let input = token.ok_or(HdfsError::SASLError(
                    "Token not provided during kerberos SASL negotiation".to_string(),
                ))?;
                let unwrapped = ctx.unwrap(input)?;
                if unwrapped.len() != 4 {
                    return Err(HdfsError::SASLError("Bad final token".to_string()));
                }

                let supported_sec = unwrapped[0];

                let (response, wrap) = if supported_sec & SecurityLayer::Confidentiality as u8 > 0 {
                    (
                        [SecurityLayer::Confidentiality as u8, 0xFF, 0xFF, 0xFF],
                        Some(true),
                    )
                } else if supported_sec & SecurityLayer::Integrity as u8 > 0 {
                    (
                        [SecurityLayer::Integrity as u8, 0xFF, 0xFF, 0xFF],
                        Some(false),
                    )
                } else if supported_sec & SecurityLayer::None as u8 > 0 {
                    ([SecurityLayer::None as u8, 0x00, 0x00, 0x00], None)
                } else {
                    return Err(HdfsError::SASLError(
                        "No supported security layer found".to_string(),
                    ));
                };

                let wrapped = ctx.wrap(false, &response)?;
                self.state = GssapiState::Completed(wrap.map(|e| (ctx, e)));
                Ok((wrapped.to_vec(), true))
            }
            GssapiState::Completed(..) | GssapiState::Errored => {
                Err(HdfsError::SASLError("Mechanism done".to_string()))
            }
        }
    }

    fn has_security_layer(&self) -> bool {
        matches!(self.state, GssapiState::Completed(Some(_)))
    }

    fn encode(&mut self, buf: &[u8]) -> crate::Result<Vec<u8>> {
        match self.state {
            GssapiState::Completed(Some((ref mut ctx, encrypt))) => {
                let unwrapped = ctx.wrap(encrypt, buf)?;
                Ok(unwrapped.to_vec())
            }
            _ => Err(HdfsError::SASLError(
                "SASL session doesn't have security layer".to_string(),
            )),
        }
    }

    fn decode(&mut self, buf: &[u8]) -> crate::Result<Vec<u8>> {
        match self.state {
            GssapiState::Completed(Some((ref mut ctx, _))) => {
                let unwrapped = ctx.unwrap(buf)?;
                Ok(unwrapped.to_vec())
            }
            _ => Err(HdfsError::SASLError(
                "SASL session doesn't have security layer".to_string(),
            )),
        }
    }

    fn get_user_info(&self) -> crate::Result<super::user::UserInfo> {
        Ok(User::get_user_info_from_principal(&self.principal))
    }
}
