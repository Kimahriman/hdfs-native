mod digest;
pub mod gssapi;
pub mod sasl;
pub mod user;

pub use digest::DigestSaslSession;

use std::sync::Arc;

use crate::Result;

/// Kerberos credentials to use for a single Hadoop client.
#[derive(Clone, Default)]
pub struct KerberosCredentials {
    pub principal: Option<String>,
    pub keytab: Option<String>,
    pub cache: Option<String>,
}

impl KerberosCredentials {
    /// Creates credentials, validating that explicit keytabs and caches include a principal.
    pub fn new(
        principal: Option<String>,
        keytab: Option<String>,
        cache: Option<String>,
    ) -> Result<Option<Self>> {
        if principal.is_none() && keytab.is_none() && cache.is_none() {
            return Ok(None);
        }
        if principal.is_none() {
            return Err(crate::HadoopError::InvalidArgument(
                "Kerberos principal is required when keytab or cache credentials are supplied"
                    .to_string(),
            ));
        }
        if principal.as_deref().is_some_and(str::is_empty) {
            return Err(crate::HadoopError::InvalidArgument(
                "Kerberos principal must not be empty".to_string(),
            ));
        }
        let cache = match (&keytab, cache) {
            (Some(_), None) => Some(format!("MEMORY:hadoop-native-{}", uuid::Uuid::new_v4())),
            (_, cache) => cache,
        };
        Ok(Some(Self {
            principal,
            keytab,
            cache,
        }))
    }
}

/// Authentication state shared by connections that belong to one client.
#[derive(Debug)]
pub struct ClientAuth {
    kerberos: KerberosCredentials,
}

impl ClientAuth {
    /// Creates client authentication state from explicit Kerberos credentials.
    pub fn new(kerberos: KerberosCredentials) -> Arc<Self> {
        Arc::new(Self { kerberos })
    }

    /// Returns the explicit credentials associated with this client.
    pub fn credentials(&self) -> Option<&KerberosCredentials> {
        Some(&self.kerberos)
    }
}

impl std::fmt::Debug for KerberosCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KerberosCredentials")
            .field("principal", &self.principal)
            .field("keytab", &self.keytab.as_ref().map(|_| "[REDACTED]"))
            .field("cache", &self.cache.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}
