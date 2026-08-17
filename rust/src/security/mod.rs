mod digest;
pub(crate) mod gssapi;
#[cfg(feature = "kms")]
pub(crate) mod kms;
pub mod sasl;
pub mod user;

/// Kerberos credentials to use for a single HDFS client.
///
/// Explicit credentials avoid relying on process-global state such as
/// `KRB5CCNAME`, allowing clients with different identities to coexist.
#[derive(Clone)]
pub enum KerberosCredentials {
    /// Acquire credentials for `principal` from the named credential cache.
    ///
    /// `cache` uses the Kerberos credential-cache syntax, for example
    /// `FILE:/run/krb5/client.ccache` or `DIR:/run/krb5/ccaches`.
    CredentialCache { principal: String, cache: String },
}

impl KerberosCredentials {
    pub(crate) fn principal(&self) -> &str {
        match self {
            Self::CredentialCache { principal, .. } => principal,
        }
    }
}

impl std::fmt::Debug for KerberosCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CredentialCache { principal, .. } => f
                .debug_struct("CredentialCache")
                .field("principal", principal)
                .field("cache", &"[REDACTED]")
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KerberosCredentials;

    #[test]
    fn kerberos_credentials_debug_redacts_cache() {
        let credentials = KerberosCredentials::CredentialCache {
            principal: "alice@EXAMPLE.COM".to_string(),
            cache: "FILE:/run/secrets/alice.ccache".to_string(),
        };

        let debug = format!("{credentials:?}");
        assert!(debug.contains("alice@EXAMPLE.COM"));
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("/run/secrets/alice.ccache"));
    }
}
