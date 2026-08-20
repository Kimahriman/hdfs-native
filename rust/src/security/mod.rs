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
    /// Acquire credentials for `principal` directly from a keytab.
    ///
    /// A private in-memory credential cache is allocated for the client so
    /// that credential acquisition and renewal do not use process-global state.
    Keytab { principal: String, keytab: String },
}

#[derive(Clone)]
pub(crate) enum ResolvedKerberosCredentials {
    CredentialCache {
        principal: String,
        cache: String,
    },
    Keytab {
        principal: String,
        keytab: String,
        cache: String,
    },
}

impl KerberosCredentials {
    pub(crate) fn resolve(self) -> ResolvedKerberosCredentials {
        match self {
            Self::CredentialCache { principal, cache } => {
                ResolvedKerberosCredentials::CredentialCache { principal, cache }
            }
            Self::Keytab { principal, keytab } => ResolvedKerberosCredentials::Keytab {
                principal,
                keytab,
                cache: format!("MEMORY:hdfs-native-{}", uuid::Uuid::new_v4()),
            },
        }
    }
}

impl ResolvedKerberosCredentials {
    pub(crate) fn principal(&self) -> &str {
        match self {
            Self::CredentialCache { principal, .. } | Self::Keytab { principal, .. } => principal,
        }
    }
}

impl std::fmt::Debug for ResolvedKerberosCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CredentialCache { principal, .. } => f
                .debug_struct("CredentialCache")
                .field("principal", principal)
                .field("cache", &"[REDACTED]")
                .finish(),
            Self::Keytab { principal, .. } => f
                .debug_struct("Keytab")
                .field("principal", principal)
                .field("keytab", &"[REDACTED]")
                .field("cache", &"[REDACTED]")
                .finish(),
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
            Self::Keytab { principal, .. } => f
                .debug_struct("Keytab")
                .field("principal", principal)
                .field("keytab", &"[REDACTED]")
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

    #[test]
    fn kerberos_credentials_debug_redacts_keytab() {
        let credentials = KerberosCredentials::Keytab {
            principal: "alice@EXAMPLE.COM".to_string(),
            keytab: "/run/secrets/alice.keytab".to_string(),
        };

        let debug = format!("{credentials:?}");
        assert!(debug.contains("alice@EXAMPLE.COM"));
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("/run/secrets/alice.keytab"));
    }
}
