mod digest;
pub(crate) mod gssapi;
#[cfg(feature = "kms")]
pub(crate) mod kms;
pub mod sasl;
pub mod user;

/// Kerberos credentials to use for a single HDFS client.
///
/// The fields are independent: a keytab and cache may be supplied together so
/// acquired tickets are written to that cache. If a keytab is supplied without
/// a cache, the client allocates a private in-memory cache. Supplying only a
/// principal selects that identity from the process-default credential store.
#[derive(Clone, Default)]
pub(crate) struct KerberosCredentials {
    pub principal: Option<String>,
    pub keytab: Option<String>,
    pub cache: Option<String>,
}

#[derive(Clone)]
pub(crate) struct ResolvedKerberosCredentials {
    pub(crate) principal: Option<String>,
    pub(crate) keytab: Option<String>,
    pub(crate) cache: Option<String>,
}

impl KerberosCredentials {
    pub(crate) fn resolve(self) -> Option<ResolvedKerberosCredentials> {
        if self.principal.is_none() && self.keytab.is_none() && self.cache.is_none() {
            return None;
        }
        let cache = match (&self.keytab, self.cache) {
            (Some(_), None) => Some(format!("MEMORY:hdfs-native-{}", uuid::Uuid::new_v4())),
            (_, cache) => cache,
        };
        Some(ResolvedKerberosCredentials {
            principal: self.principal,
            keytab: self.keytab,
            cache,
        })
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

impl std::fmt::Debug for ResolvedKerberosCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedKerberosCredentials")
            .field("principal", &self.principal)
            .field("keytab", &self.keytab.as_ref().map(|_| "[REDACTED]"))
            .field("cache", &self.cache.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::KerberosCredentials;

    #[test]
    fn kerberos_credentials_are_composable_and_redacted() {
        let credentials = KerberosCredentials {
            principal: Some("alice@EXAMPLE.COM".to_string()),
            keytab: Some("/run/secrets/alice.keytab".to_string()),
            cache: Some("FILE:/run/krb5/alice.ccache".to_string()),
        };

        let debug = format!("{credentials:?}");
        assert!(debug.contains("alice@EXAMPLE.COM"));
        assert!(!debug.contains("/run/secrets/alice.keytab"));
        assert!(!debug.contains("/run/krb5/alice.ccache"));

        let resolved = credentials.resolve().unwrap();
        assert_eq!(
            resolved.keytab.as_deref(),
            Some("/run/secrets/alice.keytab")
        );
        assert_eq!(
            resolved.cache.as_deref(),
            Some("FILE:/run/krb5/alice.ccache")
        );
    }

    #[test]
    fn keytab_without_cache_gets_private_memory_cache() {
        let resolved = KerberosCredentials {
            principal: None,
            keytab: Some("/run/secrets/alice.keytab".to_string()),
            cache: None,
        }
        .resolve()
        .unwrap();

        assert!(resolved.cache.unwrap().starts_with("MEMORY:hdfs-native-"));
    }

    #[test]
    fn empty_credentials_use_default_path() {
        assert!(KerberosCredentials::default().resolve().is_none());
    }
}
