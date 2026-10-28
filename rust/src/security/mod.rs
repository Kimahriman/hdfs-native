mod digest;
pub(crate) mod gssapi;
#[cfg(feature = "kms")]
pub(crate) mod kms;
pub mod sasl;
pub mod user;

use std::sync::Arc;

use crate::Result;

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

impl KerberosCredentials {
    pub(crate) fn new(
        principal: Option<String>,
        keytab: Option<String>,
        cache: Option<String>,
    ) -> Result<Option<Self>> {
        if principal.is_none() && keytab.is_none() && cache.is_none() {
            // Calling the explicit-credentials builder with all fields unset is
            // deliberately equivalent to not calling it. This preserves the
            // existing process-default GSSAPI behavior.
            return Ok(None);
        }
        if principal.is_none() {
            return Err(crate::HdfsError::InvalidArgument(
                "Kerberos principal is required when keytab or cache credentials are supplied"
                    .to_string(),
            ));
        }
        if principal.as_deref().is_some_and(str::is_empty) {
            return Err(crate::HdfsError::InvalidArgument(
                "Kerberos principal must not be empty".to_string(),
            ));
        }
        let cache = match (&keytab, cache) {
            (Some(_), None) => Some(format!("MEMORY:hdfs-native-{}", uuid::Uuid::new_v4())),
            (_, cache) => cache,
        };
        Ok(Some(Self {
            principal,
            keytab,
            cache,
        }))
    }
}

/// Authentication state shared by every connection belonging to one client.
///
/// GSS credential handles remain session-local; this context owns the
/// per-client credential configuration used to acquire them.
#[derive(Debug)]
pub(crate) struct ClientAuth {
    pub(crate) kerberos: KerberosCredentials,
}

impl ClientAuth {
    pub(crate) fn new(kerberos: KerberosCredentials) -> Arc<Self> {
        Arc::new(Self { kerberos })
    }

    pub(crate) fn credentials(&self) -> Option<&KerberosCredentials> {
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

#[cfg(test)]
mod tests {
    use super::KerberosCredentials;

    #[test]
    fn kerberos_credentials_are_composable_and_redacted() {
        let credentials = KerberosCredentials::new(
            Some("alice@EXAMPLE.COM".to_string()),
            Some("/run/secrets/alice.keytab".to_string()),
            Some("FILE:/run/krb5/alice.ccache".to_string()),
        )
        .unwrap()
        .unwrap();

        let debug = format!("{credentials:?}");
        assert!(debug.contains("alice@EXAMPLE.COM"));
        assert!(!debug.contains("/run/secrets/alice.keytab"));
        assert!(!debug.contains("/run/krb5/alice.ccache"));

        let resolved = credentials;
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
        let resolved = KerberosCredentials::new(
            Some("alice@EXAMPLE.COM".to_string()),
            Some("/run/secrets/alice.keytab".to_string()),
            None,
        )
        .unwrap()
        .unwrap();

        assert!(resolved.cache.unwrap().starts_with("MEMORY:hdfs-native-"));
    }

    #[test]
    fn empty_credentials_use_default_path() {
        assert!(
            KerberosCredentials::new(None, None, None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn explicit_credentials_require_a_principal() {
        let error =
            KerberosCredentials::new(None, Some("/run/secrets/alice.keytab".to_string()), None)
                .unwrap_err();
        assert!(error.to_string().contains("principal is required"));
    }
}
