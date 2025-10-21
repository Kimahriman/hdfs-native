use std::fs;
use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, DigitallySignedStruct};
use rustls_pemfile::{certs, private_key};
use webpki_roots;

use crate::{HdfsError, Result};

/// Configuration for mutual TLS authentication
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the client certificate file (PEM format)
    pub client_cert_path: String,
    /// Path to the client private key file (PEM format)  
    pub client_key_path: String,
    /// Path to the CA certificate file (PEM format)
    pub ca_cert_path: Option<String>,
    /// Whether to verify the server certificate
    pub verify_server: bool,
    /// Expected server hostname for certificate validation
    pub server_hostname: Option<String>,
}

impl TlsConfig {
    pub fn new(client_cert_path: String, client_key_path: String) -> Self {
        Self {
            client_cert_path,
            client_key_path,
            ca_cert_path: None,
            verify_server: true,
            server_hostname: None,
        }
    }

    pub fn with_ca_cert(mut self, ca_cert_path: String) -> Self {
        self.ca_cert_path = Some(ca_cert_path);
        self
    }

    pub fn with_server_verification(mut self, verify: bool) -> Self {
        self.verify_server = verify;
        self
    }

    pub fn with_server_hostname(mut self, hostname: String) -> Self {
        self.server_hostname = Some(hostname);
        self
    }

    /// Load client certificates from PEM file
    pub fn load_client_certs(&self) -> Result<Vec<CertificateDer<'static>>> {
        let cert_file = fs::File::open(&self.client_cert_path)
            .map_err(|e| HdfsError::SASLError(format!("Failed to open client certificate file {}: {}", self.client_cert_path, e)))?;
        
        let mut cert_reader = BufReader::new(cert_file);
        let cert_chain = certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| HdfsError::SASLError(format!("Failed to parse client certificates: {}", e)))?;

        if cert_chain.is_empty() {
            return Err(HdfsError::SASLError("No certificates found in client certificate file".to_string()));
        }

        Ok(cert_chain)
    }

    /// Load client private key from PEM file
    pub fn load_client_key(&self) -> Result<PrivateKeyDer<'static>> {
        let key_file = fs::File::open(&self.client_key_path)
            .map_err(|e| HdfsError::SASLError(format!("Failed to open client key file {}: {}", self.client_key_path, e)))?;
        
        let mut key_reader = BufReader::new(key_file);
        let private_key = private_key(&mut key_reader)
            .map_err(|e| HdfsError::SASLError(format!("Failed to parse client private key: {}", e)))?
            .ok_or_else(|| HdfsError::SASLError("No private key found in client key file".to_string()))?;

        Ok(private_key)
    }

    /// Load CA certificates from PEM file if specified
    pub fn load_ca_certs(&self) -> Result<Option<RootCertStore>> {
        if let Some(ca_path) = &self.ca_cert_path {
            let ca_file = fs::File::open(ca_path)
                .map_err(|e| HdfsError::SASLError(format!("Failed to open CA certificate file {}: {}", ca_path, e)))?;
            
            let mut ca_reader = BufReader::new(ca_file);
            let ca_certs = certs(&mut ca_reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| HdfsError::SASLError(format!("Failed to parse CA certificates: {}", e)))?;

            let mut root_store = RootCertStore::empty();
            for cert in ca_certs {
                root_store.add(cert)
                    .map_err(|e| HdfsError::SASLError(format!("Failed to add CA certificate to trust store: {}", e)))?;
            }

            Ok(Some(root_store))
        } else {
            Ok(None)
        }
    }

    /// Create a rustls ClientConfig from this TLS configuration
    pub fn build_client_config(&self) -> Result<ClientConfig> {
        // Initialize crypto provider if not already done
        let _ = rustls::crypto::ring::default_provider().install_default();
        
        let certs = self.load_client_certs()?;
        let key = self.load_client_key()?;

        let root_store = if let Some(root_store) = self.load_ca_certs()? {
            root_store
        } else {
            // Use webpki-roots for default CA certificates
            let mut root_store = RootCertStore::empty();
            root_store.extend(
                webpki_roots::TLS_SERVER_ROOTS.iter().cloned()
            );
            root_store
        };

        // Create Arc for sharing the root store
        let root_store_arc = Arc::new(root_store);

        let mut client_config = ClientConfig::builder()
            .with_root_certificates(root_store_arc.as_ref().clone())
            .with_client_auth_cert(certs, key)
            .map_err(|e| HdfsError::SASLError(format!("Failed to configure client authentication: {}", e)))?;

        // Skip hostname verification by default (like Go client)
        if !self.verify_server {
            client_config.dangerous().set_certificate_verifier(std::sync::Arc::new(NoHostnameVerifier::new(root_store_arc)));
        }

        Ok(client_config)
    }
}

/// Custom certificate verifier that skips hostname verification
/// This is similar to Go's InsecureSkipVerify but still validates the certificate chain
#[derive(Debug)]
struct NoHostnameVerifier {
    inner: std::sync::Arc<rustls::client::WebPkiServerVerifier>,
}

impl NoHostnameVerifier {
    fn new(root_store: Arc<RootCertStore>) -> Self {
        Self {
            inner: rustls::client::WebPkiServerVerifier::builder(root_store)
                .build()
                .unwrap(),
        }
    }
}

impl rustls::client::danger::ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Always use a dummy hostname to skip hostname verification
        // but still validate the certificate chain and other properties
        let dummy_hostname = ServerName::try_from("namenode.hopsworks").unwrap();
        self.inner.verify_server_cert(
            end_entity,
            intermediates,
            &dummy_hostname,
            ocsp_response,
            now,
        )
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}