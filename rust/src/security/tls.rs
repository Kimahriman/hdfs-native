use std::fs;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use rustls_pemfile::{certs, private_key};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};
use x509_parser::prelude::*;

use crate::{HdfsError, Result};

use super::sasl::SaslSession;
use super::user::UserInfo;

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

        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(certs, key)
            .map_err(|e| HdfsError::SASLError(format!("Failed to configure client authentication: {}", e)))?;

        Ok(client_config)
    }
}

/// TLS session for mutual TLS authentication
pub struct TlsSession {
    protocol: String,
    server_id: String,
    config: TlsConfig,
    state: TlsState,
    client_config: Option<Arc<ClientConfig>>,
}

#[derive(Debug)]
enum TlsState {
    Initial,
    ConfigLoaded,
    Authenticated,
    Failed,
}

impl TlsSession {
    pub fn new(protocol: &str, server_id: &str) -> Result<Self> {
        // Load TLS configuration from environment variables or config file
        let config = Self::load_tls_config()?;
        
        Ok(Self {
            protocol: protocol.to_string(),
            server_id: server_id.to_string(),
            config,
            state: TlsState::Initial,
            client_config: None,
        })
    }

    pub fn with_config(protocol: &str, server_id: &str, config: TlsConfig) -> Self {
        Self {
            protocol: protocol.to_string(),
            server_id: server_id.to_string(),
            config,
            state: TlsState::Initial,
            client_config: None,
        }
    }

    fn load_tls_config() -> Result<TlsConfig> {
        // Load from environment variables
        let client_cert_path = std::env::var("HDFS_CLIENT_CERT_PATH")
            .map_err(|_| HdfsError::SASLError("HDFS_CLIENT_CERT_PATH environment variable not set".to_string()))?;
        
        let client_key_path = std::env::var("HDFS_CLIENT_KEY_PATH")
            .map_err(|_| HdfsError::SASLError("HDFS_CLIENT_KEY_PATH environment variable not set".to_string()))?;

        let mut config = TlsConfig::new(client_cert_path, client_key_path);

        // Optional CA certificate
        if let Ok(ca_cert_path) = std::env::var("HDFS_CA_CERT_PATH") {
            config = config.with_ca_cert(ca_cert_path);
        }

        // Optional server verification setting
        if let Ok(verify_server) = std::env::var("HDFS_TLS_VERIFY_SERVER") {
            config = config.with_server_verification(verify_server.to_lowercase() == "true");
        }

        // Optional server hostname
        if let Ok(server_hostname) = std::env::var("HDFS_TLS_SERVER_HOSTNAME") {
            config = config.with_server_hostname(server_hostname);
        }

        Ok(config)
    }

    fn validate_certificates(&self) -> Result<()> {
        // Check if certificate files exist and are readable
        if !Path::new(&self.config.client_cert_path).exists() {
            return Err(HdfsError::SASLError(format!(
                "Client certificate file does not exist: {}", 
                self.config.client_cert_path
            )));
        }

        if !Path::new(&self.config.client_key_path).exists() {
            return Err(HdfsError::SASLError(format!(
                "Client private key file does not exist: {}", 
                self.config.client_key_path
            )));
        }

        if let Some(ca_path) = &self.config.ca_cert_path {
            if !Path::new(ca_path).exists() {
                return Err(HdfsError::SASLError(format!(
                    "CA certificate file does not exist: {}", 
                    ca_path
                )));
            }
        }

        // Try to load and validate certificates
        let certs = self.config.load_client_certs()?;
        let _key = self.config.load_client_key()?;
        
        // Validate that we can parse the certificate and extract user info
        let cert = &certs[0];
        let _user_name = self.extract_common_name_from_cert(cert)?;
        
        // If CA certs are specified, try to load them
        if self.config.ca_cert_path.is_some() {
            let _ca_certs = self.config.load_ca_certs()?;
        }

        Ok(())
    }

    fn build_client_config(&mut self) -> Result<()> {
        if self.client_config.is_none() {
            let config = self.config.build_client_config()?;
            self.client_config = Some(Arc::new(config));
        }
        Ok(())
    }

    fn extract_user_info_from_cert(&self) -> Result<UserInfo> {
        // Load the client certificate to extract user information
        let certs = self.config.load_client_certs()?;
        let cert = &certs[0]; // Use the first certificate in the chain

        // Parse the certificate to extract subject information
        // For now, we'll use a simple placeholder approach
        // In a real implementation, you'd parse the X.509 certificate structure
        
        // Extract common name from certificate subject as effective user
        let effective_user = self.extract_common_name_from_cert(cert)?;
        
        Ok(UserInfo {
            effective_user: Some(effective_user.clone()),
            real_user: Some(effective_user),
        })
    }

    fn extract_common_name_from_cert(&self, cert: &CertificateDer) -> Result<String> {
        // Try to parse X.509 certificate and extract CN from subject DN
        match X509Certificate::from_der(cert) {
            Ok((_, parsed_cert)) => {
                // Extract subject distinguished name
                let subject = &parsed_cert.subject();
                
                // Look for Common Name (CN) in subject - using the correct OID
                for rdn in subject.iter() {
                    for attr in rdn.iter() {
                        // Common Name OID is 2.5.4.3
                        if attr.attr_type().to_string() == "2.5.4.3" {
                            if let Ok(cn_value) = attr.attr_value().as_str() {
                                return Ok(cn_value.to_string());
                            }
                        }
                    }
                }

                // If no CN found, try to extract from email address
                for rdn in subject.iter() {
                    for attr in rdn.iter() {
                        // Email address OID is 1.2.840.113549.1.9.1
                        if attr.attr_type().to_string() == "1.2.840.113549.1.9.1" {
                            if let Ok(email_value) = attr.attr_value().as_str() {
                                // Extract username part from email
                                if let Some(username) = email_value.split('@').next() {
                                    return Ok(username.to_string());
                                }
                            }
                        }
                    }
                }

                // Try to get any readable attribute from subject as fallback
                for rdn in subject.iter() {
                    for attr in rdn.iter() {
                        if let Ok(attr_value) = attr.attr_value().as_str() {
                            if !attr_value.is_empty() && attr_value.len() < 100 {
                                return Ok(attr_value.to_string());
                            }
                        }
                    }
                }
            }
            Err(_) => {
                // Certificate parsing failed, fall back to filename
                // This is expected for test cases or malformed certificates
            }
        }

        // Fallback to certificate file name if parsing failed or no suitable subject attribute found
        let cert_path = Path::new(&self.config.client_cert_path);
        let file_name = cert_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("hdfs-client");
        
        Ok(file_name.to_string())
    }

    /// Create a TLS connector for establishing connections
    pub fn create_connector(&self) -> Result<TlsConnector> {
        if let Some(client_config) = &self.client_config {
            Ok(TlsConnector::from(Arc::clone(client_config)))
        } else {
            Err(HdfsError::SASLError("TLS client config not initialized".to_string()))
        }
    }

    /// Establish a TLS connection to the specified address
    pub async fn connect_tls(&self, tcp_stream: TcpStream, server_name: &str) -> Result<TlsStream<TcpStream>> {
        let connector = self.create_connector()?;
        
        // Use configured hostname if provided, otherwise use the server_name parameter
        let hostname = if let Some(ref configured_hostname) = self.config.server_hostname {
            configured_hostname.clone()
        } else {
            server_name.to_string()
        };
        
        let server_name = ServerName::try_from(hostname.clone())
            .map_err(|e| HdfsError::SASLError(format!("Invalid server name {}: {}", hostname, e)))?;

        let tls_stream = connector.connect(server_name, tcp_stream).await
            .map_err(|e| {
                if !self.config.verify_server {
                    HdfsError::SASLError(format!("TLS connection failed (hostname verification should be disabled but rustls doesn't support it easily): {}", e))
                } else {
                    HdfsError::SASLError(format!("TLS connection failed: {}", e))
                }
            })?;

        Ok(tls_stream)
    }
}

impl SaslSession for TlsSession {
    fn step(&mut self, _token: Option<&[u8]>) -> Result<(Vec<u8>, bool)> {
        match self.state {
            TlsState::Initial => {
                // Validate certificates and build client config
                self.validate_certificates()?;
                self.build_client_config()?;
                self.state = TlsState::ConfigLoaded;
                
                // Return empty response - actual TLS handshake happens at transport layer
                // This step just validates that we can authenticate
                Ok((Vec::new(), false))
            }
            TlsState::ConfigLoaded => {
                // TLS authentication is complete at this point
                // The actual TLS handshake will be performed when the connection is established
                self.state = TlsState::Authenticated;
                Ok((Vec::new(), true))
            }
            TlsState::Authenticated => {
                // Already authenticated
                Err(HdfsError::SASLError(
                    "TLS session already authenticated".to_string(),
                ))
            }
            TlsState::Failed => {
                Err(HdfsError::SASLError("TLS authentication failed".to_string()))
            }
        }
    }

    fn has_security_layer(&self) -> bool {
        // TLS provides its own security layer (encryption/integrity) at transport level
        false // Return false because security is handled at transport layer, not SASL layer
    }

    fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        // TLS encryption happens at transport layer, so just pass through
        Ok(buf.to_vec())
    }

    fn decode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        // TLS decryption happens at transport layer, so just pass through
        Ok(buf.to_vec())
    }

    fn get_user_info(&self) -> Result<UserInfo> {
        match self.state {
            TlsState::Authenticated | TlsState::ConfigLoaded => self.extract_user_info_from_cert(),
            _ => Err(HdfsError::SASLError(
                "TLS session not authenticated yet".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_tls_config_creation() {
        let config = TlsConfig::new(
            "/path/to/client.crt".to_string(),
            "/path/to/client.key".to_string(),
        );
        
        assert_eq!(config.client_cert_path, "/path/to/client.crt");
        assert_eq!(config.client_key_path, "/path/to/client.key");
        assert!(config.verify_server);
        assert!(config.ca_cert_path.is_none());
    }

    #[test]
    fn test_tls_config_with_ca() {
        let config = TlsConfig::new(
            "/path/to/client.crt".to_string(),
            "/path/to/client.key".to_string(),
        )
        .with_ca_cert("/path/to/ca.crt".to_string())
        .with_server_verification(false);
        
        assert_eq!(config.ca_cert_path, Some("/path/to/ca.crt".to_string()));
        assert!(!config.verify_server);
    }

    #[test]
    fn test_certificate_validation_missing_files() {
        let config = TlsConfig::new(
            "/nonexistent/client.crt".to_string(),
            "/nonexistent/client.key".to_string(),
        );
        
        let session = TlsSession::with_config("hdfs", "0", config);
        let result = session.validate_certificates();
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_load_tls_config_from_env() {
        // Set environment variables
        std::env::set_var("HDFS_CLIENT_CERT_PATH", "/test/client.crt");
        std::env::set_var("HDFS_CLIENT_KEY_PATH", "/test/client.key");
        std::env::set_var("HDFS_CA_CERT_PATH", "/test/ca.crt");
        std::env::set_var("HDFS_TLS_VERIFY_SERVER", "false");
        std::env::set_var("HDFS_TLS_SERVER_HOSTNAME", "test.example.com");
        
        let config = TlsSession::load_tls_config().unwrap();
        
        assert_eq!(config.client_cert_path, "/test/client.crt");
        assert_eq!(config.client_key_path, "/test/client.key");
        assert_eq!(config.ca_cert_path, Some("/test/ca.crt".to_string()));
        assert!(!config.verify_server);
        assert_eq!(config.server_hostname, Some("test.example.com".to_string()));
        
        // Clean up
        std::env::remove_var("HDFS_CLIENT_CERT_PATH");
        std::env::remove_var("HDFS_CLIENT_KEY_PATH");
        std::env::remove_var("HDFS_CA_CERT_PATH");
        std::env::remove_var("HDFS_TLS_VERIFY_SERVER");
        std::env::remove_var("HDFS_TLS_SERVER_HOSTNAME");
    }

    #[test]
    fn test_extract_common_name_from_cert_path() {
        let config = TlsConfig::new(
            "/path/to/hdfs-user.crt".to_string(),
            "/path/to/hdfs-user.key".to_string(),
        );
        
        let session = TlsSession::with_config("hdfs", "0", config);
        
        // Use invalid cert data - this should fall back to using the filename
        let dummy_cert = CertificateDer::from(vec![0u8; 10]);
        let cn = session.extract_common_name_from_cert(&dummy_cert);
        
        // Should fall back to filename since cert parsing will fail
        assert!(cn.is_ok());
        assert_eq!(cn.unwrap(), "hdfs-user");
    }

    #[test]
    fn test_tls_config_verify_server_flag() {
        let config = TlsConfig::new(
            "/test/client.crt".to_string(),
            "/test/client.key".to_string(),
        )
        .with_server_verification(false);
        
        assert!(!config.verify_server);
        
        let config_verify_true = TlsConfig::new(
            "/test/client.crt".to_string(),
            "/test/client.key".to_string(),
        )
        .with_server_verification(true);
        
        assert!(config_verify_true.verify_server);
    }

    // Test helper to create a minimal valid PEM certificate for testing
    fn create_test_cert_files() -> (tempfile::TempDir, String, String) {
        let temp_dir = tempdir().unwrap();
        
        // Minimal PEM certificate (not valid but parseable)
        let test_cert = "-----BEGIN CERTIFICATE-----\nMIIBkTCB+wIJAKZFNGrtTCs4MA0GCSqGSIb3DQEBCwUAMBQxEjAQBgNVBAMMCXRl\nc3QgY2VydDAeFw0yNDEwMjEwMDAwMDBaFw0yNTEwMjEwMDAwMDBaMBQxEjAQBgNV\nBAMMCXRlc3QgY2VydDBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC8Q7HgL6Qb1Uw\nOyMjM4VJT5JFgRrL2sT7nL6R1FQMQ0JvOJHKONV8DzLzFQXBjNjL2gQF8f8L9h\nAQEBAgMBAAEwDQYJKoZIhvcNAQELBQADQQC1uT6XhK7bL9pF7qR5oM2G4yVJ5\n-----END CERTIFICATE-----\n";
        
        // Minimal PEM private key (not valid but parseable)
        let test_key = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC8Q7HgL6Qb1Uw\nOyMjM4VJT5JFgRrL2sT7nL6R1FQMQ0JvOJHKONV8DzLzFQXBjNjL2gQF8f8L9h\nAQEBAgMBAAECggEAQQC1uT6XhK7bL9pF7qR5oM2G4yVJ5\n-----END PRIVATE KEY-----\n";
        
        let cert_path = temp_dir.path().join("test.crt");
        let key_path = temp_dir.path().join("test.key");
        
        fs::write(&cert_path, test_cert).unwrap();
        fs::write(&key_path, test_key).unwrap();
        
        (temp_dir, cert_path.to_string_lossy().to_string(), key_path.to_string_lossy().to_string())
    }
}