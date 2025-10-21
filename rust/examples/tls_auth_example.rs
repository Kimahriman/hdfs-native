// Example of how to use mutual TLS authentication with hdfs-native
// This file demonstrates the configuration and usage patterns

use std::collections::HashMap;
use hdfs_native::common::config::Configuration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example 1: Configure TLS through configuration map
    let mut tls_config = HashMap::new();
    
    // Enable TLS authentication
    tls_config.insert("hdfs.tls.enabled".to_string(), "true".to_string());
    
    // Client certificate and private key (required for mutual TLS)
    tls_config.insert("hdfs.tls.client.cert.path".to_string(), "/path/to/client.crt".to_string());
    tls_config.insert("hdfs.tls.client.key.path".to_string(), "/path/to/client.key".to_string());
    
    // Optional: CA certificate for server verification
    tls_config.insert("hdfs.tls.ca.cert.path".to_string(), "/path/to/ca.crt".to_string());
    
    // Optional: Enable/disable server certificate verification (default: true)
    tls_config.insert("hdfs.tls.verify.server".to_string(), "true".to_string());
    
    // Optional: Expected server hostname for certificate validation
    tls_config.insert("hdfs.tls.server.hostname".to_string(), "namenode.example.com".to_string());
    
    // Create configuration with TLS settings
    let config = Configuration::new_with_config(tls_config)?;
    
    // TODO: Use this configuration to create HDFS client connections
    // The actual client connection code would automatically use TLS
    // when hdfs.tls.enabled is set to true
    
    println!("TLS configuration loaded successfully");
    println!("Client cert: {:?}", config.get("hdfs.tls.client.cert.path"));
    println!("TLS enabled: {}", config.tls_enabled());
    
    Ok(())
}

// Example environment variables that could be used:
// export HDFS_TLS_ENABLED=true
// export HDFS_TLS_CLIENT_CERT_PATH=/etc/hdfs/ssl/client.crt
// export HDFS_TLS_CLIENT_KEY_PATH=/etc/hdfs/ssl/client.key
// export HDFS_TLS_CA_CERT_PATH=/etc/hdfs/ssl/ca.crt
// export HDFS_TLS_VERIFY_SERVER=true
// export HDFS_TLS_SERVER_HOSTNAME=namenode.example.com

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tls_config_from_map() {
        let mut config_map = HashMap::new();
        config_map.insert("hdfs.tls.enabled".to_string(), "true".to_string());
        config_map.insert("hdfs.tls.client.cert.path".to_string(), "/test/client.crt".to_string());
        config_map.insert("hdfs.tls.client.key.path".to_string(), "/test/client.key".to_string());
        
        let config = Configuration::new_with_config(config_map).unwrap();
        
        assert!(config.tls_enabled());
        assert_eq!(config.get("hdfs.tls.client.cert.path"), Some("/test/client.crt"));
        
        let tls_config = config.get_tls_config().unwrap();
        assert_eq!(tls_config.client_cert_path, "/test/client.crt");
        assert_eq!(tls_config.client_key_path, "/test/client.key");
    }
    
    #[test]
    fn test_tls_disabled_by_default() {
        let config = Configuration::new_with_config(HashMap::new()).unwrap();
        assert!(!config.tls_enabled());
        assert!(config.get_tls_config().is_none());
    }
}