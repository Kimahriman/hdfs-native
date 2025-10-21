#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use hdfs_native::{
        client::ClientBuilder,
        security::tls::TlsConfig,
        Client, Result,
    };
    use serial_test::serial;
    use std::env;

    /// Get TLS configuration from environment variables
    fn get_tls_config() -> Option<(String, TlsConfig)> {
        let namenode_url = env::var("HDFS_NAMENODE_URL").ok()?;
        let cert_path = env::var("HDFS_CLIENT_CERT_PATH").ok()?;
        let key_path = env::var("HDFS_CLIENT_KEY_PATH").ok()?;

        let mut tls_config = TlsConfig::new(cert_path, key_path);

        if let Ok(ca_path) = env::var("HDFS_CA_CERT_PATH") {
            tls_config = tls_config.with_ca_cert(ca_path);
        }

        if let Ok(verify_str) = env::var("HDFS_TLS_VERIFY_SERVER") {
            let verify = verify_str.to_lowercase() == "true";
            tls_config = tls_config.with_server_verification(verify);
        }

        if let Ok(hostname) = env::var("HDFS_TLS_SERVER_HOSTNAME") {
            tls_config = tls_config.with_server_hostname(hostname);
        }

        Some((namenode_url, tls_config))
    }

    /// Test connecting without TLS to see what authentication methods are available
    #[tokio::test]
    #[serial]
    async fn test_debug_auth_methods() -> Result<()> {
        let (namenode_url, _) = get_tls_config()
            .expect("TLS integration test requires environment configuration");

        println!("Attempting to connect to: {}", namenode_url);

        // Try to connect without TLS first to see what auth methods are offered
        let client = ClientBuilder::new()
            .with_url(&namenode_url)
            .build();

        match client {
            Ok(client) => {
                println!("Successfully connected without TLS configuration");
                
                // Try to list root directory to see if it works
                match client.list_status("/", false).await {
                    Ok(files) => {
                        println!("Successfully listed {} files without TLS", files.len());
                    }
                    Err(e) => {
                        println!("Failed to list files without TLS: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("Failed to connect without TLS: {}", e);
            }
        }

        Ok(())
    }

    /// Test connecting with TLS config but debug the error
    #[tokio::test]
    #[serial]
    async fn test_debug_tls_error() -> Result<()> {
        let (namenode_url, tls_config) = get_tls_config()
            .expect("TLS integration test requires environment configuration");

        println!("Attempting to connect with TLS to: {}", namenode_url);
        println!("TLS Config:");
        println!("  Cert path: {}", tls_config.client_cert_path);
        println!("  Key path: {}", tls_config.client_key_path);
        println!("  CA path: {:?}", tls_config.ca_cert_path);
        println!("  Verify server: {}", tls_config.verify_server);
        println!("  Server hostname: {:?}", tls_config.server_hostname);

        // Convert TLS config to configuration map
        let mut config_map = std::collections::HashMap::new();
        config_map.insert("hdfs.tls.enabled".to_string(), "true".to_string());
        config_map.insert("hdfs.tls.client.cert.path".to_string(), tls_config.client_cert_path.clone());
        config_map.insert("hdfs.tls.client.key.path".to_string(), tls_config.client_key_path.clone());
        
        if let Some(ca_cert_path) = &tls_config.ca_cert_path {
            config_map.insert("hdfs.tls.ca.cert.path".to_string(), ca_cert_path.clone());
        }
        
        config_map.insert("hdfs.tls.verify.server".to_string(), tls_config.verify_server.to_string());
        
        if let Some(server_hostname) = &tls_config.server_hostname {
            config_map.insert("hdfs.tls.server.hostname".to_string(), server_hostname.clone());
        }

        println!("Configuration map: {:?}", config_map);

        let client = ClientBuilder::new()
            .with_url(&namenode_url)
            .with_config(config_map)
            .build();

        match client {
            Ok(client) => {
                println!("Successfully created TLS client");
                
                // Try to list root directory
                match client.list_status("/", false).await {
                    Ok(files) => {
                        println!("Successfully listed {} files with TLS", files.len());
                    }
                    Err(e) => {
                        println!("Failed to list files with TLS: {}", e);
                        println!("Error debug: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("Failed to create TLS client: {}", e);
                println!("Error debug: {:?}", e);
            }
        }

        Ok(())
    }
}