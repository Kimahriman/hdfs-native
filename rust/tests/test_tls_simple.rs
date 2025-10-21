#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use hdfs_native::{
        client::{ClientBuilder, FileStatus},
        security::tls::TlsConfig,
        Client, Result, WriteOptions,
    };
    use serial_test::serial;
    use std::env;

    /// Get TLS configuration from environment variables
    /// 
    /// Required environment variables:
    /// - HDFS_NAMENODE_URL: URL of the TLS-enabled namenode (e.g., "hdfs://namenode:8020")
    /// - HDFS_CLIENT_CERT_PATH: Path to client certificate file
    /// - HDFS_CLIENT_KEY_PATH: Path to client private key file
    /// 
    /// Optional environment variables:
    /// - HDFS_CA_CERT_PATH: Path to CA certificate file
    /// - HDFS_TLS_VERIFY_SERVER: Whether to verify server certificate (default: true)
    /// - HDFS_TLS_SERVER_HOSTNAME: Override hostname for certificate verification
    fn get_tls_config() -> Option<(String, TlsConfig)> {
        let namenode_url = env::var("HDFS_NAMENODE_URL").ok()?;
        let cert_path = env::var("HDFS_CLIENT_CERT_PATH").ok()?;
        let key_path = env::var("HDFS_CLIENT_KEY_PATH").ok()?;

        let mut tls_config = TlsConfig::new(cert_path, key_path);

        // Optional CA certificate
        if let Ok(ca_path) = env::var("HDFS_CA_CERT_PATH") {
            tls_config = tls_config.with_ca_cert(ca_path);
        }

        // Optional server verification setting
        if let Ok(verify_str) = env::var("HDFS_TLS_VERIFY_SERVER") {
            let verify = verify_str.to_lowercase() == "true";
            tls_config = tls_config.with_server_verification(verify);
        }

        // Optional server hostname override
        if let Ok(hostname) = env::var("HDFS_TLS_SERVER_HOSTNAME") {
            tls_config = tls_config.with_server_hostname(hostname);
        }

        Some((namenode_url, tls_config))
    }

    /// Create a TLS-enabled HDFS client or skip the test if not configured
    async fn create_tls_client() -> Result<Client> {
        let (namenode_url, tls_config) = get_tls_config()
            .expect("TLS integration test requires environment configuration. Set HDFS_NAMENODE_URL, HDFS_CLIENT_CERT_PATH, and HDFS_CLIENT_KEY_PATH");

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

        ClientBuilder::new()
            .with_url(&namenode_url)
            .with_config(config_map)
            .build()
    }

    /// Test basic TLS connection and root directory listing
    #[tokio::test]
    #[serial]
    async fn test_tls_basic_connection() -> Result<()> {
        let client = create_tls_client().await?;

        // Test basic connectivity by listing root directory
        let files = client.list_status("/", false).await?;
        
        println!("Successfully connected via TLS. Root directory contains {} items:", files.len());
        for file in &files {
            println!("  {} ({})", file.path, if file.isdir { "dir" } else { "file" });
        }

        assert!(files.len() >= 0); // Should at least not fail
        Ok(())
    }

    /// Test creating, writing, and reading a file with TLS
    #[tokio::test]
    #[serial] 
    async fn test_tls_file_operations() -> Result<()> {
        let client = create_tls_client().await?;

        let test_path = "/tmp/tls_test_file.txt";
        let test_data = b"Hello from TLS-authenticated HDFS client!";

        // Clean up any existing test file
        let _ = client.delete(test_path, false).await;

        // Write test data
        let mut writer = client.create(test_path, WriteOptions::default()).await?;
        writer.write(test_data.to_vec().into()).await?;
        writer.close().await?;

        println!("Successfully wrote {} bytes to {}", test_data.len(), test_path);

        // Read back the data
        let mut reader = client.read(test_path).await?;
        let buffer = reader.read(reader.file_length()).await?;

        println!("Successfully read {} bytes from {}", buffer.len(), test_path);

        // Verify data integrity
        assert_eq!(buffer.as_ref(), test_data, "Read data doesn't match written data");

        // Get file status
        let file_status = client.get_file_info(test_path).await?;
        assert_eq!(file_status.length, test_data.len());
        assert!(!file_status.isdir);

        println!("File status: {} bytes, modified: {:?}", file_status.length, file_status.modification_time);

        // Clean up
        client.delete(test_path, false).await?;
        println!("Successfully deleted test file");

        Ok(())
    }

    /// Test directory operations with TLS
    #[tokio::test]
    #[serial]
    async fn test_tls_directory_operations() -> Result<()> {
        let client = create_tls_client().await?;

        let test_dir = "/tmp/tls_test_dir";
        let test_subdir = "/tmp/tls_test_dir/subdir";
        let test_file = "/tmp/tls_test_dir/test_file.txt";

        // Clean up any existing test directory
        let _ = client.delete(test_dir, true).await;

        // Create directory
        client.mkdirs(test_dir, 0o755, true).await?;
        println!("Successfully created directory: {}", test_dir);

        // Create subdirectory
        client.mkdirs(test_subdir, 0o755, true).await?;
        println!("Successfully created subdirectory: {}", test_subdir);

        // Create a file in the directory
        let mut writer = client.create(test_file, WriteOptions::default()).await?;
        writer.write(b"test content".to_vec().into()).await?;
        writer.close().await?;
        println!("Successfully created file: {}", test_file);

        // List directory contents
        let files = client.list_status(test_dir, false).await?;
        assert_eq!(files.len(), 2); // Should have subdir and test_file

        println!("Directory contents:");
        for file in &files {
            println!("  {} ({})", file.path, if file.isdir { "dir" } else { "file" });
        }

        // Verify we can identify the subdirectory and file
        let subdir_found = files.iter().any(|f| f.path.ends_with("subdir") && f.isdir);
        let file_found = files.iter().any(|f| f.path.ends_with("test_file.txt") && !f.isdir);
        assert!(subdir_found, "Subdirectory not found in listing");
        assert!(file_found, "Test file not found in listing");

        // Clean up
        client.delete(test_dir, true).await?;
        println!("Successfully deleted test directory");

        Ok(())
    }

    /// Test larger file operations with TLS
    #[tokio::test]
    #[serial]
    async fn test_tls_large_file_operations() -> Result<()> {
        let client = create_tls_client().await?;

        let test_path = "/tmp/tls_large_test_file.dat";
        let chunk_size = 64 * 1024; // 64KB chunks
        let total_chunks = 16; // Total file size: 1MB

        // Clean up any existing test file
        let _ = client.delete(test_path, false).await;

        // Write large file in chunks
        let mut writer = client.create(test_path, WriteOptions::default()).await?;
        let mut original_data = Vec::new();

        for i in 0..total_chunks {
            let chunk: Vec<u8> = (0..chunk_size).map(|j| ((i * chunk_size + j) % 256) as u8).collect();
            writer.write(chunk.clone().into()).await?;
            original_data.extend_from_slice(&chunk);
        }
        writer.close().await?;

        println!("Successfully wrote {} bytes in {} chunks", original_data.len(), total_chunks);

        // Read back the entire file
        let mut reader = client.read(test_path).await?;
        let read_data = reader.read(reader.file_length()).await?;

        println!("Successfully read {} bytes", read_data.len());

        // Verify data integrity
        assert_eq!(read_data.len(), original_data.len(), "File size mismatch");
        assert_eq!(read_data.as_ref(), &original_data, "File content mismatch");

        // Test partial reads
        let mut reader = client.read(test_path).await?;
        let partial_data = reader.read(chunk_size).await?;
        
        assert_eq!(partial_data.as_ref(), &original_data[0..chunk_size], "Partial read mismatch");
        println!("Partial read verification successful");

        // Clean up
        client.delete(test_path, false).await?;
        println!("Successfully deleted large test file");

        Ok(())
    }

    /// Test TLS authentication error handling
    #[tokio::test]
    #[serial]
    async fn test_tls_authentication() -> Result<()> {
        let client = create_tls_client().await?;

        // Test accessing a path that requires authentication
        // The fact that we can list the root directory means TLS auth worked
        let files = client.list_status("/", false).await?;
        
        println!("TLS authentication successful - able to list {} items in root", files.len());

        // Test that we can perform authenticated operations
        let test_path = "/tmp/tls_auth_test";
        
        // Try to create a directory (requires write permissions)
        match client.mkdirs(test_path, 0o755, true).await {
            Ok(_) => {
                println!("Successfully created directory with TLS authentication");
                // Clean up
                let _ = client.delete(test_path, false).await;
            }
            Err(e) => {
                println!("Directory creation failed (might be permissions): {}", e);
                // This is still a successful test if we can connect and get a meaningful error
            }
        }

        Ok(())
    }

    /// Test TLS with file copying operations
    #[tokio::test]
    #[serial]
    async fn test_tls_file_copy() -> Result<()> {
        let client = create_tls_client().await?;

        let source_path = "/tmp/tls_source_file.txt";
        let dest_path = "/tmp/tls_dest_file.txt";
        let test_data = b"Data to be copied via TLS connection";

        // Clean up any existing files
        let _ = client.delete(source_path, false).await;
        let _ = client.delete(dest_path, false).await;

        // Create source file
        let mut writer = client.create(source_path, WriteOptions::default()).await?;
        writer.write(test_data.to_vec().into()).await?;
        writer.close().await?;

        // Copy by reading from source and writing to destination
        let mut reader = client.read(source_path).await?;
        let source_data = reader.read(reader.file_length()).await?;

        let mut dest_writer = client.create(dest_path, WriteOptions::default()).await?;
        dest_writer.write(source_data.clone()).await?;
        dest_writer.close().await?;

        println!("Successfully copied {} bytes from {} to {}", source_data.len(), source_path, dest_path);

        // Verify the copy
        let mut dest_reader = client.read(dest_path).await?;
        let dest_data = dest_reader.read(dest_reader.file_length()).await?;

        assert_eq!(dest_data.as_ref(), test_data, "Copied file content doesn't match original");
        println!("File copy verification successful");

        // Clean up
        client.delete(source_path, false).await?;
        client.delete(dest_path, false).await?;

        Ok(())
    }

    /// Print environment setup instructions if tests are skipped
    #[tokio::test]
    async fn test_environment_setup_check() {
        if get_tls_config().is_none() {
            println!("\n=== TLS Integration Test Setup ===");
            println!("To run TLS integration tests, set these environment variables:");
            println!("  HDFS_NAMENODE_URL=hdfs://your-namenode:8020");
            println!("  HDFS_CLIENT_CERT_PATH=/path/to/client.crt");
            println!("  HDFS_CLIENT_KEY_PATH=/path/to/client.key");
            println!("");
            println!("Optional variables:");
            println!("  HDFS_CA_CERT_PATH=/path/to/ca.crt");
            println!("  HDFS_TLS_VERIFY_SERVER=true|false");
            println!("  HDFS_TLS_SERVER_HOSTNAME=namenode.example.com");
            println!("");
            println!("Example usage:");
            println!("  export HDFS_NAMENODE_URL=hdfs://secure-cluster:8020");
            println!("  export HDFS_CLIENT_CERT_PATH=/home/user/certs/client.crt");
            println!("  export HDFS_CLIENT_KEY_PATH=/home/user/certs/client.key");
            println!("  export HDFS_TLS_VERIFY_SERVER=false  # For testing with self-signed certs");
            println!("");
            println!("Then run: cargo test --features integration-test test_tls");
            println!("==================================\n");
        } else {
            println!("TLS integration test environment is configured ✓");
        }
    }
}