# Mutual TLS Authentication Implementation

## Overview

This implementation adds mutual TLS authentication support to hdfs-native, allowing clients to authenticate using X.509 certificates. The implementation uses **transport-layer TLS** (not SASL-based authentication) to establish encrypted connections with mutual certificate authentication.

## Architecture

### Core Components

1. **TLS Configuration** (`/src/security/tls.rs`)
   - `TlsConfig` struct for certificate and TLS settings
   - Support for client certificates, CA certificates, and hostname verification control
   - Integration with rustls for secure TLS implementation

2. **Connection Stream Abstraction** (`/src/hdfs/connection_stream.rs`)
   - `ConnectionStream` enum supporting both TCP and TLS connections
   - Zero-cost abstraction for transparent protocol handling
   - Implements `AsyncRead` and `AsyncWrite` for both connection types

3. **Transport Layer Integration** (`/src/hdfs/connection.rs`)
   - `connect_tls()` function for establishing TLS connections
   - Direct TLS configuration and handshake at transport layer
   - Hostname verification with skip option (default: disabled)

4. **Configuration Support** (`/src/common/config.rs`)
   - Environment variable and configuration file support
   - Integration with existing HDFS configuration system

### Key Design Decisions

- **Transport-Layer TLS**: TLS encryption and authentication happen at the transport layer, not as a SASL mechanism
- **Hostname Verification Skip**: Disabled by default (like Go client) to handle IP-based connections
- **Certificate Validation**: Full certificate chain validation using configured CA certificates
- **Zero-Cost Abstraction**: `ConnectionStream` enum allows transparent handling of TCP vs TLS connections

## Configuration Options

| Configuration Key | Description | Default |
|-------------------|-------------|---------|
| `hdfs.tls.enabled` | Enable/disable TLS authentication | `false` |
| `hdfs.tls.client.cert.path` | Path to client certificate (PEM) | Required when TLS enabled |
| `hdfs.tls.client.key.path` | Path to client private key (PEM) | Required when TLS enabled |
| `hdfs.tls.ca.cert.path` | Path to CA certificate (PEM) | Optional (uses system CA if not provided) |
| `hdfs.tls.verify.server` | Enable hostname verification | `false` |
| `hdfs.tls.server.hostname` | Override server hostname for verification | Optional |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `HDFS_CLIENT_CERT_PATH` | Path to client certificate file |
| `HDFS_CLIENT_KEY_PATH` | Path to client private key file |
| `HDFS_CA_CERT_PATH` | Path to CA certificate file |
| `HDFS_TLS_VERIFY_SERVER` | Enable/disable server verification (`true`/`false`) |
| `HDFS_TLS_SERVER_HOSTNAME` | Server hostname for verification |

## Implementation Details

### TLS Handshake Flow

1. **Connection Establishment**:
   ```rust
   // TCP connection established first
   let tcp_stream = TcpStream::connect(addr).await?;
   
   // TLS configuration built from certificates
   let tls_config = config.build_client_config()?;
   let connector = TlsConnector::from(Arc::new(tls_config));
   
   // TLS handshake with mutual authentication
   let tls_stream = connector.connect(server_name, tcp_stream).await?;
   ```

2. **Certificate Verification**:
   - Client presents certificate to server for mutual authentication
   - Server certificate validated against configured CA
   - Custom `NoHostnameVerifier` allows skipping hostname verification
   - Full certificate chain validation performed

3. **Connection Abstraction**:
   ```rust
   pub enum ConnectionStream {
       Tcp(TcpStream),
       Tls(TlsStream<TcpStream>),
   }
   ```

### Dependencies

- **rustls** (v0.23): Modern TLS library with ring crypto provider
- **tokio-rustls** (v0.26): Async TLS integration for tokio
- **rustls-pemfile** (v2.0): PEM certificate/key file parsing
- **webpki-roots** (v0.26): Default CA certificate roots

## Usage Example

### Environment-Based Configuration

```bash
# Set up environment variables
export HDFS_CLIENT_CERT_PATH="/path/to/client.pem"
export HDFS_CLIENT_KEY_PATH="/path/to/client.key"
export HDFS_CA_CERT_PATH="/path/to/ca.pem"
export HDFS_TLS_VERIFY_SERVER="false"

# Use with hdfs-native client
export HDFS_NAMENODE_URL="hdfs://namenode.example.com:8020"
```

### Programmatic Configuration

```rust
use std::collections::HashMap;
use hdfs_native::client::ClientBuilder;

let mut config = HashMap::new();
config.insert("hdfs.tls.enabled".to_string(), "true".to_string());
config.insert("hdfs.tls.client.cert.path".to_string(), "/etc/ssl/client.pem".to_string());
config.insert("hdfs.tls.client.key.path".to_string(), "/etc/ssl/client.key".to_string());
config.insert("hdfs.tls.ca.cert.path".to_string(), "/etc/ssl/ca.pem".to_string());
config.insert("hdfs.tls.verify.server".to_string(), "false".to_string());

let client = ClientBuilder::new()
    .with_url("hdfs://namenode.example.com:8020")
    .with_config(config)
    .build()?;
```

## Testing

### Integration Tests

The implementation includes comprehensive integration tests:

- **Real Cluster Testing**: Tests against actual HDFS clusters with TLS enabled
- **Certificate Validation**: Tests with real certificates (Hopsworks cluster)
- **Environment Configuration**: Tests loading configuration from environment variables
- **Connection Establishment**: Tests TLS handshake and connection setup

### Test Files

- `tests/test_tls_simple.rs`: Basic TLS operations (list, read, write, copy)
- `tests/test_tls_debug.rs`: Debug tests for TLS connection troubleshooting

## Security Features

### Certificate Validation

- **Mutual Authentication**: Both client and server certificates validated
- **Certificate Chain Validation**: Full chain validation using CA certificates
- **Hostname Verification**: Optional (disabled by default for flexibility)
- **Crypto Provider**: Uses ring crypto provider for security

### Custom Certificate Verifier

```rust
struct NoHostnameVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}
```

- Validates certificate chain and signatures
- Skips hostname verification when configured
- Maintains security while allowing IP-based connections

## Deployment Considerations

### Certificate Management

1. **Client Certificates**: Must be accessible to the application with proper file permissions
2. **CA Certificates**: Should include the full certificate chain for validation
3. **Private Keys**: Must be protected with appropriate file system permissions (0600)

### Network Configuration

1. **DNS vs IP**: Use DNS names when possible for proper hostname verification
2. **Port Configuration**: Ensure TLS is enabled on the correct HDFS ports
3. **Firewall Rules**: TLS connections still use standard HDFS ports

### Monitoring

1. **Certificate Expiration**: Monitor certificate validity periods
2. **TLS Handshake Failures**: Monitor connection establishment errors
3. **Performance**: TLS adds handshake overhead to connection establishment

## Status

✅ **Complete Implementation**
- Transport-layer TLS with mutual authentication
- Certificate loading and validation
- Hostname verification with skip option
- Integration with HDFS client
- Comprehensive testing with real cluster

The mutual TLS implementation is **production-ready** and has been tested with real HDFS clusters using proper certificates.