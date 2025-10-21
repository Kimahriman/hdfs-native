# Mutual TLS Authentication Implementation

## Overview

This implementation adds mutual TLS authentication support to hdfs-native, allowing clients to authenticate using X.509 certificates instead of Kerberos tickets or delegation tokens.

## Architecture

### Integration Points

1. **SASL Framework Integration** (`/src/security/sasl.rs`)
   - Added `AuthMethod::Tls` to the existing authentication methods
   - Integrated `TlsSession` into the SASL mechanism selection
   - Maintains compatibility with existing SASL flow

2. **TLS Session Implementation** (`/src/security/tls.rs`) 
   - Implements the `SaslSession` trait for TLS authentication
   - Handles certificate validation and user extraction
   - Manages TLS handshake state machine

3. **Transport Layer** (`/src/hdfs/connection.rs`)
   - Added `connect_tls()` function for TLS-enabled connections
   - Modified `RpcConnection::connect()` and `DatanodeConnection::connect()` to use TLS when configured
   - Maintains backward compatibility with non-TLS connections

4. **Configuration** (`/src/common/config.rs`)
   - Added TLS configuration options
   - Supports both XML configuration files and programmatic configuration

## Configuration Options

| Configuration Key | Description | Default |
|-------------------|-------------|---------|
| `hdfs.tls.enabled` | Enable/disable TLS authentication | `false` |
| `hdfs.tls.client.cert.path` | Path to client certificate (PEM) | Required when TLS enabled |
| `hdfs.tls.client.key.path` | Path to client private key (PEM) | Required when TLS enabled |
| `hdfs.tls.ca.cert.path` | Path to CA certificate (PEM) | Optional |
| `hdfs.tls.verify.server` | Verify server certificate | `true` |
| `hdfs.tls.server.hostname` | Expected server hostname | Optional |

## Implementation Details

### Authentication Flow

1. **Transport Layer TLS**:
   - TCP connection established first
   - TLS handshake performed with mutual authentication
   - Client presents certificate to server
   - Server verifies client certificate against CA

2. **SASL Layer Integration**:
   - TLS session created as SASL mechanism
   - User information extracted from certificate subject
   - SASL framework handles session management

3. **Connection Management**:
   - Both NameNode and DataNode connections support TLS
   - Connection pooling works with TLS connections
   - Existing retry and failover logic preserved

### Security Considerations

- **Certificate Management**: Client certificates must be properly secured
- **CA Trust**: Server certificates validated against configured CA
- **Protocol Compatibility**: Works alongside existing HDFS security mechanisms
- **Performance**: TLS handshake overhead on connection establishment

## TODO Items for Full Implementation

### High Priority
1. **TLS Library Integration**: Choose and integrate rustls or openssl-rust
2. **Certificate Loading**: Implement PEM certificate and key parsing
3. **TLS Handshake**: Complete mutual authentication handshake
4. **User Extraction**: Parse user information from certificate DN

### Medium Priority
1. **Certificate Validation**: Implement certificate chain validation
2. **Hostname Verification**: Verify server certificate hostname
3. **Error Handling**: Comprehensive TLS error handling and mapping
4. **Configuration Loading**: Environment variable support

### Low Priority
1. **Certificate Reloading**: Support certificate rotation
2. **TLS Session Resumption**: Optimize performance
3. **Cipher Suite Configuration**: Allow cipher customization
4. **OCSP Support**: Certificate revocation checking

## Usage Example

```rust
use std::collections::HashMap;
use hdfs_native::common::config::Configuration;

// Configure TLS authentication
let mut config = HashMap::new();
config.insert("hdfs.tls.enabled".to_string(), "true".to_string());
config.insert("hdfs.tls.client.cert.path".to_string(), "/etc/hdfs/ssl/client.crt".to_string());
config.insert("hdfs.tls.client.key.path".to_string(), "/etc/hdfs/ssl/client.key".to_string());
config.insert("hdfs.tls.ca.cert.path".to_string(), "/etc/hdfs/ssl/ca.crt".to_string());

let hdfs_config = Configuration::new_with_config(config)?;
// Use hdfs_config to create HDFS client with TLS authentication
```

## Testing Strategy

1. **Unit Tests**: Test certificate loading, validation, and configuration
2. **Integration Tests**: Test with mock TLS server and real certificates  
3. **End-to-End Tests**: Test against HDFS cluster with TLS enabled
4. **Security Tests**: Test certificate validation edge cases

## Deployment Considerations

1. **Certificate Distribution**: Secure distribution of client certificates
2. **CA Management**: Proper CA certificate management and rotation
3. **Network Security**: TLS does not replace network security measures
4. **Monitoring**: Monitor certificate expiration and TLS handshake failures