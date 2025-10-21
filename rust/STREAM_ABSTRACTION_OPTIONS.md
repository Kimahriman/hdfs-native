# Stream Abstraction Options for TLS Integration

## Current Problem

The hdfs-native crate currently uses `TcpStream` throughout the connection infrastructure, but TLS requires `TlsStream<TcpStream>`. Both types implement the same async I/O traits, but they're different concrete types, causing compilation issues.

### Current Usage Pattern
```rust
// Current: Hard-coded TcpStream usage
pub(crate) struct SaslDatanodeConnection {
    stream: BufStream<TcpStream>,  // ← Problem: Only works with TCP
}

impl SaslDatanodeConnection {
    pub fn create(stream: TcpStream) -> Self {  // ← Problem: Only accepts TCP
        Self {
            stream: BufStream::new(stream),
        }
    }
}
```

### TLS Integration Challenge
```rust
// TLS produces this type:
TlsStream<TcpStream>  // Different from TcpStream but implements same traits

// Both implement: AsyncRead + AsyncWrite + Unpin + Send
```

## 🎯 Option 1: Trait Object Abstraction

**Best for**: Clean API, maximum flexibility, minimal code changes

### Implementation
```rust
use tokio::io::{AsyncRead, AsyncWrite};
use std::pin::Pin;

// Define a trait alias for our stream requirements
pub trait HdfsStream: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static {}

// Implement for both stream types
impl HdfsStream for TcpStream {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> HdfsStream for TlsStream<T> {}

// Update connection structures
pub(crate) struct SaslDatanodeConnection {
    stream: BufStream<Box<dyn HdfsStream>>,
}

impl SaslDatanodeConnection {
    pub fn create_tcp(stream: TcpStream) -> Self {
        Self {
            stream: BufStream::new(Box::new(stream)),
        }
    }
    
    pub fn create_tls(stream: TlsStream<TcpStream>) -> Self {
        Self {
            stream: BufStream::new(Box::new(stream)),
        }
    }
}
```

**Pros:**
- ✅ Clean API - consumers don't see the complexity
- ✅ Easy to extend for other stream types (Unix sockets, etc.)
- ✅ Minimal changes to existing logic
- ✅ Standard Rust pattern

**Cons:**
- ❌ Small runtime overhead due to dynamic dispatch
- ❌ Requires heap allocation for Box<dyn Trait>
- ❌ Loss of compile-time optimizations

---

## 🔧 Option 2: Enum Wrapper

**Best for**: Zero-cost abstraction, compile-time optimizations

### Implementation
```rust
// Define enum for different stream types
pub enum ConnectionStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

// Implement AsyncRead for the enum
impl AsyncRead for ConnectionStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ConnectionStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            ConnectionStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

// Implement AsyncWrite for the enum
impl AsyncWrite for ConnectionStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            ConnectionStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            ConnectionStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }
    
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            ConnectionStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            ConnectionStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }
    
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            ConnectionStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            ConnectionStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

// Update connection structures
pub(crate) struct SaslDatanodeConnection {
    stream: BufStream<ConnectionStream>,
}

impl SaslDatanodeConnection {
    pub fn create_tcp(stream: TcpStream) -> Self {
        Self {
            stream: BufStream::new(ConnectionStream::Tcp(stream)),
        }
    }
    
    pub fn create_tls(stream: TlsStream<TcpStream>) -> Self {
        Self {
            stream: BufStream::new(ConnectionStream::Tls(stream)),
        }
    }
}
```

**Pros:**
- ✅ Zero runtime overhead - compiles to match statements
- ✅ No heap allocation
- ✅ Maintains compile-time optimizations
- ✅ Type-safe and exhaustive

**Cons:**
- ❌ More boilerplate code (trait implementations)
- ❌ Need to add new variants for each stream type
- ❌ More complex implementation

---

## 📦 Option 3: Generic Abstraction

**Best for**: Maximum performance, type safety, compile-time dispatch

### Implementation
```rust
// Make connection structures generic over stream type
pub(crate) struct SaslDatanodeConnection<S> 
where 
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static
{
    stream: BufStream<S>,
}

impl<S> SaslDatanodeConnection<S> 
where 
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static
{
    pub fn create(stream: S) -> Self {
        Self {
            stream: BufStream::new(stream),
        }
    }
    
    pub async fn negotiate(
        mut self,
        // ... same parameters
    ) -> Result<(SaslDatanodeReader<S>, SaslDatanodeWriter<S>)> {
        // ... implementation stays mostly the same
    }
}

// Type aliases for convenience
pub type TcpSaslConnection = SaslDatanodeConnection<TcpStream>;
pub type TlsSaslConnection = SaslDatanodeConnection<TlsStream<TcpStream>>;
```

**Pros:**
- ✅ Maximum performance - full compile-time optimization
- ✅ Zero runtime overhead
- ✅ Type-safe and flexible
- ✅ Clean, composable design

**Cons:**
- ❌ Complex type signatures throughout codebase
- ❌ Requires generic parameters to bubble up through call stack
- ❌ More complex API for consumers
- ❌ Potential compilation complexity

---

## 🚀 Option 4: Hybrid Approach (Recommended)

**Best for**: Balance of performance, simplicity, and maintainability

### Implementation Strategy
Combine enum wrapper for public API with trait objects for internal flexibility:

```rust
// Public enum for the main connection types we support
#[derive(Debug)]
pub enum ConnectionStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

// Internal trait for flexibility
trait StreamExt: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync> StreamExt for T {}

impl ConnectionStream {
    // Convert to trait object for internal use where performance isn't critical
    pub(crate) fn into_boxed_stream(self) -> Box<dyn StreamExt> {
        match self {
            ConnectionStream::Tcp(stream) => Box::new(stream),
            ConnectionStream::Tls(stream) => Box::new(stream),
        }
    }
    
    // Keep direct access for performance-critical paths
    pub fn is_tls(&self) -> bool {
        matches!(self, ConnectionStream::Tls(_))
    }
}

// Implement AsyncRead/AsyncWrite for ConnectionStream (enum approach)
// Use direct matching for zero-cost abstraction

// Update connection functions
async fn connect(addr: &str, handle: &Handle) -> Result<ConnectionStream> {
    let stream = handle.spawn(TcpStream::connect(addr)).await.unwrap()?;
    Ok(ConnectionStream::Tcp(stream))
}

async fn connect_tls(addr: &str, tls_config: &TlsConfig, handle: &Handle) -> Result<ConnectionStream> {
    let tcp_stream = handle.spawn(TcpStream::connect(addr)).await.unwrap()?;
    let tls_session = TlsSession::with_config("hdfs", "0", tls_config.clone());
    let tls_stream = tls_session.connect_tls(tcp_stream, tls_config.server_hostname.as_deref().unwrap_or("localhost")).await?;
    Ok(ConnectionStream::Tls(tls_stream))
}
```

**Pros:**
- ✅ Balanced approach - performance where needed, flexibility where useful
- ✅ Clean public API
- ✅ Relatively simple implementation
- ✅ Easy to test and maintain
- ✅ Future-proof for additional stream types

**Cons:**
- ❌ Slightly more complex than single approach
- ❌ Some code duplication in trait implementations

---

## 📊 Recommendation: Option 4 (Hybrid Approach)

### Why Option 4 is Best:

1. **Performance**: Zero-cost abstraction for critical paths (data transfer)
2. **Simplicity**: Clean API that doesn't expose complexity to users
3. **Maintainability**: Clear separation between public interface and internal implementation
4. **Testability**: Easy to mock and test different stream types
5. **Future-proof**: Easy to add new stream types (Unix sockets, QUIC, etc.)

### Implementation Steps:

1. **Phase 1**: Create `ConnectionStream` enum with AsyncRead/AsyncWrite implementations
2. **Phase 2**: Update connection functions to return `ConnectionStream`
3. **Phase 3**: Update `SaslDatanodeConnection` and related structures
4. **Phase 4**: Update `RpcConnection` and related structures
5. **Phase 5**: Add comprehensive tests for both TCP and TLS paths

### Impact Assessment:
- **Low Risk**: Changes are mostly internal, public API remains similar
- **Incremental**: Can be implemented step by step
- **Backward Compatible**: Existing TCP functionality unchanged
- **Forward Compatible**: Easy to extend for new authentication methods

This approach gives us the best of both worlds: the performance of compile-time dispatch where it matters (data transfer) and the flexibility of trait objects where it doesn't (connection setup).