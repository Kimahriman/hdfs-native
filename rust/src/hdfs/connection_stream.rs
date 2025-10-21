use tokio_rustls::client::TlsStream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use std::task::{Context, Poll};
use std::pin::Pin;
use tokio::net::TcpStream;

/// Abstraction over different connection stream types
#[derive(Debug)]
pub enum ConnectionStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl ConnectionStream {
    /// Check if this is a TLS connection
    pub fn is_tls(&self) -> bool {
        matches!(self, ConnectionStream::Tls(_))
    }

    /// Get the peer address if available
    pub fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        match self {
            ConnectionStream::Tcp(stream) => stream.peer_addr(),
            ConnectionStream::Tls(stream) => stream.get_ref().0.peer_addr(),
        }
    }

    /// Split the stream into read and write halves
    pub fn into_split(self) -> (ConnectionStreamReadHalf, ConnectionStreamWriteHalf) {
        match self {
            ConnectionStream::Tcp(stream) => {
                let (read, write) = stream.into_split();
                (ConnectionStreamReadHalf::Tcp(read), ConnectionStreamWriteHalf::Tcp(write))
            }
            ConnectionStream::Tls(stream) => {
                let (read, write) = tokio::io::split(stream);
                (ConnectionStreamReadHalf::Tls(read), ConnectionStreamWriteHalf::Tls(write))
            }
        }
    }
}

/// Read half of a ConnectionStream
#[derive(Debug)]
pub enum ConnectionStreamReadHalf {
    Tcp(tokio::net::tcp::OwnedReadHalf),
    Tls(tokio::io::ReadHalf<TlsStream<TcpStream>>),
}

impl AsyncRead for ConnectionStreamReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnectionStreamReadHalf::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            ConnectionStreamReadHalf::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

/// Write half of a ConnectionStream  
#[derive(Debug)]
pub enum ConnectionStreamWriteHalf {
    Tcp(tokio::net::tcp::OwnedWriteHalf),
    Tls(tokio::io::WriteHalf<TlsStream<TcpStream>>),
}

impl AsyncWrite for ConnectionStreamWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ConnectionStreamWriteHalf::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            ConnectionStreamWriteHalf::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnectionStreamWriteHalf::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            ConnectionStreamWriteHalf::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnectionStreamWriteHalf::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            ConnectionStreamWriteHalf::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

// Implement AsyncRead for ConnectionStream
impl AsyncRead for ConnectionStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnectionStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            ConnectionStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

// Implement AsyncWrite for ConnectionStream
impl AsyncWrite for ConnectionStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ConnectionStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            ConnectionStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnectionStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            ConnectionStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnectionStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            ConnectionStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
