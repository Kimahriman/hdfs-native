use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::runtime::Handle;
use tokio::time::Sleep;

pub(crate) struct IdleTimeoutReader<R> {
    inner: R,
    timeout: Option<(Duration, Handle)>,
    idle: Option<Pin<Box<Sleep>>>,
}

impl<R> IdleTimeoutReader<R> {
    pub(crate) fn new(inner: R, timeout: Option<(Duration, Handle)>) -> Self {
        Self {
            inner,
            timeout,
            idle: None,
        }
    }

    pub(crate) fn into_parts(self) -> (R, Option<(Duration, Handle)>) {
        (self.inner, self.timeout)
    }

    pub(crate) fn set_timeout(&mut self, timeout: Option<(Duration, Handle)>) {
        self.timeout = timeout;
        self.idle = None;
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for IdleTimeoutReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if let Poll::Ready(result) = Pin::new(&mut this.inner).poll_read(cx, buf) {
            this.idle = None;
            return Poll::Ready(result);
        }

        let Some((timeout, handle)) = &this.timeout else {
            return Poll::Pending;
        };

        let idle = this.idle.get_or_insert_with(|| {
            let _runtime = handle.enter();
            Box::pin(tokio::time::sleep(*timeout))
        });
        if idle.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("No data received for {}ms", timeout.as_millis()),
        )))
    }
}

impl<R: AsyncWrite + Unpin> AsyncWrite for IdleTimeoutReader<R> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
