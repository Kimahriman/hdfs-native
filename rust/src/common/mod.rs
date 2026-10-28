pub mod config;

use crate::{HdfsError, Result};

/// Run synchronous work that may block without occupying a Tokio worker thread.
pub(crate) async fn run_blocking<T, F>(operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| {
            HdfsError::OperationFailed(format!("Blocking operation failed: {error}"))
        })?
}

#[cfg(test)]
mod tests {
    use super::run_blocking;

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_operation_runs_off_the_async_worker() {
        let async_thread = std::thread::current().id();
        let blocking_thread = run_blocking(|| Ok(std::thread::current().id()))
            .await
            .unwrap();

        assert_ne!(blocking_thread, async_thread);
    }
}
