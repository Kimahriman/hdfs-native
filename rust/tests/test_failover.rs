#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use crate::common::EnvVarGuard;
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use hdfs_native::{
        Client, ClientBuilder, Result,
        minidfs::{DfsFeatures, MiniDfs},
        test::{NAMENODE_RESPONSE_FAULT_INJECTOR, NAMENODE_STANDBY_FAULT_INJECTOR},
    };
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_standby_failover_retry() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA]));
        let client = Client::default();

        client.mkdirs("/pre-failover", 0o755, true).await?;

        NAMENODE_STANDBY_FAULT_INJECTOR.store(true, Ordering::SeqCst);

        let write_during_failover = tokio::spawn({
            let client = client.clone();
            async move { client.mkdirs("/during-failover", 0o755, true).await }
        });

        tokio::time::sleep(Duration::from_secs(2)).await;
        NAMENODE_STANDBY_FAULT_INJECTOR.store(false, Ordering::SeqCst);

        write_during_failover.await.unwrap()?;
        assert!(client.get_file_info("/during-failover").await.is_ok());

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_ambiguous_write_is_not_retried() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA]));
        let client = Client::default();

        // Warm up the observer/active connections and complete the write-side msync so the
        // injected failure is consumed by the mkdirs response itself.
        client.mkdirs("/pre-ambiguous-write", 0o755, true).await?;

        NAMENODE_RESPONSE_FAULT_INJECTOR.store(true, Ordering::SeqCst);
        let result = client.mkdirs("/ambiguous-write", 0o755, true).await;
        NAMENODE_RESPONSE_FAULT_INJECTOR.store(false, Ordering::SeqCst);

        assert!(result.is_err());
        // The response was lost after the NameNode committed the mutation. A later read proves
        // that the failed write reached the server without allowing the client to replay it.
        assert!(client.get_file_info("/ambiguous-write").await.is_ok());

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_explicit_keytab_survives_ha_reconnect() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::Security, DfsFeatures::HA]));
        let _cache_guard = EnvVarGuard::set("KRB5CCNAME", "FILE:target/test/nonexistent-cache");
        let client = ClientBuilder::new()
            .with_kerberos_credentials(
                Some("hdfs/localhost".to_string()),
                Some("target/test/hdfs.keytab".to_string()),
                None,
            )
            .build()?;

        client
            .mkdirs("/keytab-before-failover", 0o755, true)
            .await?;
        NAMENODE_STANDBY_FAULT_INJECTOR.store(true, Ordering::SeqCst);
        let operation = tokio::spawn({
            let client = client.clone();
            async move { client.mkdirs("/keytab-during-failover", 0o755, true).await }
        });
        tokio::time::sleep(Duration::from_secs(2)).await;
        NAMENODE_STANDBY_FAULT_INJECTOR.store(false, Ordering::SeqCst);

        operation.await.unwrap()?;
        assert!(
            client
                .get_file_info("/keytab-during-failover")
                .await
                .is_ok()
        );
        Ok(())
    }
}
