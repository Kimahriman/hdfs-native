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
        test::NAMENODE_STANDBY_FAULT_INJECTOR,
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
