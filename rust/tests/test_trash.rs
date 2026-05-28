#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use hdfs_native::{
        Client, ClientBuilder, Result, WriteOptions,
        minidfs::{DfsFeatures, MiniDfs},
    };
    use serial_test::serial;
    use std::collections::HashSet;
    use whoami::username;

    async fn touch(client: &Client, path: &str) -> Result<()> {
        client
            .create(path, WriteOptions::default())
            .await?
            .close()
            .await
    }

    #[tokio::test]
    #[serial]
    async fn test_trash() {
        let _ = env_logger::builder().is_test(true).try_init();

        let features = HashSet::from([DfsFeatures::Trash]);
        let _dfs = MiniDfs::with_features(&features);
        let client = ClientBuilder::new().build().unwrap();

        test_trash_behavior(&client).await.unwrap();
    }

    async fn test_trash_behavior(client: &Client) -> Result<()> {
        let trash_current = format!("/user/{}/.Trash/Current", username());

        touch(client, "/trash_normal/file").await?;
        assert!(client.trash("/trash_normal/file").await?);
        assert!(client.get_file_info("/trash_normal/file").await.is_err());
        assert!(
            client
                .get_file_info(&format!("{trash_current}/trash_normal/file"))
                .await
                .is_ok()
        );

        assert!(
            !client
                .trash(&format!("{trash_current}/trash_normal/file"))
                .await?
        );

        touch(client, "/trash_collision/file").await?;
        touch(client, &format!("{trash_current}/trash_collision/file")).await?;
        assert!(client.trash("/trash_collision/file").await?);
        assert!(client.get_file_info("/trash_collision/file").await.is_err());
        assert!(
            client
                .get_file_info(&format!("{trash_current}/trash_collision/file"))
                .await
                .is_ok()
        );

        let collision_entries = client
            .list_status(&format!("{trash_current}/trash_collision"), false)
            .await?;
        assert!(
            collision_entries.iter().any(|status| {
                status
                    .path
                    .starts_with(&format!("{trash_current}/trash_collision/file"))
                    && status.path != format!("{trash_current}/trash_collision/file")
            }),
            "{collision_entries:?}"
        );

        touch(client, "/trash_obstructed/child").await?;
        touch(client, &format!("{trash_current}/trash_obstructed")).await?;
        assert!(client.trash("/trash_obstructed/child").await?);
        assert!(
            client
                .get_file_info("/trash_obstructed/child")
                .await
                .is_err()
        );

        let root_entries = client.list_status(&trash_current, false).await?;
        let fallback_dir = root_entries
            .iter()
            .find(|status| {
                status.isdir
                    && status
                        .path
                        .starts_with(&format!("{trash_current}/trash_obstructed"))
                    && status.path != format!("{trash_current}/trash_obstructed")
            })
            .expect("non-directory ancestor should get a timestamped fallback directory");
        assert!(
            client
                .get_file_info(&format!("{}/child", fallback_dir.path))
                .await
                .is_ok()
        );

        Ok(())
    }
}
