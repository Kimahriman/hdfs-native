#[cfg(all(feature = "integration-test", unix))]
mod test {
    use hdfs_native::{
        Client,
        minidfs::{DfsFeatures, MiniDfs},
    };
    use serial_test::serial;
    use std::collections::HashSet;
    use std::process::Command;

    const DIRS: usize = 1000;
    const NAME_PADDING: usize = 200;
    const WARMUP_ROUNDS: usize = 50;
    const MEASURED_ROUNDS: usize = 500;
    const SAMPLE_EVERY: usize = 100;
    const MAX_GROWTH_BYTES: u64 = 32 * 1024 * 1024;

    fn rss_bytes() -> u64 {
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .unwrap();
        String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap()
            * 1024
    }

    async fn list_round(client: &Client) {
        let listing = client.list_status("/leak", false).await.unwrap();
        assert_eq!(listing.len(), DIRS);
    }

    #[tokio::test]
    #[serial]
    async fn test_privacy_list_status_rss_stable() {
        let _dfs = MiniDfs::with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
        ]));
        let client = Client::default();

        client.mkdirs("/leak", 0o755, true).await.unwrap();
        for i in 0..DIRS {
            client
                .mkdirs(
                    &format!("/leak/dir{i:04}{}", "x".repeat(NAME_PADDING)),
                    0o755,
                    false,
                )
                .await
                .unwrap();
        }

        for _ in 0..WARMUP_ROUNDS {
            list_round(&client).await;
        }
        let before = rss_bytes();
        for round in 1..=MEASURED_ROUNDS {
            list_round(&client).await;
            if round % SAMPLE_EVERY == 0 {
                println!("round {round} rss={}MB", rss_bytes() >> 20);
            }
        }
        let after = rss_bytes();

        let growth = after.saturating_sub(before);
        println!(
            "rss before={}MB after={}MB growth={}MB over {} listings of {} entries",
            before >> 20,
            after >> 20,
            growth >> 20,
            MEASURED_ROUNDS,
            DIRS
        );
        assert!(
            growth < MAX_GROWTH_BYTES,
            "RSS grew {}MB over {} SASL-wrapped listings",
            growth >> 20,
            MEASURED_ROUNDS
        );
    }
}
