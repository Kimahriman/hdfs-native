mod common;

#[cfg(feature = "integration-test")]
mod test {
    use crate::common::test_with_features;
    use hdfs_native::minidfs::DfsFeatures;
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_basic() {
        test_with_features(&HashSet::new()).await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "kerberos")]
    async fn test_security_kerberos() {
        test_with_features(&HashSet::from([DfsFeatures::SECURITY]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "token")]
    async fn test_security_token() {
        test_with_features(&HashSet::from([DfsFeatures::SECURITY, DfsFeatures::TOKEN]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    #[cfg(feature = "token")]
    async fn test_privacy_token() {
        test_with_features(&HashSet::from([
            DfsFeatures::SECURITY,
            DfsFeatures::TOKEN,
            DfsFeatures::PRIVACY,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "kerberos")]
    async fn test_privacy_kerberos() {
        test_with_features(&HashSet::from([
            DfsFeatures::SECURITY,
            DfsFeatures::PRIVACY,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_basic_ha() {
        test_with_features(&HashSet::from([DfsFeatures::HA]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "kerberos")]
    async fn test_security_privacy_ha() {
        test_with_features(&HashSet::from([
            DfsFeatures::SECURITY,
            DfsFeatures::PRIVACY,
            DfsFeatures::HA,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "token")]
    async fn test_security_token_ha() {
        test_with_features(&HashSet::from([
            DfsFeatures::SECURITY,
            DfsFeatures::TOKEN,
            DfsFeatures::HA,
        ]))
        .await
        .unwrap();
    }
}
