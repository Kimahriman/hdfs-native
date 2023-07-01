mod common;

use std::collections::HashSet;

use common::minidfs::DfsFeatures;

use common::test_with_features;

#[tokio::test]
async fn test_basic() {
    test_with_features(&HashSet::new()).await.unwrap();
}

#[tokio::test]
async fn test_security_kerberos() {
    test_with_features(&HashSet::from([DfsFeatures::SECURITY]))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn test_security_token() {
    test_with_features(&HashSet::from([DfsFeatures::SECURITY, DfsFeatures::TOKEN]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_security_privacy() {
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
async fn test_security_privacy_ha() {
    test_with_features(&HashSet::from([
        DfsFeatures::SECURITY,
        DfsFeatures::PRIVACY,
        DfsFeatures::HA,
    ]))
    .await
    .unwrap();
}
