mod common;

use std::collections::HashSet;

use common::minidfs::DfsFeatures;

use common::test_with_features;

#[test]
fn test_basic() {
    test_with_features(&HashSet::new()).unwrap();
}

#[test]
fn test_security_kerberos() {
    test_with_features(&HashSet::from([DfsFeatures::SECURITY])).unwrap();
}

#[test]
fn test_security_token() {
    test_with_features(&HashSet::from([DfsFeatures::SECURITY, DfsFeatures::TOKEN])).unwrap();
}

#[test]
fn test_security_privacy() {
    test_with_features(&HashSet::from([
        DfsFeatures::SECURITY,
        DfsFeatures::PRIVACY,
    ]))
    .unwrap();
}

#[test]
fn test_basic_ha() {
    test_with_features(&HashSet::from([DfsFeatures::HA])).unwrap();
}

#[test]
fn test_security_privacy_ha() {
    test_with_features(&HashSet::from([
        DfsFeatures::SECURITY,
        DfsFeatures::PRIVACY,
        DfsFeatures::HA,
    ]))
    .unwrap();
}
