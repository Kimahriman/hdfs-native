#![cfg(feature = "integration-test")]

use std::{collections::HashMap, sync::Arc};

use prost::Message;
use tokio::runtime::Handle;
use yarn_native::{
    config::{Configuration, RM_ADDRESS},
    miniyarn::MiniYarn,
    proto::yarn::{GetClusterMetricsRequestProto, GetClusterMetricsResponseProto},
    proxy::{RpcProtocol, YarnProxy},
};

#[test]
fn mini_yarn_resource_manager_rpc_round_trip() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create Tokio runtime");

    runtime.block_on(async {
        let cluster = MiniYarn::new().expect("failed to start MiniYARNCluster");
        let conf_dir = tempfile::tempdir().expect("failed to create isolated config directory");
        let config = Arc::new(
            Configuration::new(
                Some(conf_dir.path().to_string_lossy().into_owned()),
                Some(HashMap::from([(
                    RM_ADDRESS.to_owned(),
                    cluster.resource_manager_address().to_owned(),
                )])),
            )
            .expect("failed to create YARN configuration"),
        );

        let proxy = YarnProxy::new(
            config,
            RpcProtocol::application_client(cluster.resource_manager_address()),
            None,
            None,
            Handle::current(),
        )
        .expect("failed to create ResourceManager proxy");

        let request = GetClusterMetricsRequestProto::default().encode_length_delimited_to_vec();
        let response = proxy
            .call("getClusterMetrics", &request, false)
            .await
            .expect("ResourceManager RPC failed");
        let response = GetClusterMetricsResponseProto::decode_length_delimited(response)
            .expect("failed to decode ResourceManager response");

        assert!(response.cluster_metrics.is_some());
    });
}
