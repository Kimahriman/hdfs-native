use crate::Result;
use crate::proto::yarn;
use crate::proxy::{Protocol, ServiceProxy};

pub(crate) const PROTOCOL: Protocol = Protocol {
    name: "org.apache.hadoop.yarn.api.ContainerManagementProtocolPB",
    token_kind: "NMToken",
};

/// A NodeManager's container management protocol. NodeManagers always authenticate these
/// connections with the application attempt's NMToken for that node.
#[derive(Debug)]
pub(crate) struct ContainerManagementProtocol {
    proxy: ServiceProxy,
}

impl ContainerManagementProtocol {
    pub(crate) fn new(proxy: ServiceProxy) -> Self {
        Self { proxy }
    }

    pub(crate) async fn start_containers(
        &self,
        requests: Vec<yarn::StartContainerRequestProto>,
    ) -> Result<yarn::StartContainersResponseProto> {
        let message = yarn::StartContainersRequestProto {
            start_container_request: requests,
        };
        self.proxy.call("startContainers", message, true).await
    }

    pub(crate) async fn stop_containers(
        &self,
        container_ids: Vec<yarn::ContainerIdProto>,
    ) -> Result<yarn::StopContainersResponseProto> {
        let message = yarn::StopContainersRequestProto {
            container_id: container_ids,
        };
        self.proxy.call("stopContainers", message, true).await
    }

    pub(crate) async fn get_container_statuses(
        &self,
        container_ids: Vec<yarn::ContainerIdProto>,
    ) -> Result<yarn::GetContainerStatusesResponseProto> {
        let message = yarn::GetContainerStatusesRequestProto {
            container_id: container_ids,
        };
        self.proxy
            .call("getContainerStatuses", message, false)
            .await
    }

    pub(crate) async fn signal_to_container(
        &self,
        container_id: yarn::ContainerIdProto,
        command: yarn::SignalContainerCommandProto,
    ) -> Result<yarn::SignalContainerResponseProto> {
        let message = yarn::SignalContainerRequestProto {
            container_id,
            command: command as i32,
        };
        self.proxy.call("signalToContainer", message, true).await
    }
}
