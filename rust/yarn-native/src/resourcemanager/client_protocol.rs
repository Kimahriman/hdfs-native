use crate::Result;
use crate::proto::{common, yarn};
use crate::proxy::{Protocol, ServiceProxy};

pub(crate) const PROTOCOL: Protocol = Protocol {
    name: "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB",
    token_kind: "RM_DELEGATION_TOKEN",
};

/// The ResourceManager's client protocol, used to submit and inspect applications.
///
/// Each call says whether it is a `write` that must not be repeated after an ambiguous failure.
/// Calls Hadoop annotates `@Idempotent` are not: like Hadoop's `RMProxy`, they fail over to
/// another ResourceManager even when the connection broke after the request was sent.
#[derive(Debug)]
pub(crate) struct ApplicationClientProtocol {
    proxy: ServiceProxy,
}

impl ApplicationClientProtocol {
    pub(crate) fn new(proxy: ServiceProxy) -> Self {
        Self { proxy }
    }

    pub(crate) fn token_service(&self) -> &str {
        self.proxy.token_service()
    }

    pub(crate) async fn get_new_application(&self) -> Result<yarn::GetNewApplicationResponseProto> {
        // Retrying only reserves another application ID, which is harmless
        self.proxy
            .call(
                "getNewApplication",
                yarn::GetNewApplicationRequestProto::default(),
                false,
            )
            .await
    }

    pub(crate) async fn submit_application(
        &self,
        context: yarn::ApplicationSubmissionContextProto,
    ) -> Result<yarn::SubmitApplicationResponseProto> {
        let message = yarn::SubmitApplicationRequestProto {
            application_submission_context: Some(context),
        };
        self.proxy.call("submitApplication", message, false).await
    }

    pub(crate) async fn get_application_report(
        &self,
        application_id: yarn::ApplicationIdProto,
    ) -> Result<yarn::GetApplicationReportResponseProto> {
        let message = yarn::GetApplicationReportRequestProto {
            application_id: Some(application_id),
        };
        self.proxy
            .call("getApplicationReport", message, false)
            .await
    }

    pub(crate) async fn get_applications(
        &self,
        message: yarn::GetApplicationsRequestProto,
    ) -> Result<yarn::GetApplicationsResponseProto> {
        self.proxy.call("getApplications", message, false).await
    }

    pub(crate) async fn force_kill_application(
        &self,
        application_id: yarn::ApplicationIdProto,
        diagnostics: Option<String>,
    ) -> Result<yarn::KillApplicationResponseProto> {
        let message = yarn::KillApplicationRequestProto {
            application_id: Some(application_id),
            diagnostics,
        };
        self.proxy
            .call("forceKillApplication", message, false)
            .await
    }

    pub(crate) async fn fail_application_attempt(
        &self,
        attempt_id: yarn::ApplicationAttemptIdProto,
    ) -> Result<yarn::FailApplicationAttemptResponseProto> {
        let message = yarn::FailApplicationAttemptRequestProto {
            application_attempt_id: Some(attempt_id),
        };
        self.proxy
            .call("failApplicationAttempt", message, true)
            .await
    }

    pub(crate) async fn move_application_across_queues(
        &self,
        application_id: yarn::ApplicationIdProto,
        target_queue: String,
    ) -> Result<yarn::MoveApplicationAcrossQueuesResponseProto> {
        let message = yarn::MoveApplicationAcrossQueuesRequestProto {
            application_id,
            target_queue,
        };
        self.proxy
            .call("moveApplicationAcrossQueues", message, false)
            .await
    }

    pub(crate) async fn update_application_priority(
        &self,
        application_id: yarn::ApplicationIdProto,
        priority: i32,
    ) -> Result<yarn::UpdateApplicationPriorityResponseProto> {
        let message = yarn::UpdateApplicationPriorityRequestProto {
            application_id,
            application_priority: yarn::PriorityProto {
                priority: Some(priority),
            },
        };
        self.proxy
            .call("updateApplicationPriority", message, false)
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

    pub(crate) async fn get_cluster_metrics(&self) -> Result<yarn::GetClusterMetricsResponseProto> {
        self.proxy
            .call(
                "getClusterMetrics",
                yarn::GetClusterMetricsRequestProto::default(),
                false,
            )
            .await
    }

    pub(crate) async fn get_cluster_nodes(
        &self,
        node_states: Vec<yarn::NodeStateProto>,
    ) -> Result<yarn::GetClusterNodesResponseProto> {
        let message = yarn::GetClusterNodesRequestProto {
            node_states: node_states.into_iter().map(|state| state as i32).collect(),
        };
        self.proxy.call("getClusterNodes", message, false).await
    }

    pub(crate) async fn get_queue_info(
        &self,
        queue_name: String,
        include_applications: bool,
        include_child_queues: bool,
        recursive: bool,
    ) -> Result<yarn::GetQueueInfoResponseProto> {
        let message = yarn::GetQueueInfoRequestProto {
            queue_name: Some(queue_name),
            include_applications: Some(include_applications),
            include_child_queues: Some(include_child_queues),
            recursive: Some(recursive),
            sub_cluster_id: None,
        };
        self.proxy.call("getQueueInfo", message, false).await
    }

    pub(crate) async fn get_application_attempt_report(
        &self,
        attempt_id: yarn::ApplicationAttemptIdProto,
    ) -> Result<yarn::GetApplicationAttemptReportResponseProto> {
        let message = yarn::GetApplicationAttemptReportRequestProto {
            application_attempt_id: Some(attempt_id),
        };
        self.proxy
            .call("getApplicationAttemptReport", message, false)
            .await
    }

    pub(crate) async fn get_application_attempts(
        &self,
        application_id: yarn::ApplicationIdProto,
    ) -> Result<yarn::GetApplicationAttemptsResponseProto> {
        let message = yarn::GetApplicationAttemptsRequestProto {
            application_id: Some(application_id),
        };
        self.proxy
            .call("getApplicationAttempts", message, false)
            .await
    }

    pub(crate) async fn get_container_report(
        &self,
        container_id: yarn::ContainerIdProto,
    ) -> Result<yarn::GetContainerReportResponseProto> {
        let message = yarn::GetContainerReportRequestProto {
            container_id: Some(container_id),
        };
        self.proxy.call("getContainerReport", message, false).await
    }

    pub(crate) async fn get_containers(
        &self,
        attempt_id: yarn::ApplicationAttemptIdProto,
    ) -> Result<yarn::GetContainersResponseProto> {
        let message = yarn::GetContainersRequestProto {
            application_attempt_id: Some(attempt_id),
        };
        self.proxy.call("getContainers", message, false).await
    }

    pub(crate) async fn get_delegation_token(
        &self,
        renewer: String,
    ) -> Result<common::GetDelegationTokenResponseProto> {
        let message = common::GetDelegationTokenRequestProto { renewer };
        self.proxy.call("getDelegationToken", message, false).await
    }

    pub(crate) async fn renew_delegation_token(
        &self,
        token: common::TokenProto,
    ) -> Result<common::RenewDelegationTokenResponseProto> {
        let message = common::RenewDelegationTokenRequestProto { token };
        self.proxy
            .call("renewDelegationToken", message, false)
            .await
    }

    pub(crate) async fn cancel_delegation_token(
        &self,
        token: common::TokenProto,
    ) -> Result<common::CancelDelegationTokenResponseProto> {
        let message = common::CancelDelegationTokenRequestProto { token };
        self.proxy
            .call("cancelDelegationToken", message, false)
            .await
    }
}
