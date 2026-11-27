use std::collections::HashMap;
use std::sync::Arc;

use hadoop_native::security::user::Token;

use crate::am::NmTokenCache;
use crate::client::{ClientContext, ClientOptions, client_options_methods};
use crate::ids::{ContainerId, NodeId};
use crate::nodemanager::protocol::{self, ContainerManagementProtocol};
use crate::proto::yarn::{
    ContainerExceptionMapProto, SignalContainerCommandProto, StartContainerRequestProto,
};
use crate::proxy::ServiceProxy;
use crate::records::{Container, ContainerLaunchContext, ContainerStatus, SignalContainerCommand};
use crate::{Result, YarnError};

/// Builds a new [NmClient] instance.
#[derive(Default)]
pub struct NmClientBuilder {
    options: ClientOptions,
    nm_tokens: Option<NmTokenCache>,
}

impl NmClientBuilder {
    /// Create a new [NmClientBuilder]
    pub fn new() -> Self {
        Self::default()
    }

    client_options_methods!();

    /// Look up NMTokens in `cache`, usually [`AmRmClient::nm_token_cache`](crate::AmRmClient::nm_token_cache).
    pub fn with_nm_token_cache(mut self, cache: NmTokenCache) -> Self {
        self.nm_tokens = Some(cache);
        self
    }

    /// Synchronously create the [NmClient] instance from the provided settings.
    pub fn build(mut self) -> Result<NmClient> {
        let options = std::mem::take(&mut self.options);
        Ok(NmClient::new(
            options.build_context()?,
            self.nm_tokens.unwrap_or_default(),
        ))
    }

    /// Asynchronously create an [NmClient] instance, loading XML config files without blocking a
    /// Tokio worker thread.
    pub async fn build_async(mut self) -> Result<NmClient> {
        let options = std::mem::take(&mut self.options);
        Ok(NmClient::new(
            options.build_context_async().await?,
            self.nm_tokens.unwrap_or_default(),
        ))
    }
}

/// A connection to one NodeManager and the NMToken it was opened with.
type NodeConnection = (Token, Arc<ContainerManagementProtocol>);

/// A client to the NodeManagers of a YARN cluster, for an ApplicationMaster to start, stop and
/// monitor its containers. NodeManagers authenticate the application attempt by its NMToken
/// for each node, looked up in the client's [`NmTokenCache`].
#[derive(Clone, Debug)]
pub struct NmClient {
    context: ClientContext,
    nm_tokens: NmTokenCache,
    connections: Arc<tokio::sync::Mutex<HashMap<NodeId, NodeConnection>>>,
}

impl NmClient {
    fn new(context: ClientContext, nm_tokens: NmTokenCache) -> Self {
        Self {
            context,
            nm_tokens,
            connections: Arc::default(),
        }
    }

    pub fn nm_token_cache(&self) -> NmTokenCache {
        self.nm_tokens.clone()
    }

    /// The protocol for a node, reconnecting when the node's NMToken changed.
    async fn protocol(&self, node_id: &NodeId) -> Result<Arc<ContainerManagementProtocol>> {
        let mut token = self.nm_tokens.get(node_id).ok_or_else(|| {
            YarnError::InvalidArgument(format!(
                "No NMToken for node {node_id}; tokens come from AmRmClient allocate responses"
            ))
        })?;

        let mut connections = self.connections.lock().await;
        if let Some((connected_token, protocol)) = connections.get(node_id)
            && connected_token.identifier() == token.identifier()
        {
            return Ok(Arc::clone(protocol));
        }

        let address = node_id.to_string();
        let token_service = self.context.config.token_service(&address);
        token.set_service(token_service.clone());
        let proxy = ServiceProxy::new(
            protocol::PROTOCOL,
            vec![address],
            token_service,
            Some(token.clone()),
            self.context.config.nm_failover_settings(),
            Arc::clone(&self.context.config),
            self.context.rt_holder.get_handle(),
            self.context.user.clone(),
            self.context.auth.clone(),
        )?;
        let protocol = Arc::new(ContainerManagementProtocol::new(proxy));
        connections.insert(node_id.clone(), (token, Arc::clone(&protocol)));
        Ok(protocol)
    }

    /// Starts an allocated container on its NodeManager.
    pub async fn start_container(
        &self,
        container: &Container,
        launch_context: ContainerLaunchContext,
    ) -> Result<()> {
        let container_token = container.container_token.as_ref().ok_or_else(|| {
            YarnError::InvalidArgument(format!("Container {} has no container token", container.id))
        })?;
        let response = self
            .protocol(&container.node_id)
            .await?
            .start_containers(vec![StartContainerRequestProto {
                container_launch_context: Some(launch_context.into()),
                container_token: Some(container_token.to_proto()),
            }])
            .await?;
        check_failures(response.failed_requests)
    }

    /// Stops a running container.
    pub async fn stop_container(&self, container_id: ContainerId, node_id: &NodeId) -> Result<()> {
        let response = self
            .protocol(node_id)
            .await?
            .stop_containers(vec![container_id.into()])
            .await?;
        check_failures(response.failed_requests)
    }

    pub async fn get_container_status(
        &self,
        container_id: ContainerId,
        node_id: &NodeId,
    ) -> Result<ContainerStatus> {
        let response = self
            .protocol(node_id)
            .await?
            .get_container_statuses(vec![container_id.into()])
            .await?;
        check_failures(response.failed_requests)?;
        response
            .status
            .into_iter()
            .next()
            .map(Into::into)
            .ok_or_else(|| {
                YarnError::InternalError(format!(
                    "getContainerStatuses returned no status for {container_id}"
                ))
            })
    }

    pub async fn signal_container(
        &self,
        container_id: ContainerId,
        node_id: &NodeId,
        command: SignalContainerCommand,
    ) -> Result<()> {
        self.protocol(node_id)
            .await?
            .signal_to_container(
                container_id.into(),
                SignalContainerCommandProto::from(command),
            )
            .await?;
        Ok(())
    }
}

/// Turns the first per-container failure in a NodeManager response into an error.
fn check_failures(failed_requests: Vec<ContainerExceptionMapProto>) -> Result<()> {
    match failed_requests.into_iter().next() {
        None => Ok(()),
        Some(failure) => {
            let container_id = failure
                .container_id
                .map(|id| ContainerId::from(id).to_string())
                .unwrap_or_default();
            let exception = failure.exception.unwrap_or_default();
            Err(YarnError::ContainerError(
                container_id,
                exception.class_name.unwrap_or_default(),
                exception.message.unwrap_or_default(),
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::proto::yarn::SerializedExceptionProto;

    #[tokio::test]
    async fn test_missing_nm_token_is_an_error() {
        let client = NmClientBuilder::new().build().unwrap();
        let error = client
            .get_container_status(
                "container_1_0001_01_000001".parse().unwrap(),
                &NodeId::new("nm", 1),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, YarnError::InvalidArgument(_)), "{error:?}");
    }

    #[test]
    fn test_container_failures_become_errors() {
        assert!(check_failures(Vec::new()).is_ok());

        let error = check_failures(vec![ContainerExceptionMapProto {
            container_id: Some(
                "container_1_0001_01_000002"
                    .parse::<ContainerId>()
                    .unwrap()
                    .into(),
            ),
            exception: Some(SerializedExceptionProto {
                message: Some("Container already started".to_string()),
                class_name: Some("org.apache.hadoop.yarn.exceptions.YarnException".to_string()),
                ..Default::default()
            }),
        }])
        .unwrap_err();
        match error {
            YarnError::ContainerError(container, class, message) => {
                assert_eq!(container, "container_1_0001_01_000002");
                assert_eq!(class, "org.apache.hadoop.yarn.exceptions.YarnException");
                assert_eq!(message, "Container already started");
            }
            error => panic!("unexpected error {error:?}"),
        }
    }
}
