use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hadoop_native::security::user::Token;
use log::{debug, info, warn};

use crate::client::{ClientContext, ClientOptions, RuntimeHolder, client_options_methods};
use crate::config;
use crate::ids::{ContainerId, NodeId};
use crate::proto::yarn::{
    self, AllocateRequestProto, FinalApplicationStatusProto, NmTokenProto,
    RegisterApplicationMasterRequestProto, ResourceBlacklistRequestProto,
};
use crate::proxy::ServiceProxy;
use crate::records::{
    ANY_RESOURCE, AmCommand, Container, ContainerStatus, FinalApplicationStatus, NodeReport,
    Resource, ResourceRequest,
};
use crate::resourcemanager::master_protocol::{self, ApplicationMasterProtocol};
use crate::{Result, YarnError};

/// How many times one heartbeat registers again after the ResourceManager lost the
/// ApplicationMaster.
const MAX_RESYNCS: u32 = 3;

/// How long to wait between attempts to unregister, like Hadoop's `AMRMClientImpl`.
const UNREGISTER_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// NMTokens of an application attempt, keyed by node. The ResourceManager hands these out in
/// allocate responses; [`NmClient`](crate::NmClient) needs the token for a node to talk to it.
/// Clones share the same tokens.
#[derive(Clone, Debug, Default)]
pub struct NmTokenCache {
    tokens: Arc<Mutex<HashMap<NodeId, Token>>>,
}

impl NmTokenCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, node_id: &NodeId) -> Option<Token> {
        self.tokens.lock().unwrap().get(node_id).cloned()
    }

    pub fn insert(&self, node_id: NodeId, token: Token) {
        self.tokens.lock().unwrap().insert(node_id, token);
    }

    pub fn remove(&self, node_id: &NodeId) -> Option<Token> {
        self.tokens.lock().unwrap().remove(node_id)
    }

    pub fn len(&self) -> usize {
        self.tokens.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn insert_all(&self, nm_tokens: Vec<NmTokenProto>) {
        let mut tokens = self.tokens.lock().unwrap();
        for nm_token in nm_tokens {
            if let (Some(node_id), Some(token)) = (nm_token.node_id, nm_token.token) {
                let node_id = NodeId::from(node_id);
                debug!("Received NMToken for {node_id}");
                tokens.insert(node_id, token.into());
            }
        }
    }
}

/// Builds a new [AmRmClient] instance.
///
/// The client authenticates with the attempt's AMRMToken. By default it is read from the token
/// file in `HADOOP_TOKEN_FILE_LOCATION`, which YARN sets for ApplicationMaster containers. An
/// unmanaged ApplicationMaster gets its token from
/// [`YarnClient::get_am_rm_token`](crate::YarnClient::get_am_rm_token) and passes it to
/// [`Self::with_am_rm_token`].
#[derive(Default)]
pub struct AmRmClientBuilder {
    options: ClientOptions,
    am_rm_token: Option<Token>,
    nm_tokens: Option<NmTokenCache>,
}

impl AmRmClientBuilder {
    /// Create a new [AmRmClientBuilder]
    pub fn new() -> Self {
        Self::default()
    }

    client_options_methods!();

    /// Set the AMRMToken to authenticate with instead of reading it from the token file.
    pub fn with_am_rm_token(mut self, token: Token) -> Self {
        self.am_rm_token = Some(token);
        self
    }

    /// Store received NMTokens in `cache` instead of a new cache.
    pub fn with_nm_token_cache(mut self, cache: NmTokenCache) -> Self {
        self.nm_tokens = Some(cache);
        self
    }

    /// Synchronously create the [AmRmClient] instance from the provided settings.
    pub fn build(mut self) -> Result<AmRmClient> {
        let options = std::mem::take(&mut self.options);
        self.build_with_context(options.build_context()?)
    }

    /// Asynchronously create an [AmRmClient] instance, loading XML config files without
    /// blocking a Tokio worker thread.
    pub async fn build_async(mut self) -> Result<AmRmClient> {
        let options = std::mem::take(&mut self.options);
        self.build_with_context(options.build_context_async().await?)
    }

    fn build_with_context(self, context: ClientContext) -> Result<AmRmClient> {
        let mut token = match self.am_rm_token {
            Some(token) => token,
            None => Token::from_token_file_env()
                .into_iter()
                .find(|token| token.kind() == master_protocol::PROTOCOL.token_kind)
                .ok_or_else(|| {
                    YarnError::InvalidArgument(format!(
                        "No {} found in HADOOP_TOKEN_FILE_LOCATION; pass one with with_am_rm_token",
                        master_protocol::PROTOCOL.token_kind
                    ))
                })?,
        };

        let addresses = context.config.rm_addresses(
            config::RM_SCHEDULER_ADDRESS,
            config::DEFAULT_RM_SCHEDULER_PORT,
        )?;
        // Like Hadoop's ClientRMProxy, point the token at the scheduler addresses
        let token_service = context.config.rm_token_service(&addresses);
        token.set_service(token_service.clone());

        let proxy = ServiceProxy::new(
            master_protocol::PROTOCOL,
            addresses,
            token_service,
            Some(token),
            context.config.rm_failover_settings(),
            Arc::clone(&context.config),
            context.rt_holder.get_handle(),
            context.user,
            context.auth,
        )?;

        Ok(AmRmClient {
            protocol: Arc::new(ApplicationMasterProtocol::new(proxy)),
            nm_tokens: self.nm_tokens.unwrap_or_default(),
            state: Arc::default(),
            rt_holder: context.rt_holder,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RegisterResponse {
    /// The most resources the cluster grants a single container.
    pub maximum_resource_capability: Resource,
    pub queue: String,
    /// Containers kept from earlier attempts when the application keeps containers across
    /// attempts.
    pub containers_from_previous_attempts: Vec<Container>,
}

/// A heartbeat to the ResourceManager. See [`ResourceRequest`] for how asks replace
/// outstanding requests.
#[derive(Debug, Clone, Default)]
pub struct AllocateRequest {
    pub asks: Vec<ResourceRequest>,
    /// Containers the application no longer needs.
    pub releases: Vec<ContainerId>,
    /// Progress of the application between 0 and 1.
    pub progress: f32,
    /// Hosts or racks to stop allocating containers on.
    pub blacklist_additions: Vec<String>,
    pub blacklist_removals: Vec<String>,
    pub tracking_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AllocateResponse {
    pub allocated_containers: Vec<Container>,
    pub completed_containers: Vec<ContainerStatus>,
    /// Resources the application can still be allocated.
    pub available_resources: Resource,
    pub updated_nodes: Vec<NodeReport>,
    pub num_cluster_nodes: i32,
    /// An instruction from the ResourceManager, such as to shut down.
    pub am_command: Option<AmCommand>,
    /// Whether the ResourceManager had lost this ApplicationMaster, for example after a
    /// restart or failover, so the client registered again and re-sent its outstanding
    /// requests, pending releases and blacklist before this heartbeat.
    pub resynced: bool,
}

/// Identifies an outstanding container request the way the ResourceManager's scheduler does.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AskKey {
    priority: i32,
    resource_name: String,
    allocation_request_id: i64,
}

/// YARN's allocation request ID when none is set. Containers carry it explicitly.
const DEFAULT_ALLOCATION_REQUEST_ID: i64 = -1;

impl AskKey {
    fn new(priority: i32, resource_name: &str, allocation_request_id: Option<i64>) -> Self {
        Self {
            priority,
            resource_name: resource_name.to_string(),
            allocation_request_id: allocation_request_id.unwrap_or(DEFAULT_ALLOCATION_REQUEST_ID),
        }
    }
}

impl From<&ResourceRequest> for AskKey {
    /// Hadoop 3's scheduler keys requests by priority and allocation request ID, then resource
    /// name, so a new capability for the same key replaces the old request.
    fn from(request: &ResourceRequest) -> Self {
        Self::new(
            request.priority,
            &request.resource_name,
            request.allocation_request_id,
        )
    }
}

/// What the ResourceManager knows about this ApplicationMaster, so a heartbeat that failed can
/// be re-sent and the ResourceManager can be brought back in sync after it loses its state,
/// like Hadoop's `AMRMClientImpl`.
#[derive(Debug, Default)]
struct AllocateState {
    last_response_id: i32,
    /// The registration to repeat when the ResourceManager has lost this ApplicationMaster.
    registration: Option<RegisterApplicationMasterRequestProto>,
    /// The requests the ResourceManager still holds, kept up to date as containers arrive.
    asks: HashMap<AskKey, ResourceRequest>,
    /// Requests not yet acknowledged by a successful allocate.
    unsent_asks: HashSet<AskKey>,
    /// Released containers the ResourceManager has not reported as completed yet.
    pending_releases: HashSet<ContainerId>,
    unsent_releases: HashSet<ContainerId>,
    blacklist: HashSet<String>,
    unsent_blacklist_additions: HashSet<String>,
    unsent_blacklist_removals: HashSet<String>,
}

impl AllocateState {
    /// Records a heartbeat's changes, which stay unsent until an allocate succeeds.
    fn add_request(&mut self, request: AllocateRequest) {
        for ask in request.asks {
            let key = AskKey::from(&ask);
            self.asks.insert(key.clone(), ask);
            self.unsent_asks.insert(key);
        }
        for container_id in request.releases {
            self.pending_releases.insert(container_id);
            self.unsent_releases.insert(container_id);
        }
        for host in request.blacklist_additions {
            self.unsent_blacklist_removals.remove(&host);
            self.blacklist.insert(host.clone());
            self.unsent_blacklist_additions.insert(host);
        }
        for host in request.blacklist_removals {
            self.unsent_blacklist_additions.remove(&host);
            self.blacklist.remove(&host);
            self.unsent_blacklist_removals.insert(host);
        }
    }

    /// Marks everything the ResourceManager should know as unsent, after it lost its state.
    fn resend_all(&mut self) {
        self.last_response_id = 0;
        self.unsent_asks = self.asks.keys().cloned().collect();
        self.unsent_releases = self.pending_releases.clone();
        self.unsent_blacklist_additions = self.blacklist.clone();
        self.unsent_blacklist_removals.clear();
    }

    fn build_request(&self, progress: f32, tracking_url: Option<String>) -> AllocateRequestProto {
        let blacklist_request = if self.unsent_blacklist_additions.is_empty()
            && self.unsent_blacklist_removals.is_empty()
        {
            None
        } else {
            Some(ResourceBlacklistRequestProto {
                blacklist_additions: self.unsent_blacklist_additions.iter().cloned().collect(),
                blacklist_removals: self.unsent_blacklist_removals.iter().cloned().collect(),
            })
        };
        AllocateRequestProto {
            ask: self
                .unsent_asks
                .iter()
                .filter_map(|key| self.asks.get(key))
                .cloned()
                .map(Into::into)
                .collect(),
            release: self
                .unsent_releases
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
            blacklist_request,
            response_id: Some(self.last_response_id),
            progress: Some(progress),
            tracking_url,
            ..Default::default()
        }
    }

    /// Updates the state after a successful allocate.
    fn on_response(
        &mut self,
        response_id: i32,
        allocated: &[Container],
        completed: &[ContainerStatus],
    ) {
        self.last_response_id = response_id;
        self.unsent_releases.clear();
        self.unsent_blacklist_additions.clear();
        self.unsent_blacklist_removals.clear();
        let sent = std::mem::take(&mut self.unsent_asks);
        for container in allocated {
            self.remove_satisfied_asks(container, &sent);
        }
        // Requests for no containers only needed to reach the ResourceManager once
        for key in sent {
            if self
                .asks
                .get(&key)
                .is_some_and(|ask| ask.num_containers <= 0)
            {
                self.asks.remove(&key);
            }
        }
        for status in completed {
            self.pending_releases.remove(&status.container_id);
        }
    }

    /// The scheduler decrements the requests a container satisfies, so do the same: the
    /// off-switch (`*`) request with the container's priority and allocation request ID, and
    /// the request for its host. Rack requests are not tracked down because a container doesn't
    /// name its rack, so after a resync they may ask for more than needed until the next update.
    ///
    /// Requests the ApplicationMaster set in the heartbeat that returned the container (`sent`)
    /// are left alone: the scheduler allocated the container before that heartbeat, from the old
    /// count, and then replaced the count with the one sent.
    fn remove_satisfied_asks(&mut self, container: &Container, sent: &HashSet<AskKey>) {
        for resource_name in [ANY_RESOURCE, container.node_id.host.as_str()] {
            let key = AskKey::new(
                container.priority,
                resource_name,
                container.allocation_request_id,
            );
            if sent.contains(&key) {
                continue;
            }
            if let Some(ask) = self.asks.get_mut(&key) {
                ask.num_containers -= 1;
                if ask.num_containers <= 0 {
                    self.asks.remove(&key);
                }
            }
        }
    }
}

/// A client to the ResourceManager's scheduler, for an application's ApplicationMaster.
///
/// The ApplicationMaster registers, then calls [`Self::allocate`] periodically as a heartbeat to
/// request and release containers, and finally unregisters. Allocated containers are started
/// with an [`NmClient`](crate::NmClient) that shares this client's [`NmTokenCache`].
///
/// Like Hadoop's `AMRMClient`, the client remembers outstanding requests, pending releases and
/// the blacklist. Changes from a heartbeat that failed are sent with the next one, and when the
/// ResourceManager has lost the ApplicationMaster, for example after a restart or failover, the
/// client registers again and re-sends them.
#[derive(Clone, Debug)]
pub struct AmRmClient {
    protocol: Arc<ApplicationMasterProtocol>,
    nm_tokens: NmTokenCache,
    // The ResourceManager rejects an allocate whose response ID does not follow the last one,
    // so allocate calls are serialized.
    state: Arc<tokio::sync::Mutex<AllocateState>>,
    #[allow(dead_code)]
    rt_holder: RuntimeHolder,
}

impl AmRmClient {
    /// The cache that NMTokens from this ResourceManager are stored in.
    pub fn nm_token_cache(&self) -> NmTokenCache {
        self.nm_tokens.clone()
    }

    /// Registers the ApplicationMaster. `host`, `rpc_port` and `tracking_url` are shown to
    /// clients in the application report.
    pub async fn register_application_master(
        &self,
        host: &str,
        rpc_port: i32,
        tracking_url: Option<&str>,
    ) -> Result<RegisterResponse> {
        let request = RegisterApplicationMasterRequestProto {
            host: Some(host.to_string()),
            rpc_port: Some(rpc_port),
            tracking_url: tracking_url.map(str::to_string),
            ..Default::default()
        };
        let mut state = self.state.lock().await;
        let response = self.register(&mut state, request).await?;

        Ok(RegisterResponse {
            maximum_resource_capability: response.maximum_capability.unwrap_or_default().into(),
            queue: response.queue.unwrap_or_default(),
            containers_from_previous_attempts: response
                .containers_from_previous_attempts
                .into_iter()
                .map(Into::into)
                .collect(),
        })
    }

    async fn register(
        &self,
        state: &mut AllocateState,
        request: RegisterApplicationMasterRequestProto,
    ) -> Result<yarn::RegisterApplicationMasterResponseProto> {
        let response = self
            .protocol
            .register_application_master(request.clone())
            .await?;
        state.last_response_id = 0;
        state.registration = Some(request);
        self.nm_tokens
            .insert_all(response.nm_tokens_from_previous_attempts.clone());
        Ok(response)
    }

    /// Registers again with the original settings after the ResourceManager lost this
    /// ApplicationMaster.
    async fn reregister(&self, state: &mut AllocateState) -> Result<()> {
        let registration = state.registration.clone().ok_or_else(|| {
            YarnError::InvalidArgument(
                "The ApplicationMaster must register before it heartbeats".to_string(),
            )
        })?;
        warn!("ApplicationMaster is out of sync with the ResourceManager, registering again");
        self.register(state, registration).await?;
        Ok(())
    }

    /// Sends a heartbeat that asks for and releases containers. NMTokens in the response are
    /// stored in the [`NmTokenCache`], and a rolled AMRMToken is used for new connections.
    pub async fn allocate(&self, request: AllocateRequest) -> Result<AllocateResponse> {
        let progress = request.progress;
        let tracking_url = request.tracking_url.clone();

        let mut state = self.state.lock().await;
        state.add_request(request);
        let mut resynced = false;
        let mut resyncs = 0;
        let response = loop {
            match self
                .protocol
                .allocate(state.build_request(progress, tracking_url.clone()))
                .await
            {
                // The ResourceManager can lose the ApplicationMaster again, for example in
                // back-to-back failovers, so resync more than once
                Err(YarnError::ApplicationMasterNotRegistered(_)) if resyncs < MAX_RESYNCS => {
                    self.reregister(&mut state).await?;
                    state.resend_all();
                    resynced = true;
                    resyncs += 1;
                }
                response => break response?,
            }
        };

        let allocated_containers: Vec<Container> = response
            .allocated_containers
            .into_iter()
            .map(Into::into)
            .collect();
        let completed_containers: Vec<ContainerStatus> = response
            .completed_container_statuses
            .into_iter()
            .map(Into::into)
            .collect();
        state.on_response(
            response.response_id.unwrap_or_default(),
            &allocated_containers,
            &completed_containers,
        );
        drop(state);

        self.nm_tokens.insert_all(response.nm_tokens);
        if let Some(am_rm_token) = response.am_rm_token {
            info!("Received a new AMRMToken");
            let mut token = Token::from(am_rm_token);
            token.set_service(self.protocol.token_service());
            self.protocol.set_token(token);
        }

        Ok(AllocateResponse {
            am_command: response
                .a_m_command
                .and_then(|command| yarn::AmCommandProto::try_from(command).ok())
                .map(Into::into),
            num_cluster_nodes: response.num_cluster_nodes.unwrap_or_default(),
            allocated_containers,
            completed_containers,
            available_resources: response.limit.unwrap_or_default().into(),
            updated_nodes: response.updated_nodes.into_iter().map(Into::into).collect(),
            resynced,
        })
    }

    /// Unregisters the ApplicationMaster with its final status, waiting until the
    /// ResourceManager confirms, like Hadoop's `AMRMClient.unregisterApplicationMaster`.
    pub async fn unregister_application_master(
        &self,
        status: FinalApplicationStatus,
        diagnostics: Option<&str>,
        tracking_url: Option<&str>,
    ) -> Result<()> {
        let request = yarn::FinishApplicationMasterRequestProto {
            diagnostics: diagnostics.map(str::to_string),
            tracking_url: tracking_url.map(str::to_string),
            final_application_status: Some(FinalApplicationStatusProto::from(status) as i32),
        };
        let mut resyncs = 0;
        loop {
            match self
                .protocol
                .finish_application_master(request.clone())
                .await
            {
                Ok(response) if response.is_unregistered() => return Ok(()),
                Ok(_) => tokio::time::sleep(UNREGISTER_POLL_INTERVAL).await,
                Err(YarnError::ApplicationMasterNotRegistered(_)) if resyncs < MAX_RESYNCS => {
                    let mut state = self.state.lock().await;
                    self.reregister(&mut state).await?;
                    resyncs += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// The AMRMToken new connections authenticate with.
    pub fn am_rm_token(&self) -> Option<Token> {
        self.protocol.token()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::proto::common::TokenProto;
    use crate::proto::yarn::NodeIdProto;

    fn token() -> Token {
        Token::new(b"id".to_vec(), b"pw".to_vec(), "YARN_AM_RM_TOKEN", "")
    }

    #[test]
    fn test_build_points_the_token_at_the_scheduler() {
        let client = AmRmClientBuilder::new()
            .with_config([
                ("yarn.resourcemanager.scheduler.address", "127.0.0.1:18030"),
                ("hadoop.security.token.service.use_ip", "false"),
            ])
            .with_am_rm_token(token())
            .build()
            .unwrap();
        assert_eq!(client.am_rm_token().unwrap().service(), "127.0.0.1:18030");
    }

    #[test]
    fn test_build_without_a_token_fails() {
        // SAFETY: tests in this crate don't read this variable concurrently
        unsafe { std::env::remove_var("HADOOP_TOKEN_FILE_LOCATION") };
        assert!(matches!(
            AmRmClientBuilder::new().build(),
            Err(YarnError::InvalidArgument(_))
        ));
    }

    #[test]
    fn test_nm_token_cache_stores_received_tokens() {
        let cache = NmTokenCache::new();
        cache.insert_all(vec![
            NmTokenProto {
                node_id: Some(NodeIdProto {
                    host: Some("nm".to_string()),
                    port: Some(1),
                }),
                token: Some(TokenProto {
                    identifier: b"id".to_vec(),
                    password: b"pw".to_vec(),
                    kind: "NMToken".to_string(),
                    service: "nm:1".to_string(),
                }),
            },
            // Incomplete entries are ignored
            NmTokenProto {
                node_id: None,
                token: None,
            },
        ]);
        assert_eq!(cache.len(), 1);
        let shared = cache.clone();
        assert_eq!(shared.get(&NodeId::new("nm", 1)).unwrap().kind(), "NMToken");
    }

    fn container(sequence: i64, host: &str, memory_mb: i64, priority: i32) -> Container {
        Container {
            id: format!("container_1_0001_01_{sequence:06}")
                .parse()
                .unwrap(),
            node_id: NodeId::new(host, 1),
            node_http_address: String::new(),
            resource: Resource::new(memory_mb, 1),
            priority,
            container_token: None,
            // YARN sets the default ID explicitly on every container
            allocation_request_id: Some(-1),
        }
    }

    fn sent_asks(state: &AllocateState) -> HashMap<String, i32> {
        state
            .build_request(0.0, None)
            .ask
            .into_iter()
            .map(|ask| (ask.resource_name.unwrap(), ask.num_containers.unwrap()))
            .collect()
    }

    #[test]
    fn test_unsent_changes_survive_a_failed_heartbeat() {
        let mut state = AllocateState::default();
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(512, 1), 2)],
            releases: vec!["container_1_0001_01_000009".parse().unwrap()],
            blacklist_additions: vec!["bad-host".to_string()],
            ..Default::default()
        });

        // Nothing was acknowledged, so a later heartbeat sends everything
        state.add_request(AllocateRequest::default());
        let request = state.build_request(0.5, None);
        assert_eq!(request.ask.len(), 1);
        assert_eq!(request.release.len(), 1);
        assert_eq!(
            request.blacklist_request.unwrap().blacklist_additions,
            vec!["bad-host"]
        );
        assert_eq!(request.response_id, Some(0));

        state.on_response(1, &[], &[]);
        let request = state.build_request(0.5, None);
        assert!(request.ask.is_empty());
        assert!(request.release.is_empty());
        assert!(request.blacklist_request.is_none());
        assert_eq!(request.response_id, Some(1));
    }

    /// The outstanding request counts, keyed by resource name
    fn outstanding(state: &AllocateState) -> HashMap<String, i32> {
        state
            .asks
            .iter()
            .map(|(key, ask)| (key.resource_name.clone(), ask.num_containers))
            .collect()
    }

    #[test]
    fn test_allocated_containers_decrement_outstanding_asks() {
        let mut state = AllocateState::default();
        let capability = Resource::new(512, 1);
        let mut on_host = ResourceRequest::new(1, capability, 2);
        on_host.resource_name = "host-a".to_string();
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, capability, 2), on_host],
            ..Default::default()
        });
        state.on_response(1, &[], &[]);

        // The scheduler rounds capabilities up, so a larger container still matches. A
        // container at another priority does not.
        state.on_response(
            2,
            &[
                container(1, "host-a", 1024, 1),
                container(2, "host-b", 512, 7),
            ],
            &[],
        );
        assert_eq!(
            outstanding(&state),
            HashMap::from([(ANY_RESOURCE.to_string(), 1), ("host-a".to_string(), 1)])
        );

        state.on_response(3, &[container(3, "host-a", 512, 1)], &[]);
        assert!(outstanding(&state).is_empty());
    }

    #[test]
    fn test_resync_after_allocation_asks_only_for_missing_containers() {
        // The review's scenario: default requests, containers as YARN sends them, then a
        // ResourceManager failover
        let mut state = AllocateState::default();
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(0, Resource::new(128, 1), 3)],
            ..Default::default()
        });
        state.on_response(1, &[], &[]);
        state.on_response(
            2,
            &[
                container(1, "host-a", 128, 0),
                container(2, "host-b", 128, 0),
            ],
            &[],
        );

        state.resend_all();
        assert_eq!(
            sent_asks(&state),
            HashMap::from([(ANY_RESOURCE.to_string(), 1)])
        );
    }

    #[test]
    fn test_asks_updated_in_the_same_heartbeat_are_not_decremented() {
        let mut state = AllocateState::default();
        let ask = ResourceRequest::new(1, Resource::new(512, 1), 2);
        state.add_request(AllocateRequest {
            asks: vec![ask.clone()],
            ..Default::default()
        });
        state.on_response(1, &[], &[]);

        // The scheduler allocated this container from the old count of 2 before the heartbeat
        // replaced the count with 2 again, so 2 are still outstanding
        state.add_request(AllocateRequest {
            asks: vec![ask],
            ..Default::default()
        });
        state.on_response(2, &[container(1, "host-a", 512, 1)], &[]);
        assert_eq!(
            outstanding(&state),
            HashMap::from([(ANY_RESOURCE.to_string(), 2)])
        );
    }

    #[test]
    fn test_a_new_capability_replaces_the_request() {
        let mut state = AllocateState::default();
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(1024, 1), 1)],
            ..Default::default()
        });
        state.on_response(1, &[], &[]);
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(2048, 1), 1)],
            ..Default::default()
        });
        state.on_response(2, &[], &[]);

        state.resend_all();
        let request = state.build_request(0.0, None);
        assert_eq!(request.ask.len(), 1);
        assert_eq!(
            request.ask[0].capability.as_ref().unwrap().memory,
            Some(2048)
        );
    }

    #[test]
    fn test_cancelled_asks_are_sent_once() {
        let mut state = AllocateState::default();
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(512, 1), 3)],
            ..Default::default()
        });
        state.on_response(1, &[], &[]);
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(512, 1), 0)],
            ..Default::default()
        });
        assert_eq!(
            sent_asks(&state),
            HashMap::from([(ANY_RESOURCE.to_string(), 0)])
        );

        state.on_response(2, &[], &[]);
        state.resend_all();
        assert!(sent_asks(&state).is_empty());
    }

    #[test]
    fn test_releases_are_pending_until_completed() {
        let mut state = AllocateState::default();
        let released: ContainerId = "container_1_0001_01_000004".parse().unwrap();
        state.add_request(AllocateRequest {
            releases: vec![released],
            ..Default::default()
        });
        state.on_response(1, &[], &[]);

        state.resend_all();
        assert_eq!(state.build_request(0.0, None).release.len(), 1);

        state.on_response(
            1,
            &[],
            &[ContainerStatus {
                container_id: released,
                state: crate::records::ContainerState::Complete,
                exit_status: -100,
                diagnostics: String::new(),
            }],
        );
        state.resend_all();
        assert!(state.build_request(0.0, None).release.is_empty());
    }

    #[test]
    fn test_blacklist_changes_cancel_out_and_resync_sends_the_current_list() {
        let mut state = AllocateState::default();
        state.add_request(AllocateRequest {
            blacklist_additions: vec!["a".to_string(), "b".to_string()],
            ..Default::default()
        });
        state.on_response(1, &[], &[]);
        state.add_request(AllocateRequest {
            blacklist_removals: vec!["a".to_string()],
            blacklist_additions: vec!["c".to_string()],
            ..Default::default()
        });
        state.add_request(AllocateRequest {
            blacklist_removals: vec!["c".to_string()],
            ..Default::default()
        });
        let blacklist = state.build_request(0.0, None).blacklist_request.unwrap();
        assert!(blacklist.blacklist_additions.is_empty());
        let mut removals = blacklist.blacklist_removals;
        removals.sort();
        assert_eq!(removals, vec!["a", "c"]);

        state.on_response(2, &[], &[]);
        state.resend_all();
        let blacklist = state.build_request(0.0, None).blacklist_request.unwrap();
        assert_eq!(blacklist.blacklist_additions, vec!["b"]);
        assert!(blacklist.blacklist_removals.is_empty());
        assert_eq!(state.build_request(0.0, None).response_id, Some(0));
    }
}
