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

/// The ResourceManager's answer to a registration.
///
/// If the response to a registration was lost and calling
/// [`AmRmClient::register_application_master`] again found the ApplicationMaster already
/// registered, the registration is complete but its response can't be fetched again:
/// `recovered` is set, the capability and queue are `None`, and containers and NMTokens from
/// previous attempts are not known.
#[derive(Debug, Clone)]
pub struct RegisterResponse {
    /// The most resources the cluster grants a single container.
    pub maximum_resource_capability: Option<Resource>,
    pub queue: Option<String>,
    /// Containers kept from earlier attempts when the application keeps containers across
    /// attempts.
    pub containers_from_previous_attempts: Vec<Container>,
    /// Whether this completes a registration whose response was lost.
    pub recovered: bool,
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

impl AllocateResponse {
    /// Combines a replayed heartbeat's response with the next one's.
    fn merge(mut self, later: AllocateResponse) -> AllocateResponse {
        self.allocated_containers.extend(later.allocated_containers);
        self.completed_containers.extend(later.completed_containers);
        AllocateResponse {
            allocated_containers: self.allocated_containers,
            completed_containers: self.completed_containers,
            am_command: later.am_command.or(self.am_command),
            ..later
        }
    }
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

/// Exceptions the ResourceManager throws when it rejects a heartbeat's contents before
/// handling it, so none of the heartbeat took effect.
const REQUEST_VALIDATION_EXCEPTIONS: &[&str] = &[
    "org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException",
    "org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException",
    "org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException",
    "org.apache.hadoop.yarn.exceptions.InvalidLabelResourceRequestException",
    "org.apache.hadoop.yarn.exceptions.SchedulerInvalidResoureRequestException",
];

const INVALID_AM_REQUEST_EXCEPTION: &str =
    "org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException";

/// Whether the ResourceManager rejected a heartbeat's contents, so none of it took effect.
fn is_request_validation_error(error: &YarnError) -> bool {
    matches!(error, YarnError::RPCError(class, _) if REQUEST_VALIDATION_EXCEPTIONS.contains(&class.as_str()))
}

/// Whether the ResourceManager refused a registration because the attempt is already
/// registered, as when the response to an earlier registration was lost.
fn is_already_registered_error(error: &YarnError) -> bool {
    matches!(
        error,
        YarnError::RPCError(class, message)
            if class == INVALID_AM_REQUEST_EXCEPTION && message.contains("already registered")
    )
}

/// The requests, releases and blacklist the ApplicationMaster wants, and which changes to them
/// the ResourceManager hasn't received yet.
#[derive(Clone, Debug, Default)]
struct Changes {
    /// The requests the ResourceManager should hold, kept up to date as containers arrive.
    asks: HashMap<AskKey, ResourceRequest>,
    unsent_asks: HashSet<AskKey>,
    /// Released containers the ResourceManager has not reported as completed yet.
    pending_releases: HashSet<ContainerId>,
    unsent_releases: HashSet<ContainerId>,
    blacklist: HashSet<String>,
    unsent_blacklist_additions: HashSet<String>,
    unsent_blacklist_removals: HashSet<String>,
}

impl Changes {
    fn has_unsent(&self) -> bool {
        !self.unsent_asks.is_empty()
            || !self.unsent_releases.is_empty()
            || !self.unsent_blacklist_additions.is_empty()
            || !self.unsent_blacklist_removals.is_empty()
    }
}

/// A heartbeat sent without a response. It is re-sent unchanged, with the same response ID:
/// if the ResourceManager handled it, it answers with the response it cached and ignores the
/// request's contents, so changes made since must go in the next heartbeat.
#[derive(Debug)]
struct InFlight {
    request: AllocateRequestProto,
    asks: HashSet<AskKey>,
    /// The state before this heartbeat's changes, restored if the ResourceManager rejects them.
    rollback: Option<Box<Changes>>,
}

/// What the ResourceManager knows about this ApplicationMaster, so a heartbeat that failed can
/// be re-sent and the ResourceManager can be brought back in sync after it loses its state,
/// like Hadoop's `AMRMClientImpl`.
#[derive(Debug, Default)]
struct AllocateState {
    last_response_id: i32,
    /// The registration to repeat when the ResourceManager has lost this ApplicationMaster,
    /// with the latest tracking URL.
    registration: Option<RegisterApplicationMasterRequestProto>,
    /// Set while registering again, so a registration whose response was lost is completed
    /// before the next heartbeat.
    reregistering: bool,
    /// A first registration whose outcome is unknown because its response was lost.
    unconfirmed_registration: Option<RegisterApplicationMasterRequestProto>,
    changes: Changes,
    /// The state before the unsent changes, restored if the ResourceManager rejects them.
    unsent_rollback: Option<Box<Changes>>,
    in_flight: Option<InFlight>,
    /// A response that arrived in a call that then failed, returned by the next call.
    undelivered: Option<AllocateResponse>,
}

impl AllocateState {
    /// Records a heartbeat's changes, which stay unsent until an allocate succeeds.
    fn add_request(&mut self, request: AllocateRequest) {
        if self.unsent_rollback.is_none() {
            self.unsent_rollback = Some(Box::new(self.changes.clone()));
        }
        let changes = &mut self.changes;
        for ask in request.asks {
            let key = AskKey::from(&ask);
            changes.asks.insert(key.clone(), ask);
            changes.unsent_asks.insert(key);
        }
        for container_id in request.releases {
            changes.pending_releases.insert(container_id);
            changes.unsent_releases.insert(container_id);
        }
        for host in request.blacklist_additions {
            changes.unsent_blacklist_removals.remove(&host);
            changes.blacklist.insert(host.clone());
            changes.unsent_blacklist_additions.insert(host);
        }
        for host in request.blacklist_removals {
            changes.unsent_blacklist_additions.remove(&host);
            changes.blacklist.remove(&host);
            changes.unsent_blacklist_removals.insert(host);
        }
    }

    /// Marks everything the ResourceManager should know as unsent, after it lost its state.
    fn resend_all(&mut self) {
        self.last_response_id = 0;
        if let Some(in_flight) = self.in_flight.take()
            && self.unsent_rollback.is_none()
        {
            self.unsent_rollback = in_flight.rollback;
        }
        let changes = &mut self.changes;
        changes.unsent_asks = changes.asks.keys().cloned().collect();
        changes.unsent_releases = changes.pending_releases.clone();
        changes.unsent_blacklist_additions = changes.blacklist.clone();
        changes.unsent_blacklist_removals.clear();
    }

    /// The heartbeat to send: the one without a response if there is one, otherwise a new one
    /// with the unsent changes.
    fn next_heartbeat(
        &mut self,
        progress: f32,
        tracking_url: Option<String>,
    ) -> AllocateRequestProto {
        if let Some(in_flight) = &self.in_flight {
            return in_flight.request.clone();
        }
        let changes = &mut self.changes;
        let blacklist_request = if changes.unsent_blacklist_additions.is_empty()
            && changes.unsent_blacklist_removals.is_empty()
        {
            None
        } else {
            Some(ResourceBlacklistRequestProto {
                blacklist_additions: changes.unsent_blacklist_additions.drain().collect(),
                blacklist_removals: changes.unsent_blacklist_removals.drain().collect(),
            })
        };
        let asks = std::mem::take(&mut changes.unsent_asks);
        let request = AllocateRequestProto {
            ask: asks
                .iter()
                .filter_map(|key| changes.asks.get(key))
                .cloned()
                .map(Into::into)
                .collect(),
            release: changes.unsent_releases.drain().map(Into::into).collect(),
            blacklist_request,
            response_id: Some(self.last_response_id),
            progress: Some(progress),
            tracking_url,
            ..Default::default()
        };
        self.in_flight = Some(InFlight {
            request: request.clone(),
            asks,
            rollback: self.unsent_rollback.take(),
        });
        request
    }

    /// Undoes the changes in the heartbeat without a response, and any made since, after the
    /// ResourceManager rejected it.
    fn reject_in_flight(&mut self) {
        if let Some(in_flight) = self.in_flight.take() {
            if let Some(rollback) = in_flight.rollback.or_else(|| self.unsent_rollback.take()) {
                self.changes = *rollback;
            }
            self.unsent_rollback = None;
        }
    }

    /// Updates the state after the heartbeat without a response got one.
    fn on_response(
        &mut self,
        response_id: i32,
        allocated: &[Container],
        completed: &[ContainerStatus],
    ) {
        self.last_response_id = response_id;
        let sent = self
            .in_flight
            .take()
            .map(|in_flight| in_flight.asks)
            .unwrap_or_default();
        for container in allocated {
            self.remove_satisfied_asks(container, &sent);
        }
        // Requests for no containers only needed to reach the ResourceManager once, unless
        // they changed since
        for key in sent {
            if !self.changes.unsent_asks.contains(&key)
                && self
                    .changes
                    .asks
                    .get(&key)
                    .is_some_and(|ask| ask.num_containers <= 0)
            {
                self.changes.asks.remove(&key);
            }
        }
        for status in completed {
            self.changes.pending_releases.remove(&status.container_id);
        }
    }

    /// The scheduler decrements the requests a container satisfies, so do the same: the
    /// off-switch (`*`) request with the container's priority and allocation request ID, and
    /// the request for its host. Rack requests are not tracked down because a container doesn't
    /// name its rack, so after a resync they may ask for more than needed until the next update.
    ///
    /// Requests set in the heartbeat that returned the container (`sent`) are left alone: the
    /// scheduler allocated the container before that heartbeat, from the old count, and then
    /// replaced the count with the one sent. So are requests changed since, whose new count the
    /// next heartbeat sends.
    fn remove_satisfied_asks(&mut self, container: &Container, sent: &HashSet<AskKey>) {
        for resource_name in [ANY_RESOURCE, container.node_id.host.as_str()] {
            let key = AskKey::new(
                container.priority,
                resource_name,
                container.allocation_request_id,
            );
            if sent.contains(&key) || self.changes.unsent_asks.contains(&key) {
                continue;
            }
            if let Some(ask) = self.changes.asks.get_mut(&key) {
                ask.num_containers -= 1;
                if ask.num_containers <= 0 {
                    self.changes.asks.remove(&key);
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
        let response = match self.register(&mut state, request.clone()).await {
            Ok(response) => response,
            // The ResourceManager rejects a second registration, so if the response to an
            // earlier one with the same settings was lost, this means it succeeded
            Err(error)
                if is_already_registered_error(&error)
                    && state.registration.is_none()
                    && state.unconfirmed_registration.as_ref() == Some(&request) =>
            {
                warn!(
                    "The response to registering the ApplicationMaster was lost, but the \
                     registration succeeded"
                );
                state.unconfirmed_registration = None;
                state.last_response_id = 0;
                state.registration = Some(request);
                return Ok(RegisterResponse {
                    maximum_resource_capability: None,
                    queue: None,
                    containers_from_previous_attempts: Vec::new(),
                    recovered: true,
                });
            }
            Err(error) => {
                // Unless the ResourceManager answered, it may have registered the
                // ApplicationMaster without the response arriving
                if !matches!(
                    error,
                    YarnError::RPCError(..) | YarnError::FatalRPCError(..)
                ) {
                    state.unconfirmed_registration = Some(request);
                }
                return Err(error);
            }
        };
        state.unconfirmed_registration = None;

        Ok(RegisterResponse {
            maximum_resource_capability: response.maximum_capability.map(Into::into),
            queue: response.queue,
            containers_from_previous_attempts: response
                .containers_from_previous_attempts
                .into_iter()
                .map(Into::into)
                .collect(),
            recovered: false,
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

    /// Registers again with the original settings and the latest tracking URL after the
    /// ResourceManager lost this ApplicationMaster, then re-sends everything it should know.
    ///
    /// The ResourceManager resets the response ID when it accepts the registration, so if the
    /// response is lost the next heartbeat registers again first. The ResourceManager then
    /// refuses the registration as a duplicate, which means the earlier one succeeded.
    async fn reregister(&self, state: &mut AllocateState) -> Result<()> {
        let registration = state.registration.clone().ok_or_else(|| {
            YarnError::InvalidArgument(
                "The ApplicationMaster must register before it heartbeats".to_string(),
            )
        })?;
        warn!("ApplicationMaster is out of sync with the ResourceManager, registering again");
        state.reregistering = true;
        match self.register(state, registration).await {
            Ok(_) => {}
            Err(error) if is_already_registered_error(&error) => {
                debug!("The earlier registration succeeded");
                state.last_response_id = 0;
            }
            Err(error) => return Err(error),
        }
        state.reregistering = false;
        state.resend_all();
        Ok(())
    }

    /// Sends a heartbeat that asks for and releases containers. NMTokens in the response are
    /// stored in the [`NmTokenCache`], and a rolled AMRMToken is used for new connections.
    ///
    /// If an earlier heartbeat got no response, it is re-sent unchanged first, and this
    /// heartbeat's changes follow in a second one. When the ResourceManager rejects a heartbeat's
    /// contents, for example a release of a container from another attempt, the error is
    /// returned and the changes not yet acknowledged, including this heartbeat's, are dropped.
    pub async fn allocate(&self, request: AllocateRequest) -> Result<AllocateResponse> {
        let progress = request.progress;
        let tracking_url = request.tracking_url.clone();

        let mut state = self.state.lock().await;
        state.add_request(request);
        let mut resynced = false;
        if state.reregistering {
            self.reregister(&mut state).await?;
            resynced = true;
        }

        let mut result: Option<AllocateResponse> = None;
        let mut resyncs = 0;
        let mut sent_current = false;
        while !sent_current {
            // A replayed heartbeat carries an earlier call's changes, so this call's follow
            sent_current = state.in_flight.is_none();
            let heartbeat = state.next_heartbeat(progress, tracking_url.clone());
            let response = match self.protocol.allocate(heartbeat).await {
                Ok(response) => response,
                // The ResourceManager can lose the ApplicationMaster again, for example in
                // back-to-back failovers, so resync more than once
                Err(YarnError::ApplicationMasterNotRegistered(_)) if resyncs < MAX_RESYNCS => {
                    self.reregister(&mut state).await?;
                    resynced = true;
                    resyncs += 1;
                    sent_current = false;
                    continue;
                }
                Err(error) => {
                    if is_request_validation_error(&error) {
                        state.reject_in_flight();
                    }
                    if let Some(response) = result {
                        state.undelivered = Some(match state.undelivered.take() {
                            Some(earlier) => earlier.merge(response),
                            None => response,
                        });
                    }
                    return Err(error);
                }
            };
            let response = self.handle_response(&mut state, response);
            result = Some(match result {
                Some(earlier) => earlier.merge(response),
                None => response,
            });
            if sent_current
                && let (Some(url), Some(registration)) =
                    (tracking_url.as_ref(), state.registration.as_mut())
            {
                registration.tracking_url = Some(url.clone());
            }
            // A replayed heartbeat with nothing new since doesn't need a second one
            if !sent_current && !state.changes.has_unsent() && tracking_url.is_none() {
                break;
            }
        }

        let mut response = result.expect("a heartbeat was sent");
        if let Some(earlier) = state.undelivered.take() {
            response = earlier.merge(response);
        }
        response.resynced = resynced;
        Ok(response)
    }

    fn handle_response(
        &self,
        state: &mut AllocateState,
        response: yarn::AllocateResponseProto,
    ) -> AllocateResponse {
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

        self.nm_tokens.insert_all(response.nm_tokens);
        if let Some(am_rm_token) = response.am_rm_token {
            info!("Received a new AMRMToken");
            let mut token = Token::from(am_rm_token);
            token.set_service(self.protocol.token_service());
            self.protocol.set_token(token);
        }

        AllocateResponse {
            am_command: response
                .a_m_command
                .and_then(|command| yarn::AmCommandProto::try_from(command).ok())
                .map(Into::into),
            num_cluster_nodes: response.num_cluster_nodes.unwrap_or_default(),
            allocated_containers,
            completed_containers,
            available_resources: response.limit.unwrap_or_default().into(),
            updated_nodes: response.updated_nodes.into_iter().map(Into::into).collect(),
            resynced: false,
        }
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
            let reregistering = self.state.lock().await.reregistering;
            if reregistering {
                let mut state = self.state.lock().await;
                self.reregister(&mut state).await?;
            }
            match self
                .protocol
                .finish_application_master(request.clone())
                .await
            {
                Ok(response) if response.is_unregistered() => return Ok(()),
                Ok(_) => self.rt_holder.sleep(UNREGISTER_POLL_INTERVAL).await,
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

    /// Sends a heartbeat unless one is waiting for a response, and acknowledges it.
    fn ack(
        state: &mut AllocateState,
        response_id: i32,
        allocated: &[Container],
        completed: &[ContainerStatus],
    ) {
        state.next_heartbeat(0.0, None);
        state.on_response(response_id, allocated, completed);
    }

    fn sent_asks(state: &mut AllocateState) -> HashMap<String, i32> {
        state
            .next_heartbeat(0.0, None)
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
        let request = state.next_heartbeat(0.5, None);
        assert_eq!(request.ask.len(), 1);
        assert_eq!(request.release.len(), 1);
        assert_eq!(
            request.blacklist_request.unwrap().blacklist_additions,
            vec!["bad-host"]
        );
        assert_eq!(request.response_id, Some(0));

        ack(&mut state, 1, &[], &[]);
        let request = state.next_heartbeat(0.5, None);
        assert!(request.ask.is_empty());
        assert!(request.release.is_empty());
        assert!(request.blacklist_request.is_none());
        assert_eq!(request.response_id, Some(1));
    }

    /// The outstanding request counts, keyed by resource name
    fn outstanding(state: &AllocateState) -> HashMap<String, i32> {
        state
            .changes
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
        ack(&mut state, 1, &[], &[]);

        // The scheduler rounds capabilities up, so a larger container still matches. A
        // container at another priority does not.
        ack(
            &mut state,
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

        ack(&mut state, 3, &[container(3, "host-a", 512, 1)], &[]);
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
        ack(&mut state, 1, &[], &[]);
        ack(
            &mut state,
            2,
            &[
                container(1, "host-a", 128, 0),
                container(2, "host-b", 128, 0),
            ],
            &[],
        );

        state.resend_all();
        assert_eq!(
            sent_asks(&mut state),
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
        ack(&mut state, 1, &[], &[]);

        // The scheduler allocated this container from the old count of 2 before the heartbeat
        // replaced the count with 2 again, so 2 are still outstanding
        state.add_request(AllocateRequest {
            asks: vec![ask],
            ..Default::default()
        });
        ack(&mut state, 2, &[container(1, "host-a", 512, 1)], &[]);
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
        ack(&mut state, 1, &[], &[]);
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(2048, 1), 1)],
            ..Default::default()
        });
        ack(&mut state, 2, &[], &[]);

        state.resend_all();
        let request = state.next_heartbeat(0.0, None);
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
        ack(&mut state, 1, &[], &[]);
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(512, 1), 0)],
            ..Default::default()
        });
        assert_eq!(
            sent_asks(&mut state),
            HashMap::from([(ANY_RESOURCE.to_string(), 0)])
        );

        ack(&mut state, 2, &[], &[]);
        state.resend_all();
        assert!(sent_asks(&mut state).is_empty());
    }

    #[test]
    fn test_releases_are_pending_until_completed() {
        let mut state = AllocateState::default();
        let released: ContainerId = "container_1_0001_01_000004".parse().unwrap();
        state.add_request(AllocateRequest {
            releases: vec![released],
            ..Default::default()
        });
        ack(&mut state, 1, &[], &[]);

        state.resend_all();
        assert_eq!(state.next_heartbeat(0.0, None).release.len(), 1);

        ack(
            &mut state,
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
        assert!(state.next_heartbeat(0.0, None).release.is_empty());
    }

    #[test]
    fn test_blacklist_changes_cancel_out_and_resync_sends_the_current_list() {
        let mut state = AllocateState::default();
        state.add_request(AllocateRequest {
            blacklist_additions: vec!["a".to_string(), "b".to_string()],
            ..Default::default()
        });
        ack(&mut state, 1, &[], &[]);
        state.add_request(AllocateRequest {
            blacklist_removals: vec!["a".to_string()],
            blacklist_additions: vec!["c".to_string()],
            ..Default::default()
        });
        state.add_request(AllocateRequest {
            blacklist_removals: vec!["c".to_string()],
            ..Default::default()
        });
        let blacklist = state.next_heartbeat(0.0, None).blacklist_request.unwrap();
        assert!(blacklist.blacklist_additions.is_empty());
        let mut removals = blacklist.blacklist_removals;
        removals.sort();
        assert_eq!(removals, vec!["a", "c"]);

        ack(&mut state, 2, &[], &[]);
        state.resend_all();
        let blacklist = state.next_heartbeat(0.0, None).blacklist_request.unwrap();
        assert_eq!(blacklist.blacklist_additions, vec!["b"]);
        assert!(blacklist.blacklist_removals.is_empty());
        assert_eq!(state.next_heartbeat(0.0, None).response_id, Some(0));
    }
    #[test]
    fn test_a_replayed_heartbeat_keeps_its_contents_and_newer_changes_follow() {
        let mut state = AllocateState::default();
        let first = ResourceRequest::new(1, Resource::new(512, 1), 1);
        state.add_request(AllocateRequest {
            asks: vec![first],
            ..Default::default()
        });
        let sent = state.next_heartbeat(0.1, None);

        // The response was lost. The ResourceManager may have handled the heartbeat, in which
        // case it answers a replay with its cached response and ignores the contents, so newer
        // changes must not be merged into it
        let released: ContainerId = "container_1_0001_01_000002".parse().unwrap();
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(2, Resource::new(512, 1), 1)],
            releases: vec![released],
            blacklist_additions: vec!["bad-host".to_string()],
            ..Default::default()
        });
        assert_eq!(state.next_heartbeat(0.2, None), sent);

        state.on_response(1, &[], &[]);
        let next = state.next_heartbeat(0.2, None);
        assert_eq!(next.response_id, Some(1));
        assert_eq!(next.ask.len(), 1);
        assert_eq!(next.ask[0].priority.as_ref().unwrap().priority, Some(2));
        assert_eq!(next.release.len(), 1);
        assert_eq!(
            next.blacklist_request.unwrap().blacklist_additions,
            vec!["bad-host"]
        );
    }

    #[test]
    fn test_a_rejected_heartbeat_is_rolled_back() {
        let mut state = AllocateState::default();
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(512, 1), 2)],
            ..Default::default()
        });
        ack(&mut state, 1, &[], &[]);

        // A release of another attempt's container is rejected with the rest of the heartbeat
        let foreign: ContainerId = "container_1_0001_02_000002".parse().unwrap();
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(512, 1), 5)],
            releases: vec![foreign],
            blacklist_additions: vec!["host".to_string()],
            ..Default::default()
        });
        state.next_heartbeat(0.0, None);
        state.reject_in_flight();

        let next = state.next_heartbeat(0.0, None);
        assert!(next.ask.is_empty());
        assert!(next.release.is_empty());
        assert!(next.blacklist_request.is_none());
        assert_eq!(next.response_id, Some(1));
        assert!(state.changes.pending_releases.is_empty());
        assert!(state.changes.blacklist.is_empty());
        assert_eq!(
            outstanding(&state),
            HashMap::from([(ANY_RESOURCE.to_string(), 2)])
        );
    }

    #[test]
    fn test_a_resync_replaces_the_heartbeat_without_a_response() {
        let mut state = AllocateState::default();
        state.add_request(AllocateRequest {
            asks: vec![ResourceRequest::new(1, Resource::new(512, 1), 2)],
            ..Default::default()
        });
        ack(&mut state, 4, &[], &[]);
        state.add_request(AllocateRequest {
            blacklist_additions: vec!["host".to_string()],
            ..Default::default()
        });
        state.next_heartbeat(0.0, None);

        state.resend_all();
        let next = state.next_heartbeat(0.0, None);
        assert_eq!(next.response_id, Some(0));
        assert_eq!(next.ask.len(), 1);
        assert_eq!(
            next.blacklist_request.unwrap().blacklist_additions,
            vec!["host"]
        );
    }

    #[test]
    fn test_request_errors_are_classified() {
        let release = YarnError::RPCError(
            "org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException".to_string(),
            "Cannot release container".to_string(),
        );
        assert!(is_request_validation_error(&release));
        assert!(!is_request_validation_error(&YarnError::IOError(
            std::io::ErrorKind::ConnectionReset.into()
        )));

        let registered = YarnError::RPCError(
            INVALID_AM_REQUEST_EXCEPTION.to_string(),
            "Application Master is already registered : application_1_0001".to_string(),
        );
        assert!(is_already_registered_error(&registered));
        assert!(!is_already_registered_error(&YarnError::RPCError(
            INVALID_AM_REQUEST_EXCEPTION.to_string(),
            "Invalid responseId in AllocateRequest".to_string(),
        )));
    }

    /// A ResourceManager that drops the first registration's response, then rejects the retry
    /// as a duplicate, like a real one that handled the first.
    async fn lossy_resource_manager() -> String {
        use hadoop_native::proto::common::{
            RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto,
            rpc_response_header_proto::RpcStatusProto,
            rpc_sasl_proto::{SaslAuth, SaslState},
        };
        use prost::Message;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn read_message(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
            let mut length = [0u8; 4];
            socket.read_exact(&mut length).await.unwrap();
            let mut message = vec![0u8; u32::from_be_bytes(length) as usize];
            socket.read_exact(&mut message).await.unwrap();
            message
        }

        async fn send(socket: &mut tokio::net::TcpStream, messages: &[Vec<u8>]) {
            let length: usize = messages.iter().map(Vec::len).sum();
            socket
                .write_all(&(length as u32).to_be_bytes())
                .await
                .unwrap();
            for message in messages {
                socket.write_all(message).await.unwrap();
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            for connection in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut header = [0u8; 7];
                socket.read_exact(&mut header).await.unwrap();
                read_message(&mut socket).await;
                let sasl_header = RpcResponseHeaderProto {
                    call_id: -33i32 as u32,
                    status: RpcStatusProto::Success as i32,
                    ..Default::default()
                };
                let negotiate = RpcSaslProto {
                    state: SaslState::Negotiate as i32,
                    auths: vec![SaslAuth {
                        method: "SIMPLE".to_string(),
                        mechanism: String::new(),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                send(
                    &mut socket,
                    &[
                        sasl_header.encode_length_delimited_to_vec(),
                        negotiate.encode_length_delimited_to_vec(),
                    ],
                )
                .await;
                // The client's choice of SIMPLE, the connection context, then the registration
                read_message(&mut socket).await;
                read_message(&mut socket).await;
                let request = read_message(&mut socket).await;
                if connection == 0 {
                    // The ResourceManager registers the ApplicationMaster, but the response
                    // never arrives
                    continue;
                }
                let call_id = RpcRequestHeaderProto::decode_length_delimited(&request[..])
                    .unwrap()
                    .call_id;
                let error = RpcResponseHeaderProto {
                    call_id: call_id as u32,
                    status: RpcStatusProto::Error as i32,
                    exception_class_name: Some(INVALID_AM_REQUEST_EXCEPTION.to_string()),
                    error_msg: Some(
                        "Application Master is already registered : application_1_0001".to_string(),
                    ),
                    ..Default::default()
                };
                send(&mut socket, &[error.encode_length_delimited_to_vec()]).await;
                let mut rest = Vec::new();
                let _ = socket.read_to_end(&mut rest).await;
            }
        });
        address
    }

    #[tokio::test]
    async fn test_a_registration_whose_response_was_lost_is_recovered() {
        let _ = env_logger::builder().is_test(true).try_init();
        let address = lossy_resource_manager().await;
        let client = AmRmClientBuilder::new()
            .with_config([
                ("yarn.resourcemanager.scheduler.address", address.as_str()),
                ("ipc.client.fallback-to-simple-auth-allowed", "true"),
                ("ipc.client.rpc-timeout.ms", "3000"),
            ])
            .with_am_rm_token(token())
            .build()
            .unwrap();

        let error = client
            .register_application_master("am-host", 0, None)
            .await
            .unwrap_err();
        assert!(matches!(error, YarnError::IOError(_)), "{error:?}");

        let response = client
            .register_application_master("am-host", 0, None)
            .await
            .unwrap();
        assert!(response.recovered);
        assert!(response.maximum_resource_capability.is_none());
        let state = client.state.lock().await;
        assert!(state.registration.is_some());
        assert_eq!(state.last_response_id, 0);
    }
}
