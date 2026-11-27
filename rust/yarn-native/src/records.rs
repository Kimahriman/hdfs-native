//! Records exchanged with the ResourceManager and NodeManagers.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use hadoop_native::security::user::{Credentials, SerializedFormat, Token};
use url::Url;

use crate::ids::{ApplicationAttemptId, ApplicationId, ContainerId, NodeId};
use crate::proto::yarn::{
    AmCommandProto, ApplicationAttemptReportProto, ApplicationReportProto,
    ApplicationSubmissionContextProto, ContainerLaunchContextProto, ContainerProto,
    ContainerReportProto, ContainerStateProto, ContainerStatusProto, FinalApplicationStatusProto,
    LocalResourceProto, LocalResourceTypeProto, LocalResourceVisibilityProto, NodeReportProto,
    NodeStateProto, PriorityProto, QueueInfoProto, QueueStateProto, ResourceProto,
    ResourceRequestProto, SignalContainerCommandProto, StringBytesMapProto,
    StringLocalResourceMapProto, StringStringMapProto, UrlProto, YarnApplicationAttemptStateProto,
    YarnApplicationStateProto, YarnClusterMetricsProto,
};
use crate::{Result, YarnError};

/// The resource name that matches any host or rack in a [`ResourceRequest`].
pub const ANY_RESOURCE: &str = "*";

/// Defines a public enum with lossless conversions to and from its protobuf enum.
macro_rules! proto_enum {
    (
        $(#[$meta:meta])*
        $name:ident <=> $proto:ident { $($variant:ident <=> $proto_variant:ident),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl From<$proto> for $name {
            fn from(value: $proto) -> Self {
                match value {
                    $($proto::$proto_variant => Self::$variant),+
                }
            }
        }

        impl From<$name> for $proto {
            fn from(value: $name) -> Self {
                match value {
                    $($name::$variant => Self::$proto_variant),+
                }
            }
        }
    };
}

proto_enum! {
    /// The lifecycle state of an application.
    ApplicationState <=> YarnApplicationStateProto {
        New <=> New,
        NewSaving <=> NewSaving,
        Submitted <=> Submitted,
        Accepted <=> Accepted,
        Running <=> Running,
        Finished <=> Finished,
        Failed <=> Failed,
        Killed <=> Killed,
    }
}

impl ApplicationState {
    /// Whether the application has stopped and will not change state again.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Finished | Self::Failed | Self::Killed)
    }
}

proto_enum! {
    /// The outcome an ApplicationMaster reports when it unregisters.
    FinalApplicationStatus <=> FinalApplicationStatusProto {
        Undefined <=> AppUndefined,
        Succeeded <=> AppSucceeded,
        Failed <=> AppFailed,
        Killed <=> AppKilled,
        Ended <=> AppEnded,
    }
}

proto_enum! {
    /// The lifecycle state of an application attempt.
    ApplicationAttemptState <=> YarnApplicationAttemptStateProto {
        New <=> AppAttemptNew,
        Submitted <=> AppAttemptSubmitted,
        Scheduled <=> AppAttemptScheduled,
        AllocatedSaving <=> AppAttemptAllocatedSaving,
        Allocated <=> AppAttemptAllocated,
        Launched <=> AppAttemptLaunched,
        Failed <=> AppAttemptFailed,
        Running <=> AppAttemptRunning,
        Finishing <=> AppAttemptFinishing,
        Finished <=> AppAttemptFinished,
        Killed <=> AppAttemptKilled,
    }
}

proto_enum! {
    ContainerState <=> ContainerStateProto {
        New <=> CNew,
        Running <=> CRunning,
        Complete <=> CComplete,
    }
}

proto_enum! {
    NodeState <=> NodeStateProto {
        New <=> NsNew,
        Running <=> NsRunning,
        Unhealthy <=> NsUnhealthy,
        Decommissioned <=> NsDecommissioned,
        Lost <=> NsLost,
        Rebooted <=> NsRebooted,
        Decommissioning <=> NsDecommissioning,
        Shutdown <=> NsShutdown,
    }
}

proto_enum! {
    QueueState <=> QueueStateProto {
        Stopped <=> QStopped,
        Running <=> QRunning,
        Draining <=> QDraining,
    }
}

proto_enum! {
    /// How a NodeManager unpacks a [`LocalResource`].
    LocalResourceType <=> LocalResourceTypeProto {
        Archive <=> Archive,
        File <=> File,
        Pattern <=> Pattern,
    }
}

proto_enum! {
    /// Who may share a [`LocalResource`] once it is localized on a node.
    LocalResourceVisibility <=> LocalResourceVisibilityProto {
        Public <=> Public,
        Private <=> Private,
        Application <=> Application,
    }
}

proto_enum! {
    /// An instruction from the ResourceManager to an ApplicationMaster.
    AmCommand <=> AmCommandProto {
        Resync <=> AmResync,
        Shutdown <=> AmShutdown,
    }
}

proto_enum! {
    SignalContainerCommand <=> SignalContainerCommandProto {
        OutputThreadDump <=> OutputThreadDump,
        GracefulShutdown <=> GracefulShutdown,
        ForcefulShutdown <=> ForcefulShutdown,
    }
}

/// An amount of memory and CPU.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Resource {
    pub memory_mb: i64,
    pub vcores: i32,
}

impl Resource {
    pub fn new(memory_mb: i64, vcores: i32) -> Self {
        Self { memory_mb, vcores }
    }
}

impl From<ResourceProto> for Resource {
    fn from(value: ResourceProto) -> Self {
        Self::new(value.memory(), value.virtual_cores())
    }
}

impl From<Resource> for ResourceProto {
    fn from(value: Resource) -> Self {
        Self {
            memory: Some(value.memory_mb),
            virtual_cores: Some(value.vcores),
            ..Default::default()
        }
    }
}

fn priority(value: Option<PriorityProto>) -> i32 {
    value
        .map(|priority| priority.priority())
        .unwrap_or_default()
}

fn priority_proto(value: i32) -> Option<PriorityProto> {
    Some(PriorityProto {
        priority: Some(value),
    })
}

fn token(value: Option<crate::proto::common::TokenProto>) -> Option<Token> {
    value.map(Token::from)
}

#[derive(Debug, Clone)]
pub struct ApplicationReport {
    pub application_id: ApplicationId,
    pub current_attempt_id: Option<ApplicationAttemptId>,
    pub user: String,
    pub queue: String,
    pub name: String,
    /// Host of the ApplicationMaster's RPC server, or `N/A`.
    pub host: String,
    pub rpc_port: i32,
    pub state: ApplicationState,
    pub final_status: FinalApplicationStatus,
    pub diagnostics: String,
    pub tracking_url: String,
    pub original_tracking_url: String,
    pub progress: f32,
    pub application_type: String,
    pub tags: Vec<String>,
    pub priority: i32,
    pub unmanaged: bool,
    pub submit_time: i64,
    pub launch_time: i64,
    pub start_time: i64,
    pub finish_time: i64,
}

impl From<ApplicationReportProto> for ApplicationReport {
    fn from(value: ApplicationReportProto) -> Self {
        Self {
            state: value.yarn_application_state().into(),
            final_status: value.final_application_status().into(),
            rpc_port: value.rpc_port(),
            progress: value.progress(),
            unmanaged: value.unmanaged_application(),
            submit_time: value.submit_time(),
            launch_time: value.launch_time(),
            start_time: value.start_time(),
            finish_time: value.finish_time(),
            priority: priority(value.priority),
            application_id: value.application_id.unwrap_or_default().into(),
            current_attempt_id: value.current_application_attempt_id.map(Into::into),
            user: value.user.unwrap_or_default(),
            queue: value.queue.unwrap_or_default(),
            name: value.name.unwrap_or_default(),
            host: value.host.unwrap_or_default(),
            diagnostics: value.diagnostics.unwrap_or_default(),
            tracking_url: value.tracking_url.unwrap_or_default(),
            original_tracking_url: value.original_tracking_url.unwrap_or_default(),
            application_type: value.application_type.unwrap_or_default(),
            tags: value.application_tags,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApplicationAttemptReport {
    pub attempt_id: ApplicationAttemptId,
    pub state: ApplicationAttemptState,
    pub am_container_id: Option<ContainerId>,
    pub host: String,
    pub rpc_port: i32,
    pub tracking_url: String,
    pub diagnostics: String,
    pub start_time: i64,
    pub finish_time: i64,
}

impl From<ApplicationAttemptReportProto> for ApplicationAttemptReport {
    fn from(value: ApplicationAttemptReportProto) -> Self {
        Self {
            state: value.yarn_application_attempt_state().into(),
            rpc_port: value.rpc_port(),
            start_time: value.start_time(),
            finish_time: value.finish_time(),
            attempt_id: value.application_attempt_id.unwrap_or_default().into(),
            am_container_id: value.am_container_id.map(Into::into),
            host: value.host.unwrap_or_default(),
            tracking_url: value.tracking_url.unwrap_or_default(),
            diagnostics: value.diagnostics.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContainerReport {
    pub container_id: ContainerId,
    pub node_id: NodeId,
    pub resource: Resource,
    pub priority: i32,
    pub state: ContainerState,
    pub exit_status: i32,
    pub diagnostics: String,
    pub log_url: String,
    pub creation_time: i64,
    pub finish_time: i64,
}

impl From<ContainerReportProto> for ContainerReport {
    fn from(value: ContainerReportProto) -> Self {
        Self {
            state: value.container_state().into(),
            exit_status: value.container_exit_status(),
            creation_time: value.creation_time(),
            finish_time: value.finish_time(),
            priority: priority(value.priority),
            container_id: value.container_id.unwrap_or_default().into(),
            node_id: value.node_id.unwrap_or_default().into(),
            resource: value.resource.unwrap_or_default().into(),
            diagnostics: value.diagnostics_info.unwrap_or_default(),
            log_url: value.log_url.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeReport {
    pub node_id: NodeId,
    pub state: NodeState,
    pub http_address: String,
    pub rack_name: String,
    pub used: Resource,
    pub capability: Resource,
    pub num_containers: i32,
    pub health_report: String,
    pub last_health_report_time: i64,
    pub node_labels: Vec<String>,
}

impl From<NodeReportProto> for NodeReport {
    fn from(value: NodeReportProto) -> Self {
        Self {
            state: value.node_state().into(),
            num_containers: value.num_containers(),
            last_health_report_time: value.last_health_report_time(),
            node_id: value.node_id.unwrap_or_default().into(),
            http_address: value.http_address.unwrap_or_default(),
            rack_name: value.rack_name.unwrap_or_default(),
            used: value.used.unwrap_or_default().into(),
            capability: value.capability.unwrap_or_default().into(),
            health_report: value.health_report.unwrap_or_default(),
            node_labels: value.node_labels,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterMetrics {
    pub num_node_managers: i32,
    pub num_active_node_managers: i32,
    pub num_decommissioning_node_managers: i32,
    pub num_decommissioned_node_managers: i32,
    pub num_lost_node_managers: i32,
    pub num_unhealthy_node_managers: i32,
    pub num_rebooted_node_managers: i32,
    pub num_shutdown_node_managers: i32,
}

impl From<YarnClusterMetricsProto> for ClusterMetrics {
    fn from(value: YarnClusterMetricsProto) -> Self {
        Self {
            num_node_managers: value.num_node_managers(),
            num_active_node_managers: value.num_active_nms(),
            num_decommissioning_node_managers: value.num_decommissioning_nms(),
            num_decommissioned_node_managers: value.num_decommissioned_nms(),
            num_lost_node_managers: value.num_lost_nms(),
            num_unhealthy_node_managers: value.num_unhealthy_nms(),
            num_rebooted_node_managers: value.num_rebooted_nms(),
            num_shutdown_node_managers: value.num_shutdown_nms(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueueInfo {
    pub queue_name: String,
    pub queue_path: String,
    pub state: QueueState,
    /// Configured capacity as a fraction of the parent queue.
    pub capacity: f32,
    /// Maximum capacity as a fraction of the parent queue.
    pub maximum_capacity: f32,
    /// Used capacity as a fraction of the queue's capacity.
    pub current_capacity: f32,
    pub child_queues: Vec<QueueInfo>,
    pub applications: Vec<ApplicationReport>,
}

impl From<QueueInfoProto> for QueueInfo {
    fn from(value: QueueInfoProto) -> Self {
        Self {
            state: value.state().into(),
            capacity: value.capacity(),
            maximum_capacity: value.maximum_capacity(),
            current_capacity: value.current_capacity(),
            queue_name: value.queue_name.unwrap_or_default(),
            queue_path: value.queue_path.unwrap_or_default(),
            child_queues: value.child_queues.into_iter().map(Into::into).collect(),
            applications: value.applications.into_iter().map(Into::into).collect(),
        }
    }
}

/// The location of a [`LocalResource`], such as `hdfs://namenode:8020/apps/app.jar`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceUrl {
    pub scheme: String,
    pub host: Option<String>,
    pub port: Option<i32>,
    pub file: String,
    pub user_info: Option<String>,
}

impl FromStr for ResourceUrl {
    type Err = YarnError;

    fn from_str(value: &str) -> Result<Self> {
        let url = Url::parse(value).map_err(|error| {
            YarnError::InvalidArgument(format!("invalid resource URL {value:?}: {error}"))
        })?;
        let user_info = match (url.username(), url.password()) {
            ("", None) => None,
            (user, None) => Some(user.to_string()),
            (user, Some(password)) => Some(format!("{user}:{password}")),
        };
        Ok(Self {
            scheme: url.scheme().to_string(),
            host: url
                .host_str()
                .filter(|host| !host.is_empty())
                .map(str::to_string),
            port: url.port().map(i32::from),
            file: url.path().to_string(),
            user_info,
        })
    }
}

impl fmt::Display for ResourceUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:", self.scheme)?;
        if let Some(host) = &self.host {
            write!(f, "//")?;
            if let Some(user_info) = &self.user_info {
                write!(f, "{user_info}@")?;
            }
            write!(f, "{host}")?;
            if let Some(port) = self.port {
                write!(f, ":{port}")?;
            }
        }
        write!(f, "{}", self.file)
    }
}

/// Java's `URL.fromURI` stores `java.net.URI`'s "no port" value, -1.
const NO_PORT: i32 = -1;

impl From<UrlProto> for ResourceUrl {
    fn from(value: UrlProto) -> Self {
        Self {
            scheme: value.scheme.unwrap_or_default(),
            host: value.host.filter(|host| !host.is_empty()),
            port: value.port.filter(|port| *port > 0),
            file: value.file.unwrap_or_default(),
            user_info: value.user_info,
        }
    }
}

impl From<ResourceUrl> for UrlProto {
    fn from(value: ResourceUrl) -> Self {
        Self {
            scheme: Some(value.scheme),
            host: value.host,
            // The proto's default port is 0, which Java's `URL.toPath` would write into the URI
            // as `host:0`. Send Java's "no port" value instead, as `URL.fromURI` does.
            port: Some(value.port.unwrap_or(NO_PORT)),
            file: Some(value.file),
            user_info: value.user_info,
        }
    }
}

/// A file or archive that a NodeManager downloads into a container's working directory before
/// starting it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalResource {
    pub url: ResourceUrl,
    /// Size of the file in bytes. Must match the file, or localization fails.
    pub size: i64,
    /// Modification time of the file in milliseconds. Must match the file, or localization
    /// fails.
    pub timestamp: i64,
    pub resource_type: LocalResourceType,
    pub visibility: LocalResourceVisibility,
    /// For [`LocalResourceType::Pattern`], the entries of the archive to extract.
    pub pattern: Option<String>,
}

impl LocalResource {
    /// A file visible only to this application.
    pub fn file(url: ResourceUrl, size: i64, timestamp: i64) -> Self {
        Self {
            url,
            size,
            timestamp,
            resource_type: LocalResourceType::File,
            visibility: LocalResourceVisibility::Application,
            pattern: None,
        }
    }
}

impl From<LocalResource> for LocalResourceProto {
    fn from(value: LocalResource) -> Self {
        Self {
            resource: Some(value.url.into()),
            size: Some(value.size),
            timestamp: Some(value.timestamp),
            r#type: Some(LocalResourceTypeProto::from(value.resource_type) as i32),
            visibility: Some(LocalResourceVisibilityProto::from(value.visibility) as i32),
            pattern: value.pattern,
            should_be_uploaded_to_shared_cache: None,
        }
    }
}

/// Everything a NodeManager needs to start a container.
#[derive(Debug, Clone, Default)]
pub struct ContainerLaunchContext {
    /// Shell commands to run, joined with spaces. `<LOG_DIR>` is replaced with the
    /// container's log directory.
    pub commands: Vec<String>,
    pub environment: HashMap<String, String>,
    /// Files to download into the working directory, keyed by link name.
    pub local_resources: HashMap<String, LocalResource>,
    /// Data for NodeManager auxiliary services, keyed by service name.
    pub service_data: HashMap<String, Vec<u8>>,
    /// Tokens and secret keys for the container. The NodeManager writes them to a token file
    /// and points `HADOOP_TOKEN_FILE_LOCATION` at it.
    pub credentials: Credentials,
}

impl ContainerLaunchContext {
    /// Set the commands to run
    pub fn commands(mut self, commands: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.commands = commands.into_iter().map(Into::into).collect();
        self
    }

    /// Add an environment variable
    pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.environment.insert(name.into(), value.into());
        self
    }

    /// Add a local resource, linked as `name` in the working directory
    pub fn local_resource(mut self, name: impl Into<String>, resource: LocalResource) -> Self {
        self.local_resources.insert(name.into(), resource);
        self
    }

    /// Set the tokens and secret keys for the container
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }
}

impl From<ContainerLaunchContext> for ContainerLaunchContextProto {
    fn from(value: ContainerLaunchContext) -> Self {
        Self {
            command: value.commands,
            environment: value
                .environment
                .into_iter()
                .map(|(key, value)| StringStringMapProto {
                    key: Some(key),
                    value: Some(value),
                })
                .collect(),
            local_resources: value
                .local_resources
                .into_iter()
                .map(|(key, value)| StringLocalResourceMapProto {
                    key: Some(key),
                    value: Some(value.into()),
                })
                .collect(),
            service_data: value
                .service_data
                .into_iter()
                .map(|(key, value)| StringBytesMapProto {
                    key: Some(key),
                    value: Some(value),
                })
                .collect(),
            // The format Java's ContainerLaunchContext users write and NodeManagers read
            tokens: (!value.credentials.is_empty())
                .then(|| value.credentials.to_bytes(SerializedFormat::Writable)),
            ..Default::default()
        }
    }
}

/// Describes an application to submit to the ResourceManager.
#[derive(Debug, Clone)]
pub struct ApplicationSubmissionContext {
    pub application_id: ApplicationId,
    pub name: String,
    /// Defaults to `default`.
    pub queue: String,
    pub application_type: String,
    pub priority: Option<i32>,
    pub tags: Vec<String>,
    /// How to start the ApplicationMaster. Unused for unmanaged applications.
    pub am_container: ContainerLaunchContext,
    /// Resources for the ApplicationMaster container.
    pub am_resource: Resource,
    /// When true, YARN does not start an ApplicationMaster; the submitter runs it, using the
    /// AMRMToken from [`YarnClient::get_am_rm_token`](crate::YarnClient::get_am_rm_token).
    pub unmanaged_am: bool,
    /// Defaults to the cluster's `yarn.resourcemanager.am.max-attempts`.
    pub max_app_attempts: Option<i32>,
    pub keep_containers_across_application_attempts: bool,
    pub node_label_expression: Option<String>,
}

impl ApplicationSubmissionContext {
    pub fn new(application_id: ApplicationId, name: impl Into<String>) -> Self {
        Self {
            application_id,
            name: name.into(),
            queue: "default".to_string(),
            application_type: "YARN".to_string(),
            priority: None,
            tags: Vec::new(),
            am_container: ContainerLaunchContext::default(),
            am_resource: Resource::new(1024, 1),
            unmanaged_am: false,
            max_app_attempts: None,
            keep_containers_across_application_attempts: false,
            node_label_expression: None,
        }
    }
}

impl From<ApplicationSubmissionContext> for ApplicationSubmissionContextProto {
    fn from(value: ApplicationSubmissionContext) -> Self {
        Self {
            application_id: Some(value.application_id.into()),
            application_name: Some(value.name),
            queue: Some(value.queue),
            application_type: Some(value.application_type),
            priority: value.priority.and_then(priority_proto),
            application_tags: value.tags,
            am_container_spec: Some(value.am_container.into()),
            resource: Some(value.am_resource.into()),
            unmanaged_am: Some(value.unmanaged_am),
            max_app_attempts: value.max_app_attempts,
            keep_containers_across_application_attempts: Some(
                value.keep_containers_across_application_attempts,
            ),
            node_label_expression: value.node_label_expression,
            ..Default::default()
        }
    }
}

/// Asks the ResourceManager for containers.
///
/// A request is keyed by its priority, resource name, capability and allocation request ID.
/// Sending a request replaces the outstanding request with the same key, so `num_containers`
/// is the total still needed, not an increment.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceRequest {
    pub priority: i32,
    /// A host, a rack, or [`ANY_RESOURCE`].
    pub resource_name: String,
    pub capability: Resource,
    pub num_containers: i32,
    /// Whether the scheduler may place containers on other hosts or racks.
    pub relax_locality: bool,
    pub node_label_expression: Option<String>,
    pub allocation_request_id: Option<i64>,
}

impl ResourceRequest {
    /// Request containers on any node.
    pub fn new(priority: i32, capability: Resource, num_containers: i32) -> Self {
        Self {
            priority,
            resource_name: ANY_RESOURCE.to_string(),
            capability,
            num_containers,
            relax_locality: true,
            node_label_expression: None,
            allocation_request_id: None,
        }
    }
}

impl From<ResourceRequest> for ResourceRequestProto {
    fn from(value: ResourceRequest) -> Self {
        Self {
            priority: priority_proto(value.priority),
            resource_name: Some(value.resource_name),
            capability: Some(value.capability.into()),
            num_containers: Some(value.num_containers),
            relax_locality: Some(value.relax_locality),
            node_label_expression: value.node_label_expression,
            allocation_request_id: value.allocation_request_id,
            ..Default::default()
        }
    }
}

/// A container allocated to an application.
#[derive(Debug, Clone)]
pub struct Container {
    pub id: ContainerId,
    pub node_id: NodeId,
    pub node_http_address: String,
    pub resource: Resource,
    pub priority: i32,
    /// Authorizes starting this container on its NodeManager.
    pub container_token: Option<Token>,
    pub allocation_request_id: Option<i64>,
}

impl From<ContainerProto> for Container {
    fn from(value: ContainerProto) -> Self {
        Self {
            id: value.id.unwrap_or_default().into(),
            node_id: value.node_id.unwrap_or_default().into(),
            node_http_address: value.node_http_address.unwrap_or_default(),
            resource: value.resource.unwrap_or_default().into(),
            priority: priority(value.priority),
            container_token: token(value.container_token),
            allocation_request_id: value.allocation_request_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContainerStatus {
    pub container_id: ContainerId,
    pub state: ContainerState,
    /// The process exit code once the container is complete. YARN uses negative values for
    /// containers it stopped, such as -100 for aborted and -105 for killed by the
    /// ApplicationMaster.
    pub exit_status: i32,
    pub diagnostics: String,
}

impl From<ContainerStatusProto> for ContainerStatus {
    fn from(value: ContainerStatusProto) -> Self {
        Self {
            state: value.state().into(),
            exit_status: value.exit_status(),
            container_id: value.container_id.unwrap_or_default().into(),
            diagnostics: value.diagnostics.unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enums_round_trip() {
        for state in [
            ApplicationState::New,
            ApplicationState::Running,
            ApplicationState::Killed,
        ] {
            assert_eq!(
                ApplicationState::from(YarnApplicationStateProto::from(state)),
                state
            );
        }
        assert!(ApplicationState::Finished.is_terminal());
        assert!(!ApplicationState::Accepted.is_terminal());
        assert_eq!(
            FinalApplicationStatusProto::from(FinalApplicationStatus::Succeeded),
            FinalApplicationStatusProto::AppSucceeded
        );
    }

    #[test]
    fn test_resource_urls_round_trip() {
        for value in [
            "hdfs://namenode:8020/apps/app.jar",
            "hdfs://user@namenode/apps/app.jar",
            "file:/tmp/app.jar",
        ] {
            let url: ResourceUrl = value.parse().unwrap();
            assert_eq!(url.to_string(), value);
            assert_eq!(ResourceUrl::from(UrlProto::from(url.clone())), url);
        }

        let url: ResourceUrl = "hdfs://namenode:8020/apps/app.jar".parse().unwrap();
        assert_eq!(url.scheme, "hdfs");
        assert_eq!(url.host.as_deref(), Some("namenode"));
        assert_eq!(url.port, Some(8020));
        assert_eq!(url.file, "/apps/app.jar");
        assert!("not a url".parse::<ResourceUrl>().is_err());
    }

    #[test]
    fn test_portless_resource_urls_match_java() {
        // An HA nameservice has no port. Java must get -1, not the proto default 0, or its
        // URL.toPath produces hdfs://nameservice:0/...
        let url: ResourceUrl = "hdfs://nameservice/apps/app.tgz".parse().unwrap();
        assert_eq!(url.port, None);
        assert_eq!(UrlProto::from(url.clone()).port, Some(-1));

        // Java sends -1 for a missing port
        let from_java = ResourceUrl::from(UrlProto {
            scheme: Some("hdfs".to_string()),
            host: Some("nameservice".to_string()),
            port: Some(-1),
            file: Some("/apps/app.tgz".to_string()),
            user_info: None,
        });
        assert_eq!(from_java, url);
        assert_eq!(from_java.to_string(), "hdfs://nameservice/apps/app.tgz");

        // A path on the default filesystem has no host
        let default_fs: ResourceUrl = "hdfs:///apps/app.tgz".parse().unwrap();
        assert_eq!(default_fs.host, None);
        assert_eq!(default_fs.file, "/apps/app.tgz");
    }

    #[test]
    fn test_launch_context_to_proto() {
        let resource = LocalResource::file("hdfs://namenode:8020/app.jar".parse().unwrap(), 10, 20);
        let context = ContainerLaunchContext::default()
            .commands(["java", "-jar", "app.jar"])
            .env("KEY", "value")
            .local_resource("app.jar", resource);

        let proto = ContainerLaunchContextProto::from(context.clone());
        assert_eq!(proto.tokens, None);
        assert_eq!(proto.command, vec!["java", "-jar", "app.jar"]);
        assert_eq!(proto.environment.len(), 1);
        assert_eq!(proto.environment[0].value.as_deref(), Some("value"));
        let local = proto.local_resources[0].value.clone().unwrap();
        assert_eq!(local.size, Some(10));
        assert_eq!(local.r#type(), LocalResourceTypeProto::File);
        assert_eq!(
            local.visibility(),
            LocalResourceVisibilityProto::Application
        );

        let mut credentials = Credentials::new();
        credentials.add_token(
            "nn:8020",
            Token::new(
                b"id".to_vec(),
                b"pw".to_vec(),
                "HDFS_DELEGATION_TOKEN",
                "nn:8020",
            ),
        );
        let proto = ContainerLaunchContextProto::from(context.credentials(credentials.clone()));
        let tokens = proto.tokens.expect("credentials are sent");
        assert_eq!(&tokens[..5], b"HDTS\x00");
        assert_eq!(Credentials::from_bytes(&tokens).unwrap(), credentials);
    }

    #[test]
    fn test_submission_context_defaults() {
        let proto = ApplicationSubmissionContextProto::from(ApplicationSubmissionContext::new(
            ApplicationId::new(1, 2),
            "app",
        ));
        assert_eq!(proto.queue.as_deref(), Some("default"));
        assert_eq!(proto.application_type.as_deref(), Some("YARN"));
        assert_eq!(proto.unmanaged_am, Some(false));
        assert_eq!(proto.priority, None);
        assert_eq!(proto.resource.unwrap().memory, Some(1024));
    }

    #[test]
    fn test_resource_request_defaults_to_any_node() {
        let proto = ResourceRequestProto::from(ResourceRequest::new(1, Resource::new(512, 1), 3));
        assert_eq!(proto.resource_name.as_deref(), Some(ANY_RESOURCE));
        assert_eq!(proto.num_containers, Some(3));
        assert_eq!(proto.relax_locality, Some(true));
        assert_eq!(proto.priority.unwrap().priority, Some(1));
    }
}
