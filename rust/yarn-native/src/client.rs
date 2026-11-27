use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use hadoop_native::security::user::Token;
use hadoop_native::security::{ClientAuth, KerberosCredentials};
use tokio::runtime::{Handle, Runtime};

use crate::config::{self, Configuration};
use crate::ids::{ApplicationAttemptId, ApplicationId, ContainerId};
use crate::proto::yarn::{
    ApplicationSubmissionContextProto, GetApplicationsRequestProto, NodeStateProto,
    SignalContainerCommandProto, YarnApplicationStateProto,
};
use crate::proxy::ServiceProxy;
use crate::records::{
    ApplicationAttemptReport, ApplicationReport, ApplicationState, ApplicationSubmissionContext,
    ClusterMetrics, ContainerReport, NodeReport, NodeState, QueueInfo, Resource,
    SignalContainerCommand,
};
use crate::resourcemanager::client_protocol::{self, ApplicationClientProtocol};
use crate::{Result, YarnError};

/// Holds either a [Runtime] or a [Handle] to an existing runtime for IO tasks
#[derive(Debug)]
pub enum IORuntime {
    Runtime(Runtime),
    Handle(Handle),
}

impl From<Runtime> for IORuntime {
    fn from(value: Runtime) -> Self {
        Self::Runtime(value)
    }
}

impl From<Handle> for IORuntime {
    fn from(value: Handle) -> Self {
        Self::Handle(value)
    }
}

impl IORuntime {
    fn handle(&self) -> Handle {
        match self {
            Self::Runtime(runtime) => runtime.handle().clone(),
            Self::Handle(handle) => handle.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeHolder {
    Custom(Arc<IORuntime>),
    Default(Arc<OnceLock<Runtime>>),
}

impl RuntimeHolder {
    fn new(rt: Option<IORuntime>) -> Self {
        if let Some(rt) = rt {
            Self::Custom(Arc::new(rt))
        } else {
            Self::Default(Arc::new(OnceLock::new()))
        }
    }

    pub(crate) fn get_handle(&self) -> Handle {
        match self {
            Self::Custom(rt) => rt.handle().clone(),
            Self::Default(rt) => match Handle::try_current() {
                Ok(handle) => handle,
                Err(_) => rt
                    .get_or_init(|| Runtime::new().expect("Failed to create tokio runtime"))
                    .handle()
                    .clone(),
            },
        }
    }
}

/// Settings shared by the [`YarnClientBuilder`], [`AmRmClientBuilder`](crate::AmRmClientBuilder)
/// and [`NmClientBuilder`](crate::NmClientBuilder).
#[derive(Default)]
pub(crate) struct ClientOptions {
    pub(crate) config: Option<HashMap<String, String>>,
    pub(crate) config_dir: Option<String>,
    pub(crate) runtime: Option<IORuntime>,
    pub(crate) user: Option<String>,
    pub(crate) kerberos_principal: Option<String>,
    pub(crate) kerberos_keytab: Option<String>,
    pub(crate) kerberos_cache: Option<String>,
}

/// What every client needs to open connections, built from [`ClientOptions`].
#[derive(Clone, Debug)]
pub(crate) struct ClientContext {
    pub(crate) config: Arc<Configuration>,
    pub(crate) rt_holder: RuntimeHolder,
    pub(crate) user: Option<String>,
    pub(crate) auth: Option<Arc<ClientAuth>>,
}

impl ClientOptions {
    pub(crate) fn build_context(mut self) -> Result<ClientContext> {
        let config = Configuration::new(self.config_dir.take(), self.config.take())?;
        self.build_context_with_config(config)
    }

    pub(crate) async fn build_context_async(mut self) -> Result<ClientContext> {
        let config = Configuration::new_async(self.config_dir.take(), self.config.take()).await?;
        self.build_context_with_config(config)
    }

    fn build_context_with_config(self, config: Configuration) -> Result<ClientContext> {
        let auth = KerberosCredentials::new(
            self.kerberos_principal,
            self.kerberos_keytab,
            self.kerberos_cache,
        )?
        .map(ClientAuth::new);

        Ok(ClientContext {
            config: Arc::new(config),
            rt_holder: RuntimeHolder::new(self.runtime),
            user: self.user,
            auth,
        })
    }
}

/// Adds the settings in [`ClientOptions`] to a builder with an `options` field.
macro_rules! client_options_methods {
    () => {
        /// Set configs to use for the client. The provided configs will override any found in the config files loaded
        pub fn with_config(
            mut self,
            config: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        ) -> Self {
            self.options.config = Some(
                config
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
            );
            self
        }

        /// Set the configration directory path to read from. The provided path will override the one provided by environment variable.
        pub fn with_config_dir(mut self, config_dir: impl Into<String>) -> Self {
            self.options.config_dir = Some(config_dir.into());
            self
        }

        /// Use a dedicated tokio runtime for spawned tasks and IO operations. Can either take ownership of a whole [Runtime](tokio::runtime::Runtime)
        /// or take a [Handle](tokio::runtime::Handle) to an externally owned runtime.
        pub fn with_io_runtime(mut self, runtime: impl Into<$crate::client::IORuntime>) -> Self {
            self.options.runtime = Some(runtime.into());
            self
        }

        /// Set the effective user for the client. If not set, the client will detect user from environment variables `HADOOP_USER_NAME` or `HADOOP_PROXY_USER`.
        pub fn with_user(mut self, user: impl Into<String>) -> Self {
            self.options.user = Some(user.into());
            self
        }

        /// Set the Kerberos principal used by this client.
        pub fn with_kerberos_principal(mut self, principal: impl Into<String>) -> Self {
            self.options.kerberos_principal = Some(principal.into());
            self
        }

        /// Set the Kerberos keytab used by this client.
        pub fn with_kerberos_keytab(mut self, keytab: impl Into<String>) -> Self {
            self.options.kerberos_keytab = Some(keytab.into());
            self
        }

        /// Set the Kerberos credential cache used by this client.
        pub fn with_kerberos_cache(mut self, cache: impl Into<String>) -> Self {
            self.options.kerberos_cache = Some(cache.into());
            self
        }
    };
}
pub(crate) use client_options_methods;

/// Builds a new [YarnClient] instance. By default, configs will be loaded from the default config directories with the following precedence:
/// - If `HADOOP_CONF_DIR` environment variable is defined, configs will be loaded from `${HADOOP_CONF_DIR}/{core,yarn}-site.xml`
/// - If `HADOOP_HOME` environment variable is defined, configs will be loaded from `${HADOOP_HOME}/etc/hadoop/{core,yarn}-site.xml`
/// - Otherwise no default configs are defined
///
/// The ResourceManager to connect to comes from `yarn.resourcemanager.address`, or from
/// `yarn.resourcemanager.ha.rm-ids` when `yarn.resourcemanager.ha.enabled` is true.
///
/// # Examples
///
/// Create a client to the ResourceManager in the default configs
/// ```rust
/// # use yarn_native::YarnClientBuilder;
/// let client = YarnClientBuilder::new().build().unwrap();
/// ```
///
/// Create a client to a specific ResourceManager
/// ```rust
/// # use yarn_native::YarnClientBuilder;
/// let client = YarnClientBuilder::new()
///     .with_config(vec![("yarn.resourcemanager.address", "rm.example.com:8032")])
///     .build()
///     .unwrap();
/// ```
#[derive(Default)]
pub struct YarnClientBuilder {
    options: ClientOptions,
}

impl YarnClientBuilder {
    /// Create a new [YarnClientBuilder]
    pub fn new() -> Self {
        Self::default()
    }

    client_options_methods!();

    /// Synchronously create the [YarnClient] instance from the provided settings.
    ///
    /// Use [`Self::build_async`] from async code when Hadoop XML config files may be loaded from
    /// disk.
    pub fn build(self) -> Result<YarnClient> {
        YarnClient::build(self.options.build_context()?)
    }

    /// Asynchronously create a [YarnClient] instance, loading XML config files without blocking a
    /// Tokio worker thread.
    pub async fn build_async(self) -> Result<YarnClient> {
        YarnClient::build(self.options.build_context_async().await?)
    }
}

/// A newly reserved application ID, from [`YarnClient::create_application`].
#[derive(Debug, Clone)]
pub struct NewApplication {
    pub application_id: ApplicationId,
    /// The most resources the cluster grants a single container.
    pub maximum_resource_capability: Resource,
}

/// Filters for [`YarnClient::get_applications`]. Empty fields match everything.
#[derive(Debug, Clone, Default)]
pub struct ApplicationFilter {
    pub states: Vec<ApplicationState>,
    pub application_types: Vec<String>,
    pub queues: Vec<String>,
    pub users: Vec<String>,
    pub tags: Vec<String>,
    pub limit: Option<i64>,
}

impl From<&ApplicationFilter> for GetApplicationsRequestProto {
    fn from(value: &ApplicationFilter) -> Self {
        Self {
            application_states: value
                .states
                .iter()
                .map(|state| YarnApplicationStateProto::from(*state) as i32)
                .collect(),
            application_types: value.application_types.clone(),
            queues: value.queues.clone(),
            users: value.users.clone(),
            application_tags: value.tags.clone(),
            limit: value.limit,
            ..Default::default()
        }
    }
}

/// A client to the ResourceManager of a YARN cluster, for submitting, monitoring and managing
/// applications.
#[derive(Clone, Debug)]
pub struct YarnClient {
    protocol: Arc<ApplicationClientProtocol>,
    config: Arc<Configuration>,
    // Store the runtime used for spawning all internal tasks. If we are not created
    // inside a tokio runtime, we will create our own to use.
    #[allow(dead_code)]
    rt_holder: RuntimeHolder,
}

impl YarnClient {
    fn build(context: ClientContext) -> Result<Self> {
        let addresses = context
            .config
            .rm_addresses(config::RM_ADDRESS, config::DEFAULT_RM_PORT)?;
        let token_service = context.config.rm_token_service(&addresses);
        let proxy = ServiceProxy::new(
            client_protocol::PROTOCOL,
            addresses,
            token_service,
            None,
            context.config.rm_failover_settings(),
            Arc::clone(&context.config),
            context.rt_holder.get_handle(),
            context.user,
            context.auth,
        )?;

        Ok(Self {
            protocol: Arc::new(ApplicationClientProtocol::new(proxy)),
            config: context.config,
            rt_holder: context.rt_holder,
        })
    }

    /// Reserves a new application ID to submit an application with.
    pub async fn create_application(&self) -> Result<NewApplication> {
        let response = self.protocol.get_new_application().await?;
        let application_id = response.application_id.ok_or_else(|| {
            YarnError::InternalError("getNewApplication returned no application ID".to_string())
        })?;
        Ok(NewApplication {
            application_id: application_id.into(),
            maximum_resource_capability: response.maximum_capability.unwrap_or_default().into(),
        })
    }

    /// Submits an application, then waits until the ResourceManager has accepted it, like
    /// Hadoop's `YarnClient.submitApplication`.
    ///
    /// Waiting polls every `yarn.client.application-client-protocol.poll-interval-ms` and gives
    /// up after `yarn.client.application-client-protocol.poll-timeout-ms`, which defaults to
    /// waiting forever.
    pub async fn submit_application(
        &self,
        context: ApplicationSubmissionContext,
    ) -> Result<ApplicationId> {
        let application_id = context.application_id;
        let context = ApplicationSubmissionContextProto::from(context);
        self.protocol.submit_application(context.clone()).await?;

        let start = Instant::now();
        loop {
            match self.get_application_report(application_id).await {
                Ok(report) => match report.state {
                    ApplicationState::New
                    | ApplicationState::NewSaving
                    | ApplicationState::Submitted => (),
                    ApplicationState::Failed | ApplicationState::Killed => {
                        return Err(YarnError::OperationFailed(format!(
                            "Failed to submit {application_id} to YARN: {}",
                            report.diagnostics
                        )));
                    }
                    _ => return Ok(application_id),
                },
                // A ResourceManager failover can lose an application that was not saved yet
                Err(YarnError::ApplicationNotFound(_)) => {
                    log::info!("Re-submitting {application_id} after the ResourceManager lost it");
                    self.protocol.submit_application(context.clone()).await?;
                }
                Err(error) => return Err(error),
            }

            self.wait_for_poll(start, || {
                format!("Timed out waiting for {application_id} to be submitted")
            })
            .await?;
        }
    }

    async fn wait_for_poll(&self, start: Instant, message: impl FnOnce() -> String) -> Result<()> {
        if let Some(timeout) = self.config.poll_timeout()
            && start.elapsed() >= timeout
        {
            return Err(YarnError::OperationFailed(message()));
        }
        tokio::time::sleep(self.config.poll_interval()).await;
        Ok(())
    }

    pub async fn get_application_report(
        &self,
        application_id: ApplicationId,
    ) -> Result<ApplicationReport> {
        self.protocol
            .get_application_report(application_id.into())
            .await?
            .application_report
            .map(Into::into)
            .ok_or_else(|| {
                YarnError::InternalError(format!(
                    "getApplicationReport returned no report for {application_id}"
                ))
            })
    }

    pub async fn get_applications(
        &self,
        filter: &ApplicationFilter,
    ) -> Result<Vec<ApplicationReport>> {
        Ok(self
            .protocol
            .get_applications(filter.into())
            .await?
            .applications
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Kills an application and waits until the ResourceManager reports the kill complete,
    /// like Hadoop's `YarnClient.killApplication`.
    pub async fn kill_application(
        &self,
        application_id: ApplicationId,
        diagnostics: Option<&str>,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            let response = self
                .protocol
                .force_kill_application(application_id.into(), diagnostics.map(str::to_string))
                .await?;
            if response.is_kill_completed() {
                return Ok(());
            }
            self.wait_for_poll(start, || {
                format!("Timed out waiting for {application_id} to be killed")
            })
            .await?;
        }
    }

    /// The AMRMToken of an unmanaged application's current attempt, which the submitter needs
    /// to run the ApplicationMaster with an [`AmRmClient`](crate::AmRmClient). It is only
    /// available once the attempt is launched, and only to the user that submitted the
    /// application.
    pub async fn get_am_rm_token(&self, application_id: ApplicationId) -> Result<Option<Token>> {
        Ok(self
            .protocol
            .get_application_report(application_id.into())
            .await?
            .application_report
            .and_then(|report| report.am_rm_token)
            .map(Token::from))
    }

    pub async fn get_application_attempts(
        &self,
        application_id: ApplicationId,
    ) -> Result<Vec<ApplicationAttemptReport>> {
        Ok(self
            .protocol
            .get_application_attempts(application_id.into())
            .await?
            .application_attempts
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn get_application_attempt_report(
        &self,
        attempt_id: ApplicationAttemptId,
    ) -> Result<ApplicationAttemptReport> {
        self.protocol
            .get_application_attempt_report(attempt_id.into())
            .await?
            .application_attempt_report
            .map(Into::into)
            .ok_or_else(|| {
                YarnError::InternalError(format!(
                    "getApplicationAttemptReport returned no report for {attempt_id}"
                ))
            })
    }

    /// Fails an application attempt, so the ResourceManager starts another one if the
    /// application has attempts left.
    pub async fn fail_application_attempt(&self, attempt_id: ApplicationAttemptId) -> Result<()> {
        self.protocol
            .fail_application_attempt(attempt_id.into())
            .await?;
        Ok(())
    }

    pub async fn get_containers(
        &self,
        attempt_id: ApplicationAttemptId,
    ) -> Result<Vec<ContainerReport>> {
        Ok(self
            .protocol
            .get_containers(attempt_id.into())
            .await?
            .containers
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn get_container_report(&self, container_id: ContainerId) -> Result<ContainerReport> {
        self.protocol
            .get_container_report(container_id.into())
            .await?
            .container_report
            .map(Into::into)
            .ok_or_else(|| {
                YarnError::InternalError(format!(
                    "getContainerReport returned no report for {container_id}"
                ))
            })
    }

    pub async fn signal_container(
        &self,
        container_id: ContainerId,
        command: SignalContainerCommand,
    ) -> Result<()> {
        self.protocol
            .signal_to_container(
                container_id.into(),
                SignalContainerCommandProto::from(command),
            )
            .await?;
        Ok(())
    }

    pub async fn move_application_across_queues(
        &self,
        application_id: ApplicationId,
        queue: &str,
    ) -> Result<()> {
        self.protocol
            .move_application_across_queues(application_id.into(), queue.to_string())
            .await?;
        Ok(())
    }

    /// Changes an application's priority, returning the priority the ResourceManager applied,
    /// which may be capped by the queue's maximum.
    pub async fn update_application_priority(
        &self,
        application_id: ApplicationId,
        priority: i32,
    ) -> Result<i32> {
        Ok(self
            .protocol
            .update_application_priority(application_id.into(), priority)
            .await?
            .application_priority
            .map(|priority| priority.priority())
            .unwrap_or(priority))
    }

    pub async fn get_cluster_metrics(&self) -> Result<ClusterMetrics> {
        Ok(self
            .protocol
            .get_cluster_metrics()
            .await?
            .cluster_metrics
            .unwrap_or_default()
            .into())
    }

    /// Reports on the cluster's NodeManagers in the given states, or in every state when
    /// `states` is empty.
    pub async fn get_nodes(&self, states: &[NodeState]) -> Result<Vec<NodeReport>> {
        Ok(self
            .protocol
            .get_cluster_nodes(
                states
                    .iter()
                    .map(|state| NodeStateProto::from(*state))
                    .collect(),
            )
            .await?
            .node_reports
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Information about a queue and, recursively, its child queues.
    pub async fn get_queue_info(&self, queue: &str) -> Result<QueueInfo> {
        self.protocol
            .get_queue_info(queue.to_string(), false, true, true)
            .await?
            .queue_info
            .map(Into::into)
            .ok_or_else(|| YarnError::InvalidArgument(format!("Unknown queue {queue}")))
    }

    /// Gets a ResourceManager delegation token that `renewer` may renew. The token's service
    /// is set to this client's ResourceManager addresses, so other clients find it in a
    /// token file.
    pub async fn get_delegation_token(&self, renewer: &str) -> Result<Token> {
        let token = self
            .protocol
            .get_delegation_token(renewer.to_string())
            .await?
            .token
            .ok_or_else(|| {
                YarnError::InternalError("getDelegationToken returned no token".to_string())
            })?;
        let mut token = Token::from(token);
        token.set_service(self.protocol.token_service());
        Ok(token)
    }

    /// The renewer to name in delegation tokens given to an application, such as HDFS tokens
    /// in its ApplicationMaster's credentials: the ResourceManager's Kerberos principal, from
    /// `yarn.resourcemanager.principal`. The ResourceManager renews these tokens while the
    /// application runs, and rejects a submission with tokens it cannot renew.
    pub fn delegation_token_renewer(&self) -> Result<String> {
        self.config.rm_principal()
    }

    /// Extend the lifetime of a ResourceManager delegation token, returning its new expiry
    /// time in milliseconds since the epoch. Only the token's renewer may renew it, and never
    /// beyond its maximum lifetime.
    pub async fn renew_delegation_token(&self, token: &Token) -> Result<u64> {
        Ok(self
            .protocol
            .renew_delegation_token(token.to_proto())
            .await?
            .new_expiry_time)
    }

    /// Cancel a ResourceManager delegation token so it can no longer be used.
    pub async fn cancel_delegation_token(&self, token: &Token) -> Result<()> {
        self.protocol
            .cancel_delegation_token(token.to_proto())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_builder_reads_rm_addresses() {
        let client = YarnClientBuilder::new()
            .with_config([
                ("yarn.resourcemanager.address", "127.0.0.1:18032"),
                ("hadoop.security.token.service.use_ip", "false"),
            ])
            .build()
            .unwrap();
        assert_eq!(client.protocol.token_service(), "127.0.0.1:18032");

        let invalid = YarnClientBuilder::new()
            .with_config([("yarn.resourcemanager.ha.enabled", "true")])
            .build();
        assert!(matches!(invalid, Err(YarnError::InvalidArgument(_))));
    }

    #[test]
    fn test_application_filter_to_proto() {
        let filter = ApplicationFilter {
            states: vec![ApplicationState::Running, ApplicationState::Accepted],
            queues: vec!["default".to_string()],
            limit: Some(10),
            ..Default::default()
        };
        let proto = GetApplicationsRequestProto::from(&filter);
        assert_eq!(
            proto.application_states,
            vec![
                YarnApplicationStateProto::Running as i32,
                YarnApplicationStateProto::Accepted as i32
            ]
        );
        assert_eq!(proto.queues, vec!["default"]);
        assert_eq!(proto.limit, Some(10));
    }
}
