use hadoop_native::security::user::Token;

use crate::Result;
use crate::proto::yarn;
use crate::proxy::{Protocol, ServiceProxy};

pub(crate) const PROTOCOL: Protocol = Protocol {
    name: "org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB",
    token_kind: "YARN_AM_RM_TOKEN",
};

/// The ResourceManager's scheduler protocol, used by an ApplicationMaster. The ResourceManager
/// always authenticates these connections with the attempt's AMRMToken, even on clusters
/// without Kerberos.
#[derive(Debug)]
pub(crate) struct ApplicationMasterProtocol {
    proxy: ServiceProxy,
}

impl ApplicationMasterProtocol {
    pub(crate) fn new(proxy: ServiceProxy) -> Self {
        Self { proxy }
    }

    pub(crate) fn token_service(&self) -> &str {
        self.proxy.token_service()
    }

    /// Replaces the AMRMToken used for new connections after the ResourceManager rolls its
    /// master key.
    pub(crate) fn set_token(&self, token: Token) {
        self.proxy.set_token(token)
    }

    pub(crate) fn token(&self) -> Option<Token> {
        self.proxy.token()
    }

    pub(crate) async fn register_application_master(
        &self,
        message: yarn::RegisterApplicationMasterRequestProto,
    ) -> Result<yarn::RegisterApplicationMasterResponseProto> {
        // Registering twice fails, so only retry when the ResourceManager can't have seen it
        self.proxy
            .call("registerApplicationMaster", message, true)
            .await
    }

    pub(crate) async fn allocate(
        &self,
        message: yarn::AllocateRequestProto,
    ) -> Result<yarn::AllocateResponseProto> {
        // The ResourceManager returns its previous response for a repeated response ID, so a
        // lost response can be retried safely
        self.proxy.call("allocate", message, false).await
    }

    pub(crate) async fn finish_application_master(
        &self,
        message: yarn::FinishApplicationMasterRequestProto,
    ) -> Result<yarn::FinishApplicationMasterResponseProto> {
        // Hadoop marks this @AtMostOnce: the ResourceManager recognizes a repeated request,
        // so it can be retried like Hadoop's RMProxy does
        self.proxy
            .call("finishApplicationMaster", message, false)
            .await
    }
}
