use crate::proto::hdfs::DatanodeInfoProto;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Policy {
    /// The feature is disabled in the entire site
    Disable,
    /// Never add a new datanode
    Never,
    /// Default policy based on replication conditions
    Default,
    /// Always add a new datanode when an existing datanode is removed
    Always,
}

pub struct ReplaceDatanodeOnFailure {
    policy: Policy,
    best_effort: bool,
}

impl ReplaceDatanodeOnFailure {
    pub fn new(policy: Policy, best_effort: bool) -> Self {
        Self {
            policy,
            best_effort,
        }
    }

    /// Best effort means that the client will try to replace the failed datanode
    /// (provided that the policy is satisfied), however, it will continue the
    /// write operation in case that the datanode replacement also fails.
    pub fn is_best_effort(&self) -> bool {
        self.best_effort
    }

    /// Does it need a replacement according to the policy?
    pub fn should_replace(
        &self,
        replication: u32,
        existing_datanodes: &[DatanodeInfoProto],
        is_append: bool,
        is_hflushed: bool,
    ) -> bool {
        let n = existing_datanodes.len();
        if n == 0 || n >= replication as usize {
            // Don't need to add datanode for any policy
            return false;
        }

        match self.policy {
            Policy::Disable | Policy::Never => false,
            Policy::Always => true,
            Policy::Default => {
                // DEFAULT condition:
                // Let r be the replication number.
                // Let n be the number of existing datanodes.
                // Add a new datanode only if r >= 3 and either
                // (1) floor(r/2) >= n; or
                // (2) r > n and the block is hflushed/appended.
                if replication < 3 {
                    false
                } else if n <= (replication as usize / 2) {
                    true
                } else {
                    is_append || is_hflushed
                }
            }
        }
    }
}

impl Default for ReplaceDatanodeOnFailure {
    fn default() -> Self {
        Self {
            policy: Policy::Default,
            best_effort: true,
        }
    }
}
