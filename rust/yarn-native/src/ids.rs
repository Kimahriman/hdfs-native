//! YARN identifiers, with string forms matching Hadoop's `ApplicationId`,
//! `ApplicationAttemptId`, `ContainerId` and `NodeId`.

use std::fmt;
use std::str::FromStr;

use crate::YarnError;
use crate::proto::yarn::{
    ApplicationAttemptIdProto, ApplicationIdProto, ContainerIdProto, NodeIdProto,
};

const APPLICATION_PREFIX: &str = "application";
const ATTEMPT_PREFIX: &str = "appattempt";
const CONTAINER_PREFIX: &str = "container";
const EPOCH_PREFIX: char = 'e';
/// The low 40 bits of a container ID hold its sequence number; the rest hold the RM epoch.
const CONTAINER_ID_BITMASK: i64 = 0xff_ffff_ffff;

fn invalid(kind: &str, value: &str) -> YarnError {
    YarnError::InvalidArgument(format!("invalid {kind} {value:?}"))
}

fn parse_number<T: FromStr>(kind: &str, value: &str, part: Option<&str>) -> Result<T, YarnError> {
    part.filter(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|part| part.parse().ok())
        .ok_or_else(|| invalid(kind, value))
}

/// Identifies an application, such as `application_1700000000000_0042`.
///
/// ```
/// use yarn_native::ids::ApplicationId;
///
/// let id: ApplicationId = "application_1700000000000_0042".parse().unwrap();
/// assert_eq!(id.id, 42);
/// assert_eq!(id.to_string(), "application_1700000000000_0042");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ApplicationId {
    /// Start time of the ResourceManager that issued the ID, in milliseconds since the epoch.
    pub cluster_timestamp: i64,
    /// Sequence number of the application within that ResourceManager.
    pub id: i32,
}

impl ApplicationId {
    pub fn new(cluster_timestamp: i64, id: i32) -> Self {
        Self {
            cluster_timestamp,
            id,
        }
    }

    fn write_parts(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{:04}", self.cluster_timestamp, self.id)
    }
}

impl fmt::Display for ApplicationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{APPLICATION_PREFIX}_")?;
        self.write_parts(f)
    }
}

impl FromStr for ApplicationId {
    type Err = YarnError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        const KIND: &str = "application ID";
        let mut parts = value.split('_');
        if parts.next() != Some(APPLICATION_PREFIX) {
            return Err(invalid(KIND, value));
        }
        let cluster_timestamp = parse_number(KIND, value, parts.next())?;
        let id = parse_number(KIND, value, parts.next())?;
        if parts.next().is_some() {
            return Err(invalid(KIND, value));
        }
        Ok(Self::new(cluster_timestamp, id))
    }
}

impl From<ApplicationIdProto> for ApplicationId {
    fn from(value: ApplicationIdProto) -> Self {
        Self::new(value.cluster_timestamp(), value.id())
    }
}

impl From<ApplicationId> for ApplicationIdProto {
    fn from(value: ApplicationId) -> Self {
        Self {
            id: Some(value.id),
            cluster_timestamp: Some(value.cluster_timestamp),
        }
    }
}

/// Identifies one attempt to run an application's ApplicationMaster, such as
/// `appattempt_1700000000000_0042_000001`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ApplicationAttemptId {
    pub application_id: ApplicationId,
    pub attempt_id: i32,
}

impl ApplicationAttemptId {
    pub fn new(application_id: ApplicationId, attempt_id: i32) -> Self {
        Self {
            application_id,
            attempt_id,
        }
    }
}

impl fmt::Display for ApplicationAttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{ATTEMPT_PREFIX}_")?;
        self.application_id.write_parts(f)?;
        write!(f, "_{:06}", self.attempt_id)
    }
}

impl FromStr for ApplicationAttemptId {
    type Err = YarnError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        const KIND: &str = "application attempt ID";
        let mut parts = value.split('_');
        if parts.next() != Some(ATTEMPT_PREFIX) {
            return Err(invalid(KIND, value));
        }
        let cluster_timestamp = parse_number(KIND, value, parts.next())?;
        let id = parse_number(KIND, value, parts.next())?;
        let attempt_id = parse_number(KIND, value, parts.next())?;
        if parts.next().is_some() {
            return Err(invalid(KIND, value));
        }
        Ok(Self::new(
            ApplicationId::new(cluster_timestamp, id),
            attempt_id,
        ))
    }
}

impl From<ApplicationAttemptIdProto> for ApplicationAttemptId {
    fn from(value: ApplicationAttemptIdProto) -> Self {
        Self::new(
            value.application_id.unwrap_or_default().into(),
            value.attempt_id(),
        )
    }
}

impl From<ApplicationAttemptId> for ApplicationAttemptIdProto {
    fn from(value: ApplicationAttemptId) -> Self {
        Self {
            application_id: Some(value.application_id.into()),
            attempt_id: Some(value.attempt_id),
        }
    }
}

/// Identifies a container, such as `container_e03_1700000000000_0042_01_000007`.
///
/// ```
/// use yarn_native::ids::ContainerId;
///
/// let id: ContainerId = "container_e03_1700000000000_0042_01_000007".parse().unwrap();
/// assert_eq!(id.epoch(), 3);
/// assert_eq!(id.sequence(), 7);
/// assert_eq!(id.to_string(), "container_e03_1700000000000_0042_01_000007");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContainerId {
    pub application_attempt_id: ApplicationAttemptId,
    /// The ResourceManager epoch in the high bits and the sequence number in the low 40 bits.
    pub id: i64,
}

impl ContainerId {
    pub fn new(application_attempt_id: ApplicationAttemptId, epoch: i64, sequence: i64) -> Self {
        Self {
            application_attempt_id,
            id: (epoch << 40) | (sequence & CONTAINER_ID_BITMASK),
        }
    }

    /// The ResourceManager epoch the container was allocated in.
    pub fn epoch(&self) -> i64 {
        self.id >> 40
    }

    /// The container's sequence number within its application attempt.
    pub fn sequence(&self) -> i64 {
        self.id & CONTAINER_ID_BITMASK
    }
}

impl fmt::Display for ContainerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{CONTAINER_PREFIX}_")?;
        let epoch = self.epoch();
        if epoch > 0 {
            write!(f, "{EPOCH_PREFIX}{epoch:02}_")?;
        }
        self.application_attempt_id.application_id.write_parts(f)?;
        write!(
            f,
            "_{:02}_{:06}",
            self.application_attempt_id.attempt_id,
            self.sequence()
        )
    }
}

impl FromStr for ContainerId {
    type Err = YarnError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        const KIND: &str = "container ID";
        let mut parts = value.split('_').peekable();
        if parts.next() != Some(CONTAINER_PREFIX) {
            return Err(invalid(KIND, value));
        }
        let epoch = match parts.peek() {
            Some(part) if part.starts_with(EPOCH_PREFIX) => {
                let epoch = parts.next().map(|part| &part[1..]);
                parse_number(KIND, value, epoch)?
            }
            _ => 0,
        };
        let cluster_timestamp = parse_number(KIND, value, parts.next())?;
        let id = parse_number(KIND, value, parts.next())?;
        let attempt_id = parse_number(KIND, value, parts.next())?;
        let sequence: i64 = parse_number(KIND, value, parts.next())?;
        if parts.next().is_some() || sequence > CONTAINER_ID_BITMASK {
            return Err(invalid(KIND, value));
        }
        Ok(Self::new(
            ApplicationAttemptId::new(ApplicationId::new(cluster_timestamp, id), attempt_id),
            epoch,
            sequence,
        ))
    }
}

impl From<ContainerIdProto> for ContainerId {
    fn from(value: ContainerIdProto) -> Self {
        let application_attempt_id = match (value.app_attempt_id, value.app_id) {
            (Some(attempt), _) => attempt.into(),
            (None, application_id) => {
                ApplicationAttemptId::new(application_id.unwrap_or_default().into(), 0)
            }
        };
        Self {
            application_attempt_id,
            id: value.id.unwrap_or_default(),
        }
    }
}

impl From<ContainerId> for ContainerIdProto {
    fn from(value: ContainerId) -> Self {
        Self {
            app_id: Some(value.application_attempt_id.application_id.into()),
            app_attempt_id: Some(value.application_attempt_id.into()),
            id: Some(value.id),
        }
    }
}

/// Identifies a NodeManager by the host and port of its RPC server, such as
/// `nm.example.com:45454`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId {
    pub host: String,
    pub port: i32,
}

impl NodeId {
    pub fn new(host: impl Into<String>, port: i32) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for NodeId {
    type Err = YarnError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (host, port) = value
            .rsplit_once(':')
            .filter(|(host, _)| !host.is_empty())
            .ok_or_else(|| invalid("node ID", value))?;
        Ok(Self::new(host, parse_number("node ID", value, Some(port))?))
    }
}

impl From<NodeIdProto> for NodeId {
    fn from(value: NodeIdProto) -> Self {
        Self::new(value.host(), value.port())
    }
}

impl From<NodeId> for NodeIdProto {
    fn from(value: NodeId) -> Self {
        Self {
            host: Some(value.host),
            port: Some(value.port),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_application_ids_round_trip() {
        let id: ApplicationId = "application_1410901177871_0001".parse().unwrap();
        assert_eq!(id, ApplicationId::new(1410901177871, 1));
        assert_eq!(id.to_string(), "application_1410901177871_0001");
        // Ids beyond four digits are not truncated
        assert_eq!(
            ApplicationId::new(1, 123456).to_string(),
            "application_1_123456"
        );
        assert_eq!(ApplicationId::from(ApplicationIdProto::from(id)), id);

        for invalid in [
            "application_1",
            "application_x_1",
            "app_1_1",
            "application_1_1_1",
            "application_1_-1",
        ] {
            assert!(invalid.parse::<ApplicationId>().is_err(), "{invalid}");
        }
    }

    #[test]
    fn test_attempt_ids_round_trip() {
        let id: ApplicationAttemptId = "appattempt_1410901177871_0001_000002".parse().unwrap();
        assert_eq!(id.attempt_id, 2);
        assert_eq!(id.application_id, ApplicationId::new(1410901177871, 1));
        assert_eq!(id.to_string(), "appattempt_1410901177871_0001_000002");
        assert_eq!(
            ApplicationAttemptId::from(ApplicationAttemptIdProto::from(id)),
            id
        );
    }

    #[test]
    fn test_container_ids_round_trip_with_and_without_epochs() {
        for value in [
            "container_1410901177871_0001_01_000005",
            "container_e17_1410901177871_0001_01_000005",
            "container_e100_1410901177871_0001_01_000005",
        ] {
            let id: ContainerId = value.parse().unwrap();
            assert_eq!(id.to_string(), value);
            assert_eq!(ContainerId::from(ContainerIdProto::from(id)), id);
        }

        let id: ContainerId = "container_e17_1410901177871_0001_01_000005"
            .parse()
            .unwrap();
        assert_eq!(id.epoch(), 17);
        assert_eq!(id.sequence(), 5);
        assert_eq!(id.id, (17 << 40) | 5);

        assert!("container_e_1_1_1_1".parse::<ContainerId>().is_err());
        assert!("container_1_1_1".parse::<ContainerId>().is_err());
    }

    #[test]
    fn test_node_ids_round_trip() {
        let id: NodeId = "nm.example.com:45454".parse().unwrap();
        assert_eq!(id, NodeId::new("nm.example.com", 45454));
        assert_eq!(id.to_string(), "nm.example.com:45454");
        assert!(":1".parse::<NodeId>().is_err());
        assert!("host".parse::<NodeId>().is_err());
    }
}
