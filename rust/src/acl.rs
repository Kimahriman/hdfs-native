use crate::{
    proto::hdfs::{
        acl_entry_proto::{AclEntryScopeProto, AclEntryTypeProto, FsActionProto},
        AclEntryProto, GetAclStatusResponseProto,
    },
    HdfsError,
};

pub enum AclEntryType {
    User,
    Group,
    Mask,
    Other,
}

impl From<AclEntryType> for AclEntryTypeProto {
    fn from(value: AclEntryType) -> Self {
        match value {
            AclEntryType::User => AclEntryTypeProto::User,
            AclEntryType::Group => AclEntryTypeProto::Group,
            AclEntryType::Mask => AclEntryTypeProto::Mask,
            AclEntryType::Other => AclEntryTypeProto::Other,
        }
    }
}

impl From<AclEntryTypeProto> for AclEntryType {
    fn from(value: AclEntryTypeProto) -> Self {
        match value {
            AclEntryTypeProto::User => AclEntryType::User,
            AclEntryTypeProto::Group => AclEntryType::Group,
            AclEntryTypeProto::Mask => AclEntryType::Mask,
            AclEntryTypeProto::Other => AclEntryType::Other,
        }
    }
}

impl TryFrom<&str> for AclEntryType {
    type Error = HdfsError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "User" => Ok(AclEntryType::User),
            "Group" => Ok(AclEntryType::Group),
            "Mask" => Ok(AclEntryType::Mask),
            "Other" => Ok(AclEntryType::Other),
            _ => Err(HdfsError::InvalidArgument(format!(
                "Unknown ACL entry type {}",
                value
            ))),
        }
    }
}

impl TryFrom<String> for AclEntryType {
    type Error = HdfsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let v: &str = value.as_ref();
        v.try_into()
    }
}

pub enum AclEntryScope {
    Access,
    Default,
}

impl From<AclEntryScope> for AclEntryScopeProto {
    fn from(value: AclEntryScope) -> Self {
        match value {
            AclEntryScope::Access => AclEntryScopeProto::Access,
            AclEntryScope::Default => AclEntryScopeProto::Default,
        }
    }
}

impl From<AclEntryScopeProto> for AclEntryScope {
    fn from(value: AclEntryScopeProto) -> Self {
        match value {
            AclEntryScopeProto::Access => AclEntryScope::Access,
            AclEntryScopeProto::Default => AclEntryScope::Default,
        }
    }
}

impl TryFrom<&str> for AclEntryScope {
    type Error = HdfsError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Access" => Ok(AclEntryScope::Access),
            "Default" => Ok(AclEntryScope::Default),
            _ => Err(HdfsError::InvalidArgument(format!(
                "Unknown ACL entry scope {}",
                value
            ))),
        }
    }
}

impl TryFrom<String> for AclEntryScope {
    type Error = HdfsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let v: &str = value.as_ref();
        v.try_into()
    }
}

pub enum FsAction {
    None = 0,
    Execute = 1,
    Write = 2,
    WriteExecute = 3,
    Read = 4,
    ReadExecute = 5,
    ReadWrite = 6,
    PermAll = 7,
}

impl From<FsAction> for FsActionProto {
    fn from(value: FsAction) -> Self {
        match value {
            FsAction::None => FsActionProto::None,
            FsAction::Execute => FsActionProto::Execute,
            FsAction::Write => FsActionProto::Write,
            FsAction::WriteExecute => FsActionProto::WriteExecute,
            FsAction::Read => FsActionProto::Read,
            FsAction::ReadExecute => FsActionProto::ReadExecute,
            FsAction::ReadWrite => FsActionProto::ReadWrite,
            FsAction::PermAll => FsActionProto::PermAll,
        }
    }
}

impl From<FsActionProto> for FsAction {
    fn from(value: FsActionProto) -> Self {
        match value {
            FsActionProto::None => FsAction::None,
            FsActionProto::Execute => FsAction::Execute,
            FsActionProto::Write => FsAction::Write,
            FsActionProto::WriteExecute => FsAction::WriteExecute,
            FsActionProto::Read => FsAction::Read,
            FsActionProto::ReadExecute => FsAction::ReadExecute,
            FsActionProto::ReadWrite => FsAction::ReadWrite,
            FsActionProto::PermAll => FsAction::PermAll,
        }
    }
}

impl TryFrom<&str> for FsAction {
    type Error = HdfsError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "---" => Ok(FsAction::None),
            "--x" => Ok(FsAction::Execute),
            "-w-" => Ok(FsAction::Write),
            "-wx" => Ok(FsAction::WriteExecute),
            "r--" => Ok(FsAction::Read),
            "r-x" => Ok(FsAction::ReadExecute),
            "rw-" => Ok(FsAction::ReadWrite),
            "rwx" => Ok(FsAction::PermAll),
            _ => Err(HdfsError::InvalidArgument(format!(
                "Unknown file system permission {}",
                value
            ))),
        }
    }
}

impl TryFrom<String> for FsAction {
    type Error = HdfsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let v: &str = value.as_ref();
        v.try_into()
    }
}

pub struct AclEntry {
    pub r#type: AclEntryType,
    pub scope: AclEntryScope,
    pub permissions: FsAction,
    pub name: Option<String>,
}

impl From<AclEntry> for AclEntryProto {
    fn from(value: AclEntry) -> Self {
        let r#type: AclEntryTypeProto = value.r#type.into();
        let scope: AclEntryScopeProto = value.scope.into();
        let permissions: FsActionProto = value.permissions.into();
        Self {
            r#type: r#type as i32,
            scope: scope as i32,
            permissions: permissions as i32,
            name: value.name,
        }
    }
}

impl FromIterator<AclEntry> for Vec<AclEntryProto> {
    fn from_iter<T: IntoIterator<Item = AclEntry>>(iter: T) -> Self {
        iter.into_iter().map(AclEntryProto::from).collect()
    }
}

impl From<AclEntryProto> for AclEntry {
    fn from(value: AclEntryProto) -> Self {
        Self {
            r#type: value.r#type().into(),
            scope: value.scope().into(),
            permissions: value.permissions().into(),
            name: value.name,
        }
    }
}

impl FromIterator<AclEntryProto> for Vec<AclEntry> {
    fn from_iter<T: IntoIterator<Item = AclEntryProto>>(iter: T) -> Self {
        iter.into_iter().map(AclEntry::from).collect()
    }
}

pub struct AclStatus {
    pub owner: String,
    pub group: String,
    pub sticky: bool,
    pub entries: Vec<AclEntry>,
    pub permission: u16,
}

impl From<GetAclStatusResponseProto> for AclStatus {
    fn from(value: GetAclStatusResponseProto) -> Self {
        Self {
            owner: value.result.owner,
            group: value.result.group,
            sticky: value.result.sticky,
            entries: value.result.entries.into_iter().collect(),
            permission: value.result.permission.unwrap().perm as u16,
        }
    }
}
