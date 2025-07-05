use std::fmt::Display;

use crate::proto::hdfs::{
    acl_entry_proto::{AclEntryScopeProto, AclEntryTypeProto, FsActionProto},
    AclEntryProto, AclStatusProto,
};

#[derive(Clone, Debug, PartialEq)]
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

impl From<&str> for AclEntryType {
    fn from(value: &str) -> Self {
        match value.to_ascii_lowercase().as_ref() {
            "user" => AclEntryType::User,
            "group" => AclEntryType::Group,
            "mask" => AclEntryType::Mask,
            "other" => AclEntryType::Other,
            _ => panic!("Unknown ACL entry type {value}"),
        }
    }
}

impl From<String> for AclEntryType {
    fn from(value: String) -> Self {
        Self::from(value.as_ref())
    }
}

impl Display for AclEntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AclEntryType::User => "user",
                AclEntryType::Group => "group",
                AclEntryType::Mask => "mask",
                AclEntryType::Other => "other",
            }
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
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

impl From<&str> for AclEntryScope {
    fn from(value: &str) -> Self {
        match value.to_ascii_lowercase().as_ref() {
            "access" => AclEntryScope::Access,
            "default" => AclEntryScope::Default,
            _ => panic!("Unknown ACL entry scope {value}"),
        }
    }
}

impl From<String> for AclEntryScope {
    fn from(value: String) -> Self {
        Self::from(value.as_ref())
    }
}

impl Display for AclEntryScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AclEntryScope::Access => "access",
                AclEntryScope::Default => "default",
            }
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
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

impl From<&str> for FsAction {
    fn from(value: &str) -> Self {
        match value {
            "---" => FsAction::None,
            "--x" => FsAction::Execute,
            "-w-" => FsAction::Write,
            "-wx" => FsAction::WriteExecute,
            "r--" => FsAction::Read,
            "r-x" => FsAction::ReadExecute,
            "rw-" => FsAction::ReadWrite,
            "rwx" => FsAction::PermAll,
            _ => panic!("Unknown file system permission {value}"),
        }
    }
}

impl From<String> for FsAction {
    fn from(value: String) -> Self {
        Self::from(value.as_ref())
    }
}

impl Display for FsAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                FsAction::None => "---",
                FsAction::Execute => "--x",
                FsAction::Write => "-w-",
                FsAction::WriteExecute => "-wx",
                FsAction::Read => "r--",
                FsAction::ReadExecute => "r-x",
                FsAction::ReadWrite => "rw-",
                FsAction::PermAll => "rwx",
            }
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AclEntry {
    pub r#type: AclEntryType,
    pub scope: AclEntryScope,
    pub permissions: FsAction,
    pub name: Option<String>,
}

impl AclEntry {
    /// Create a new ACL entry.
    pub fn new(
        r#type: impl Into<AclEntryType>,
        scope: impl Into<AclEntryScope>,
        permissions: impl Into<FsAction>,
        name: Option<String>,
    ) -> Self {
        Self {
            r#type: r#type.into(),
            scope: scope.into(),
            permissions: permissions.into(),
            name,
        }
    }
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

impl From<AclStatusProto> for AclStatus {
    fn from(value: AclStatusProto) -> Self {
        Self {
            owner: value.owner,
            group: value.group,
            sticky: value.sticky,
            entries: value.entries.into_iter().collect(),
            permission: value.permission.unwrap().perm as u16,
        }
    }
}
