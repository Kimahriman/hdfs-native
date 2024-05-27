/// *
/// File or Directory permision - same spec as posix
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FsPermissionProto {
    /// Actually a short - only 16bits used
    #[prost(uint32, required, tag = "1")]
    pub perm: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AclEntryProto {
    #[prost(enumeration = "acl_entry_proto::AclEntryTypeProto", required, tag = "1")]
    pub r#type: i32,
    #[prost(enumeration = "acl_entry_proto::AclEntryScopeProto", required, tag = "2")]
    pub scope: i32,
    #[prost(enumeration = "acl_entry_proto::FsActionProto", required, tag = "3")]
    pub permissions: i32,
    #[prost(string, optional, tag = "4")]
    pub name: ::core::option::Option<::prost::alloc::string::String>,
}
/// Nested message and enum types in `AclEntryProto`.
pub mod acl_entry_proto {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum AclEntryScopeProto {
        Access = 0,
        Default = 1,
    }
    impl AclEntryScopeProto {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                AclEntryScopeProto::Access => "ACCESS",
                AclEntryScopeProto::Default => "DEFAULT",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "ACCESS" => Some(Self::Access),
                "DEFAULT" => Some(Self::Default),
                _ => None,
            }
        }
    }
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum AclEntryTypeProto {
        User = 0,
        Group = 1,
        Mask = 2,
        Other = 3,
    }
    impl AclEntryTypeProto {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                AclEntryTypeProto::User => "USER",
                AclEntryTypeProto::Group => "GROUP",
                AclEntryTypeProto::Mask => "MASK",
                AclEntryTypeProto::Other => "OTHER",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "USER" => Some(Self::User),
                "GROUP" => Some(Self::Group),
                "MASK" => Some(Self::Mask),
                "OTHER" => Some(Self::Other),
                _ => None,
            }
        }
    }
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum FsActionProto {
        None = 0,
        Execute = 1,
        Write = 2,
        WriteExecute = 3,
        Read = 4,
        ReadExecute = 5,
        ReadWrite = 6,
        PermAll = 7,
    }
    impl FsActionProto {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                FsActionProto::None => "NONE",
                FsActionProto::Execute => "EXECUTE",
                FsActionProto::Write => "WRITE",
                FsActionProto::WriteExecute => "WRITE_EXECUTE",
                FsActionProto::Read => "READ",
                FsActionProto::ReadExecute => "READ_EXECUTE",
                FsActionProto::ReadWrite => "READ_WRITE",
                FsActionProto::PermAll => "PERM_ALL",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "NONE" => Some(Self::None),
                "EXECUTE" => Some(Self::Execute),
                "WRITE" => Some(Self::Write),
                "WRITE_EXECUTE" => Some(Self::WriteExecute),
                "READ" => Some(Self::Read),
                "READ_EXECUTE" => Some(Self::ReadExecute),
                "READ_WRITE" => Some(Self::ReadWrite),
                "PERM_ALL" => Some(Self::PermAll),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AclStatusProto {
    #[prost(string, required, tag = "1")]
    pub owner: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub group: ::prost::alloc::string::String,
    #[prost(bool, required, tag = "3")]
    pub sticky: bool,
    #[prost(message, repeated, tag = "4")]
    pub entries: ::prost::alloc::vec::Vec<AclEntryProto>,
    #[prost(message, optional, tag = "5")]
    pub permission: ::core::option::Option<FsPermissionProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyAclEntriesRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub acl_spec: ::prost::alloc::vec::Vec<AclEntryProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyAclEntriesResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveAclRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveAclResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveAclEntriesRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub acl_spec: ::prost::alloc::vec::Vec<AclEntryProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveAclEntriesResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveDefaultAclRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveDefaultAclResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetAclRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub acl_spec: ::prost::alloc::vec::Vec<AclEntryProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetAclResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetAclStatusRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetAclStatusResponseProto {
    #[prost(message, required, tag = "1")]
    pub result: AclStatusProto,
}
/// *
/// Extended block idenfies a block
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtendedBlockProto {
    /// Block pool id - globally unique across clusters
    #[prost(string, required, tag = "1")]
    pub pool_id: ::prost::alloc::string::String,
    /// the local id within a pool
    #[prost(uint64, required, tag = "2")]
    pub block_id: u64,
    #[prost(uint64, required, tag = "3")]
    pub generation_stamp: u64,
    /// len does not belong in ebid
    #[prost(uint64, optional, tag = "4", default = "0")]
    pub num_bytes: ::core::option::Option<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProvidedStorageLocationProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(int64, required, tag = "2")]
    pub offset: i64,
    #[prost(int64, required, tag = "3")]
    pub length: i64,
    #[prost(bytes = "vec", required, tag = "4")]
    pub nonce: ::prost::alloc::vec::Vec<u8>,
}
/// *
/// Identifies a Datanode
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatanodeIdProto {
    /// IP address
    #[prost(string, required, tag = "1")]
    pub ip_addr: ::prost::alloc::string::String,
    /// hostname
    #[prost(string, required, tag = "2")]
    pub host_name: ::prost::alloc::string::String,
    /// UUID assigned to the Datanode. For
    #[prost(string, required, tag = "3")]
    pub datanode_uuid: ::prost::alloc::string::String,
    /// upgraded clusters this is the same
    /// as the original StorageID of the
    /// Datanode.
    ///
    /// data streaming port
    #[prost(uint32, required, tag = "4")]
    pub xfer_port: u32,
    /// datanode http port
    #[prost(uint32, required, tag = "5")]
    pub info_port: u32,
    /// ipc server port
    #[prost(uint32, required, tag = "6")]
    pub ipc_port: u32,
    /// datanode https port
    #[prost(uint32, optional, tag = "7", default = "0")]
    pub info_secure_port: ::core::option::Option<u32>,
}
/// *
/// Datanode local information
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatanodeLocalInfoProto {
    #[prost(string, required, tag = "1")]
    pub software_version: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub config_version: ::prost::alloc::string::String,
    #[prost(uint64, required, tag = "3")]
    pub uptime: u64,
}
/// *
/// Datanode volume information
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatanodeVolumeInfoProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(enumeration = "StorageTypeProto", required, tag = "2")]
    pub storage_type: i32,
    #[prost(uint64, required, tag = "3")]
    pub used_space: u64,
    #[prost(uint64, required, tag = "4")]
    pub free_space: u64,
    #[prost(uint64, required, tag = "5")]
    pub reserved_space: u64,
    #[prost(uint64, required, tag = "6")]
    pub reserved_space_for_replicas: u64,
    #[prost(uint64, required, tag = "7")]
    pub num_blocks: u64,
}
/// *
/// DatanodeInfo array
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatanodeInfosProto {
    #[prost(message, repeated, tag = "1")]
    pub datanodes: ::prost::alloc::vec::Vec<DatanodeInfoProto>,
}
/// *
/// The status of a Datanode
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatanodeInfoProto {
    #[prost(message, required, tag = "1")]
    pub id: DatanodeIdProto,
    #[prost(uint64, optional, tag = "2", default = "0")]
    pub capacity: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "3", default = "0")]
    pub dfs_used: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "4", default = "0")]
    pub remaining: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "5", default = "0")]
    pub block_pool_used: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "6", default = "0")]
    pub last_update: ::core::option::Option<u64>,
    #[prost(uint32, optional, tag = "7", default = "0")]
    pub xceiver_count: ::core::option::Option<u32>,
    #[prost(string, optional, tag = "8")]
    pub location: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, optional, tag = "9")]
    pub non_dfs_used: ::core::option::Option<u64>,
    #[prost(
        enumeration = "datanode_info_proto::AdminState",
        optional,
        tag = "10",
        default = "Normal"
    )]
    pub admin_state: ::core::option::Option<i32>,
    #[prost(uint64, optional, tag = "11", default = "0")]
    pub cache_capacity: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "12", default = "0")]
    pub cache_used: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "13", default = "0")]
    pub last_update_monotonic: ::core::option::Option<u64>,
    #[prost(string, optional, tag = "14")]
    pub upgrade_domain: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, optional, tag = "15", default = "0")]
    pub last_block_report_time: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "16", default = "0")]
    pub last_block_report_monotonic: ::core::option::Option<u64>,
    #[prost(uint32, optional, tag = "17", default = "0")]
    pub num_blocks: ::core::option::Option<u32>,
}
/// Nested message and enum types in `DatanodeInfoProto`.
pub mod datanode_info_proto {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum AdminState {
        Normal = 0,
        DecommissionInprogress = 1,
        Decommissioned = 2,
        EnteringMaintenance = 3,
        InMaintenance = 4,
    }
    impl AdminState {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                AdminState::Normal => "NORMAL",
                AdminState::DecommissionInprogress => "DECOMMISSION_INPROGRESS",
                AdminState::Decommissioned => "DECOMMISSIONED",
                AdminState::EnteringMaintenance => "ENTERING_MAINTENANCE",
                AdminState::InMaintenance => "IN_MAINTENANCE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "NORMAL" => Some(Self::Normal),
                "DECOMMISSION_INPROGRESS" => Some(Self::DecommissionInprogress),
                "DECOMMISSIONED" => Some(Self::Decommissioned),
                "ENTERING_MAINTENANCE" => Some(Self::EnteringMaintenance),
                "IN_MAINTENANCE" => Some(Self::InMaintenance),
                _ => None,
            }
        }
    }
}
/// *
/// Represents a storage available on the datanode
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatanodeStorageProto {
    #[prost(string, required, tag = "1")]
    pub storage_uuid: ::prost::alloc::string::String,
    #[prost(
        enumeration = "datanode_storage_proto::StorageState",
        optional,
        tag = "2",
        default = "Normal"
    )]
    pub state: ::core::option::Option<i32>,
    #[prost(enumeration = "StorageTypeProto", optional, tag = "3", default = "Disk")]
    pub storage_type: ::core::option::Option<i32>,
}
/// Nested message and enum types in `DatanodeStorageProto`.
pub mod datanode_storage_proto {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum StorageState {
        Normal = 0,
        ReadOnlyShared = 1,
    }
    impl StorageState {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                StorageState::Normal => "NORMAL",
                StorageState::ReadOnlyShared => "READ_ONLY_SHARED",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "NORMAL" => Some(Self::Normal),
                "READ_ONLY_SHARED" => Some(Self::ReadOnlyShared),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageReportProto {
    #[deprecated]
    #[prost(string, required, tag = "1")]
    pub storage_uuid: ::prost::alloc::string::String,
    #[prost(bool, optional, tag = "2", default = "false")]
    pub failed: ::core::option::Option<bool>,
    #[prost(uint64, optional, tag = "3", default = "0")]
    pub capacity: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "4", default = "0")]
    pub dfs_used: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "5", default = "0")]
    pub remaining: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "6", default = "0")]
    pub block_pool_used: ::core::option::Option<u64>,
    /// supersedes StorageUuid
    #[prost(message, optional, tag = "7")]
    pub storage: ::core::option::Option<DatanodeStorageProto>,
    #[prost(uint64, optional, tag = "8")]
    pub non_dfs_used: ::core::option::Option<u64>,
    #[prost(string, optional, tag = "9")]
    pub mount: ::core::option::Option<::prost::alloc::string::String>,
}
/// *
/// Summary of a file or directory
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContentSummaryProto {
    #[prost(uint64, required, tag = "1")]
    pub length: u64,
    #[prost(uint64, required, tag = "2")]
    pub file_count: u64,
    #[prost(uint64, required, tag = "3")]
    pub directory_count: u64,
    #[prost(uint64, required, tag = "4")]
    pub quota: u64,
    #[prost(uint64, required, tag = "5")]
    pub space_consumed: u64,
    #[prost(uint64, required, tag = "6")]
    pub space_quota: u64,
    #[prost(message, optional, tag = "7")]
    pub type_quota_infos: ::core::option::Option<StorageTypeQuotaInfosProto>,
    #[prost(uint64, optional, tag = "8")]
    pub snapshot_length: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "9")]
    pub snapshot_file_count: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "10")]
    pub snapshot_directory_count: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "11")]
    pub snapshot_space_consumed: ::core::option::Option<u64>,
    #[prost(string, optional, tag = "12")]
    pub erasure_coding_policy: ::core::option::Option<::prost::alloc::string::String>,
}
/// *
/// Summary of quota usage of a directory
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QuotaUsageProto {
    #[prost(uint64, required, tag = "1")]
    pub file_and_directory_count: u64,
    #[prost(uint64, required, tag = "2")]
    pub quota: u64,
    #[prost(uint64, required, tag = "3")]
    pub space_consumed: u64,
    #[prost(uint64, required, tag = "4")]
    pub space_quota: u64,
    #[prost(message, optional, tag = "5")]
    pub type_quota_infos: ::core::option::Option<StorageTypeQuotaInfosProto>,
}
/// *
/// Storage type quota and usage information of a file or directory
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageTypeQuotaInfosProto {
    #[prost(message, repeated, tag = "1")]
    pub type_quota_info: ::prost::alloc::vec::Vec<StorageTypeQuotaInfoProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageTypeQuotaInfoProto {
    #[prost(enumeration = "StorageTypeProto", optional, tag = "1", default = "Disk")]
    pub r#type: ::core::option::Option<i32>,
    #[prost(uint64, required, tag = "2")]
    pub quota: u64,
    #[prost(uint64, required, tag = "3")]
    pub consumed: u64,
}
/// *
/// Contains a list of paths corresponding to corrupt files and a cookie
/// used for iterative calls to NameNode.listCorruptFileBlocks.
///
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CorruptFileBlocksProto {
    #[prost(string, repeated, tag = "1")]
    pub files: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, required, tag = "2")]
    pub cookie: ::prost::alloc::string::String,
}
/// *
/// A list of storage types.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageTypesProto {
    #[prost(enumeration = "StorageTypeProto", repeated, packed = "false", tag = "1")]
    pub storage_types: ::prost::alloc::vec::Vec<i32>,
}
/// *
/// Block replica storage policy.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockStoragePolicyProto {
    #[prost(uint32, required, tag = "1")]
    pub policy_id: u32,
    #[prost(string, required, tag = "2")]
    pub name: ::prost::alloc::string::String,
    /// a list of storage types for storing the block replicas when creating a
    /// block.
    #[prost(message, required, tag = "3")]
    pub creation_policy: StorageTypesProto,
    /// A list of storage types for creation fallback storage.
    #[prost(message, optional, tag = "4")]
    pub creation_fallback_policy: ::core::option::Option<StorageTypesProto>,
    #[prost(message, optional, tag = "5")]
    pub replication_fallback_policy: ::core::option::Option<StorageTypesProto>,
}
/// *
/// A LocatedBlock gives information about a block and its location.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocatedBlockProto {
    #[prost(message, required, tag = "1")]
    pub b: ExtendedBlockProto,
    /// offset of first byte of block in the file
    #[prost(uint64, required, tag = "2")]
    pub offset: u64,
    /// Locations ordered by proximity to client ip
    #[prost(message, repeated, tag = "3")]
    pub locs: ::prost::alloc::vec::Vec<DatanodeInfoProto>,
    /// true if all replicas of a block are corrupt, else false
    #[prost(bool, required, tag = "4")]
    pub corrupt: bool,
    #[prost(message, required, tag = "5")]
    pub block_token: super::common::TokenProto,
    /// if a location in locs is cached
    #[prost(bool, repeated, tag = "6")]
    pub is_cached: ::prost::alloc::vec::Vec<bool>,
    #[prost(enumeration = "StorageTypeProto", repeated, packed = "false", tag = "7")]
    pub storage_types: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, repeated, tag = "8")]
    pub storage_i_ds: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// striped block related fields
    ///
    /// used for striped block to indicate block index for each storage
    #[prost(bytes = "vec", optional, tag = "9")]
    pub block_indices: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    /// each internal block has a block token
    #[prost(message, repeated, tag = "10")]
    pub block_tokens: ::prost::alloc::vec::Vec<super::common::TokenProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchedListingKeyProto {
    #[prost(bytes = "vec", required, tag = "1")]
    pub checksum: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, required, tag = "2")]
    pub path_index: u32,
    #[prost(bytes = "vec", required, tag = "3")]
    pub start_after: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataEncryptionKeyProto {
    #[prost(uint32, required, tag = "1")]
    pub key_id: u32,
    #[prost(string, required, tag = "2")]
    pub block_pool_id: ::prost::alloc::string::String,
    #[prost(bytes = "vec", required, tag = "3")]
    pub nonce: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", required, tag = "4")]
    pub encryption_key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, required, tag = "5")]
    pub expiry_date: u64,
    #[prost(string, optional, tag = "6")]
    pub encryption_algorithm: ::core::option::Option<::prost::alloc::string::String>,
}
/// *
/// Encryption information for a file.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileEncryptionInfoProto {
    #[prost(enumeration = "CipherSuiteProto", required, tag = "1")]
    pub suite: i32,
    #[prost(enumeration = "CryptoProtocolVersionProto", required, tag = "2")]
    pub crypto_protocol_version: i32,
    #[prost(bytes = "vec", required, tag = "3")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", required, tag = "4")]
    pub iv: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, required, tag = "5")]
    pub key_name: ::prost::alloc::string::String,
    #[prost(string, required, tag = "6")]
    pub ez_key_version_name: ::prost::alloc::string::String,
}
/// *
/// Encryption information for an individual
/// file within an encryption zone
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PerFileEncryptionInfoProto {
    #[prost(bytes = "vec", required, tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", required, tag = "2")]
    pub iv: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, required, tag = "3")]
    pub ez_key_version_name: ::prost::alloc::string::String,
}
/// *
/// Encryption information for an encryption
/// zone
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ZoneEncryptionInfoProto {
    #[prost(enumeration = "CipherSuiteProto", required, tag = "1")]
    pub suite: i32,
    #[prost(enumeration = "CryptoProtocolVersionProto", required, tag = "2")]
    pub crypto_protocol_version: i32,
    #[prost(string, required, tag = "3")]
    pub key_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub reencryption_proto: ::core::option::Option<ReencryptionInfoProto>,
}
/// *
/// Re-encryption information for an encryption zone
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReencryptionInfoProto {
    #[prost(string, required, tag = "1")]
    pub ez_key_version_name: ::prost::alloc::string::String,
    #[prost(uint64, required, tag = "2")]
    pub submission_time: u64,
    #[prost(bool, required, tag = "3")]
    pub canceled: bool,
    #[prost(int64, required, tag = "4")]
    pub num_reencrypted: i64,
    #[prost(int64, required, tag = "5")]
    pub num_failures: i64,
    #[prost(uint64, optional, tag = "6")]
    pub completion_time: ::core::option::Option<u64>,
    #[prost(string, optional, tag = "7")]
    pub last_file: ::core::option::Option<::prost::alloc::string::String>,
}
/// *
/// Cipher option
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CipherOptionProto {
    #[prost(enumeration = "CipherSuiteProto", required, tag = "1")]
    pub suite: i32,
    #[prost(bytes = "vec", optional, tag = "2")]
    pub in_key: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "3")]
    pub in_iv: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "4")]
    pub out_key: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "5")]
    pub out_iv: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// *
/// A set of file blocks and their locations.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocatedBlocksProto {
    #[prost(uint64, required, tag = "1")]
    pub file_length: u64,
    #[prost(message, repeated, tag = "2")]
    pub blocks: ::prost::alloc::vec::Vec<LocatedBlockProto>,
    #[prost(bool, required, tag = "3")]
    pub under_construction: bool,
    #[prost(message, optional, tag = "4")]
    pub last_block: ::core::option::Option<LocatedBlockProto>,
    #[prost(bool, required, tag = "5")]
    pub is_last_block_complete: bool,
    #[prost(message, optional, tag = "6")]
    pub file_encryption_info: ::core::option::Option<FileEncryptionInfoProto>,
    /// Optional field for erasure coding
    #[prost(message, optional, tag = "7")]
    pub ec_policy: ::core::option::Option<ErasureCodingPolicyProto>,
}
/// *
/// ECSchema options entry
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EcSchemaOptionEntryProto {
    #[prost(string, required, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
/// *
/// ECSchema for erasurecoding
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EcSchemaProto {
    #[prost(string, required, tag = "1")]
    pub codec_name: ::prost::alloc::string::String,
    #[prost(uint32, required, tag = "2")]
    pub data_units: u32,
    #[prost(uint32, required, tag = "3")]
    pub parity_units: u32,
    #[prost(message, repeated, tag = "4")]
    pub options: ::prost::alloc::vec::Vec<EcSchemaOptionEntryProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ErasureCodingPolicyProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "2")]
    pub schema: ::core::option::Option<EcSchemaProto>,
    #[prost(uint32, optional, tag = "3")]
    pub cell_size: ::core::option::Option<u32>,
    /// Actually a byte - only 8 bits used
    #[prost(uint32, required, tag = "4")]
    pub id: u32,
    #[prost(
        enumeration = "ErasureCodingPolicyState",
        optional,
        tag = "5",
        default = "Enabled"
    )]
    pub state: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddErasureCodingPolicyResponseProto {
    #[prost(message, required, tag = "1")]
    pub policy: ErasureCodingPolicyProto,
    #[prost(bool, required, tag = "2")]
    pub succeed: bool,
    #[prost(string, optional, tag = "3")]
    pub error_msg: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EcTopologyVerifierResultProto {
    #[prost(string, required, tag = "1")]
    pub result_message: ::prost::alloc::string::String,
    #[prost(bool, required, tag = "2")]
    pub is_supported: bool,
}
/// *
/// Placeholder type for consistent HDFS operations.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HdfsPathHandleProto {
    #[prost(uint64, optional, tag = "1")]
    pub inode_id: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub mtime: ::core::option::Option<u64>,
    #[prost(string, optional, tag = "3")]
    pub path: ::core::option::Option<::prost::alloc::string::String>,
}
/// *
/// Status of a file, directory or symlink
/// Optionally includes a file's block locations if requested by client on the rpc call.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HdfsFileStatusProto {
    #[prost(enumeration = "hdfs_file_status_proto::FileType", required, tag = "1")]
    pub file_type: i32,
    /// local name of inode encoded java UTF8
    #[prost(bytes = "vec", required, tag = "2")]
    pub path: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, required, tag = "3")]
    pub length: u64,
    #[prost(message, required, tag = "4")]
    pub permission: FsPermissionProto,
    #[prost(string, required, tag = "5")]
    pub owner: ::prost::alloc::string::String,
    #[prost(string, required, tag = "6")]
    pub group: ::prost::alloc::string::String,
    #[prost(uint64, required, tag = "7")]
    pub modification_time: u64,
    #[prost(uint64, required, tag = "8")]
    pub access_time: u64,
    /// Optional fields for symlink
    ///
    /// if symlink, target encoded java UTF8
    #[prost(bytes = "vec", optional, tag = "9")]
    pub symlink: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    /// Optional fields for file
    ///
    /// only 16bits used
    #[prost(uint32, optional, tag = "10", default = "0")]
    pub block_replication: ::core::option::Option<u32>,
    #[prost(uint64, optional, tag = "11", default = "0")]
    pub blocksize: ::core::option::Option<u64>,
    /// suppled only if asked by client
    #[prost(message, optional, tag = "12")]
    pub locations: ::core::option::Option<LocatedBlocksProto>,
    /// Optional field for fileId
    ///
    /// default as an invalid id
    #[prost(uint64, optional, tag = "13", default = "0")]
    pub file_id: ::core::option::Option<u64>,
    #[prost(int32, optional, tag = "14", default = "-1")]
    pub children_num: ::core::option::Option<i32>,
    /// Optional field for file encryption
    #[prost(message, optional, tag = "15")]
    pub file_encryption_info: ::core::option::Option<FileEncryptionInfoProto>,
    /// block storage policy id
    #[prost(uint32, optional, tag = "16", default = "0")]
    pub storage_policy: ::core::option::Option<u32>,
    /// Optional field for erasure coding
    #[prost(message, optional, tag = "17")]
    pub ec_policy: ::core::option::Option<ErasureCodingPolicyProto>,
    /// Set of flags
    #[prost(uint32, optional, tag = "18", default = "0")]
    pub flags: ::core::option::Option<u32>,
    #[prost(string, optional, tag = "19")]
    pub namespace: ::core::option::Option<::prost::alloc::string::String>,
}
/// Nested message and enum types in `HdfsFileStatusProto`.
pub mod hdfs_file_status_proto {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum FileType {
        IsDir = 1,
        IsFile = 2,
        IsSymlink = 3,
    }
    impl FileType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                FileType::IsDir => "IS_DIR",
                FileType::IsFile => "IS_FILE",
                FileType::IsSymlink => "IS_SYMLINK",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "IS_DIR" => Some(Self::IsDir),
                "IS_FILE" => Some(Self::IsFile),
                "IS_SYMLINK" => Some(Self::IsSymlink),
                _ => None,
            }
        }
    }
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Flags {
        /// has ACLs
        HasAcl = 1,
        /// encrypted
        HasCrypt = 2,
        /// erasure coded
        HasEc = 4,
        /// SNAPSHOT ENABLED
        SnapshotEnabled = 8,
    }
    impl Flags {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Flags::HasAcl => "HAS_ACL",
                Flags::HasCrypt => "HAS_CRYPT",
                Flags::HasEc => "HAS_EC",
                Flags::SnapshotEnabled => "SNAPSHOT_ENABLED",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "HAS_ACL" => Some(Self::HasAcl),
                "HAS_CRYPT" => Some(Self::HasCrypt),
                "HAS_EC" => Some(Self::HasEc),
                "SNAPSHOT_ENABLED" => Some(Self::SnapshotEnabled),
                _ => None,
            }
        }
    }
}
/// *
/// Algorithms/types denoting how block-level checksums are computed using
/// lower-level chunk checksums/CRCs.
/// These options should be kept in sync with
/// org.apache.hadoop.hdfs.protocol.BlockChecksumOptions.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockChecksumOptionsProto {
    #[prost(
        enumeration = "BlockChecksumTypeProto",
        optional,
        tag = "1",
        default = "Md5crc"
    )]
    pub block_checksum_type: ::core::option::Option<i32>,
    /// Only used if blockChecksumType specifies a striped format, such as
    /// COMPOSITE_CRC. If so, then the blockChecksum in the response is expected
    /// to be the concatenation of N crcs, where
    /// N == ((requestedLength - 1) / stripedLength) + 1
    #[prost(uint64, optional, tag = "2")]
    pub stripe_length: ::core::option::Option<u64>,
}
/// *
/// HDFS Server Defaults
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FsServerDefaultsProto {
    #[prost(uint64, required, tag = "1")]
    pub block_size: u64,
    #[prost(uint32, required, tag = "2")]
    pub bytes_per_checksum: u32,
    #[prost(uint32, required, tag = "3")]
    pub write_packet_size: u32,
    /// Actually a short - only 16 bits used
    #[prost(uint32, required, tag = "4")]
    pub replication: u32,
    #[prost(uint32, required, tag = "5")]
    pub file_buffer_size: u32,
    #[prost(bool, optional, tag = "6", default = "false")]
    pub encrypt_data_transfer: ::core::option::Option<bool>,
    #[prost(uint64, optional, tag = "7", default = "0")]
    pub trash_interval: ::core::option::Option<u64>,
    #[prost(
        enumeration = "ChecksumTypeProto",
        optional,
        tag = "8",
        default = "ChecksumCrc32"
    )]
    pub checksum_type: ::core::option::Option<i32>,
    #[prost(string, optional, tag = "9")]
    pub key_provider_uri: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint32, optional, tag = "10", default = "0")]
    pub policy_id: ::core::option::Option<u32>,
    #[prost(bool, optional, tag = "11", default = "false")]
    pub snapshot_trash_root_enabled: ::core::option::Option<bool>,
}
/// *
/// Directory listing
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DirectoryListingProto {
    #[prost(message, repeated, tag = "1")]
    pub partial_listing: ::prost::alloc::vec::Vec<HdfsFileStatusProto>,
    #[prost(uint32, required, tag = "2")]
    pub remaining_entries: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoteExceptionProto {
    #[prost(string, required, tag = "1")]
    pub class_name: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub message: ::core::option::Option<::prost::alloc::string::String>,
}
/// Directory listing result for a batched listing call.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchedDirectoryListingProto {
    #[prost(message, repeated, tag = "1")]
    pub partial_listing: ::prost::alloc::vec::Vec<HdfsFileStatusProto>,
    #[prost(uint32, required, tag = "2")]
    pub parent_idx: u32,
    #[prost(message, optional, tag = "3")]
    pub exception: ::core::option::Option<RemoteExceptionProto>,
}
/// *
/// Status of a snapshottable directory: besides the normal information for
/// a directory status, also include snapshot quota, number of snapshots, and
/// the full path of the parent directory.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshottableDirectoryStatusProto {
    #[prost(message, required, tag = "1")]
    pub dir_status: HdfsFileStatusProto,
    /// Fields specific for snapshottable directory
    #[prost(uint32, required, tag = "2")]
    pub snapshot_quota: u32,
    #[prost(uint32, required, tag = "3")]
    pub snapshot_number: u32,
    #[prost(bytes = "vec", required, tag = "4")]
    pub parent_fullpath: ::prost::alloc::vec::Vec<u8>,
}
/// *
/// Status of a snapshot directory: besides the normal information for
/// a directory status, also include snapshot ID, and
/// the full path of the parent directory.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotStatusProto {
    #[prost(message, required, tag = "1")]
    pub dir_status: HdfsFileStatusProto,
    /// Fields specific for snapshot directory
    #[prost(uint32, required, tag = "2")]
    pub snapshot_id: u32,
    #[prost(bytes = "vec", required, tag = "3")]
    pub parent_fullpath: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, required, tag = "4")]
    pub is_deleted: bool,
}
/// *
/// Snapshottable directory listing
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshottableDirectoryListingProto {
    #[prost(message, repeated, tag = "1")]
    pub snapshottable_dir_listing: ::prost::alloc::vec::Vec<
        SnapshottableDirectoryStatusProto,
    >,
}
/// *
/// Snapshot listing
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotListingProto {
    #[prost(message, repeated, tag = "1")]
    pub snapshot_listing: ::prost::alloc::vec::Vec<SnapshotStatusProto>,
}
/// *
/// Snapshot diff report entry
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotDiffReportEntryProto {
    #[prost(bytes = "vec", required, tag = "1")]
    pub fullpath: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, required, tag = "2")]
    pub modification_label: ::prost::alloc::string::String,
    #[prost(bytes = "vec", optional, tag = "3")]
    pub target_path: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// *
/// Snapshot diff report
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotDiffReportProto {
    /// full path of the directory where snapshots were taken
    #[prost(string, required, tag = "1")]
    pub snapshot_root: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub from_snapshot: ::prost::alloc::string::String,
    #[prost(string, required, tag = "3")]
    pub to_snapshot: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub diff_report_entries: ::prost::alloc::vec::Vec<SnapshotDiffReportEntryProto>,
}
/// *
/// Snapshot diff report listing entry
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotDiffReportListingEntryProto {
    #[prost(bytes = "vec", required, tag = "1")]
    pub fullpath: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, required, tag = "2")]
    pub dir_id: u64,
    #[prost(bool, required, tag = "3")]
    pub is_reference: bool,
    #[prost(bytes = "vec", optional, tag = "4")]
    pub target_path: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(uint64, optional, tag = "5")]
    pub file_id: ::core::option::Option<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotDiffReportCursorProto {
    #[prost(bytes = "vec", required, tag = "1")]
    pub start_path: ::prost::alloc::vec::Vec<u8>,
    #[prost(int32, required, tag = "2", default = "-1")]
    pub index: i32,
}
/// *
/// Snapshot diff report listing
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotDiffReportListingProto {
    /// full path of the directory where snapshots were taken
    #[prost(message, repeated, tag = "1")]
    pub modified_entries: ::prost::alloc::vec::Vec<SnapshotDiffReportListingEntryProto>,
    #[prost(message, repeated, tag = "2")]
    pub created_entries: ::prost::alloc::vec::Vec<SnapshotDiffReportListingEntryProto>,
    #[prost(message, repeated, tag = "3")]
    pub deleted_entries: ::prost::alloc::vec::Vec<SnapshotDiffReportListingEntryProto>,
    #[prost(bool, required, tag = "4")]
    pub is_from_earlier: bool,
    #[prost(message, optional, tag = "5")]
    pub cursor: ::core::option::Option<SnapshotDiffReportCursorProto>,
}
/// *
/// Block information
///
/// Please be wary of adding additional fields here, since INodeFiles
/// need to fit in PB's default max message size of 64MB.
/// We restrict the max # of blocks per file
/// (dfs.namenode.fs-limits.max-blocks-per-file), but it's better
/// to avoid changing this.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockProto {
    #[prost(uint64, required, tag = "1")]
    pub block_id: u64,
    #[prost(uint64, required, tag = "2")]
    pub gen_stamp: u64,
    #[prost(uint64, optional, tag = "3", default = "0")]
    pub num_bytes: ::core::option::Option<u64>,
}
/// *
/// Information related to a snapshot
/// TODO: add more information
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotInfoProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_name: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub snapshot_root: ::prost::alloc::string::String,
    #[prost(message, required, tag = "3")]
    pub permission: FsPermissionProto,
    #[prost(string, required, tag = "4")]
    pub owner: ::prost::alloc::string::String,
    #[prost(string, required, tag = "5")]
    pub group: ::prost::alloc::string::String,
    /// TODO: do we need access time?
    #[prost(string, required, tag = "6")]
    pub create_time: ::prost::alloc::string::String,
}
/// *
/// Rolling upgrade status
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollingUpgradeStatusProto {
    #[prost(string, required, tag = "1")]
    pub block_pool_id: ::prost::alloc::string::String,
    #[prost(bool, optional, tag = "2", default = "false")]
    pub finalized: ::core::option::Option<bool>,
}
/// *
/// A list of storage IDs.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageUuidsProto {
    #[prost(string, repeated, tag = "1")]
    pub storage_uuids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// *
/// Secret information for the BlockKeyProto. This is not sent on the wire as
/// such but is used to pack a byte array and encrypted and put in
/// BlockKeyProto.bytes
/// When adding further fields, make sure they are optional as they would
/// otherwise not be backwards compatible.
///
/// Note: As part of the migration from WritableUtils based tokens (aka "legacy")
/// to Protocol Buffers, we use the first byte to determine the type. If the
/// first byte is <=0 then it is a legacy token. This means that when using
/// protobuf tokens, the the first field sent must have a `field_number` less
/// than 16 to make sure that the first byte is positive. Otherwise it could be
/// parsed as a legacy token. See HDFS-11026 for more discussion.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockTokenSecretProto {
    #[prost(uint64, optional, tag = "1")]
    pub expiry_date: ::core::option::Option<u64>,
    #[prost(uint32, optional, tag = "2")]
    pub key_id: ::core::option::Option<u32>,
    #[prost(string, optional, tag = "3")]
    pub user_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "4")]
    pub block_pool_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, optional, tag = "5")]
    pub block_id: ::core::option::Option<u64>,
    #[prost(enumeration = "AccessModeProto", repeated, packed = "false", tag = "6")]
    pub modes: ::prost::alloc::vec::Vec<i32>,
    #[prost(enumeration = "StorageTypeProto", repeated, packed = "false", tag = "7")]
    pub storage_types: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, repeated, tag = "8")]
    pub storage_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bytes = "vec", optional, tag = "9")]
    pub handshake_secret: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// *
/// Clients should receive this message in RPC responses and forward it
/// in RPC requests without interpreting it. It should be encoded
/// as an obscure byte array when being sent to clients.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RouterFederatedStateProto {
    /// Last seen state IDs for multiple namespaces.
    #[prost(map = "string, int64", tag = "1")]
    pub namespace_state_ids: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        i64,
    >,
}
/// *
/// Types of recognized storage media.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum StorageTypeProto {
    Disk = 1,
    Ssd = 2,
    Archive = 3,
    RamDisk = 4,
    Provided = 5,
    Nvdimm = 6,
}
impl StorageTypeProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            StorageTypeProto::Disk => "DISK",
            StorageTypeProto::Ssd => "SSD",
            StorageTypeProto::Archive => "ARCHIVE",
            StorageTypeProto::RamDisk => "RAM_DISK",
            StorageTypeProto::Provided => "PROVIDED",
            StorageTypeProto::Nvdimm => "NVDIMM",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "DISK" => Some(Self::Disk),
            "SSD" => Some(Self::Ssd),
            "ARCHIVE" => Some(Self::Archive),
            "RAM_DISK" => Some(Self::RamDisk),
            "PROVIDED" => Some(Self::Provided),
            "NVDIMM" => Some(Self::Nvdimm),
            _ => None,
        }
    }
}
/// *
/// Types of recognized blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BlockTypeProto {
    Contiguous = 0,
    Striped = 1,
}
impl BlockTypeProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            BlockTypeProto::Contiguous => "CONTIGUOUS",
            BlockTypeProto::Striped => "STRIPED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CONTIGUOUS" => Some(Self::Contiguous),
            "STRIPED" => Some(Self::Striped),
            _ => None,
        }
    }
}
/// *
/// Cipher suite.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CipherSuiteProto {
    Unknown = 1,
    AesCtrNopadding = 2,
    Sm4CtrNopadding = 3,
}
impl CipherSuiteProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CipherSuiteProto::Unknown => "UNKNOWN",
            CipherSuiteProto::AesCtrNopadding => "AES_CTR_NOPADDING",
            CipherSuiteProto::Sm4CtrNopadding => "SM4_CTR_NOPADDING",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UNKNOWN" => Some(Self::Unknown),
            "AES_CTR_NOPADDING" => Some(Self::AesCtrNopadding),
            "SM4_CTR_NOPADDING" => Some(Self::Sm4CtrNopadding),
            _ => None,
        }
    }
}
/// *
/// Crypto protocol version used to access encrypted files.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CryptoProtocolVersionProto {
    UnknownProtocolVersion = 1,
    EncryptionZones = 2,
}
impl CryptoProtocolVersionProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CryptoProtocolVersionProto::UnknownProtocolVersion => {
                "UNKNOWN_PROTOCOL_VERSION"
            }
            CryptoProtocolVersionProto::EncryptionZones => "ENCRYPTION_ZONES",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UNKNOWN_PROTOCOL_VERSION" => Some(Self::UnknownProtocolVersion),
            "ENCRYPTION_ZONES" => Some(Self::EncryptionZones),
            _ => None,
        }
    }
}
/// *
/// EC policy state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ErasureCodingPolicyState {
    Disabled = 1,
    Enabled = 2,
    Removed = 3,
}
impl ErasureCodingPolicyState {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ErasureCodingPolicyState::Disabled => "DISABLED",
            ErasureCodingPolicyState::Enabled => "ENABLED",
            ErasureCodingPolicyState::Removed => "REMOVED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "DISABLED" => Some(Self::Disabled),
            "ENABLED" => Some(Self::Enabled),
            "REMOVED" => Some(Self::Removed),
            _ => None,
        }
    }
}
/// *
/// Checksum algorithms/types used in HDFS
/// Make sure this enum's integer values match enum values' id properties defined
/// in org.apache.hadoop.util.DataChecksum.Type
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ChecksumTypeProto {
    ChecksumNull = 0,
    ChecksumCrc32 = 1,
    ChecksumCrc32c = 2,
}
impl ChecksumTypeProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ChecksumTypeProto::ChecksumNull => "CHECKSUM_NULL",
            ChecksumTypeProto::ChecksumCrc32 => "CHECKSUM_CRC32",
            ChecksumTypeProto::ChecksumCrc32c => "CHECKSUM_CRC32C",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CHECKSUM_NULL" => Some(Self::ChecksumNull),
            "CHECKSUM_CRC32" => Some(Self::ChecksumCrc32),
            "CHECKSUM_CRC32C" => Some(Self::ChecksumCrc32c),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BlockChecksumTypeProto {
    /// BlockChecksum obtained by taking the MD5 digest of chunk CRCs
    Md5crc = 1,
    /// Chunk-independent CRC, optionally striped
    CompositeCrc = 2,
}
impl BlockChecksumTypeProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            BlockChecksumTypeProto::Md5crc => "MD5CRC",
            BlockChecksumTypeProto::CompositeCrc => "COMPOSITE_CRC",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "MD5CRC" => Some(Self::Md5crc),
            "COMPOSITE_CRC" => Some(Self::CompositeCrc),
            _ => None,
        }
    }
}
/// *
/// File access permissions mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AccessModeProto {
    Read = 1,
    Write = 2,
    Copy = 3,
    Replace = 4,
}
impl AccessModeProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AccessModeProto::Read => "READ",
            AccessModeProto::Write => "WRITE",
            AccessModeProto::Copy => "COPY",
            AccessModeProto::Replace => "REPLACE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "READ" => Some(Self::Read),
            "WRITE" => Some(Self::Write),
            "COPY" => Some(Self::Copy),
            "REPLACE" => Some(Self::Replace),
            _ => None,
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct XAttrProto {
    #[prost(enumeration = "x_attr_proto::XAttrNamespaceProto", required, tag = "1")]
    pub namespace: i32,
    #[prost(string, required, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(bytes = "vec", optional, tag = "3")]
    pub value: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// Nested message and enum types in `XAttrProto`.
pub mod x_attr_proto {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum XAttrNamespaceProto {
        User = 0,
        Trusted = 1,
        Security = 2,
        System = 3,
        Raw = 4,
    }
    impl XAttrNamespaceProto {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                XAttrNamespaceProto::User => "USER",
                XAttrNamespaceProto::Trusted => "TRUSTED",
                XAttrNamespaceProto::Security => "SECURITY",
                XAttrNamespaceProto::System => "SYSTEM",
                XAttrNamespaceProto::Raw => "RAW",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "USER" => Some(Self::User),
                "TRUSTED" => Some(Self::Trusted),
                "SECURITY" => Some(Self::Security),
                "SYSTEM" => Some(Self::System),
                "RAW" => Some(Self::Raw),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetXAttrRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub x_attr: ::core::option::Option<XAttrProto>,
    /// bits set using XAttrSetFlagProto
    #[prost(uint32, optional, tag = "3")]
    pub flag: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetXAttrResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetXAttrsRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub x_attrs: ::prost::alloc::vec::Vec<XAttrProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetXAttrsResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub x_attrs: ::prost::alloc::vec::Vec<XAttrProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListXAttrsRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListXAttrsResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub x_attrs: ::prost::alloc::vec::Vec<XAttrProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveXAttrRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub x_attr: ::core::option::Option<XAttrProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveXAttrResponseProto {}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum XAttrSetFlagProto {
    XattrCreate = 1,
    XattrReplace = 2,
}
impl XAttrSetFlagProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            XAttrSetFlagProto::XattrCreate => "XATTR_CREATE",
            XAttrSetFlagProto::XattrReplace => "XATTR_REPLACE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "XATTR_CREATE" => Some(Self::XattrCreate),
            "XATTR_REPLACE" => Some(Self::XattrReplace),
            _ => None,
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateEncryptionZoneRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub key_name: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateEncryptionZoneResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListEncryptionZonesRequestProto {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EncryptionZoneProto {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
    #[prost(string, required, tag = "2")]
    pub path: ::prost::alloc::string::String,
    #[prost(enumeration = "CipherSuiteProto", required, tag = "3")]
    pub suite: i32,
    #[prost(enumeration = "CryptoProtocolVersionProto", required, tag = "4")]
    pub crypto_protocol_version: i32,
    #[prost(string, required, tag = "5")]
    pub key_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListEncryptionZonesResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub zones: ::prost::alloc::vec::Vec<EncryptionZoneProto>,
    #[prost(bool, required, tag = "2")]
    pub has_more: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReencryptEncryptionZoneRequestProto {
    #[prost(enumeration = "ReencryptActionProto", required, tag = "1")]
    pub action: i32,
    #[prost(string, required, tag = "2")]
    pub zone: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReencryptEncryptionZoneResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListReencryptionStatusRequestProto {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ZoneReencryptionStatusProto {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
    #[prost(string, required, tag = "2")]
    pub path: ::prost::alloc::string::String,
    #[prost(enumeration = "ReencryptionStateProto", required, tag = "3")]
    pub state: i32,
    #[prost(string, required, tag = "4")]
    pub ez_key_version_name: ::prost::alloc::string::String,
    #[prost(int64, required, tag = "5")]
    pub submission_time: i64,
    #[prost(bool, required, tag = "6")]
    pub canceled: bool,
    #[prost(int64, required, tag = "7")]
    pub num_reencrypted: i64,
    #[prost(int64, required, tag = "8")]
    pub num_failures: i64,
    #[prost(int64, optional, tag = "9")]
    pub completion_time: ::core::option::Option<i64>,
    #[prost(string, optional, tag = "10")]
    pub last_file: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListReencryptionStatusResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub statuses: ::prost::alloc::vec::Vec<ZoneReencryptionStatusProto>,
    #[prost(bool, required, tag = "2")]
    pub has_more: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEzForPathRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEzForPathResponseProto {
    #[prost(message, optional, tag = "1")]
    pub zone: ::core::option::Option<EncryptionZoneProto>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ReencryptActionProto {
    CancelReencrypt = 1,
    StartReencrypt = 2,
}
impl ReencryptActionProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ReencryptActionProto::CancelReencrypt => "CANCEL_REENCRYPT",
            ReencryptActionProto::StartReencrypt => "START_REENCRYPT",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CANCEL_REENCRYPT" => Some(Self::CancelReencrypt),
            "START_REENCRYPT" => Some(Self::StartReencrypt),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ReencryptionStateProto {
    Submitted = 1,
    Processing = 2,
    Completed = 3,
}
impl ReencryptionStateProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ReencryptionStateProto::Submitted => "SUBMITTED",
            ReencryptionStateProto::Processing => "PROCESSING",
            ReencryptionStateProto::Completed => "COMPLETED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SUBMITTED" => Some(Self::Submitted),
            "PROCESSING" => Some(Self::Processing),
            "COMPLETED" => Some(Self::Completed),
            _ => None,
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventProto {
    #[prost(enumeration = "EventType", required, tag = "1")]
    pub r#type: i32,
    #[prost(bytes = "vec", required, tag = "2")]
    pub contents: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventBatchProto {
    #[prost(int64, required, tag = "1")]
    pub txid: i64,
    #[prost(message, repeated, tag = "2")]
    pub events: ::prost::alloc::vec::Vec<EventProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateEventProto {
    #[prost(enumeration = "INodeType", required, tag = "1")]
    pub r#type: i32,
    #[prost(string, required, tag = "2")]
    pub path: ::prost::alloc::string::String,
    #[prost(int64, required, tag = "3")]
    pub ctime: i64,
    #[prost(string, required, tag = "4")]
    pub owner_name: ::prost::alloc::string::String,
    #[prost(string, required, tag = "5")]
    pub group_name: ::prost::alloc::string::String,
    #[prost(message, required, tag = "6")]
    pub perms: FsPermissionProto,
    #[prost(int32, optional, tag = "7")]
    pub replication: ::core::option::Option<i32>,
    #[prost(string, optional, tag = "8")]
    pub symlink_target: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(bool, optional, tag = "9")]
    pub overwrite: ::core::option::Option<bool>,
    #[prost(int64, optional, tag = "10", default = "0")]
    pub default_block_size: ::core::option::Option<i64>,
    #[prost(bool, optional, tag = "11")]
    pub erasure_coded: ::core::option::Option<bool>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseEventProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(int64, required, tag = "2")]
    pub file_size: i64,
    #[prost(int64, required, tag = "3")]
    pub timestamp: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateEventProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(int64, required, tag = "2")]
    pub file_size: i64,
    #[prost(int64, required, tag = "3")]
    pub timestamp: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEventProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(bool, optional, tag = "2", default = "false")]
    pub new_block: ::core::option::Option<bool>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameEventProto {
    #[prost(string, required, tag = "1")]
    pub src_path: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub dest_path: ::prost::alloc::string::String,
    #[prost(int64, required, tag = "3")]
    pub timestamp: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetadataUpdateEventProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(enumeration = "MetadataUpdateType", required, tag = "2")]
    pub r#type: i32,
    #[prost(int64, optional, tag = "3")]
    pub mtime: ::core::option::Option<i64>,
    #[prost(int64, optional, tag = "4")]
    pub atime: ::core::option::Option<i64>,
    #[prost(int32, optional, tag = "5")]
    pub replication: ::core::option::Option<i32>,
    #[prost(string, optional, tag = "6")]
    pub owner_name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "7")]
    pub group_name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "8")]
    pub perms: ::core::option::Option<FsPermissionProto>,
    #[prost(message, repeated, tag = "9")]
    pub acls: ::prost::alloc::vec::Vec<AclEntryProto>,
    #[prost(message, repeated, tag = "10")]
    pub x_attrs: ::prost::alloc::vec::Vec<XAttrProto>,
    #[prost(bool, optional, tag = "11")]
    pub x_attrs_removed: ::core::option::Option<bool>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnlinkEventProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(int64, required, tag = "2")]
    pub timestamp: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventsListProto {
    /// deprecated
    #[prost(message, repeated, tag = "1")]
    pub events: ::prost::alloc::vec::Vec<EventProto>,
    #[prost(int64, required, tag = "2")]
    pub first_txid: i64,
    #[prost(int64, required, tag = "3")]
    pub last_txid: i64,
    #[prost(int64, required, tag = "4")]
    pub sync_txid: i64,
    #[prost(message, repeated, tag = "5")]
    pub batch: ::prost::alloc::vec::Vec<EventBatchProto>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EventType {
    EventCreate = 0,
    EventClose = 1,
    EventAppend = 2,
    EventRename = 3,
    EventMetadata = 4,
    EventUnlink = 5,
    EventTruncate = 6,
}
impl EventType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            EventType::EventCreate => "EVENT_CREATE",
            EventType::EventClose => "EVENT_CLOSE",
            EventType::EventAppend => "EVENT_APPEND",
            EventType::EventRename => "EVENT_RENAME",
            EventType::EventMetadata => "EVENT_METADATA",
            EventType::EventUnlink => "EVENT_UNLINK",
            EventType::EventTruncate => "EVENT_TRUNCATE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "EVENT_CREATE" => Some(Self::EventCreate),
            "EVENT_CLOSE" => Some(Self::EventClose),
            "EVENT_APPEND" => Some(Self::EventAppend),
            "EVENT_RENAME" => Some(Self::EventRename),
            "EVENT_METADATA" => Some(Self::EventMetadata),
            "EVENT_UNLINK" => Some(Self::EventUnlink),
            "EVENT_TRUNCATE" => Some(Self::EventTruncate),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum INodeType {
    ITypeFile = 0,
    ITypeDirectory = 1,
    ITypeSymlink = 2,
}
impl INodeType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            INodeType::ITypeFile => "I_TYPE_FILE",
            INodeType::ITypeDirectory => "I_TYPE_DIRECTORY",
            INodeType::ITypeSymlink => "I_TYPE_SYMLINK",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "I_TYPE_FILE" => Some(Self::ITypeFile),
            "I_TYPE_DIRECTORY" => Some(Self::ITypeDirectory),
            "I_TYPE_SYMLINK" => Some(Self::ITypeSymlink),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MetadataUpdateType {
    MetaTypeTimes = 0,
    MetaTypeReplication = 1,
    MetaTypeOwner = 2,
    MetaTypePerms = 3,
    MetaTypeAcls = 4,
    MetaTypeXattrs = 5,
}
impl MetadataUpdateType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            MetadataUpdateType::MetaTypeTimes => "META_TYPE_TIMES",
            MetadataUpdateType::MetaTypeReplication => "META_TYPE_REPLICATION",
            MetadataUpdateType::MetaTypeOwner => "META_TYPE_OWNER",
            MetadataUpdateType::MetaTypePerms => "META_TYPE_PERMS",
            MetadataUpdateType::MetaTypeAcls => "META_TYPE_ACLS",
            MetadataUpdateType::MetaTypeXattrs => "META_TYPE_XATTRS",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "META_TYPE_TIMES" => Some(Self::MetaTypeTimes),
            "META_TYPE_REPLICATION" => Some(Self::MetaTypeReplication),
            "META_TYPE_OWNER" => Some(Self::MetaTypeOwner),
            "META_TYPE_PERMS" => Some(Self::MetaTypePerms),
            "META_TYPE_ACLS" => Some(Self::MetaTypeAcls),
            "META_TYPE_XATTRS" => Some(Self::MetaTypeXattrs),
            _ => None,
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetErasureCodingPolicyRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub ec_policy_name: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetErasureCodingPolicyResponseProto {}
/// void request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetErasureCodingPoliciesRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetErasureCodingPoliciesResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub ec_policies: ::prost::alloc::vec::Vec<ErasureCodingPolicyProto>,
}
/// void request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetErasureCodingCodecsRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetErasureCodingCodecsResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub codec: ::prost::alloc::vec::Vec<CodecProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetErasureCodingPolicyRequestProto {
    /// path to get the policy info
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetErasureCodingPolicyResponseProto {
    #[prost(message, optional, tag = "1")]
    pub ec_policy: ::core::option::Option<ErasureCodingPolicyProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddErasureCodingPoliciesRequestProto {
    #[prost(message, repeated, tag = "1")]
    pub ec_policies: ::prost::alloc::vec::Vec<ErasureCodingPolicyProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddErasureCodingPoliciesResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub responses: ::prost::alloc::vec::Vec<AddErasureCodingPolicyResponseProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveErasureCodingPolicyRequestProto {
    #[prost(string, required, tag = "1")]
    pub ec_policy_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveErasureCodingPolicyResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnableErasureCodingPolicyRequestProto {
    #[prost(string, required, tag = "1")]
    pub ec_policy_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnableErasureCodingPolicyResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DisableErasureCodingPolicyRequestProto {
    #[prost(string, required, tag = "1")]
    pub ec_policy_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DisableErasureCodingPolicyResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnsetErasureCodingPolicyRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnsetErasureCodingPolicyResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEcTopologyResultForPoliciesRequestProto {
    #[prost(string, repeated, tag = "1")]
    pub policies: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEcTopologyResultForPoliciesResponseProto {
    #[prost(message, required, tag = "1")]
    pub response: EcTopologyVerifierResultProto,
}
/// *
/// Block erasure coding reconstruction info
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockEcReconstructionInfoProto {
    #[prost(message, required, tag = "1")]
    pub block: ExtendedBlockProto,
    #[prost(message, required, tag = "2")]
    pub source_dn_infos: DatanodeInfosProto,
    #[prost(message, required, tag = "3")]
    pub target_dn_infos: DatanodeInfosProto,
    #[prost(message, required, tag = "4")]
    pub target_storage_uuids: StorageUuidsProto,
    #[prost(message, required, tag = "5")]
    pub target_storage_types: StorageTypesProto,
    #[prost(bytes = "vec", required, tag = "6")]
    pub live_block_indices: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, required, tag = "7")]
    pub ec_policy: ErasureCodingPolicyProto,
    #[prost(bytes = "vec", optional, tag = "8")]
    pub exclude_reconstructed_indices: ::core::option::Option<
        ::prost::alloc::vec::Vec<u8>,
    >,
}
/// *
/// Codec and it's corresponding coders
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CodecProto {
    #[prost(string, required, tag = "1")]
    pub codec: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub coders: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetBlockLocationsRequestProto {
    /// file name
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    /// range start offset
    #[prost(uint64, required, tag = "2")]
    pub offset: u64,
    /// range length
    #[prost(uint64, required, tag = "3")]
    pub length: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetBlockLocationsResponseProto {
    #[prost(message, optional, tag = "1")]
    pub locations: ::core::option::Option<LocatedBlocksProto>,
}
/// No parameters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetServerDefaultsRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetServerDefaultsResponseProto {
    #[prost(message, required, tag = "1")]
    pub server_defaults: FsServerDefaultsProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, required, tag = "2")]
    pub masked: FsPermissionProto,
    #[prost(string, required, tag = "3")]
    pub client_name: ::prost::alloc::string::String,
    /// bits set using CreateFlag
    #[prost(uint32, required, tag = "4")]
    pub create_flag: u32,
    #[prost(bool, required, tag = "5")]
    pub create_parent: bool,
    /// Short: Only 16 bits used
    #[prost(uint32, required, tag = "6")]
    pub replication: u32,
    #[prost(uint64, required, tag = "7")]
    pub block_size: u64,
    #[prost(
        enumeration = "CryptoProtocolVersionProto",
        repeated,
        packed = "false",
        tag = "8"
    )]
    pub crypto_protocol_version: ::prost::alloc::vec::Vec<i32>,
    #[prost(message, optional, tag = "9")]
    pub unmasked: ::core::option::Option<FsPermissionProto>,
    #[prost(string, optional, tag = "10")]
    pub ec_policy_name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "11")]
    pub storage_policy: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateResponseProto {
    #[prost(message, optional, tag = "1")]
    pub fs: ::core::option::Option<HdfsFileStatusProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub client_name: ::prost::alloc::string::String,
    /// bits set using CreateFlag
    #[prost(uint32, optional, tag = "3")]
    pub flag: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendResponseProto {
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<LocatedBlockProto>,
    #[prost(message, optional, tag = "2")]
    pub stat: ::core::option::Option<HdfsFileStatusProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetReplicationRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    /// Short: Only 16 bits used
    #[prost(uint32, required, tag = "2")]
    pub replication: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetReplicationResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetStoragePolicyRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub policy_name: ::prost::alloc::string::String,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetStoragePolicyResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnsetStoragePolicyRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnsetStoragePolicyResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetStoragePolicyRequestProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetStoragePolicyResponseProto {
    #[prost(message, required, tag = "1")]
    pub storage_policy: BlockStoragePolicyProto,
}
/// void request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetStoragePoliciesRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetStoragePoliciesResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub policies: ::prost::alloc::vec::Vec<BlockStoragePolicyProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetPermissionRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, required, tag = "2")]
    pub permission: FsPermissionProto,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetPermissionResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetOwnerRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub username: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "3")]
    pub groupname: ::core::option::Option<::prost::alloc::string::String>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetOwnerResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AbandonBlockRequestProto {
    #[prost(message, required, tag = "1")]
    pub b: ExtendedBlockProto,
    #[prost(string, required, tag = "2")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, required, tag = "3")]
    pub holder: ::prost::alloc::string::String,
    /// default to GRANDFATHER_INODE_ID
    #[prost(uint64, optional, tag = "4", default = "0")]
    pub file_id: ::core::option::Option<u64>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AbandonBlockResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddBlockRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub client_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub previous: ::core::option::Option<ExtendedBlockProto>,
    #[prost(message, repeated, tag = "4")]
    pub exclude_nodes: ::prost::alloc::vec::Vec<DatanodeInfoProto>,
    /// default as a bogus id
    #[prost(uint64, optional, tag = "5", default = "0")]
    pub file_id: ::core::option::Option<u64>,
    /// the set of datanodes to use for the block
    #[prost(string, repeated, tag = "6")]
    pub favored_nodes: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// default to empty.
    #[prost(enumeration = "AddBlockFlagProto", repeated, packed = "false", tag = "7")]
    pub flags: ::prost::alloc::vec::Vec<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddBlockResponseProto {
    #[prost(message, required, tag = "1")]
    pub block: LocatedBlockProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetAdditionalDatanodeRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, required, tag = "2")]
    pub blk: ExtendedBlockProto,
    #[prost(message, repeated, tag = "3")]
    pub existings: ::prost::alloc::vec::Vec<DatanodeInfoProto>,
    #[prost(message, repeated, tag = "4")]
    pub excludes: ::prost::alloc::vec::Vec<DatanodeInfoProto>,
    #[prost(uint32, required, tag = "5")]
    pub num_additional_nodes: u32,
    #[prost(string, required, tag = "6")]
    pub client_name: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "7")]
    pub existing_storage_uuids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// default to GRANDFATHER_INODE_ID
    #[prost(uint64, optional, tag = "8", default = "0")]
    pub file_id: ::core::option::Option<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetAdditionalDatanodeResponseProto {
    #[prost(message, required, tag = "1")]
    pub block: LocatedBlockProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompleteRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub client_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub last: ::core::option::Option<ExtendedBlockProto>,
    /// default to GRANDFATHER_INODE_ID
    #[prost(uint64, optional, tag = "4", default = "0")]
    pub file_id: ::core::option::Option<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompleteResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportBadBlocksRequestProto {
    #[prost(message, repeated, tag = "1")]
    pub blocks: ::prost::alloc::vec::Vec<LocatedBlockProto>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportBadBlocksResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConcatRequestProto {
    #[prost(string, required, tag = "1")]
    pub trg: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub srcs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConcatResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(uint64, required, tag = "2")]
    pub new_length: u64,
    #[prost(string, required, tag = "3")]
    pub client_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub dst: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Rename2RequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub dst: ::prost::alloc::string::String,
    #[prost(bool, required, tag = "3")]
    pub overwrite_dest: bool,
    #[prost(bool, optional, tag = "4")]
    pub move_to_trash: ::core::option::Option<bool>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Rename2ResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(bool, required, tag = "2")]
    pub recursive: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MkdirsRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(message, required, tag = "2")]
    pub masked: FsPermissionProto,
    #[prost(bool, required, tag = "3")]
    pub create_parent: bool,
    #[prost(message, optional, tag = "4")]
    pub unmasked: ::core::option::Option<FsPermissionProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MkdirsResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetListingRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(bytes = "vec", required, tag = "2")]
    pub start_after: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, required, tag = "3")]
    pub need_location: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetListingResponseProto {
    #[prost(message, optional, tag = "1")]
    pub dir_list: ::core::option::Option<DirectoryListingProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetBatchedListingRequestProto {
    #[prost(string, repeated, tag = "1")]
    pub paths: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bytes = "vec", required, tag = "2")]
    pub start_after: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, required, tag = "3")]
    pub need_location: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetBatchedListingResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub listings: ::prost::alloc::vec::Vec<BatchedDirectoryListingProto>,
    #[prost(bool, required, tag = "2")]
    pub has_more: bool,
    #[prost(bytes = "vec", required, tag = "3")]
    pub start_after: ::prost::alloc::vec::Vec<u8>,
}
/// no input parameters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshottableDirListingRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshottableDirListingResponseProto {
    #[prost(message, optional, tag = "1")]
    pub snapshottable_dir_list: ::core::option::Option<
        SnapshottableDirectoryListingProto,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshotListingRequestProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_root: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshotListingResponseProto {
    #[prost(message, optional, tag = "1")]
    pub snapshot_list: ::core::option::Option<SnapshotListingProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshotDiffReportRequestProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_root: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub from_snapshot: ::prost::alloc::string::String,
    #[prost(string, required, tag = "3")]
    pub to_snapshot: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshotDiffReportResponseProto {
    #[prost(message, required, tag = "1")]
    pub diff_report: SnapshotDiffReportProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshotDiffReportListingRequestProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_root: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub from_snapshot: ::prost::alloc::string::String,
    #[prost(string, required, tag = "3")]
    pub to_snapshot: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub cursor: ::core::option::Option<SnapshotDiffReportCursorProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshotDiffReportListingResponseProto {
    #[prost(message, required, tag = "1")]
    pub diff_report: SnapshotDiffReportListingProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenewLeaseRequestProto {
    #[prost(string, required, tag = "1")]
    pub client_name: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub namespaces: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenewLeaseResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoverLeaseRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub client_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoverLeaseResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
/// no input paramters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFsStatusRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFsStatsResponseProto {
    #[prost(uint64, required, tag = "1")]
    pub capacity: u64,
    #[prost(uint64, required, tag = "2")]
    pub used: u64,
    #[prost(uint64, required, tag = "3")]
    pub remaining: u64,
    #[prost(uint64, required, tag = "4")]
    pub under_replicated: u64,
    #[prost(uint64, required, tag = "5")]
    pub corrupt_blocks: u64,
    #[prost(uint64, required, tag = "6")]
    pub missing_blocks: u64,
    #[prost(uint64, optional, tag = "7")]
    pub missing_repl_one_blocks: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "8")]
    pub blocks_in_future: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "9")]
    pub pending_deletion_blocks: ::core::option::Option<u64>,
}
/// no input paramters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFsReplicatedBlockStatsRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFsReplicatedBlockStatsResponseProto {
    #[prost(uint64, required, tag = "1")]
    pub low_redundancy: u64,
    #[prost(uint64, required, tag = "2")]
    pub corrupt_blocks: u64,
    #[prost(uint64, required, tag = "3")]
    pub missing_blocks: u64,
    #[prost(uint64, required, tag = "4")]
    pub missing_repl_one_blocks: u64,
    #[prost(uint64, required, tag = "5")]
    pub blocks_in_future: u64,
    #[prost(uint64, required, tag = "6")]
    pub pending_deletion_blocks: u64,
    #[prost(uint64, optional, tag = "7")]
    pub highest_prio_low_redundancy_blocks: ::core::option::Option<u64>,
}
/// no input paramters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFsEcBlockGroupStatsRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFsEcBlockGroupStatsResponseProto {
    #[prost(uint64, required, tag = "1")]
    pub low_redundancy: u64,
    #[prost(uint64, required, tag = "2")]
    pub corrupt_blocks: u64,
    #[prost(uint64, required, tag = "3")]
    pub missing_blocks: u64,
    #[prost(uint64, required, tag = "4")]
    pub blocks_in_future: u64,
    #[prost(uint64, required, tag = "5")]
    pub pending_deletion_blocks: u64,
    #[prost(uint64, optional, tag = "6")]
    pub highest_prio_low_redundancy_blocks: ::core::option::Option<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDatanodeReportRequestProto {
    #[prost(enumeration = "DatanodeReportTypeProto", required, tag = "1")]
    pub r#type: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDatanodeReportResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub di: ::prost::alloc::vec::Vec<DatanodeInfoProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDatanodeStorageReportRequestProto {
    #[prost(enumeration = "DatanodeReportTypeProto", required, tag = "1")]
    pub r#type: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatanodeStorageReportProto {
    #[prost(message, required, tag = "1")]
    pub datanode_info: DatanodeInfoProto,
    #[prost(message, repeated, tag = "2")]
    pub storage_reports: ::prost::alloc::vec::Vec<StorageReportProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDatanodeStorageReportResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub datanode_storage_reports: ::prost::alloc::vec::Vec<DatanodeStorageReportProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPreferredBlockSizeRequestProto {
    #[prost(string, required, tag = "1")]
    pub filename: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPreferredBlockSizeResponseProto {
    #[prost(uint64, required, tag = "1")]
    pub bsize: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSlowDatanodeReportRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSlowDatanodeReportResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub datanode_info_proto: ::prost::alloc::vec::Vec<DatanodeInfoProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetSafeModeRequestProto {
    #[prost(enumeration = "SafeModeActionProto", required, tag = "1")]
    pub action: i32,
    #[prost(bool, optional, tag = "2", default = "false")]
    pub checked: ::core::option::Option<bool>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetSafeModeResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SaveNamespaceRequestProto {
    #[prost(uint64, optional, tag = "1", default = "0")]
    pub time_window: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "2", default = "0")]
    pub tx_gap: ::core::option::Option<u64>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SaveNamespaceResponseProto {
    #[prost(bool, optional, tag = "1", default = "true")]
    pub saved: ::core::option::Option<bool>,
}
/// no parameters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollEditsRequestProto {}
/// response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollEditsResponseProto {
    #[prost(uint64, required, tag = "1")]
    pub new_segment_tx_id: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RestoreFailedStorageRequestProto {
    #[prost(string, required, tag = "1")]
    pub arg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RestoreFailedStorageResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
/// no parameters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RefreshNodesRequestProto {}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RefreshNodesResponseProto {}
/// no parameters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FinalizeUpgradeRequestProto {}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FinalizeUpgradeResponseProto {}
/// no parameters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpgradeStatusRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpgradeStatusResponseProto {
    #[prost(bool, required, tag = "1")]
    pub upgrade_finalized: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollingUpgradeRequestProto {
    #[prost(enumeration = "RollingUpgradeActionProto", required, tag = "1")]
    pub action: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollingUpgradeInfoProto {
    #[prost(message, required, tag = "1")]
    pub status: RollingUpgradeStatusProto,
    #[prost(uint64, required, tag = "2")]
    pub start_time: u64,
    #[prost(uint64, required, tag = "3")]
    pub finalize_time: u64,
    #[prost(bool, required, tag = "4")]
    pub created_rollback_images: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollingUpgradeResponseProto {
    #[prost(message, optional, tag = "1")]
    pub rolling_upgrade_info: ::core::option::Option<RollingUpgradeInfoProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCorruptFileBlocksRequestProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub cookie: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCorruptFileBlocksResponseProto {
    #[prost(message, required, tag = "1")]
    pub corrupt: CorruptFileBlocksProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetaSaveRequestProto {
    #[prost(string, required, tag = "1")]
    pub filename: ::prost::alloc::string::String,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetaSaveResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFileInfoRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFileInfoResponseProto {
    #[prost(message, optional, tag = "1")]
    pub fs: ::core::option::Option<HdfsFileStatusProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLocatedFileInfoRequestProto {
    #[prost(string, optional, tag = "1")]
    pub src: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(bool, optional, tag = "2", default = "false")]
    pub need_block_token: ::core::option::Option<bool>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLocatedFileInfoResponseProto {
    #[prost(message, optional, tag = "1")]
    pub fs: ::core::option::Option<HdfsFileStatusProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsFileClosedRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsFileClosedResponseProto {
    #[prost(bool, required, tag = "1")]
    pub result: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CacheDirectiveInfoProto {
    #[prost(int64, optional, tag = "1")]
    pub id: ::core::option::Option<i64>,
    #[prost(string, optional, tag = "2")]
    pub path: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint32, optional, tag = "3")]
    pub replication: ::core::option::Option<u32>,
    #[prost(string, optional, tag = "4")]
    pub pool: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "5")]
    pub expiration: ::core::option::Option<CacheDirectiveInfoExpirationProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CacheDirectiveInfoExpirationProto {
    #[prost(int64, required, tag = "1")]
    pub millis: i64,
    #[prost(bool, required, tag = "2")]
    pub is_relative: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CacheDirectiveStatsProto {
    #[prost(int64, required, tag = "1")]
    pub bytes_needed: i64,
    #[prost(int64, required, tag = "2")]
    pub bytes_cached: i64,
    #[prost(int64, required, tag = "3")]
    pub files_needed: i64,
    #[prost(int64, required, tag = "4")]
    pub files_cached: i64,
    #[prost(bool, required, tag = "5")]
    pub has_expired: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddCacheDirectiveRequestProto {
    #[prost(message, required, tag = "1")]
    pub info: CacheDirectiveInfoProto,
    /// bits set using CacheFlag
    #[prost(uint32, optional, tag = "2")]
    pub cache_flags: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddCacheDirectiveResponseProto {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyCacheDirectiveRequestProto {
    #[prost(message, required, tag = "1")]
    pub info: CacheDirectiveInfoProto,
    /// bits set using CacheFlag
    #[prost(uint32, optional, tag = "2")]
    pub cache_flags: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyCacheDirectiveResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveCacheDirectiveRequestProto {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveCacheDirectiveResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCacheDirectivesRequestProto {
    #[prost(int64, required, tag = "1")]
    pub prev_id: i64,
    #[prost(message, required, tag = "2")]
    pub filter: CacheDirectiveInfoProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CacheDirectiveEntryProto {
    #[prost(message, required, tag = "1")]
    pub info: CacheDirectiveInfoProto,
    #[prost(message, required, tag = "2")]
    pub stats: CacheDirectiveStatsProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCacheDirectivesResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub elements: ::prost::alloc::vec::Vec<CacheDirectiveEntryProto>,
    #[prost(bool, required, tag = "2")]
    pub has_more: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CachePoolInfoProto {
    #[prost(string, optional, tag = "1")]
    pub pool_name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "2")]
    pub owner_name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "3")]
    pub group_name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(int32, optional, tag = "4")]
    pub mode: ::core::option::Option<i32>,
    #[prost(int64, optional, tag = "5")]
    pub limit: ::core::option::Option<i64>,
    #[prost(int64, optional, tag = "6")]
    pub max_relative_expiry: ::core::option::Option<i64>,
    #[prost(uint32, optional, tag = "7", default = "1")]
    pub default_replication: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CachePoolStatsProto {
    #[prost(int64, required, tag = "1")]
    pub bytes_needed: i64,
    #[prost(int64, required, tag = "2")]
    pub bytes_cached: i64,
    #[prost(int64, required, tag = "3")]
    pub bytes_overlimit: i64,
    #[prost(int64, required, tag = "4")]
    pub files_needed: i64,
    #[prost(int64, required, tag = "5")]
    pub files_cached: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddCachePoolRequestProto {
    #[prost(message, required, tag = "1")]
    pub info: CachePoolInfoProto,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddCachePoolResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyCachePoolRequestProto {
    #[prost(message, required, tag = "1")]
    pub info: CachePoolInfoProto,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyCachePoolResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveCachePoolRequestProto {
    #[prost(string, required, tag = "1")]
    pub pool_name: ::prost::alloc::string::String,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveCachePoolResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCachePoolsRequestProto {
    #[prost(string, required, tag = "1")]
    pub prev_pool_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCachePoolsResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub entries: ::prost::alloc::vec::Vec<CachePoolEntryProto>,
    #[prost(bool, required, tag = "2")]
    pub has_more: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CachePoolEntryProto {
    #[prost(message, required, tag = "1")]
    pub info: CachePoolInfoProto,
    #[prost(message, required, tag = "2")]
    pub stats: CachePoolStatsProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFileLinkInfoRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFileLinkInfoResponseProto {
    #[prost(message, optional, tag = "1")]
    pub fs: ::core::option::Option<HdfsFileStatusProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetContentSummaryRequestProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetContentSummaryResponseProto {
    #[prost(message, required, tag = "1")]
    pub summary: ContentSummaryProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetQuotaUsageRequestProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetQuotaUsageResponseProto {
    #[prost(message, required, tag = "1")]
    pub usage: QuotaUsageProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetQuotaRequestProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(uint64, required, tag = "2")]
    pub namespace_quota: u64,
    #[prost(uint64, required, tag = "3")]
    pub storagespace_quota: u64,
    #[prost(enumeration = "StorageTypeProto", optional, tag = "4")]
    pub storage_type: ::core::option::Option<i32>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetQuotaResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FsyncRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub client: ::prost::alloc::string::String,
    #[prost(sint64, optional, tag = "3", default = "-1")]
    pub last_block_length: ::core::option::Option<i64>,
    /// default to GRANDFATHER_INODE_ID
    #[prost(uint64, optional, tag = "4", default = "0")]
    pub file_id: ::core::option::Option<u64>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FsyncResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetTimesRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
    #[prost(uint64, required, tag = "2")]
    pub mtime: u64,
    #[prost(uint64, required, tag = "3")]
    pub atime: u64,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetTimesResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSymlinkRequestProto {
    #[prost(string, required, tag = "1")]
    pub target: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub link: ::prost::alloc::string::String,
    #[prost(message, required, tag = "3")]
    pub dir_perm: FsPermissionProto,
    #[prost(bool, required, tag = "4")]
    pub create_parent: bool,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSymlinkResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLinkTargetRequestProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLinkTargetResponseProto {
    #[prost(string, optional, tag = "1")]
    pub target_path: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateBlockForPipelineRequestProto {
    #[prost(message, required, tag = "1")]
    pub block: ExtendedBlockProto,
    #[prost(string, required, tag = "2")]
    pub client_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateBlockForPipelineResponseProto {
    #[prost(message, required, tag = "1")]
    pub block: LocatedBlockProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdatePipelineRequestProto {
    #[prost(string, required, tag = "1")]
    pub client_name: ::prost::alloc::string::String,
    #[prost(message, required, tag = "2")]
    pub old_block: ExtendedBlockProto,
    #[prost(message, required, tag = "3")]
    pub new_block: ExtendedBlockProto,
    #[prost(message, repeated, tag = "4")]
    pub new_nodes: ::prost::alloc::vec::Vec<DatanodeIdProto>,
    #[prost(string, repeated, tag = "5")]
    pub storage_i_ds: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdatePipelineResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetBalancerBandwidthRequestProto {
    #[prost(int64, required, tag = "1")]
    pub bandwidth: i64,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetBalancerBandwidthResponseProto {}
/// no parameters
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDataEncryptionKeyRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDataEncryptionKeyResponseProto {
    #[prost(message, optional, tag = "1")]
    pub data_encryption_key: ::core::option::Option<DataEncryptionKeyProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSnapshotRequestProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_root: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub snapshot_name: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSnapshotResponseProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameSnapshotRequestProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_root: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub snapshot_old_name: ::prost::alloc::string::String,
    #[prost(string, required, tag = "3")]
    pub snapshot_new_name: ::prost::alloc::string::String,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameSnapshotResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AllowSnapshotRequestProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_root: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AllowSnapshotResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DisallowSnapshotRequestProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_root: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DisallowSnapshotResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSnapshotRequestProto {
    #[prost(string, required, tag = "1")]
    pub snapshot_root: ::prost::alloc::string::String,
    #[prost(string, required, tag = "2")]
    pub snapshot_name: ::prost::alloc::string::String,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSnapshotResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CheckAccessRequestProto {
    #[prost(string, required, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(enumeration = "acl_entry_proto::FsActionProto", required, tag = "2")]
    pub mode: i32,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CheckAccessResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCurrentEditLogTxidRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCurrentEditLogTxidResponseProto {
    #[prost(int64, required, tag = "1")]
    pub txid: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEditsFromTxidRequestProto {
    #[prost(int64, required, tag = "1")]
    pub txid: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEditsFromTxidResponseProto {
    #[prost(message, required, tag = "1")]
    pub events_list: EventsListProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListOpenFilesRequestProto {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
    #[prost(enumeration = "OpenFilesTypeProto", repeated, packed = "false", tag = "2")]
    pub types: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, optional, tag = "3")]
    pub path: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenFilesBatchResponseProto {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
    #[prost(string, required, tag = "2")]
    pub path: ::prost::alloc::string::String,
    #[prost(string, required, tag = "3")]
    pub client_name: ::prost::alloc::string::String,
    #[prost(string, required, tag = "4")]
    pub client_machine: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListOpenFilesResponseProto {
    #[prost(message, repeated, tag = "1")]
    pub entries: ::prost::alloc::vec::Vec<OpenFilesBatchResponseProto>,
    #[prost(bool, required, tag = "2")]
    pub has_more: bool,
    #[prost(enumeration = "OpenFilesTypeProto", repeated, packed = "false", tag = "3")]
    pub types: ::prost::alloc::vec::Vec<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MsyncRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MsyncResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SatisfyStoragePolicyRequestProto {
    #[prost(string, required, tag = "1")]
    pub src: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SatisfyStoragePolicyResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HaServiceStateRequestProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HaServiceStateResponseProto {
    #[prost(enumeration = "super::common::HaServiceStateProto", required, tag = "1")]
    pub state: i32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CreateFlagProto {
    /// Create a file
    Create = 1,
    /// Truncate/overwrite a file. Same as POSIX O_TRUNC
    Overwrite = 2,
    /// Append to a file
    Append = 4,
    /// File with reduced durability guarantees.
    LazyPersist = 16,
    /// Write data to a new block when appending
    NewBlock = 32,
    /// Enforce to create a replicate file
    ShouldReplicate = 128,
}
impl CreateFlagProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CreateFlagProto::Create => "CREATE",
            CreateFlagProto::Overwrite => "OVERWRITE",
            CreateFlagProto::Append => "APPEND",
            CreateFlagProto::LazyPersist => "LAZY_PERSIST",
            CreateFlagProto::NewBlock => "NEW_BLOCK",
            CreateFlagProto::ShouldReplicate => "SHOULD_REPLICATE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CREATE" => Some(Self::Create),
            "OVERWRITE" => Some(Self::Overwrite),
            "APPEND" => Some(Self::Append),
            "LAZY_PERSIST" => Some(Self::LazyPersist),
            "NEW_BLOCK" => Some(Self::NewBlock),
            "SHOULD_REPLICATE" => Some(Self::ShouldReplicate),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AddBlockFlagProto {
    /// avoid writing to local node.
    NoLocalWrite = 1,
    /// write to a random node
    IgnoreClientLocality = 2,
}
impl AddBlockFlagProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AddBlockFlagProto::NoLocalWrite => "NO_LOCAL_WRITE",
            AddBlockFlagProto::IgnoreClientLocality => "IGNORE_CLIENT_LOCALITY",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NO_LOCAL_WRITE" => Some(Self::NoLocalWrite),
            "IGNORE_CLIENT_LOCALITY" => Some(Self::IgnoreClientLocality),
            _ => None,
        }
    }
}
/// type of the datanode report
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DatanodeReportTypeProto {
    All = 1,
    Live = 2,
    Dead = 3,
    Decommissioning = 4,
    EnteringMaintenance = 5,
    InMaintenance = 6,
}
impl DatanodeReportTypeProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DatanodeReportTypeProto::All => "ALL",
            DatanodeReportTypeProto::Live => "LIVE",
            DatanodeReportTypeProto::Dead => "DEAD",
            DatanodeReportTypeProto::Decommissioning => "DECOMMISSIONING",
            DatanodeReportTypeProto::EnteringMaintenance => "ENTERING_MAINTENANCE",
            DatanodeReportTypeProto::InMaintenance => "IN_MAINTENANCE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ALL" => Some(Self::All),
            "LIVE" => Some(Self::Live),
            "DEAD" => Some(Self::Dead),
            "DECOMMISSIONING" => Some(Self::Decommissioning),
            "ENTERING_MAINTENANCE" => Some(Self::EnteringMaintenance),
            "IN_MAINTENANCE" => Some(Self::InMaintenance),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SafeModeActionProto {
    SafemodeLeave = 1,
    SafemodeEnter = 2,
    SafemodeGet = 3,
    SafemodeForceExit = 4,
}
impl SafeModeActionProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SafeModeActionProto::SafemodeLeave => "SAFEMODE_LEAVE",
            SafeModeActionProto::SafemodeEnter => "SAFEMODE_ENTER",
            SafeModeActionProto::SafemodeGet => "SAFEMODE_GET",
            SafeModeActionProto::SafemodeForceExit => "SAFEMODE_FORCE_EXIT",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SAFEMODE_LEAVE" => Some(Self::SafemodeLeave),
            "SAFEMODE_ENTER" => Some(Self::SafemodeEnter),
            "SAFEMODE_GET" => Some(Self::SafemodeGet),
            "SAFEMODE_FORCE_EXIT" => Some(Self::SafemodeForceExit),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum RollingUpgradeActionProto {
    Query = 1,
    Start = 2,
    Finalize = 3,
}
impl RollingUpgradeActionProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            RollingUpgradeActionProto::Query => "QUERY",
            RollingUpgradeActionProto::Start => "START",
            RollingUpgradeActionProto::Finalize => "FINALIZE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "QUERY" => Some(Self::Query),
            "START" => Some(Self::Start),
            "FINALIZE" => Some(Self::Finalize),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CacheFlagProto {
    /// Ignore pool resource limits
    Force = 1,
}
impl CacheFlagProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CacheFlagProto::Force => "FORCE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "FORCE" => Some(Self::Force),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum OpenFilesTypeProto {
    AllOpenFiles = 1,
    BlockingDecommission = 2,
}
impl OpenFilesTypeProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            OpenFilesTypeProto::AllOpenFiles => "ALL_OPEN_FILES",
            OpenFilesTypeProto::BlockingDecommission => "BLOCKING_DECOMMISSION",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ALL_OPEN_FILES" => Some(Self::AllOpenFiles),
            "BLOCKING_DECOMMISSION" => Some(Self::BlockingDecommission),
            _ => None,
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataTransferEncryptorMessageProto {
    #[prost(
        enumeration = "data_transfer_encryptor_message_proto::DataTransferEncryptorStatus",
        required,
        tag = "1"
    )]
    pub status: i32,
    #[prost(bytes = "vec", optional, tag = "2")]
    pub payload: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(string, optional, tag = "3")]
    pub message: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, repeated, tag = "4")]
    pub cipher_option: ::prost::alloc::vec::Vec<CipherOptionProto>,
    #[prost(message, optional, tag = "5")]
    pub handshake_secret: ::core::option::Option<HandshakeSecretProto>,
    #[prost(bool, optional, tag = "6")]
    pub access_token_error: ::core::option::Option<bool>,
}
/// Nested message and enum types in `DataTransferEncryptorMessageProto`.
pub mod data_transfer_encryptor_message_proto {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum DataTransferEncryptorStatus {
        Success = 0,
        ErrorUnknownKey = 1,
        Error = 2,
    }
    impl DataTransferEncryptorStatus {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                DataTransferEncryptorStatus::Success => "SUCCESS",
                DataTransferEncryptorStatus::ErrorUnknownKey => "ERROR_UNKNOWN_KEY",
                DataTransferEncryptorStatus::Error => "ERROR",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "SUCCESS" => Some(Self::Success),
                "ERROR_UNKNOWN_KEY" => Some(Self::ErrorUnknownKey),
                "ERROR" => Some(Self::Error),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HandshakeSecretProto {
    #[prost(bytes = "vec", required, tag = "1")]
    pub secret: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, required, tag = "2")]
    pub bpid: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BaseHeaderProto {
    #[prost(message, required, tag = "1")]
    pub block: ExtendedBlockProto,
    #[prost(message, optional, tag = "2")]
    pub token: ::core::option::Option<super::common::TokenProto>,
    #[prost(message, optional, tag = "3")]
    pub trace_info: ::core::option::Option<DataTransferTraceInfoProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataTransferTraceInfoProto {
    #[prost(uint64, optional, tag = "1")]
    pub trace_id: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub parent_id: ::core::option::Option<u64>,
    #[prost(bytes = "vec", optional, tag = "3")]
    pub span_context: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClientOperationHeaderProto {
    #[prost(message, required, tag = "1")]
    pub base_header: BaseHeaderProto,
    #[prost(string, required, tag = "2")]
    pub client_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CachingStrategyProto {
    #[prost(bool, optional, tag = "1")]
    pub drop_behind: ::core::option::Option<bool>,
    #[prost(int64, optional, tag = "2")]
    pub readahead: ::core::option::Option<i64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpReadBlockProto {
    #[prost(message, required, tag = "1")]
    pub header: ClientOperationHeaderProto,
    #[prost(uint64, required, tag = "2")]
    pub offset: u64,
    #[prost(uint64, required, tag = "3")]
    pub len: u64,
    #[prost(bool, optional, tag = "4", default = "true")]
    pub send_checksums: ::core::option::Option<bool>,
    #[prost(message, optional, tag = "5")]
    pub caching_strategy: ::core::option::Option<CachingStrategyProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChecksumProto {
    #[prost(enumeration = "ChecksumTypeProto", required, tag = "1")]
    pub r#type: i32,
    #[prost(uint32, required, tag = "2")]
    pub bytes_per_checksum: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpWriteBlockProto {
    #[prost(message, required, tag = "1")]
    pub header: ClientOperationHeaderProto,
    #[prost(message, repeated, tag = "2")]
    pub targets: ::prost::alloc::vec::Vec<DatanodeInfoProto>,
    #[prost(message, optional, tag = "3")]
    pub source: ::core::option::Option<DatanodeInfoProto>,
    #[prost(
        enumeration = "op_write_block_proto::BlockConstructionStage",
        required,
        tag = "4"
    )]
    pub stage: i32,
    #[prost(uint32, required, tag = "5")]
    pub pipeline_size: u32,
    #[prost(uint64, required, tag = "6")]
    pub min_bytes_rcvd: u64,
    #[prost(uint64, required, tag = "7")]
    pub max_bytes_rcvd: u64,
    #[prost(uint64, required, tag = "8")]
    pub latest_generation_stamp: u64,
    /// *
    /// The requested checksum mechanism for this block write.
    #[prost(message, required, tag = "9")]
    pub requested_checksum: ChecksumProto,
    #[prost(message, optional, tag = "10")]
    pub caching_strategy: ::core::option::Option<CachingStrategyProto>,
    #[prost(enumeration = "StorageTypeProto", optional, tag = "11", default = "Disk")]
    pub storage_type: ::core::option::Option<i32>,
    #[prost(enumeration = "StorageTypeProto", repeated, packed = "false", tag = "12")]
    pub target_storage_types: ::prost::alloc::vec::Vec<i32>,
    /// *
    /// Hint to the DataNode that the block can be allocated on transient
    /// storage i.e. memory and written to disk lazily. The DataNode is free
    /// to ignore this hint.
    #[prost(bool, optional, tag = "13", default = "false")]
    pub allow_lazy_persist: ::core::option::Option<bool>,
    /// whether to pin the block, so Balancer won't move it.
    #[prost(bool, optional, tag = "14", default = "false")]
    pub pinning: ::core::option::Option<bool>,
    #[prost(bool, repeated, packed = "false", tag = "15")]
    pub target_pinnings: ::prost::alloc::vec::Vec<bool>,
    #[prost(string, optional, tag = "16")]
    pub storage_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "17")]
    pub target_storage_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Nested message and enum types in `OpWriteBlockProto`.
pub mod op_write_block_proto {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum BlockConstructionStage {
        PipelineSetupAppend = 0,
        /// pipeline set up for failed PIPELINE_SETUP_APPEND recovery
        PipelineSetupAppendRecovery = 1,
        /// data streaming
        DataStreaming = 2,
        /// pipeline setup for failed data streaming recovery
        PipelineSetupStreamingRecovery = 3,
        /// close the block and pipeline
        PipelineClose = 4,
        /// Recover a failed PIPELINE_CLOSE
        PipelineCloseRecovery = 5,
        /// pipeline set up for block creation
        PipelineSetupCreate = 6,
        /// transfer RBW for adding datanodes
        TransferRbw = 7,
        /// transfer Finalized for adding datanodes
        TransferFinalized = 8,
    }
    impl BlockConstructionStage {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                BlockConstructionStage::PipelineSetupAppend => "PIPELINE_SETUP_APPEND",
                BlockConstructionStage::PipelineSetupAppendRecovery => {
                    "PIPELINE_SETUP_APPEND_RECOVERY"
                }
                BlockConstructionStage::DataStreaming => "DATA_STREAMING",
                BlockConstructionStage::PipelineSetupStreamingRecovery => {
                    "PIPELINE_SETUP_STREAMING_RECOVERY"
                }
                BlockConstructionStage::PipelineClose => "PIPELINE_CLOSE",
                BlockConstructionStage::PipelineCloseRecovery => {
                    "PIPELINE_CLOSE_RECOVERY"
                }
                BlockConstructionStage::PipelineSetupCreate => "PIPELINE_SETUP_CREATE",
                BlockConstructionStage::TransferRbw => "TRANSFER_RBW",
                BlockConstructionStage::TransferFinalized => "TRANSFER_FINALIZED",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "PIPELINE_SETUP_APPEND" => Some(Self::PipelineSetupAppend),
                "PIPELINE_SETUP_APPEND_RECOVERY" => {
                    Some(Self::PipelineSetupAppendRecovery)
                }
                "DATA_STREAMING" => Some(Self::DataStreaming),
                "PIPELINE_SETUP_STREAMING_RECOVERY" => {
                    Some(Self::PipelineSetupStreamingRecovery)
                }
                "PIPELINE_CLOSE" => Some(Self::PipelineClose),
                "PIPELINE_CLOSE_RECOVERY" => Some(Self::PipelineCloseRecovery),
                "PIPELINE_SETUP_CREATE" => Some(Self::PipelineSetupCreate),
                "TRANSFER_RBW" => Some(Self::TransferRbw),
                "TRANSFER_FINALIZED" => Some(Self::TransferFinalized),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpTransferBlockProto {
    #[prost(message, required, tag = "1")]
    pub header: ClientOperationHeaderProto,
    #[prost(message, repeated, tag = "2")]
    pub targets: ::prost::alloc::vec::Vec<DatanodeInfoProto>,
    #[prost(enumeration = "StorageTypeProto", repeated, packed = "false", tag = "3")]
    pub target_storage_types: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, repeated, tag = "4")]
    pub target_storage_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpReplaceBlockProto {
    #[prost(message, required, tag = "1")]
    pub header: BaseHeaderProto,
    #[prost(string, required, tag = "2")]
    pub del_hint: ::prost::alloc::string::String,
    #[prost(message, required, tag = "3")]
    pub source: DatanodeInfoProto,
    #[prost(enumeration = "StorageTypeProto", optional, tag = "4", default = "Disk")]
    pub storage_type: ::core::option::Option<i32>,
    #[prost(string, optional, tag = "5")]
    pub storage_id: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpCopyBlockProto {
    #[prost(message, required, tag = "1")]
    pub header: BaseHeaderProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpBlockChecksumProto {
    #[prost(message, required, tag = "1")]
    pub header: BaseHeaderProto,
    #[prost(message, optional, tag = "2")]
    pub block_checksum_options: ::core::option::Option<BlockChecksumOptionsProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpBlockGroupChecksumProto {
    #[prost(message, required, tag = "1")]
    pub header: BaseHeaderProto,
    #[prost(message, required, tag = "2")]
    pub datanodes: DatanodeInfosProto,
    /// each internal block has a block token
    #[prost(message, repeated, tag = "3")]
    pub block_tokens: ::prost::alloc::vec::Vec<super::common::TokenProto>,
    #[prost(message, required, tag = "4")]
    pub ec_policy: ErasureCodingPolicyProto,
    #[prost(uint32, repeated, packed = "false", tag = "5")]
    pub block_indices: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint64, required, tag = "6")]
    pub requested_num_bytes: u64,
    #[prost(message, optional, tag = "7")]
    pub block_checksum_options: ::core::option::Option<BlockChecksumOptionsProto>,
}
/// *
/// An ID uniquely identifying a shared memory segment.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShortCircuitShmIdProto {
    #[prost(int64, required, tag = "1")]
    pub hi: i64,
    #[prost(int64, required, tag = "2")]
    pub lo: i64,
}
/// *
/// An ID uniquely identifying a slot within a shared memory segment.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShortCircuitShmSlotProto {
    #[prost(message, required, tag = "1")]
    pub shm_id: ShortCircuitShmIdProto,
    #[prost(int32, required, tag = "2")]
    pub slot_idx: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpRequestShortCircuitAccessProto {
    #[prost(message, required, tag = "1")]
    pub header: BaseHeaderProto,
    /// * In order to get short-circuit access to block data, clients must set this
    /// to the highest version of the block data that they can understand.
    /// Currently 1 is the only version, but more versions may exist in the future
    /// if the on-disk format changes.
    #[prost(uint32, required, tag = "2")]
    pub max_version: u32,
    /// *
    /// The shared memory slot to use, if we are using one.
    #[prost(message, optional, tag = "3")]
    pub slot_id: ::core::option::Option<ShortCircuitShmSlotProto>,
    /// *
    /// True if the client supports verifying that the file descriptor has been
    /// sent successfully.
    #[prost(bool, optional, tag = "4", default = "false")]
    pub supports_receipt_verification: ::core::option::Option<bool>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReleaseShortCircuitAccessRequestProto {
    #[prost(message, required, tag = "1")]
    pub slot_id: ShortCircuitShmSlotProto,
    #[prost(message, optional, tag = "2")]
    pub trace_info: ::core::option::Option<DataTransferTraceInfoProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReleaseShortCircuitAccessResponseProto {
    #[prost(enumeration = "Status", required, tag = "1")]
    pub status: i32,
    #[prost(string, optional, tag = "2")]
    pub error: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShortCircuitShmRequestProto {
    /// The name of the client requesting the shared memory segment.  This is
    /// purely for logging / debugging purposes.
    #[prost(string, required, tag = "1")]
    pub client_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub trace_info: ::core::option::Option<DataTransferTraceInfoProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShortCircuitShmResponseProto {
    #[prost(enumeration = "Status", required, tag = "1")]
    pub status: i32,
    #[prost(string, optional, tag = "2")]
    pub error: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "3")]
    pub id: ::core::option::Option<ShortCircuitShmIdProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PacketHeaderProto {
    /// All fields must be fixed-length!
    #[prost(sfixed64, required, tag = "1")]
    pub offset_in_block: i64,
    #[prost(sfixed64, required, tag = "2")]
    pub seqno: i64,
    #[prost(bool, required, tag = "3")]
    pub last_packet_in_block: bool,
    #[prost(sfixed32, required, tag = "4")]
    pub data_len: i32,
    #[prost(bool, optional, tag = "5", default = "false")]
    pub sync_block: ::core::option::Option<bool>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PipelineAckProto {
    #[prost(sint64, required, tag = "1")]
    pub seqno: i64,
    #[prost(enumeration = "Status", repeated, packed = "false", tag = "2")]
    pub reply: ::prost::alloc::vec::Vec<i32>,
    #[prost(uint64, optional, tag = "3", default = "0")]
    pub downstream_ack_time_nanos: ::core::option::Option<u64>,
    #[prost(uint32, repeated, tag = "4")]
    pub flag: ::prost::alloc::vec::Vec<u32>,
}
/// *
/// Sent as part of the BlockOpResponseProto
/// for READ_BLOCK and COPY_BLOCK operations.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadOpChecksumInfoProto {
    #[prost(message, required, tag = "1")]
    pub checksum: ChecksumProto,
    /// *
    /// The offset into the block at which the first packet
    /// will start. This is necessary since reads will align
    /// backwards to a checksum chunk boundary.
    #[prost(uint64, required, tag = "2")]
    pub chunk_offset: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockOpResponseProto {
    #[prost(enumeration = "Status", required, tag = "1")]
    pub status: i32,
    #[prost(string, optional, tag = "2")]
    pub first_bad_link: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "3")]
    pub checksum_response: ::core::option::Option<OpBlockChecksumResponseProto>,
    #[prost(message, optional, tag = "4")]
    pub read_op_checksum_info: ::core::option::Option<ReadOpChecksumInfoProto>,
    /// * explanatory text which may be useful to log on the client side
    #[prost(string, optional, tag = "5")]
    pub message: ::core::option::Option<::prost::alloc::string::String>,
    /// * If the server chooses to agree to the request of a client for
    /// short-circuit access, it will send a response message with the relevant
    /// file descriptors attached.
    ///
    /// In the body of the message, this version number will be set to the
    /// specific version number of the block data that the client is about to
    /// read.
    #[prost(uint32, optional, tag = "6")]
    pub short_circuit_access_version: ::core::option::Option<u32>,
}
/// *
/// Message sent from the client to the DN after reading the entire
/// read request.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClientReadStatusProto {
    #[prost(enumeration = "Status", required, tag = "1")]
    pub status: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DnTransferAckProto {
    #[prost(enumeration = "Status", required, tag = "1")]
    pub status: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpBlockChecksumResponseProto {
    #[prost(uint32, required, tag = "1")]
    pub bytes_per_crc: u32,
    #[prost(uint64, required, tag = "2")]
    pub crc_per_block: u64,
    #[prost(bytes = "vec", required, tag = "3")]
    pub block_checksum: ::prost::alloc::vec::Vec<u8>,
    #[prost(enumeration = "ChecksumTypeProto", optional, tag = "4")]
    pub crc_type: ::core::option::Option<i32>,
    #[prost(message, optional, tag = "5")]
    pub block_checksum_options: ::core::option::Option<BlockChecksumOptionsProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpCustomProto {
    #[prost(string, required, tag = "1")]
    pub custom_id: ::prost::alloc::string::String,
}
/// Status is a 4-bit enum
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Status {
    Success = 0,
    Error = 1,
    ErrorChecksum = 2,
    ErrorInvalid = 3,
    ErrorExists = 4,
    ErrorAccessToken = 5,
    ChecksumOk = 6,
    ErrorUnsupported = 7,
    /// Quick restart
    OobRestart = 8,
    /// Reserved
    OobReserved1 = 9,
    /// Reserved
    OobReserved2 = 10,
    /// Reserved
    OobReserved3 = 11,
    InProgress = 12,
    ErrorBlockPinned = 13,
}
impl Status {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Status::Success => "SUCCESS",
            Status::Error => "ERROR",
            Status::ErrorChecksum => "ERROR_CHECKSUM",
            Status::ErrorInvalid => "ERROR_INVALID",
            Status::ErrorExists => "ERROR_EXISTS",
            Status::ErrorAccessToken => "ERROR_ACCESS_TOKEN",
            Status::ChecksumOk => "CHECKSUM_OK",
            Status::ErrorUnsupported => "ERROR_UNSUPPORTED",
            Status::OobRestart => "OOB_RESTART",
            Status::OobReserved1 => "OOB_RESERVED1",
            Status::OobReserved2 => "OOB_RESERVED2",
            Status::OobReserved3 => "OOB_RESERVED3",
            Status::InProgress => "IN_PROGRESS",
            Status::ErrorBlockPinned => "ERROR_BLOCK_PINNED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SUCCESS" => Some(Self::Success),
            "ERROR" => Some(Self::Error),
            "ERROR_CHECKSUM" => Some(Self::ErrorChecksum),
            "ERROR_INVALID" => Some(Self::ErrorInvalid),
            "ERROR_EXISTS" => Some(Self::ErrorExists),
            "ERROR_ACCESS_TOKEN" => Some(Self::ErrorAccessToken),
            "CHECKSUM_OK" => Some(Self::ChecksumOk),
            "ERROR_UNSUPPORTED" => Some(Self::ErrorUnsupported),
            "OOB_RESTART" => Some(Self::OobRestart),
            "OOB_RESERVED1" => Some(Self::OobReserved1),
            "OOB_RESERVED2" => Some(Self::OobReserved2),
            "OOB_RESERVED3" => Some(Self::OobReserved3),
            "IN_PROGRESS" => Some(Self::InProgress),
            "ERROR_BLOCK_PINNED" => Some(Self::ErrorBlockPinned),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ShortCircuitFdResponse {
    DoNotUseReceiptVerification = 0,
    UseReceiptVerification = 1,
}
impl ShortCircuitFdResponse {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ShortCircuitFdResponse::DoNotUseReceiptVerification => {
                "DO_NOT_USE_RECEIPT_VERIFICATION"
            }
            ShortCircuitFdResponse::UseReceiptVerification => "USE_RECEIPT_VERIFICATION",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "DO_NOT_USE_RECEIPT_VERIFICATION" => Some(Self::DoNotUseReceiptVerification),
            "USE_RECEIPT_VERIFICATION" => Some(Self::UseReceiptVerification),
            _ => None,
        }
    }
}
