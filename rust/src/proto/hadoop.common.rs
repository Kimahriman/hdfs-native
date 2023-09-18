/// *
/// Security token identifier
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TokenProto {
    #[prost(bytes = "vec", required, tag = "1")]
    pub identifier: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", required, tag = "2")]
    pub password: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, required, tag = "3")]
    pub kind: ::prost::alloc::string::String,
    #[prost(string, required, tag = "4")]
    pub service: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CredentialsKvProto {
    #[prost(string, required, tag = "1")]
    pub alias: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub token: ::core::option::Option<TokenProto>,
    #[prost(bytes = "vec", optional, tag = "3")]
    pub secret: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CredentialsProto {
    #[prost(message, repeated, tag = "1")]
    pub tokens: ::prost::alloc::vec::Vec<CredentialsKvProto>,
    #[prost(message, repeated, tag = "2")]
    pub secrets: ::prost::alloc::vec::Vec<CredentialsKvProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDelegationTokenRequestProto {
    #[prost(string, required, tag = "1")]
    pub renewer: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDelegationTokenResponseProto {
    #[prost(message, optional, tag = "1")]
    pub token: ::core::option::Option<TokenProto>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenewDelegationTokenRequestProto {
    #[prost(message, required, tag = "1")]
    pub token: TokenProto,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenewDelegationTokenResponseProto {
    #[prost(uint64, required, tag = "1")]
    pub new_expiry_time: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelDelegationTokenRequestProto {
    #[prost(message, required, tag = "1")]
    pub token: TokenProto,
}
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelDelegationTokenResponseProto {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HaStateChangeRequestInfoProto {
    #[prost(enumeration = "HaRequestSource", required, tag = "1")]
    pub req_source: i32,
}
/// *
/// void request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MonitorHealthRequestProto {}
/// *
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MonitorHealthResponseProto {}
/// *
/// void request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransitionToActiveRequestProto {
    #[prost(message, required, tag = "1")]
    pub req_info: HaStateChangeRequestInfoProto,
}
/// *
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransitionToActiveResponseProto {}
/// *
/// void request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransitionToStandbyRequestProto {
    #[prost(message, required, tag = "1")]
    pub req_info: HaStateChangeRequestInfoProto,
}
/// *
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransitionToStandbyResponseProto {}
/// *
/// void request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransitionToObserverRequestProto {
    #[prost(message, required, tag = "1")]
    pub req_info: HaStateChangeRequestInfoProto,
}
/// *
/// void response
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransitionToObserverResponseProto {}
/// *
/// void request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetServiceStatusRequestProto {}
/// *
/// Returns the state of the service
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetServiceStatusResponseProto {
    #[prost(enumeration = "HaServiceStateProto", required, tag = "1")]
    pub state: i32,
    /// If state is STANDBY, indicate whether it is
    /// ready to become active.
    #[prost(bool, optional, tag = "2")]
    pub ready_to_become_active: ::core::option::Option<bool>,
    /// If not ready to become active, a textual explanation of why not
    #[prost(string, optional, tag = "3")]
    pub not_ready_reason: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum HaServiceStateProto {
    Initializing = 0,
    Active = 1,
    Standby = 2,
    Observer = 3,
}
impl HaServiceStateProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            HaServiceStateProto::Initializing => "INITIALIZING",
            HaServiceStateProto::Active => "ACTIVE",
            HaServiceStateProto::Standby => "STANDBY",
            HaServiceStateProto::Observer => "OBSERVER",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INITIALIZING" => Some(Self::Initializing),
            "ACTIVE" => Some(Self::Active),
            "STANDBY" => Some(Self::Standby),
            "OBSERVER" => Some(Self::Observer),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum HaRequestSource {
    RequestByUser = 0,
    RequestByUserForced = 1,
    RequestByZkfc = 2,
}
impl HaRequestSource {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            HaRequestSource::RequestByUser => "REQUEST_BY_USER",
            HaRequestSource::RequestByUserForced => "REQUEST_BY_USER_FORCED",
            HaRequestSource::RequestByZkfc => "REQUEST_BY_ZKFC",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "REQUEST_BY_USER" => Some(Self::RequestByUser),
            "REQUEST_BY_USER_FORCED" => Some(Self::RequestByUserForced),
            "REQUEST_BY_ZKFC" => Some(Self::RequestByZkfc),
            _ => None,
        }
    }
}
/// *
/// Used to pass through the information necessary to continue
/// a trace after an RPC is made. All we need is the traceid
/// (so we know the overarching trace this message is a part of), and
/// the id of the current span when this message was sent, so we know
/// what span caused the new span we will create when this message is received.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RpcTraceInfoProto {
    /// parentIdHigh
    #[prost(int64, optional, tag = "1")]
    pub trace_id: ::core::option::Option<i64>,
    /// parentIdLow
    #[prost(int64, optional, tag = "2")]
    pub parent_id: ::core::option::Option<i64>,
    /// Trace SpanContext
    #[prost(bytes = "vec", optional, tag = "3")]
    pub span_context: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// *
/// Used to pass through the call context entry after an RPC is made.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RpcCallerContextProto {
    #[prost(string, required, tag = "1")]
    pub context: ::prost::alloc::string::String,
    #[prost(bytes = "vec", optional, tag = "2")]
    pub signature: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// the header for the RpcRequest
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RpcRequestHeaderProto {
    #[prost(enumeration = "RpcKindProto", optional, tag = "1")]
    pub rpc_kind: ::core::option::Option<i32>,
    #[prost(
        enumeration = "rpc_request_header_proto::OperationProto",
        optional,
        tag = "2"
    )]
    pub rpc_op: ::core::option::Option<i32>,
    /// a sequence number that is sent back in response
    #[prost(sint32, required, tag = "3")]
    pub call_id: i32,
    /// Globally unique client ID
    #[prost(bytes = "vec", required, tag = "4")]
    pub client_id: ::prost::alloc::vec::Vec<u8>,
    /// clientId + callId uniquely identifies a request
    /// retry count, 1 means this is the first retry
    #[prost(sint32, optional, tag = "5", default = "-1")]
    pub retry_count: ::core::option::Option<i32>,
    /// tracing info
    #[prost(message, optional, tag = "6")]
    pub trace_info: ::core::option::Option<RpcTraceInfoProto>,
    /// call context
    #[prost(message, optional, tag = "7")]
    pub caller_context: ::core::option::Option<RpcCallerContextProto>,
    /// The last seen Global State ID
    #[prost(int64, optional, tag = "8")]
    pub state_id: ::core::option::Option<i64>,
    /// Alignment context info for use with routers.
    /// The client should not interpret these bytes, but only forward bytes
    /// received from RpcResponseHeaderProto.routerFederatedState.
    #[prost(bytes = "vec", optional, tag = "9")]
    pub router_federated_state: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// Nested message and enum types in `RpcRequestHeaderProto`.
pub mod rpc_request_header_proto {
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
    pub enum OperationProto {
        /// The final RPC Packet
        RpcFinalPacket = 0,
        /// not implemented yet
        RpcContinuationPacket = 1,
        /// close the rpc connection
        RpcCloseConnection = 2,
    }
    impl OperationProto {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                OperationProto::RpcFinalPacket => "RPC_FINAL_PACKET",
                OperationProto::RpcContinuationPacket => "RPC_CONTINUATION_PACKET",
                OperationProto::RpcCloseConnection => "RPC_CLOSE_CONNECTION",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "RPC_FINAL_PACKET" => Some(Self::RpcFinalPacket),
                "RPC_CONTINUATION_PACKET" => Some(Self::RpcContinuationPacket),
                "RPC_CLOSE_CONNECTION" => Some(Self::RpcCloseConnection),
                _ => None,
            }
        }
    }
}
/// *
/// Rpc Response Header
/// +------------------------------------------------------------------+
/// | Rpc total response length in bytes (4 bytes int)                 |
/// |  (sum of next two parts)                                         |
/// +------------------------------------------------------------------+
/// | RpcResponseHeaderProto - serialized delimited ie has len         |
/// +------------------------------------------------------------------+
/// | if request is successful:                                        |
/// |   - RpcResponse -  The actual rpc response  bytes follow         |
/// |     the response header                                          |
/// |     This response is serialized based on RpcKindProto            |
/// | if request fails :                                               |
/// |   The rpc response header contains the necessary info            |
/// +------------------------------------------------------------------+
///
/// Note that rpc response header is also used when connection setup fails.
/// Ie the response looks like a rpc response with a fake callId.
///
/// *
///
/// RpcStastus - success or failure
/// The reponseHeader's errDetail,  exceptionClassName and errMsg contains
/// further details on the error
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RpcResponseHeaderProto {
    /// callId used in Request
    #[prost(uint32, required, tag = "1")]
    pub call_id: u32,
    #[prost(
        enumeration = "rpc_response_header_proto::RpcStatusProto",
        required,
        tag = "2"
    )]
    pub status: i32,
    /// Sent if success or fail
    #[prost(uint32, optional, tag = "3")]
    pub server_ipc_version_num: ::core::option::Option<u32>,
    /// if request fails
    #[prost(string, optional, tag = "4")]
    pub exception_class_name: ::core::option::Option<::prost::alloc::string::String>,
    /// if request fails, often contains strack trace
    #[prost(string, optional, tag = "5")]
    pub error_msg: ::core::option::Option<::prost::alloc::string::String>,
    /// in case of error
    #[prost(
        enumeration = "rpc_response_header_proto::RpcErrorCodeProto",
        optional,
        tag = "6"
    )]
    pub error_detail: ::core::option::Option<i32>,
    /// Globally unique client ID
    #[prost(bytes = "vec", optional, tag = "7")]
    pub client_id: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(sint32, optional, tag = "8", default = "-1")]
    pub retry_count: ::core::option::Option<i32>,
    /// The last written Global State ID
    #[prost(int64, optional, tag = "9")]
    pub state_id: ::core::option::Option<i64>,
    /// Alignment context info for use with routers.
    /// The client should not interpret these bytes, but only
    /// forward them to the router using RpcRequestHeaderProto.routerFederatedState.
    #[prost(bytes = "vec", optional, tag = "10")]
    pub router_federated_state: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// Nested message and enum types in `RpcResponseHeaderProto`.
pub mod rpc_response_header_proto {
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
    pub enum RpcStatusProto {
        /// RPC succeeded
        Success = 0,
        /// RPC or error - connection left open for future calls
        Error = 1,
        /// Fatal error - connection closed
        Fatal = 2,
    }
    impl RpcStatusProto {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                RpcStatusProto::Success => "SUCCESS",
                RpcStatusProto::Error => "ERROR",
                RpcStatusProto::Fatal => "FATAL",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "SUCCESS" => Some(Self::Success),
                "ERROR" => Some(Self::Error),
                "FATAL" => Some(Self::Fatal),
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
    pub enum RpcErrorCodeProto {
        /// Non-fatal Rpc error - connection left open for future rpc calls
        ///
        /// RPC Failed - rpc app threw exception
        ErrorApplication = 1,
        /// Rpc error - no such method
        ErrorNoSuchMethod = 2,
        /// Rpc error - no such protocol
        ErrorNoSuchProtocol = 3,
        /// Rpc error on server side
        ErrorRpcServer = 4,
        /// error serializign response
        ErrorSerializingResponse = 5,
        /// Rpc protocol version mismatch
        ErrorRpcVersionMismatch = 6,
        /// Fatal Server side Rpc error - connection closed
        ///
        /// unknown Fatal error
        FatalUnknown = 10,
        /// IPC layer serilization type invalid
        FatalUnsupportedSerialization = 11,
        /// fields of RpcHeader are invalid
        FatalInvalidRpcHeader = 12,
        /// could not deserilize rpc request
        FatalDeserializingRequest = 13,
        /// Ipc Layer version mismatch
        FatalVersionMismatch = 14,
        /// Auth failed
        FatalUnauthorized = 15,
    }
    impl RpcErrorCodeProto {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                RpcErrorCodeProto::ErrorApplication => "ERROR_APPLICATION",
                RpcErrorCodeProto::ErrorNoSuchMethod => "ERROR_NO_SUCH_METHOD",
                RpcErrorCodeProto::ErrorNoSuchProtocol => "ERROR_NO_SUCH_PROTOCOL",
                RpcErrorCodeProto::ErrorRpcServer => "ERROR_RPC_SERVER",
                RpcErrorCodeProto::ErrorSerializingResponse => {
                    "ERROR_SERIALIZING_RESPONSE"
                }
                RpcErrorCodeProto::ErrorRpcVersionMismatch => {
                    "ERROR_RPC_VERSION_MISMATCH"
                }
                RpcErrorCodeProto::FatalUnknown => "FATAL_UNKNOWN",
                RpcErrorCodeProto::FatalUnsupportedSerialization => {
                    "FATAL_UNSUPPORTED_SERIALIZATION"
                }
                RpcErrorCodeProto::FatalInvalidRpcHeader => "FATAL_INVALID_RPC_HEADER",
                RpcErrorCodeProto::FatalDeserializingRequest => {
                    "FATAL_DESERIALIZING_REQUEST"
                }
                RpcErrorCodeProto::FatalVersionMismatch => "FATAL_VERSION_MISMATCH",
                RpcErrorCodeProto::FatalUnauthorized => "FATAL_UNAUTHORIZED",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "ERROR_APPLICATION" => Some(Self::ErrorApplication),
                "ERROR_NO_SUCH_METHOD" => Some(Self::ErrorNoSuchMethod),
                "ERROR_NO_SUCH_PROTOCOL" => Some(Self::ErrorNoSuchProtocol),
                "ERROR_RPC_SERVER" => Some(Self::ErrorRpcServer),
                "ERROR_SERIALIZING_RESPONSE" => Some(Self::ErrorSerializingResponse),
                "ERROR_RPC_VERSION_MISMATCH" => Some(Self::ErrorRpcVersionMismatch),
                "FATAL_UNKNOWN" => Some(Self::FatalUnknown),
                "FATAL_UNSUPPORTED_SERIALIZATION" => {
                    Some(Self::FatalUnsupportedSerialization)
                }
                "FATAL_INVALID_RPC_HEADER" => Some(Self::FatalInvalidRpcHeader),
                "FATAL_DESERIALIZING_REQUEST" => Some(Self::FatalDeserializingRequest),
                "FATAL_VERSION_MISMATCH" => Some(Self::FatalVersionMismatch),
                "FATAL_UNAUTHORIZED" => Some(Self::FatalUnauthorized),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RpcSaslProto {
    #[prost(uint32, optional, tag = "1")]
    pub version: ::core::option::Option<u32>,
    #[prost(enumeration = "rpc_sasl_proto::SaslState", required, tag = "2")]
    pub state: i32,
    #[prost(bytes = "vec", optional, tag = "3")]
    pub token: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(message, repeated, tag = "4")]
    pub auths: ::prost::alloc::vec::Vec<rpc_sasl_proto::SaslAuth>,
}
/// Nested message and enum types in `RpcSaslProto`.
pub mod rpc_sasl_proto {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct SaslAuth {
        #[prost(string, required, tag = "1")]
        pub method: ::prost::alloc::string::String,
        #[prost(string, required, tag = "2")]
        pub mechanism: ::prost::alloc::string::String,
        #[prost(string, optional, tag = "3")]
        pub protocol: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "4")]
        pub server_id: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(bytes = "vec", optional, tag = "5")]
        pub challenge: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
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
    pub enum SaslState {
        Success = 0,
        Negotiate = 1,
        Initiate = 2,
        Challenge = 3,
        Response = 4,
        Wrap = 5,
    }
    impl SaslState {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                SaslState::Success => "SUCCESS",
                SaslState::Negotiate => "NEGOTIATE",
                SaslState::Initiate => "INITIATE",
                SaslState::Challenge => "CHALLENGE",
                SaslState::Response => "RESPONSE",
                SaslState::Wrap => "WRAP",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "SUCCESS" => Some(Self::Success),
                "NEGOTIATE" => Some(Self::Negotiate),
                "INITIATE" => Some(Self::Initiate),
                "CHALLENGE" => Some(Self::Challenge),
                "RESPONSE" => Some(Self::Response),
                "WRAP" => Some(Self::Wrap),
                _ => None,
            }
        }
    }
}
/// *
/// RpcKind determine the rpcEngine and the serialization of the rpc request
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum RpcKindProto {
    /// Used for built in calls by tests
    RpcBuiltin = 0,
    /// Use WritableRpcEngine
    RpcWritable = 1,
    /// Use ProtobufRpcEngine
    RpcProtocolBuffer = 2,
}
impl RpcKindProto {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            RpcKindProto::RpcBuiltin => "RPC_BUILTIN",
            RpcKindProto::RpcWritable => "RPC_WRITABLE",
            RpcKindProto::RpcProtocolBuffer => "RPC_PROTOCOL_BUFFER",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "RPC_BUILTIN" => Some(Self::RpcBuiltin),
            "RPC_WRITABLE" => Some(Self::RpcWritable),
            "RPC_PROTOCOL_BUFFER" => Some(Self::RpcProtocolBuffer),
            _ => None,
        }
    }
}
/// *
/// Spec for UserInformationProto is specified in ProtoUtil#makeIpcConnectionContext
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserInformationProto {
    #[prost(string, optional, tag = "1")]
    pub effective_user: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "2")]
    pub real_user: ::core::option::Option<::prost::alloc::string::String>,
}
/// *
/// The connection context is sent as part of the connection establishment.
/// It establishes the context for ALL Rpc calls within the connection.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IpcConnectionContextProto {
    /// UserInfo beyond what is determined as part of security handshake
    /// at connection time (kerberos, tokens etc).
    #[prost(message, optional, tag = "2")]
    pub user_info: ::core::option::Option<UserInformationProto>,
    /// Protocol name for next rpc layer.
    /// The client created a proxy with this protocol name
    #[prost(string, optional, tag = "3")]
    pub protocol: ::core::option::Option<::prost::alloc::string::String>,
}
/// *
/// This message is the header for the Protobuf Rpc Engine
/// when sending a RPC request from  RPC client to the RPC server.
/// The actual request (serialized as protobuf) follows this request.
///
/// No special header is needed for the Rpc Response for Protobuf Rpc Engine.
/// The normal RPC response header (see RpcHeader.proto) are sufficient.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestHeaderProto {
    /// * Name of the RPC method
    #[prost(string, required, tag = "1")]
    pub method_name: ::prost::alloc::string::String,
    /// *
    /// RPCs for a particular interface (ie protocol) are done using a
    /// IPC connection that is setup using rpcProxy.
    /// The rpcProxy's has a declared protocol name that is
    /// sent form client to server at connection time.
    ///
    /// Each Rpc call also sends a protocol name
    /// (called declaringClassprotocolName). This name is usually the same
    /// as the connection protocol name except in some cases.
    /// For example metaProtocols such ProtocolInfoProto which get metainfo
    /// about the protocol reuse the connection but need to indicate that
    /// the actual protocol is different (i.e. the protocol is
    /// ProtocolInfoProto) since they reuse the connection; in this case
    /// the declaringClassProtocolName field is set to the ProtocolInfoProto
    #[prost(string, required, tag = "2")]
    pub declaring_class_protocol_name: ::prost::alloc::string::String,
    /// * protocol version of class declaring the called method
    #[prost(uint64, required, tag = "3")]
    pub client_protocol_version: u64,
}
