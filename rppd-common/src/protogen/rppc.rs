/* 
DO NOT MODIFY!!!
generated by build.rs  from 'protos/rppc.proto' into tempdir: 'protogen', than moved to 'src/protogen/rppc.rs' 
*/
// This file is @generated by prost-build.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DbEventRequest {
    /// The name includes schema, i.e.: schema.table
    #[prost(string, tag = "1")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(enumeration = "DbAction", tag = "2")]
    pub event_type: i32,
    /// if false, then pk_value took by previous call of EventResponse.column_name
    /// this is implemented to avoid complex solution for a topic names
    #[prost(bool, tag = "3")]
    pub id_value: bool,
    /// return on second call with this values
    /// the function might pass non PK values OR combined PK
    #[prost(message, repeated, tag = "5")]
    pub pks: ::prost::alloc::vec::Vec<PkColumn>,
    /// rppd_config.id of caller host or None if called by trigger
    #[prost(oneof = "db_event_request::OptionalCaller", tags = "4")]
    pub optional_caller: ::core::option::Option<db_event_request::OptionalCaller>,
}
/// Nested message and enum types in `DbEventRequest`.
pub mod db_event_request {
    /// rppd_config.id of caller host or None if called by trigger
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
    pub enum OptionalCaller {
        #[prost(int32, tag = "4")]
        CallBy(i32),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PkColumn {
    #[prost(string, tag = "1")]
    pub column_name: ::prost::alloc::string::String,
    /// default Int
    #[prost(enumeration = "PkColumnType", tag = "2")]
    pub column_type: i32,
    /// see PkColumnType
    #[prost(oneof = "pk_column::PkValue", tags = "5, 6, 7")]
    pub pk_value: ::core::option::Option<pk_column::PkValue>,
}
/// Nested message and enum types in `PkColumn`.
pub mod pk_column {
    /// see PkColumnType
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PkValue {
        #[prost(int32, tag = "5")]
        IntValue(i32),
        #[prost(int64, tag = "6")]
        BigintValue(i64),
        #[prost(string, tag = "7")]
        StringValue(::prost::alloc::string::String),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DbEventResponse {
    /// indicate succesful trigger execution on a RPPD server, not much logic in trigger
    #[prost(bool, tag = "1")]
    pub saved: bool,
    /// needs to call from DB again with column's data specifyed by name
    #[prost(message, repeated, tag = "2")]
    pub repeat_with: ::prost::alloc::vec::Vec<PkColumn>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusRequest {
    /// The name includes schema, i.e.: schema.table
    /// default is public.rppd_config
    /// if .<table> then default use schema is public
    /// if <schema>. then default use table is rppd_config
    #[prost(string, tag = "1")]
    pub config_schema_table: ::prost::alloc::string::String,
    /// node id to check configuration consistency. Take it from rppd_config.id
    #[prost(int32, tag = "2")]
    pub node_id: i32,
    /// optional function status
    #[prost(oneof = "status_request::FnLog", tags = "4, 3")]
    pub fn_log: ::core::option::Option<status_request::FnLog>,
}
/// Nested message and enum types in `StatusRequest`.
pub mod status_request {
    /// optional function status
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FnLog {
        /// function status as id from rppd_fn_log
        #[prost(int64, tag = "4")]
        FnLogId(i64),
        /// function status as uuid when not saved to rppd_fn_log
        #[prost(string, tag = "3")]
        Uuid(::prost::alloc::string::String),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct FnStatus {
    #[prost(oneof = "fn_status::Status", tags = "1, 2, 3")]
    pub status: ::core::option::Option<fn_status::Status>,
}
/// Nested message and enum types in `FnStatus`.
pub mod fn_status {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
    pub enum Status {
        #[prost(uint32, tag = "1")]
        QueuePos(u32),
        #[prost(uint32, tag = "2")]
        InProcSec(u32),
        #[prost(int32, tag = "3")]
        RemoteHost(i32),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusFnsResponse {
    #[prost(string, repeated, tag = "1")]
    pub uuid: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusResponse {
    /// this node id
    #[prost(int32, tag = "1")]
    pub node_id: i32,
    /// master node id to check configuration consistency
    #[prost(bool, tag = "2")]
    pub is_master: bool,
    /// total in queue
    #[prost(int32, tag = "3")]
    pub queued: i32,
    /// total in process
    #[prost(int32, tag = "4")]
    pub in_proc: i32,
    /// total python to progress pooled connections
    #[prost(int32, tag = "5")]
    pub pool: i32,
    #[prost(oneof = "status_response::FnLog", tags = "6, 7")]
    pub fn_log: ::core::option::Option<status_response::FnLog>,
}
/// Nested message and enum types in `StatusResponse`.
pub mod status_response {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FnLog {
        /// status of requested function, NA=0 if no function requested
        #[prost(message, tag = "6")]
        Status(super::FnStatus),
        #[prost(message, tag = "7")]
        Uuid(super::StatusFnsResponse),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DbAction {
    Update = 0,
    Insert = 1,
    Delete = 2,
    Truncate = 3,
    /// from etcd message
    Dual = 4,
}
impl DbAction {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Update => "update",
            Self::Insert => "insert",
            Self::Delete => "delete",
            Self::Truncate => "truncate",
            Self::Dual => "dual",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "update" => Some(Self::Update),
            "insert" => Some(Self::Insert),
            "delete" => Some(Self::Delete),
            "truncate" => Some(Self::Truncate),
            "dual" => Some(Self::Dual),
            _ => None,
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PkColumnType {
    Int = 0,
    BigInt = 1,
    /// to support other types include etcd messages
    String = 2,
}
impl PkColumnType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Int => "Int",
            Self::BigInt => "BigInt",
            Self::String => "String",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Int" => Some(Self::Int),
            "BigInt" => Some(Self::BigInt),
            "String" => Some(Self::String),
            _ => None,
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FnAction {
    /// not found, might be completed and cleaned
    Na = 0,
    /// recently done
    Queueing = 1,
    /// executing
    InProgress = 2,
    /// executing on remote
    OnRemote = 3,
}
impl FnAction {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Na => "NA",
            Self::Queueing => "QUEUEING",
            Self::InProgress => "IN_PROGRESS",
            Self::OnRemote => "ON_REMOTE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NA" => Some(Self::Na),
            "QUEUEING" => Some(Self::Queueing),
            "IN_PROGRESS" => Some(Self::InProgress),
            "ON_REMOTE" => Some(Self::OnRemote),
            _ => None,
        }
    }
}
