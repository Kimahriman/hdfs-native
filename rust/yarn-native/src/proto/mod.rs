/// Protobuf types shared by Hadoop services.
#[allow(clippy::all, dead_code)]
pub use hadoop_native::proto::common;

/// YARN protobuf messages and protocol definitions.
#[allow(clippy::all, dead_code)]
pub mod yarn {
    #[cfg(feature = "generate-protobuf")]
    include!(concat!(env!("OUT_DIR"), "/hadoop.yarn.rs"));
    #[cfg(not(feature = "generate-protobuf"))]
    include!("hadoop.yarn.rs");
}
