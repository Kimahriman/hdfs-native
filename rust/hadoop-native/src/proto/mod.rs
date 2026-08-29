/// Protobuf types shared by Hadoop services.
#[allow(clippy::all, dead_code)]
pub mod common {
    #[cfg(feature = "generate-protobuf")]
    include!(concat!(env!("OUT_DIR"), "/hadoop.common.rs"));
    #[cfg(not(feature = "generate-protobuf"))]
    include!("hadoop.common.rs");
}
