#[allow(clippy::all, dead_code)]
pub mod common {
    #[cfg(feature = "generate-protobuf")]
    include!(concat!(env!("OUT_DIR"), "/hadoop.common.rs"));
    #[cfg(not(feature = "generate-protobuf"))]
    include!(concat!("hadoop.common.rs"));
}

#[allow(clippy::all, dead_code)]
pub mod hdfs {
    #[cfg(feature = "generate-protobuf")]
    include!(concat!(env!("OUT_DIR"), "/hadoop.hdfs.rs"));
    #[cfg(not(feature = "generate-protobuf"))]
    include!(concat!("hadoop.hdfs.rs"));
}
