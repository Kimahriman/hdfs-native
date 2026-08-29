#[allow(clippy::all, dead_code)]
pub use hadoop_native::proto::common;

#[allow(clippy::all, dead_code)]
pub mod hdfs {
    #[cfg(feature = "generate-protobuf")]
    include!(concat!(env!("OUT_DIR"), "/hadoop.hdfs.rs"));
    #[cfg(not(feature = "generate-protobuf"))]
    include!(concat!("hadoop.hdfs.rs"));
}
