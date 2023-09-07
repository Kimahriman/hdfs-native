use protobuf_src;
use std::io::Result;

fn main() -> Result<()> {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    prost_build::compile_protos(
        &[
            "src/proto/hdfs/ClientNamenodeProtocol.proto",
            "src/proto/hdfs/datatransfer.proto",
            "src/proto/common/RpcHeader.proto",
            "src/proto/common/IpcConnectionContext.proto",
            "src/proto/common/ProtobufRpcEngine.proto",
        ],
        &["src/proto/common", "src/proto/hdfs"],
    )?;
    Ok(())
}
