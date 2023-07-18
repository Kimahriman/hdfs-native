use std::io::Result;
fn main() -> Result<()> {
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
