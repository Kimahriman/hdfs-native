use std::io::Result;

fn main() -> Result<()> {
    println!(
        "cargo::metadata=proto_root={}/src/proto/common",
        std::env::var("CARGO_MANIFEST_DIR").unwrap()
    );

    #[cfg(feature = "generate-protobuf")]
    {
        unsafe {
            std::env::set_var("PROTOC", protobuf_src::protoc());
        }
        prost_build::compile_protos(
            &[
                "src/proto/common/RpcHeader.proto",
                "src/proto/common/IpcConnectionContext.proto",
                "src/proto/common/ProtobufRpcEngine.proto",
                "src/proto/common/Security.proto",
                "src/proto/common/HAServiceProtocol.proto",
            ],
            &["src/proto/common"],
        )?;
    }
    Ok(())
}
