use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rustc-link-lib=gsasl");

    #[cfg(feature = "generate-protobuf")]
    {
        use protobuf_src;

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
    }

    #[cfg(feature = "integration-test")]
    {
        // Copy the minidfs src to the build directory so we can run it in downstream tests
        let status = std::process::Command::new("cp")
            .args(["-R", "minidfs", &std::env::var("OUT_DIR").unwrap()])
            .status()
            .expect("Failed to copy minidfs src");

        assert!(status.success(), "Failed to copy minidfs src to out dir");
    }

    Ok(())
}
