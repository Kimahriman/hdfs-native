use std::io::Result;

fn main() -> Result<()> {
    #[cfg(feature = "generate-protobuf")]
    {
        let common_proto_root = std::env::var("DEP_HADOOP_NATIVE_PROTO_ROOT")
            .expect("hadoop-native must expose its common protobuf source directory");
        unsafe {
            std::env::set_var("PROTOC", protobuf_src::protoc());
        }

        prost_build::compile_protos(
            &[
                "src/proto/yarn/applicationclient_protocol.proto",
                "src/proto/yarn/applicationmaster_protocol.proto",
                "src/proto/yarn/containermanagement_protocol.proto",
                "src/proto/yarn/yarn_security_token.proto",
            ],
            &[&common_proto_root, "src/proto/yarn"],
        )?;
    }

    #[cfg(feature = "integration-test")]
    {
        // Copy the MiniYARNCluster launcher to the build directory so downstream tests can run it
        let status = std::process::Command::new("cp")
            .args(["-R", "miniyarn", &std::env::var("OUT_DIR").unwrap()])
            .status()
            .expect("Failed to copy miniyarn src");

        assert!(status.success(), "Failed to copy miniyarn src to out dir");
    }

    Ok(())
}
