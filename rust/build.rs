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
        println!("cargo:rerun-if-changed=minidfs");

        use std::process::{Command, Stdio};
        use which::which;
        let mvn_exc = which("mvn").expect("Failed to find mvn executable");

        Command::new(mvn_exc)
            .args([
                "-f",
                "minidfs",
                "--quiet",
                "clean",
                "package",
                &format!("-DbuildDirectory={}", std::env::var("OUT_DIR").unwrap()),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .status()
            .unwrap();
    }

    Ok(())
}
