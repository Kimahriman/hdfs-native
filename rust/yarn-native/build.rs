use std::io::Result;

#[cfg(feature = "integration-test")]
use std::{fs, path::Path};

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
                "src/proto/yarn/yarn_protos.proto",
                "src/proto/yarn/yarn_service_protos.proto",
            ],
            &[&common_proto_root, "src/proto/yarn"],
        )?;
    }

    #[cfg(feature = "integration-test")]
    {
        let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR must be set by Cargo");
        let destination = Path::new(&out_dir).join("miniyarn");
        if destination.exists() {
            fs::remove_dir_all(&destination)?;
        }
        copy_dir_all("miniyarn", destination)?;
    }

    Ok(())
}

#[cfg(feature = "integration-test")]
fn copy_dir_all(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
    let source = source.as_ref();
    let destination = destination.as_ref();
    fs::create_dir_all(destination)?;
    println!("cargo::rerun-if-changed={}", source.display());

    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        let target = destination.join(entry.file_name());
        if path.is_dir() {
            copy_dir_all(&path, &target)?;
        } else {
            fs::copy(path, target)?;
        }
    }

    Ok(())
}
