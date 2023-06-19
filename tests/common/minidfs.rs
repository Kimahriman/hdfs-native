use std::{
    collections::HashSet,
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
};
use which::which;

#[derive(PartialEq, Eq, Hash, Debug)]
pub(crate) enum DfsFeatures {
    SECURITY,
    TOKEN,
    PRIVACY,
    HA,
}

pub(crate) struct MiniDfs {
    process: Child,
    pub krb_conf: Option<String>,
    pub url: String,
}

impl MiniDfs {
    pub fn with_features(features: &HashSet<DfsFeatures>) -> Self {
        let mvn_exc = which("mvn").expect("Failed to find mvn executable");

        let mut feature_args: Vec<&str> = Vec::new();
        for feature in features.iter() {
            let s = match feature {
                DfsFeatures::SECURITY => "security",
                DfsFeatures::TOKEN => "token",
                DfsFeatures::PRIVACY => "privacy",
                DfsFeatures::HA => "ha",
            };
            feature_args.push(s);
        }

        let mut child = Command::new(mvn_exc)
            .args([
                "-f",
                "minidfs",
                "--quiet",
                "clean",
                "compile",
                "exec:java",
                format!("-Dexec.args={}", feature_args.join(" ")).as_str(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
        assert_eq!(output.next().unwrap().unwrap(), "Ready!");
        let krb_conf = if features.contains(&DfsFeatures::SECURITY) {
            Some(output.next().unwrap().unwrap())
        } else {
            None
        };

        let url = if features.contains(&DfsFeatures::HA) {
            "hdfs://minidfs-ns"
        } else {
            "hdfs://localhost:9000"
        };

        MiniDfs {
            process: child,
            krb_conf,
            url: url.to_string(),
        }
    }
}

impl Drop for MiniDfs {
    fn drop(&mut self) {
        println!("Dropping and killing minidfs");
        let mut stdin = self.process.stdin.take().unwrap();
        stdin.write_all(b"\n").unwrap();
        self.process.kill().unwrap();
        self.process.wait().unwrap();
    }
}
