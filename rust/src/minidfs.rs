use std::{
    collections::HashSet,
    env,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
};
use which::which;

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum DfsFeatures {
    SECURITY,
    TOKEN,
    PRIVACY,
    HA,
    EC,
}

impl DfsFeatures {
    pub fn as_str(&self) -> &str {
        match self {
            DfsFeatures::EC => "ec",
            DfsFeatures::HA => "ha",
            DfsFeatures::PRIVACY => "privacy",
            DfsFeatures::SECURITY => "security",
            DfsFeatures::TOKEN => "token",
        }
    }

    pub fn from(value: &str) -> Option<Self> {
        match value {
            "ec" => Some(DfsFeatures::EC),
            "ha" => Some(DfsFeatures::HA),
            "privacy" => Some(DfsFeatures::PRIVACY),
            "security" => Some(DfsFeatures::SECURITY),
            "token" => Some(DfsFeatures::TOKEN),
            _ => None,
        }
    }
}

pub struct MiniDfs {
    process: Child,
    pub url: String,
}

impl MiniDfs {
    pub fn with_features(features: &HashSet<DfsFeatures>) -> Self {
        let mvn_exec = which("mvn").expect("Failed to find java executable");

        let mut feature_args: Vec<&str> = Vec::new();
        for feature in features.iter() {
            feature_args.push(feature.as_str());
        }
        let mut child = Command::new(mvn_exec)
            .args([
                "-f",
                concat!(env!("OUT_DIR"), "/minidfs"),
                "--quiet",
                "compile",
                "exec:java",
                &format!("-Dexec.args={}", feature_args.join(" ")),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let mut output = BufReader::new(child.stdout.take().unwrap()).lines();

        let ready = output.next().unwrap().unwrap();
        if ready != "Ready!" {
            println!("Failed to start minidfs");
            println!("{}", ready);
            while let Some(line) = output.next() {
                println!("{}", line.unwrap());
            }
            panic!();
        }

        // Make sure this doesn't care over from a token test to a non-token test
        env::remove_var("HADOOP_TOKEN_FILE_LOCATION");

        if features.contains(&DfsFeatures::SECURITY) {
            let krb_conf = output.next().unwrap().unwrap();
            let kdestroy_exec = which("kdestroy").expect("Failed to find kdestroy executable");
            Command::new(kdestroy_exec).spawn().unwrap().wait().unwrap();

            if !PathBuf::from("target/test/hdfs.keytab").exists() {
                panic!("Failed to find keytab");
            }

            if !PathBuf::from(&krb_conf).exists() {
                panic!("Failed to find krb5.conf");
            }

            env::set_var("KRB5_CONFIG", &krb_conf);
            env::set_var(
                "HADOOP_OPTS",
                &format!("-Djava.security.krb5.conf={}", &krb_conf),
            );

            // If we testing token auth, set the path to the file and make sure we don't have an old kinit, otherwise kinit
            if features.contains(&DfsFeatures::TOKEN) {
                env::set_var("HADOOP_TOKEN_FILE_LOCATION", "target/test/delegation_token");
            } else {
                let kinit_exec = which("kinit").expect("Failed to find kinit executable");
                env::set_var("KRB5CCNAME", "FILE:target/test/krbcache");
                Command::new(kinit_exec)
                    .args(["-kt", "target/test/hdfs.keytab", "hdfs/localhost"])
                    .spawn()
                    .unwrap()
                    .wait()
                    .unwrap();
            }
        }

        let url = if features.contains(&DfsFeatures::HA) {
            "hdfs://minidfs-ns"
        } else {
            "hdfs://127.0.0.1:9000"
        };

        env::set_var("HADOOP_CONF_DIR", "target/test");

        MiniDfs {
            process: child,
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
