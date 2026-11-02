use std::{
    collections::HashSet,
    env,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use which::which;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DfsFeatures {
    Security,
    Token,
    Integrity,
    Privacy,
    AES,
    DataTransferEncryption,
    HA,
    ViewFS,
    EC,
    RBF,
    Trash,
    Kms,
    Crc32,
}

impl DfsFeatures {
    pub fn as_str(&self) -> &str {
        match self {
            DfsFeatures::EC => "ec",
            DfsFeatures::HA => "ha",
            DfsFeatures::ViewFS => "viewfs",
            DfsFeatures::Privacy => "privacy",
            DfsFeatures::Security => "security",
            DfsFeatures::Integrity => "integrity",
            DfsFeatures::AES => "aes",
            DfsFeatures::DataTransferEncryption => "data_transfer_encryption",
            DfsFeatures::Token => "token",
            DfsFeatures::RBF => "rbf",
            DfsFeatures::Trash => "trash",
            DfsFeatures::Kms => "kms",
            DfsFeatures::Crc32 => "crc32",
        }
    }

    pub fn from(value: &str) -> Option<Self> {
        match value {
            "ec" => Some(DfsFeatures::EC),
            "ha" => Some(DfsFeatures::HA),
            "privacy" => Some(DfsFeatures::Privacy),
            "security" => Some(DfsFeatures::Security),
            "token" => Some(DfsFeatures::Token),
            "trash" => Some(DfsFeatures::Trash),
            "kms" => Some(DfsFeatures::Kms),
            "crc32" => Some(DfsFeatures::Crc32),
            _ => None,
        }
    }
}

pub struct MiniDfs {
    features: HashSet<DfsFeatures>,
    process: Child,
    stderr_thread: Option<std::thread::JoinHandle<()>>,
    pub url: String,
}

impl MiniDfs {
    pub fn with_features(features: &HashSet<DfsFeatures>) -> Self {
        let mvn_exec = which("mvn").expect("Failed to find mvn executable");

        let mut feature_args: Vec<&str> = Vec::new();
        for feature in features.iter() {
            feature_args.push(feature.as_str());
        }

        let mut command = Command::new(mvn_exec);
        command
            .args([
                "-f",
                concat!(env!("OUT_DIR"), "/minidfs"),
                "--quiet",
                "--errors",
                "compile",
                "exec:exec",
                &format!(
                    "-Dexec.args=-cp %classpath main.Main {}",
                    feature_args.join(" ")
                ),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Hadoop 3.5's ProviderUtils.locatePassword only resolves the JKS
        // password from the classpath or this env var — there's no filesystem
        // fallback. Plumbing it through the env keeps the Java side from
        // having to write into target/classes.
        if features.contains(&DfsFeatures::Kms) {
            command.env("HADOOP_KEYSTORE_PASSWORD", "kmspassword");
        }

        let mut child = command.spawn().unwrap();

        // Drain stderr while waiting for Ready! so a full pipe cannot block
        // startup. Keep only the startup output for failure diagnostics.
        let stderr_output = Arc::new(Mutex::new(Vec::new()));
        let capture_stderr = Arc::new(AtomicBool::new(true));
        let stderr = child.stderr.take().unwrap();
        let stderr_thread = {
            let stderr_output = Arc::clone(&stderr_output);
            let capture_stderr = Arc::clone(&capture_stderr);
            std::thread::spawn(move || {
                let mut stderr = BufReader::new(stderr);
                let mut line = Vec::new();
                while let Ok(bytes_read) = stderr.read_until(b'\n', &mut line) {
                    if bytes_read == 0 {
                        break;
                    }
                    if capture_stderr.load(Ordering::Relaxed) {
                        let mut output = stderr_output.lock().unwrap();
                        output.extend_from_slice(&line);
                        const MAX_CAPTURE_BYTES: usize = 64 * 1024;
                        if output.len() > MAX_CAPTURE_BYTES {
                            let excess = output.len() - MAX_CAPTURE_BYTES;
                            output.drain(..excess);
                        }
                    }
                    line.clear();
                }
            })
        };

        let mut output = BufReader::new(child.stdout.take().unwrap()).lines();

        let ready = output.next().unwrap().unwrap();
        if ready != "Ready!" {
            println!("Failed to start minidfs");
            println!("{ready}");
            for line in output.by_ref() {
                println!("{}", line.unwrap());
            }
            capture_stderr.store(false, Ordering::Relaxed);
            let _ = stderr_thread.join();
            let stderr = stderr_output.lock().unwrap();
            if !stderr.is_empty() {
                println!("MiniDFS stderr during startup:");
                print!("{}", String::from_utf8_lossy(&stderr));
            }
            panic!();
        }

        capture_stderr.store(false, Ordering::Relaxed);

        // Make sure this doesn't care over from a token test to a non-token test
        unsafe { env::remove_var("HADOOP_TOKEN_FILE_LOCATION") };

        if features.contains(&DfsFeatures::Security) {
            let krb_conf = output.next().unwrap().unwrap();
            Self::kdestroy();

            if !PathBuf::from("target/test/hdfs.keytab").exists() {
                panic!("Failed to find keytab");
            }

            if !PathBuf::from(&krb_conf).exists() {
                panic!("Failed to find krb5.conf");
            }

            unsafe {
                env::set_var("KRB5_CONFIG", &krb_conf);
                env::set_var(
                    "HADOOP_OPTS",
                    format!("-Djava.security.krb5.conf={}", krb_conf),
                );
            }

            // If we testing token auth, set the path to the file and make sure we don't have an old kinit, otherwise kinit
            if features.contains(&DfsFeatures::Token) {
                unsafe {
                    env::set_var("HADOOP_TOKEN_FILE_LOCATION", "target/test/delegation_token")
                };
            } else {
                let kinit_exec = which("kinit").expect("Failed to find kinit executable");
                unsafe { env::set_var("KRB5CCNAME", "FILE:target/test/krbcache") };
                Command::new(kinit_exec)
                    .args(["-kt", "target/test/hdfs.keytab", "hdfs/localhost"])
                    .spawn()
                    .unwrap()
                    .wait()
                    .unwrap();
            }
        }

        let url = if features.contains(&DfsFeatures::ViewFS) {
            "viewfs://minidfs-viewfs"
        } else if features.contains(&DfsFeatures::RBF) {
            "hdfs://fed"
        } else if features.contains(&DfsFeatures::HA) {
            "hdfs://minidfs-ns"
        } else {
            "hdfs://127.0.0.1:9000"
        };

        unsafe { env::set_var("HADOOP_CONF_DIR", "target/test") };
        MiniDfs {
            features: features.clone(),
            process: child,
            stderr_thread: Some(stderr_thread),
            url: url.to_string(),
        }
    }

    fn kdestroy() {
        if let Ok(kdestroy_exec) = which("kdestroy") {
            Command::new(kdestroy_exec)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
                .wait()
                .unwrap();
        }
    }
}

impl Default for MiniDfs {
    fn default() -> Self {
        Self::with_features(&HashSet::new())
    }
}

impl Drop for MiniDfs {
    fn drop(&mut self) {
        println!("Dropping and killing minidfs");
        let mut stdin = self.process.stdin.take().unwrap();
        stdin.write_all(b"\n").unwrap();
        self.process.kill().unwrap();
        self.process.wait().unwrap();
        if let Some(stderr_thread) = self.stderr_thread.take() {
            let _ = stderr_thread.join();
        }

        if self.features.contains(&DfsFeatures::Security) {
            Self::kdestroy();
        }
    }
}
