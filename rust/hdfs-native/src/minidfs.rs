use std::{
    collections::HashSet,
    env,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
};
#[cfg(unix)]
use std::{
    thread,
    time::{Duration, Instant},
};
use which::which;

#[cfg(unix)]
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(unix)]
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(50);

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
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());

        // Hadoop 3.5's ProviderUtils.locatePassword only resolves the JKS
        // password from the classpath or this env var — there's no filesystem
        // fallback. Plumbing it through the env keeps the Java side from
        // having to write into target/classes.
        if features.contains(&DfsFeatures::Kms) {
            command.env("HADOOP_KEYSTORE_PASSWORD", "kmspassword");
        }

        // Maven's exec:exec goal starts the MiniDFS JVM as a child process.
        // Keep it in the same process group so Drop can terminate both.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }

        let mut child = command.spawn().unwrap();

        let mut output = BufReader::new(child.stdout.take().unwrap()).lines();

        let ready = output.next().unwrap().unwrap();
        if ready != "Ready!" {
            println!("Failed to start minidfs");
            println!("{ready}");
            for line in output.by_ref() {
                println!("{}", line.unwrap());
            }
            panic!();
        }

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
        terminate_process_tree(&mut self.process);
        let _ = self.process.wait();

        if self.features.contains(&DfsFeatures::Security) {
            Self::kdestroy();
        }
    }
}

#[cfg(unix)]
fn terminate_process_tree(child: &mut Child) {
    let process_group = child.id() as libc::pid_t;
    signal_process_group(process_group, libc::SIGTERM);
    if !wait_for_process_group_exit(child, process_group, SHUTDOWN_TIMEOUT) {
        signal_process_group(process_group, libc::SIGKILL);
    }
}

#[cfg(unix)]
fn wait_for_process_group_exit(
    child: &mut Child,
    process_group: libc::pid_t,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    loop {
        if let Err(error) = child.try_wait() {
            eprintln!("Failed to check MiniDFS process status: {error}");
        }

        // Maven exits before its Java child has finished running shutdown hooks,
        // so check the whole process group instead of only the Maven process.
        if !process_group_exists(process_group) {
            return true;
        }

        if start.elapsed() >= timeout {
            return false;
        }
        thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn process_group_exists(process_group: libc::pid_t) -> bool {
    if unsafe { libc::kill(-process_group, 0) } == 0 {
        return true;
    }

    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::ESRCH) => false,
        Some(libc::EPERM) => true,
        _ => true,
    }
}

#[cfg(unix)]
fn signal_process_group(process_group: libc::pid_t, signal: libc::c_int) {
    // SAFETY: process_group is the process group ID assigned to the Maven
    // child in with_features; a negative PID targets only that group.
    if unsafe { libc::kill(-process_group, signal) } == -1 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            eprintln!("Failed to signal MiniDFS process group: {error}");
        }
    }
}

#[cfg(windows)]
fn terminate_process_tree(child: &mut Child) {
    let pid = child.id().to_string();
    let result = Command::new("taskkill")
        .arg("/PID")
        .arg(pid)
        .arg("/T")
        .arg("/F")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    if !matches!(result, Ok(status) if status.success()) {
        let _ = child.kill();
    }
}

#[cfg(not(any(unix, windows)))]
fn terminate_process_tree(child: &mut Child) {
    let _ = child.kill();
}
