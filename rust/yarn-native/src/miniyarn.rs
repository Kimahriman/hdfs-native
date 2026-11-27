use std::{
    collections::HashSet,
    env, fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use which::which;

#[cfg(unix)]
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(unix)]
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(50);

const RESTART_TRIGGER: &str = "target/test/restart-rm";
const RESTART_DONE: &str = "target/test/rm-restarted";
const FAILOVER_TRIGGER: &str = "target/test/failover-rm";
const FAILOVER_DONE: &str = "target/test/rm-failed-over";
const RESTART_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum YarnFeatures {
    /// ResourceManager recovery with a state store on disk, so
    /// [`MiniYarn::restart_resource_manager`] keeps running applications.
    Recovery,
    /// Two ResourceManagers, `rm0` (active) and `rm1`, sharing a recovery state store, so
    /// [`MiniYarn::fail_over_resource_manager`] keeps running applications.
    HA,
    /// Kerberos for the ResourceManager and NodeManager, plus a Kerberized HDFS at
    /// `hdfs://127.0.0.1:19000`. Clients authenticate as `yarn/localhost`.
    Security,
}

impl YarnFeatures {
    pub fn as_str(&self) -> &str {
        match self {
            YarnFeatures::Recovery => "recovery",
            YarnFeatures::HA => "ha",
            YarnFeatures::Security => "security",
        }
    }
}

/// A MiniYARNCluster with one ResourceManager and one NodeManager, for integration tests.
///
/// Starting it writes the cluster's `yarn-site.xml` to `target/test` and points
/// `HADOOP_CONF_DIR` there, so clients built with default settings connect to it.
pub struct MiniYarn {
    features: HashSet<YarnFeatures>,
    process: Child,
}

impl MiniYarn {
    pub fn new() -> Self {
        Self::with_features(&HashSet::new())
    }

    pub fn with_features(features: &HashSet<YarnFeatures>) -> Self {
        let feature_args: Vec<&str> = features.iter().map(YarnFeatures::as_str).collect();
        let mvn_exec = which("mvn").expect("Failed to find mvn executable");

        let mut command = Command::new(mvn_exec);
        command
            .args([
                "-f",
                concat!(env!("OUT_DIR"), "/miniyarn"),
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

        // Maven's exec:exec goal starts the MiniYARNCluster JVM as a child process.
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
            println!("Failed to start miniyarn");
            println!("{ready}");
            for line in output.by_ref() {
                println!("{}", line.unwrap());
            }
            panic!();
        }

        // Make sure an AMRMToken file from another test isn't picked up
        unsafe { env::remove_var("HADOOP_TOKEN_FILE_LOCATION") };

        if features.contains(&YarnFeatures::Security) {
            let krb_conf = output.next().unwrap().unwrap();
            Self::kdestroy();

            if !PathBuf::from("target/test/yarn.keytab").exists() {
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

            let kinit_exec = which("kinit").expect("Failed to find kinit executable");
            unsafe { env::set_var("KRB5CCNAME", "FILE:target/test/krbcache") };
            Command::new(kinit_exec)
                .args(["-kt", "target/test/yarn.keytab", "yarn/localhost"])
                .spawn()
                .unwrap()
                .wait()
                .unwrap();
        }
        unsafe { env::set_var("HADOOP_CONF_DIR", "target/test") };
        MiniYarn {
            features: features.clone(),
            process: child,
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

    /// Restarts the ResourceManager and waits until the NodeManager has registered with it
    /// again. The cluster must have [`YarnFeatures::Recovery`].
    pub fn restart_resource_manager(&self) {
        assert!(
            self.features.contains(&YarnFeatures::Recovery),
            "restarting the ResourceManager needs YarnFeatures::Recovery"
        );
        Self::trigger(
            RESTART_TRIGGER,
            RESTART_DONE,
            "restarting the ResourceManager",
        );
    }

    /// Makes `rm1` the active ResourceManager instead of `rm0` and waits until the
    /// NodeManager has registered with it. The cluster must have [`YarnFeatures::HA`].
    pub fn fail_over_resource_manager(&self) {
        assert!(
            self.features.contains(&YarnFeatures::HA),
            "failing over the ResourceManager needs YarnFeatures::HA"
        );
        Self::trigger(
            FAILOVER_TRIGGER,
            FAILOVER_DONE,
            "failing over the ResourceManager",
        );
    }

    /// Asks the launcher to act by creating `trigger`, then waits for it to create `done`.
    fn trigger(trigger: &str, done: &str, action: &str) {
        let _ = fs::remove_file(done);
        fs::write(trigger, b"").unwrap();
        let start = Instant::now();
        while !Path::new(done).exists() {
            assert!(start.elapsed() < RESTART_TIMEOUT, "Timed out {action}");
            thread::sleep(Duration::from_millis(100));
        }
        fs::remove_file(done).unwrap();
    }
}

impl Default for MiniYarn {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for MiniYarn {
    fn drop(&mut self) {
        println!("Dropping and killing miniyarn");
        terminate_process_tree(&mut self.process);
        let _ = self.process.wait();

        if self.features.contains(&YarnFeatures::Security) {
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
            eprintln!("Failed to check MiniYARN process status: {error}");
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
            eprintln!("Failed to signal MiniYARN process group: {error}");
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
