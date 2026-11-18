use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::{Duration, Instant},
};

use crate::{Result, YarnError};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(300);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(unix)]
const TERMINATION_GRACE_PERIOD: Duration = Duration::from_secs(1);
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A local YARN cluster managed by the Java `MiniYARNCluster` test harness.
pub struct MiniYarn {
    resource_manager_address: String,
    process: Child,
    stdout_thread: Option<thread::JoinHandle<()>>,
    _work_dir: tempfile::TempDir,
}

impl MiniYarn {
    /// Start one ResourceManager and one NodeManager on ephemeral ports.
    ///
    /// Set `YARN_NATIVE_MAVEN_REPOSITORY` to override Maven's local artifact cache.
    pub fn new() -> Result<Self> {
        let mvn = which::which("mvn").map_err(|error| {
            YarnError::OperationFailed(format!("failed to find mvn executable: {error}"))
        })?;
        let work_dir = tempfile::tempdir()?;
        let mut command = Command::new(mvn);
        command
            .args([
                "-f",
                concat!(env!("OUT_DIR"), "/miniyarn"),
                "--quiet",
                "--errors",
            ])
            .env("YARN_NATIVE_TEST_DIR", work_dir.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        if let Some(repository) = std::env::var_os("YARN_NATIVE_MAVEN_REPOSITORY") {
            let repository = PathBuf::from(repository);
            command.arg(format!("-Dmaven.repo.local={}", repository.display()));
        }
        command.args([
            "compile",
            "org.codehaus.mojo:exec-maven-plugin:3.6.4:exec",
            "-Dexec.args=-cp %classpath main.Main",
        ]);

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }

        let mut process = command.spawn()?;
        let Some(stdout) = process.stdout.take() else {
            terminate_process_tree(&mut process);
            let _ = process.wait();
            return Err(YarnError::OperationFailed(
                "MiniYARNCluster process did not expose stdout".to_owned(),
            ));
        };

        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let stdout_thread = thread::spawn(move || {
            let mut ready_sender = Some(ready_sender);
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => {
                        if let Some(address) = line.strip_prefix("READY\t")
                            && let Some(sender) = ready_sender.take()
                        {
                            let _ = sender.send(Ok(address.trim().to_owned()));
                        } else if !line.is_empty() {
                            eprintln!("[miniyarn] {line}");
                        }
                    }
                    Err(error) => {
                        if let Some(sender) = ready_sender.take() {
                            let _ = sender.send(Err(format!(
                                "failed to read MiniYARNCluster output: {error}"
                            )));
                        }
                        break;
                    }
                }
            }

            if let Some(sender) = ready_sender {
                let _ = sender.send(Err(
                    "MiniYARNCluster exited before announcing its ResourceManager address"
                        .to_owned(),
                ));
            }
        });

        let resource_manager_address = match ready_receiver.recv_timeout(STARTUP_TIMEOUT) {
            Ok(Ok(address)) if !address.is_empty() => address,
            Ok(Ok(_)) => {
                terminate_process_tree(&mut process);
                let _ = process.wait();
                let _ = stdout_thread.join();
                return Err(YarnError::OperationFailed(
                    "MiniYARNCluster announced an empty ResourceManager address".to_owned(),
                ));
            }
            Ok(Err(error)) => {
                terminate_process_tree(&mut process);
                let _ = process.wait();
                let _ = stdout_thread.join();
                return Err(YarnError::OperationFailed(error));
            }
            Err(RecvTimeoutError::Timeout) => {
                terminate_process_tree(&mut process);
                let _ = process.wait();
                let _ = stdout_thread.join();
                return Err(YarnError::OperationFailed(format!(
                    "timed out starting MiniYARNCluster after {} seconds",
                    STARTUP_TIMEOUT.as_secs()
                )));
            }
            Err(RecvTimeoutError::Disconnected) => {
                terminate_process_tree(&mut process);
                let _ = process.wait();
                let _ = stdout_thread.join();
                return Err(YarnError::OperationFailed(
                    "MiniYARNCluster startup monitor disconnected".to_owned(),
                ));
            }
        };

        if process.stdin.is_none() {
            terminate_process_tree(&mut process);
            let _ = process.wait();
            let _ = stdout_thread.join();
            return Err(YarnError::OperationFailed(
                "MiniYARNCluster process did not expose stdin".to_owned(),
            ));
        }

        Ok(Self {
            resource_manager_address,
            process,
            stdout_thread: Some(stdout_thread),
            _work_dir: work_dir,
        })
    }

    /// Return the ResourceManager RPC address selected by MiniYARNCluster.
    pub fn resource_manager_address(&self) -> &str {
        &self.resource_manager_address
    }
}

impl Drop for MiniYarn {
    fn drop(&mut self) {
        if let Some(mut stdin) = self.process.stdin.take() {
            let _ = stdin.write_all(b"\n");
        }

        if !wait_for_child_exit(&mut self.process, SHUTDOWN_TIMEOUT) {
            terminate_process_tree(&mut self.process);
        }
        let _ = self.process.wait();

        if let Some(stdout_thread) = self.stdout_thread.take() {
            let _ = stdout_thread.join();
        }
    }
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(error) => {
                eprintln!("Failed to check MiniYARNCluster process status: {error}");
                return false;
            }
        }

        if start.elapsed() >= timeout {
            return false;
        }
        thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn terminate_process_tree(child: &mut Child) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }

    let process_group = child.id() as libc::pid_t;
    signal_process_group(process_group, libc::SIGTERM);
    if !wait_for_child_exit(child, TERMINATION_GRACE_PERIOD) {
        signal_process_group(process_group, libc::SIGKILL);
    }
}

#[cfg(unix)]
fn signal_process_group(process_group: libc::pid_t, signal: libc::c_int) {
    // SAFETY: the Maven child was started in a process group with its PID.
    if unsafe { libc::kill(-process_group, signal) } == -1 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            eprintln!("Failed to signal MiniYARNCluster process group: {error}");
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
