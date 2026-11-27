//! A minimal ApplicationMaster that the integration tests have YARN launch, showing how an
//! application such as Sail runs inside YARN.
//!
//! It reads its AMRMToken from the container's token file and registers an endpoint that
//! clients find through the application report. When a client sends `start` to the endpoint,
//! it starts `YARN_NATIVE_TEST_WORKERS` worker containers running
//! `YARN_NATIVE_TEST_WORKER_COMMAND`, with `{container_id}` replaced by the worker's container
//! ID, passing on its own credentials without the AMRMToken. Once a client sends `stop` and
//! every worker has completed, it unregisters: succeeded when every worker exited with 0,
//! failed otherwise. An error after registering unregisters it as failed, with the error as
//! its diagnostics.
//!
//! With `YARN_NATIVE_TEST_PRINCIPAL` and `YARN_NATIVE_TEST_KEYTAB`, it logs in to HDFS with the
//! keytab and gives workers fresh HDFS delegation tokens, renewable by
//! `YARN_NATIVE_TEST_RENEWER`, in place of the HDFS tokens it was submitted with, as a
//! long-running application must before those reach their maximum lifetime.

use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use yarn_native::am::AllocateRequest;
use yarn_native::ids::ContainerId;
use yarn_native::records::{
    ContainerLaunchContext, FinalApplicationStatus, Resource, ResourceRequest,
};
use yarn_native::{AmRmClient, AmRmClientBuilder, Credentials, NmClient, NmClientBuilder};

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const AM_RM_TOKEN_KIND: &str = "YARN_AM_RM_TOKEN";
const HDFS_DELEGATION_TOKEN_KIND: &str = "HDFS_DELEGATION_TOKEN";
/// Environment the workers need to run Hadoop commands against the test cluster
const FORWARDED_ENV: [&str; 4] = ["HADOOP_CONF_DIR", "HADOOP_OPTS", "JAVA_HOME", "KRB5_CONFIG"];

/// Commands a client can send to the endpoint
#[derive(Debug, PartialEq)]
enum Command {
    Start,
    Stop,
}

fn main() {
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(error) = runtime.block_on(run()) {
        eprintln!("ApplicationMaster failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> std::result::Result<(), Box<dyn Error>> {
    let workers: usize = env::var("YARN_NATIVE_TEST_WORKERS")
        .ok()
        .and_then(|workers| workers.parse().ok())
        .unwrap_or(0);
    let worker_command = env::var("YARN_NATIVE_TEST_WORKER_COMMAND").unwrap_or_default();

    // The endpoint clients connect to, like a Spark Connect server
    let listener = TcpListener::bind("0.0.0.0:0")?;
    let port = listener.local_addr()?.port();
    let (commands, received) = mpsc::channel();
    std::thread::spawn(move || serve(listener, commands));

    // YARN puts the AMRMToken in the token file, so no other credentials are needed
    let am = AmRmClientBuilder::new().build_async().await?;
    let host = env::var("NM_HOST").unwrap_or_else(|_| "localhost".to_string());
    am.register_application_master(&host, i32::from(port), None)
        .await?;
    let nm = NmClientBuilder::new()
        .with_nm_token_cache(am.nm_token_cache())
        .build_async()
        .await?;

    match run_workers(&am, &nm, received, workers, &worker_command).await {
        Ok(failures) if failures.is_empty() => {
            am.unregister_application_master(FinalApplicationStatus::Succeeded, None, None)
                .await?;
        }
        Ok(failures) => {
            am.unregister_application_master(
                FinalApplicationStatus::Failed,
                Some(&failures.join("; ")),
                None,
            )
            .await?;
        }
        Err(error) => {
            // Report the error to YARN, so it shows in the application's diagnostics
            let _ = am
                .unregister_application_master(
                    FinalApplicationStatus::Failed,
                    Some(&error.to_string()),
                    None,
                )
                .await;
            return Err(error);
        }
    }
    Ok(())
}

/// Heartbeats until a client sends `stop`, starting the workers when one sends `start`.
/// Returns the workers that failed.
async fn run_workers(
    am: &AmRmClient,
    nm: &NmClient,
    received: mpsc::Receiver<Command>,
    workers: usize,
    worker_command: &str,
) -> std::result::Result<Vec<String>, Box<dyn Error>> {
    let mut worker_credentials = worker_credentials()?;
    if let (Ok(principal), Ok(keytab), Ok(renewer)) = (
        env::var("YARN_NATIVE_TEST_PRINCIPAL"),
        env::var("YARN_NATIVE_TEST_KEYTAB"),
        env::var("YARN_NATIVE_TEST_RENEWER"),
    ) {
        // Replace the submitted HDFS tokens, which stop working at their maximum lifetime
        remove_tokens(&mut worker_credentials, HDFS_DELEGATION_TOKEN_KIND);
        worker_credentials.merge(fresh_hdfs_tokens(&principal, &keytab, &renewer).await?);
    }

    let mut asks = Vec::new();
    let mut releases = Vec::new();
    let mut started: HashSet<ContainerId> = HashSet::new();
    let mut completed = 0;
    let mut failures = Vec::new();
    let mut stop_requested = false;
    // Workers to run, once a client sends `start`
    let mut requested = 0;
    loop {
        while let Ok(command) = received.try_recv() {
            match command {
                Command::Start if workers > 0 && requested == 0 => {
                    requested = workers;
                    asks.push(ResourceRequest::new(
                        0,
                        Resource::new(128, 1),
                        workers as i32,
                    ));
                }
                Command::Start => {}
                Command::Stop => stop_requested = true,
            }
        }
        if stop_requested && started.len() == requested && completed == requested {
            break;
        }

        let response = am
            .allocate(AllocateRequest {
                asks: std::mem::take(&mut asks),
                releases: std::mem::take(&mut releases),
                progress: completed as f32 / requested.max(1) as f32,
                ..Default::default()
            })
            .await?;
        for container in response.allocated_containers {
            if started.len() < requested {
                let mut context = ContainerLaunchContext::default()
                    .commands([worker_command.replace("{container_id}", &container.id.to_string())])
                    .credentials(worker_credentials.clone());
                for name in FORWARDED_ENV {
                    if let Ok(value) = env::var(name) {
                        context = context.env(name, value);
                    }
                }
                nm.start_container(&container, context).await?;
                started.insert(container.id);
            } else {
                releases.push(container.id);
            }
        }
        for status in response.completed_containers {
            if started.contains(&status.container_id) {
                completed += 1;
                if status.exit_status != 0 {
                    failures.push(format!(
                        "{} exited with {}: {}",
                        status.container_id, status.exit_status, status.diagnostics
                    ));
                }
            }
        }
        tokio::time::sleep(HEARTBEAT_INTERVAL).await;
    }

    Ok(failures)
}

/// Logs in with the keytab and gets new HDFS delegation tokens.
async fn fresh_hdfs_tokens(
    principal: &str,
    keytab: &str,
    renewer: &str,
) -> std::result::Result<Credentials, Box<dyn Error>> {
    // YARN localizes the keytab into the working directory
    let keytab = env::current_dir()?.join(keytab);
    let hdfs = hdfs_native::ClientBuilder::new()
        .with_kerberos_principal(principal)
        .with_kerberos_keytab(keytab.to_str().unwrap())
        .build_async()
        .await?;
    Ok(Credentials::from(
        hdfs.get_delegation_tokens(renewer).await?,
    ))
}

/// The container's credentials without the AMRMToken, which only the ApplicationMaster may use.
fn worker_credentials() -> std::result::Result<Credentials, Box<dyn Error>> {
    let mut credentials = match env::var("HADOOP_TOKEN_FILE_LOCATION") {
        Ok(path) => Credentials::read_token_file(Path::new(&path))?,
        Err(_) => Credentials::new(),
    };
    remove_tokens(&mut credentials, AM_RM_TOKEN_KIND);
    Ok(credentials)
}

fn remove_tokens(credentials: &mut Credentials, kind: &str) {
    let aliases: Vec<String> = credentials
        .tokens()
        .iter()
        .filter(|token| token.kind() == kind)
        .map(|token| token.alias().to_string())
        .collect();
    for alias in aliases {
        credentials.remove_token(&alias);
    }
}

/// Greets each client and passes on its commands until one sends `stop`.
fn serve(listener: TcpListener, commands: mpsc::Sender<Command>) {
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else {
            continue;
        };
        if writeln!(stream, "yarn-native-test-am").is_err() {
            continue;
        }
        for line in BufReader::new(&stream).lines() {
            let Ok(line) = line else {
                break;
            };
            let command = match line.trim() {
                "start" => Command::Start,
                "stop" => Command::Stop,
                _ => continue,
            };
            let stop = command == Command::Stop;
            if commands.send(command).is_err() || stop {
                return;
            }
        }
    }
}
