//! Hadoop KMS REST client for HDFS Transparent Data Encryption.
//!
//! The Hadoop KMS exposes a REST API used by HDFS clients to decrypt the
//! per-file Encrypted Data Encryption Key (EDEK) returned by the namenode.
//! The KMS endpoint is configured via the Hadoop key `hadoop.security.key.provider.path`,
//! whose value follows the form:
//!
//! ```text
//! kms://<scheme>@<host1>;<host2>;...;<hostN>:<port>/<path>
//! ```
//!
//! Multiple hosts indicate an HA setup; this client round-robins requests across
//! them and fails over on connection or 5xx errors, matching the semantics of
//! Hadoop's `LoadBalancingKMSClientProvider`.
//!
//! ## Authentication
//!
//! Three modes are supported, in order of preference per request:
//!
//! 1. **Delegation token** (`?delegation=<token>`). The KMS issues a small bearer
//!    token after one successful Kerberos authentication. We cache it on the
//!    client and reuse it across calls — this is the production-default path
//!    because it (a) avoids embedding a multi-KB SPNEGO token in every request
//!    (Active Directory PAC payloads routinely exceed Tomcat's default
//!    `maxHttpHeaderSize` of 8 KiB) and (b) replaces a server-side SPNEGO
//!    decode with a constant-time token comparison, scaling much better when
//!    a process opens many encrypted files.
//! 2. **SPNEGO/Kerberos**, only when no delegation token is cached or the
//!    cached one was rejected as expired. We do `GET /v1/?op=GETDELEGATIONTOKEN`
//!    with an `Authorization: Negotiate <token>` header, persist the returned
//!    delegation token, and resume the request flow.
//! 3. **`?user.name=<user>` pseudo-auth**, used when the KMS is in simple-auth
//!    mode (test setups, dev clusters). If a request without a delegation
//!    token comes back 200, we stay in this mode and never attempt SPNEGO.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::common::config::{Configuration, HADOOP_SECURITY_KEY_PROVIDER_PATH};
use crate::hdfs::crypto::DataEncryptionKey;
use crate::proto::hdfs::FileEncryptionInfoProto;
use crate::security::gssapi::SpnegoSession;
use crate::{HdfsError, Result};

const KMS_URI_PREFIX: &str = "kms://";
const SPNEGO_SERVICE: &str = "HTTP";
const KMS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Async client for the Hadoop KMS REST API.
#[derive(Debug)]
pub(crate) struct KmsClient {
    endpoints: Vec<Url>,
    http: reqwest::Client,
    next: AtomicUsize,
    /// Username sent as the `user.name` query parameter when no delegation token
    /// is cached (KMS simple-auth mode). Also the renewer requested when we
    /// fetch a delegation token under Kerberos.
    user: String,
    /// Cached delegation token. `None` until the first time a request is
    /// rejected with 401 (Kerberos KMS) and then populated for the lifetime of
    /// the client. The mutex serialises concurrent first-time fetches.
    delegation_token: tokio::sync::Mutex<Option<String>>,
}

impl KmsClient {
    /// Build a [`KmsClient`] from the cluster configuration. Returns `Ok(None)`
    /// if no KMS provider is configured.
    pub(crate) fn from_config(
        config: &Configuration,
        server_default_uri: Option<&str>,
        user: String,
    ) -> Result<Option<Arc<Self>>> {
        let raw = config
            .get(HADOOP_SECURITY_KEY_PROVIDER_PATH)
            .or(server_default_uri);
        let Some(raw) = raw else {
            return Ok(None);
        };
        let raw = raw.trim();
        if raw.is_empty() {
            return Ok(None);
        }

        let endpoints = parse_kms_uri(raw)?;
        let http = reqwest::Client::builder()
            .timeout(KMS_REQUEST_TIMEOUT)
            .build()
            .map_err(|e| HdfsError::OperationFailed(format!("Failed to build KMS HTTP client: {e}")))?;

        debug!("KMS client configured with {} endpoint(s) for user `{user}`", endpoints.len());
        Ok(Some(Arc::new(Self {
            endpoints,
            http,
            next: AtomicUsize::new(0),
            user,
            delegation_token: tokio::sync::Mutex::new(None),
        })))
    }

    /// Decrypt a per-file EDEK and return the plaintext data encryption key.
    pub(crate) async fn decrypt_edek(
        &self,
        info: &FileEncryptionInfoProto,
    ) -> Result<DataEncryptionKey> {
        let body = EekRequest {
            name: info.key_name.clone(),
            iv: BASE64.encode(&info.iv),
            material: BASE64.encode(&info.key),
        };
        let path = format!(
            "v1/keyversion/{}/_eek?eek_op=decrypt",
            urlencoding(&info.ez_key_version_name)
        );

        let response: EekResponse = self.send_json(&path, &body).await?;

        let material = decode_kms_base64(&response.material).map_err(|e| {
            HdfsError::OperationFailed(format!("KMS returned non-base64 material: {e}"))
        })?;
        // The KMS response does not include an IV (only the decrypted DEK
        // material plus a version name). The file-data IV used for AES-CTR is
        // the one already on the namenode's FileEncryptionInfo.
        Ok(DataEncryptionKey {
            material,
            iv: info.iv.clone(),
        })
    }

    async fn send_json<Req: Serialize, Resp: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<Resp> {
        let mut last_error: Option<HdfsError> = None;
        for _ in 0..self.endpoints.len() {
            let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
            let base = &self.endpoints[idx];
            let url = base
                .join(path)
                .map_err(|e| HdfsError::OperationFailed(format!("Invalid KMS path: {e}")))?;

            match self.send_with_auth(base, &url, body).await {
                Ok(resp) => return Ok(resp),
                Err(e) if is_retriable(&e) => {
                    warn!("KMS request to {url} failed, trying next endpoint: {e}");
                    last_error = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            HdfsError::OperationFailed("KMS client has no endpoints configured".to_string())
        }))
    }

    async fn send_with_auth<Req: Serialize, Resp: for<'de> Deserialize<'de>>(
        &self,
        base: &Url,
        url: &Url,
        body: &Req,
    ) -> Result<Resp> {
        // First attempt: use whatever auth credential we already have — a
        // delegation token if cached, otherwise the user.name pseudo-auth (for
        // simple-auth KMS deployments).
        let cached_token = self.delegation_token.lock().await.clone();
        let response = self
            .http
            .post(authed_url(url, &self.user, cached_token.as_deref()))
            .json(body)
            .send()
            .await
            .map_err(http_error)?;

        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return decode_response(response).await;
        }

        // The server rejected our credential. Either we never had a delegation
        // token, or the cached one expired. Drop any stale token and acquire a
        // fresh one via SPNEGO, then retry once.
        if cached_token.is_some() {
            self.invalidate_token(cached_token.as_deref()).await;
        }
        let token = self.acquire_delegation_token(base).await?;
        let response = self
            .http
            .post(authed_url(url, &self.user, Some(&token)))
            .json(body)
            .send()
            .await
            .map_err(http_error)?;
        decode_response(response).await
    }

    /// Returns a delegation token, fetching one if none is cached. The mutex
    /// ensures concurrent first-time callers share a single SPNEGO round trip.
    async fn acquire_delegation_token(&self, base: &Url) -> Result<String> {
        let mut guard = self.delegation_token.lock().await;
        if let Some(t) = guard.as_ref() {
            return Ok(t.clone());
        }
        let token = self.fetch_delegation_token(base).await?;
        debug!("Acquired KMS delegation token from {base}");
        *guard = Some(token.clone());
        Ok(token)
    }

    /// Drop the cached token, but only if it still matches `expected` — this
    /// avoids invalidating a fresh token that another concurrent task already
    /// rotated in.
    async fn invalidate_token(&self, expected: Option<&str>) {
        let mut guard = self.delegation_token.lock().await;
        if guard.as_deref() == expected {
            *guard = None;
        }
    }

    /// Issue `GET {base}/v1/?op=GETDELEGATIONTOKEN&renewer=<user>` with SPNEGO
    /// authentication and return the `urlString` of the resulting token.
    async fn fetch_delegation_token(&self, base: &Url) -> Result<String> {
        let mut url = base
            .join("v1/")
            .map_err(|e| HdfsError::OperationFailed(format!("Invalid KMS base URL: {e}")))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("op", "GETDELEGATIONTOKEN");
            q.append_pair("renewer", &self.user);
        }

        let host = url
            .host_str()
            .ok_or_else(|| HdfsError::OperationFailed(format!("KMS URL missing host: {url}")))?
            .to_string();

        // Initial probe — some setups answer immediately without challenging,
        // others require a SPNEGO `Negotiate` token.
        let response = self
            .http
            .get(url.clone())
            .send()
            .await
            .map_err(http_error)?;
        let response = if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            self.spnego_get(&url, &host).await?
        } else {
            response
        };

        let body: DelegationTokenResponse = decode_response(response).await?;
        let token = body.token.and_then(|t| t.url_string).ok_or_else(|| {
            HdfsError::OperationFailed(
                "KMS GETDELEGATIONTOKEN response missing Token.urlString".to_string(),
            )
        })?;
        if token.is_empty() {
            return Err(HdfsError::OperationFailed(
                "KMS issued an empty delegation token".to_string(),
            ));
        }
        Ok(token)
    }

    /// Drive a SPNEGO challenge/response handshake against a `GET` endpoint,
    /// returning the final non-401 response.
    async fn spnego_get(&self, url: &Url, host: &str) -> Result<reqwest::Response> {
        let mut session = SpnegoSession::new(SPNEGO_SERVICE, host)?;
        let mut server_token: Option<Vec<u8>> = None;

        // Bound the loop so a misbehaving server can't spin forever.
        for _ in 0..8 {
            let token = session.step(server_token.as_deref())?;
            let header = format!("Negotiate {}", BASE64.encode(&token));
            let response = self
                .http
                .get(url.clone())
                .header(reqwest::header::AUTHORIZATION, header)
                .send()
                .await
                .map_err(http_error)?;

            if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                server_token = extract_negotiate_token(&response)?;
                if server_token.is_none() && session.is_complete() {
                    return Err(HdfsError::OperationFailed(
                        "KMS rejected SPNEGO authentication".to_string(),
                    ));
                }
                continue;
            }

            return Ok(response);
        }
        Err(HdfsError::OperationFailed(
            "KMS SPNEGO handshake did not converge".to_string(),
        ))
    }
}

/// Add either a delegation token or a `user.name` query parameter to `url`.
fn authed_url(url: &Url, user: &str, token: Option<&str>) -> Url {
    let mut url = url.clone();
    {
        let mut q = url.query_pairs_mut();
        match token {
            Some(t) => {
                q.append_pair("delegation", t);
            }
            None => {
                q.append_pair("user.name", user);
            }
        }
    }
    url
}

#[derive(Serialize)]
struct EekRequest {
    name: String,
    iv: String,
    material: String,
}

#[derive(Deserialize)]
struct EekResponse {
    #[allow(dead_code)]
    #[serde(rename = "versionName", default)]
    version_name: String,
    material: String,
}

/// Response shape of `GET /v1/?op=GETDELEGATIONTOKEN`.
/// `{"Token": {"urlString": "<token>", "kind": "...", "service": "..."}}`.
#[derive(Deserialize)]
struct DelegationTokenResponse {
    #[serde(rename = "Token")]
    token: Option<DelegationTokenBody>,
}

#[derive(Deserialize)]
struct DelegationTokenBody {
    #[serde(rename = "urlString")]
    url_string: Option<String>,
}

#[derive(Deserialize, Debug)]
struct KmsErrorBody {
    #[serde(rename = "RemoteException")]
    remote_exception: Option<KmsRemoteException>,
}

#[derive(Deserialize, Debug)]
struct KmsRemoteException {
    message: Option<String>,
    exception: Option<String>,
}

async fn decode_response<Resp: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
) -> Result<Resp> {
    let status = response.status();
    let body = response.bytes().await.map_err(http_error)?;
    if !status.is_success() {
        // Try to surface the Hadoop-formatted error body, fall back to raw text.
        if let Ok(err) = serde_json::from_slice::<KmsErrorBody>(&body)
            && let Some(re) = err.remote_exception
        {
            return Err(HdfsError::OperationFailed(format!(
                "KMS request failed ({status}): {} {}",
                re.exception.unwrap_or_default(),
                re.message.unwrap_or_default()
            )));
        }
        let text = String::from_utf8_lossy(&body);
        return Err(HdfsError::OperationFailed(format!(
            "KMS request failed ({status}): {text}"
        )));
    }
    serde_json::from_slice(&body)
        .map_err(|e| HdfsError::OperationFailed(format!("Failed to decode KMS response: {e}")))
}

fn extract_negotiate_token(response: &reqwest::Response) -> Result<Option<Vec<u8>>> {
    let header = match response.headers().get(reqwest::header::WWW_AUTHENTICATE) {
        Some(h) => h,
        None => return Ok(None),
    };
    let value = header
        .to_str()
        .map_err(|e| HdfsError::OperationFailed(format!("Invalid WWW-Authenticate header: {e}")))?;
    let trimmed = value.trim();
    let token_b64 = match trimmed.strip_prefix("Negotiate") {
        Some(rest) => rest.trim(),
        None => return Ok(None),
    };
    if token_b64.is_empty() {
        return Ok(None);
    }
    let token = BASE64.decode(token_b64.as_bytes()).map_err(|e| {
        HdfsError::OperationFailed(format!("KMS returned non-base64 SPNEGO token: {e}"))
    })?;
    Ok(Some(token))
}

fn http_error(err: reqwest::Error) -> HdfsError {
    HdfsError::OperationFailed(format!("KMS HTTP error: {err}"))
}

/// Hadoop's KMS encodes binary fields with URL-safe base64 without padding,
/// while the Java client encodes outgoing fields with standard base64. Accept
/// either form on decode.
fn decode_kms_base64(s: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    BASE64.decode(s).or_else(|_| URL_SAFE_NO_PAD.decode(s))
}

fn is_retriable(err: &HdfsError) -> bool {
    let HdfsError::OperationFailed(msg) = err else {
        return false;
    };
    // We failover only on transport-level failures and 5xx server errors, never
    // on client-side errors such as 4xx or auth rejections.
    msg.contains("KMS HTTP error")
        || msg.contains("(500")
        || msg.contains("(502")
        || msg.contains("(503")
        || msg.contains("(504")
}

/// Parse a `kms://<scheme>@host1;host2:port/path` URI into one [`Url`] per host.
fn parse_kms_uri(raw: &str) -> Result<Vec<Url>> {
    let body = raw.strip_prefix(KMS_URI_PREFIX).ok_or_else(|| {
        HdfsError::OperationFailed(format!(
            "KMS URI must start with `kms://`, got `{raw}`"
        ))
    })?;

    let (scheme, rest) = body.split_once('@').ok_or_else(|| {
        HdfsError::OperationFailed(format!("KMS URI missing scheme `<scheme>@host…`: {raw}"))
    })?;
    if scheme != "http" && scheme != "https" {
        return Err(HdfsError::OperationFailed(format!(
            "Unsupported KMS scheme `{scheme}` (expected http or https)"
        )));
    }

    // Separate authority (with possible host list) from path.
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, String::from("/")),
    };

    // Authority is `host1;host2;...;hostN[:port]`. The port (if any) attaches
    // to the last host's segment; we apply it to every host.
    let (hosts_part, port) = match authority.rsplit_once(':') {
        Some((before, after)) if !after.contains(';') && !after.is_empty() => {
            (before, Some(after))
        }
        _ => (authority, None),
    };

    let hosts: Vec<&str> = hosts_part.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();
    if hosts.is_empty() {
        return Err(HdfsError::OperationFailed(format!(
            "KMS URI has no hosts: {raw}"
        )));
    }

    hosts
        .into_iter()
        .map(|host| {
            let url_str = match port {
                Some(p) => format!("{scheme}://{host}:{p}{path}"),
                None => format!("{scheme}://{host}{path}"),
            };
            // Ensure a trailing slash so `Url::join` treats the path as a base.
            let url_str = if url_str.ends_with('/') {
                url_str
            } else {
                format!("{url_str}/")
            };
            Url::parse(&url_str)
                .map_err(|e| HdfsError::OperationFailed(format!("Invalid KMS URL `{url_str}`: {e}")))
        })
        .collect()
}

fn urlencoding(s: &str) -> String {
    // The path segment for ezKeyVersionName needs minimal encoding; KMS key
    // version names are normally `keyname@<n>` plus alphanumerics. We escape
    // everything outside the unreserved set per RFC 3986 to be safe.
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use wiremock::matchers::{header_exists, method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config_with(map: HashMap<String, String>) -> Configuration {
        Configuration::new(None, Some(map)).unwrap()
    }

    #[test]
    fn parses_single_host_uri() {
        let urls = parse_kms_uri("kms://http@kms.example.com:9292/kms").unwrap();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0].as_str(), "http://kms.example.com:9292/kms/");
    }

    #[test]
    fn parses_ha_uri_with_semicolons() {
        let urls = parse_kms_uri("kms://http@h1;h2;h3:9292/kms").unwrap();
        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0].as_str(), "http://h1:9292/kms/");
        assert_eq!(urls[1].as_str(), "http://h2:9292/kms/");
        assert_eq!(urls[2].as_str(), "http://h3:9292/kms/");
    }

    #[test]
    fn parses_https_uri() {
        let urls = parse_kms_uri("kms://https@kms.example.com:9494/kms").unwrap();
        assert_eq!(urls[0].as_str(), "https://kms.example.com:9494/kms/");
    }

    #[test]
    fn parses_uri_without_port() {
        let urls = parse_kms_uri("kms://http@kms.example.com/kms").unwrap();
        assert_eq!(urls[0].as_str(), "http://kms.example.com/kms/");
    }

    #[test]
    fn rejects_uri_without_kms_prefix() {
        assert!(parse_kms_uri("http://kms:9292/kms").is_err());
    }

    #[test]
    fn rejects_unsupported_scheme() {
        assert!(parse_kms_uri("kms://ftp@h:1234/kms").is_err());
    }

    #[test]
    fn from_config_returns_none_when_unset() {
        let cfg = config_with(HashMap::new());
        let client = KmsClient::from_config(&cfg, None, "testuser".to_string()).unwrap();
        assert!(client.is_none());
    }

    #[test]
    fn from_config_uses_explicit_path() {
        let mut map = HashMap::new();
        map.insert(
            HADOOP_SECURITY_KEY_PROVIDER_PATH.to_string(),
            "kms://http@kms.example.com:9292/kms".to_string(),
        );
        let cfg = config_with(map);
        let client = KmsClient::from_config(&cfg, None, "testuser".to_string()).unwrap().unwrap();
        assert_eq!(client.endpoints.len(), 1);
    }

    #[test]
    fn from_config_falls_back_to_server_default() {
        let cfg = config_with(HashMap::new());
        let client = KmsClient::from_config(&cfg, Some("kms://http@kms:9292/kms"), "testuser".to_string())
            .unwrap()
            .unwrap();
        assert_eq!(client.endpoints.len(), 1);
    }

    fn fei() -> FileEncryptionInfoProto {
        FileEncryptionInfoProto {
            suite: 2, // AES_CTR_NOPADDING
            crypto_protocol_version: 2,
            key: b"encrypted-edek-bytes".to_vec(),
            iv: b"sixteenbyteivxxx".to_vec(),
            key_name: "my-key".to_string(),
            ez_key_version_name: "my-key@0".to_string(),
        }
    }

    #[tokio::test]
    async fn decrypt_edek_round_trip() {
        let server = MockServer::start().await;
        let plaintext_dek = b"sixteen-byte-key";

        let body = serde_json::json!({
            "versionName": "my-key@0",
            "material": BASE64.encode(plaintext_dek),
        });

        Mock::given(method("POST"))
            .and(path_regex(r"^/kms/v1/keyversion/.+/_eek$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let uri = format!("kms://http@{}/kms", server.address());
        let mut map = HashMap::new();
        map.insert(HADOOP_SECURITY_KEY_PROVIDER_PATH.to_string(), uri);
        let cfg = config_with(map);
        let client = KmsClient::from_config(&cfg, None, "testuser".to_string()).unwrap().unwrap();

        let dek = client.decrypt_edek(&fei()).await.unwrap();
        assert_eq!(dek.material, plaintext_dek);
        // IV is taken from the FileEncryptionInfo, not the KMS response.
        assert_eq!(dek.iv, fei().iv);
    }

    #[tokio::test]
    async fn ha_failover_skips_5xx_endpoint() {
        let bad = MockServer::start().await;
        let good = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&bad)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "versionName": "my-key@0",
                "material": BASE64.encode(b"sixteen-byte-key"),
            })))
            .mount(&good)
            .await;

        // Wiremock binds a different port per server, so we build the URL list
        // manually rather than going through `parse_kms_uri` (which forces one
        // shared port across all hosts).
        let endpoints = vec![
            Url::parse(&format!("http://{}/kms/", bad.address())).unwrap(),
            Url::parse(&format!("http://{}/kms/", good.address())).unwrap(),
        ];
        let client = KmsClient {
            endpoints,
            http: reqwest::Client::builder()
                .timeout(KMS_REQUEST_TIMEOUT)
                .build()
                .unwrap(),
            next: AtomicUsize::new(0),
            user: "testuser".to_string(),
            delegation_token: tokio::sync::Mutex::new(None),
        };

        let dek = client.decrypt_edek(&fei()).await.unwrap();
        assert_eq!(dek.material, b"sixteen-byte-key");
    }

    #[tokio::test]
    async fn returns_error_when_kms_returns_4xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
            .mount(&server)
            .await;

        let uri = format!("kms://http@{}/kms", server.address());
        let mut map = HashMap::new();
        map.insert(HADOOP_SECURITY_KEY_PROVIDER_PATH.to_string(), uri);
        let client = KmsClient::from_config(&config_with(map), None, "testuser".to_string())
            .unwrap()
            .unwrap();

        let err = client.decrypt_edek(&fei()).await.unwrap_err();
        assert!(matches!(err, HdfsError::OperationFailed(_)));
    }

    #[tokio::test]
    async fn cached_delegation_token_is_sent_in_query() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex(r"^/kms/v1/keyversion/.+/_eek$"))
            .and(wiremock::matchers::query_param("delegation", "deadbeef"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "versionName": "my-key@0",
                "material": BASE64.encode(b"sixteen-byte-key"),
            })))
            .mount(&server)
            .await;

        let endpoints = vec![Url::parse(&format!("http://{}/kms/", server.address())).unwrap()];
        let client = KmsClient {
            endpoints,
            http: reqwest::Client::builder()
                .timeout(KMS_REQUEST_TIMEOUT)
                .build()
                .unwrap(),
            next: AtomicUsize::new(0),
            user: "testuser".to_string(),
            delegation_token: tokio::sync::Mutex::new(Some("deadbeef".to_string())),
        };

        // The mock only matches when `?delegation=deadbeef` is present, so a
        // successful response proves the cached token is on the wire and that
        // we did not fall back to user.name.
        client.decrypt_edek(&fei()).await.unwrap();
    }

    #[tokio::test]
    async fn fetches_delegation_token_on_401_and_retries() {
        // First call: server replies 401 (no Negotiate challenge — pretend
        // delegation tokens are mandatory and the existing user.name was
        // rejected). Our client then GETs `?op=GETDELEGATIONTOKEN`, the mock
        // returns a token immediately without SPNEGO, and the retry succeeds.
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(wiremock::matchers::query_param("op", "GETDELEGATIONTOKEN"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Token": { "urlString": "abc123" },
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(wiremock::matchers::query_param("user.name", "testuser"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(wiremock::matchers::query_param("delegation", "abc123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "versionName": "my-key@0",
                "material": BASE64.encode(b"sixteen-byte-key"),
            })))
            .mount(&server)
            .await;

        let uri = format!("kms://http@{}/kms", server.address());
        let mut map = HashMap::new();
        map.insert(HADOOP_SECURITY_KEY_PROVIDER_PATH.to_string(), uri);
        let client = KmsClient::from_config(&config_with(map), None, "testuser".to_string())
            .unwrap()
            .unwrap();

        let dek = client.decrypt_edek(&fei()).await.unwrap();
        assert_eq!(dek.material, b"sixteen-byte-key");
        assert_eq!(
            client.delegation_token.lock().await.as_deref(),
            Some("abc123")
        );
    }

    #[tokio::test]
    async fn sends_post_with_expected_body_shape() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex(r"^/kms/v1/keyversion/my-key%400/_eek$"))
            .and(header_exists("content-type"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "versionName": "my-key@0",
                "material": BASE64.encode(b"sixteen-byte-key"),
            })))
            .mount(&server)
            .await;

        let uri = format!("kms://http@{}/kms", server.address());
        let mut map = HashMap::new();
        map.insert(HADOOP_SECURITY_KEY_PROVIDER_PATH.to_string(), uri);
        let client = KmsClient::from_config(&config_with(map), None, "testuser".to_string())
            .unwrap()
            .unwrap();

        // `@` in the path segment must be percent-encoded; assertion is in the
        // path_regex matcher above.
        let mut info = fei();
        info.ez_key_version_name = "my-key@0".to_string();
        client.decrypt_edek(&info).await.unwrap();
    }
}
