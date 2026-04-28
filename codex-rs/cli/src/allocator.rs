use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tempfile::TempDir;

use super::Subcommand;

const ENDPOINT_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_ENDPOINT";
const TOKEN_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_TOKEN";
const TOKEN_ENV_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_TOKEN_ENV";
const DISABLE_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_DISABLE";
const CONSUMER_ID_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_CONSUMER_ID";
const CONSUMER_KIND_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_CONSUMER_KIND";
const RUN_ID_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_RUN_ID";
const ROLE_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_ROLE";
const TASK_ID_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_TASK_ID";
const REASON_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_REASON";
const TTL_SECONDS_ENV: &str = "CODEX_ACCOUNT_ALLOCATOR_TTL_SECONDS";
const AUTH_FILE_ENV: &str = "CODEX_AUTH_FILE";
const DEFAULT_TTL_SECONDS: u64 = 18_000;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Eq, PartialEq)]
struct AllocatorConfig {
    endpoint: String,
    token: String,
    consumer_id: String,
    consumer_kind: String,
    run_id: String,
    role: String,
    task_id: Option<String>,
    reason: Option<String>,
    ttl_seconds: u64,
}

#[derive(Debug)]
pub(super) struct AllocatorLeaseGuard {
    endpoint: String,
    token: String,
    lease_id: String,
    _temp_dir: TempDir,
}

#[derive(Debug, Serialize)]
struct LeaseRequest {
    consumer_id: String,
    consumer_kind: String,
    run_id: String,
    role: String,
    task_id: Option<String>,
    reason: Option<String>,
    ttl_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct LeaseEnvelope {
    lease: LeasePayload,
}

#[derive(Debug, Deserialize)]
struct LeasePayload {
    lease_id: String,
    #[serde(default)]
    auth_bundle: Option<Value>,
}

pub(super) async fn maybe_acquire_for_subcommand(
    subcommand: &Option<Subcommand>,
) -> anyhow::Result<Option<AllocatorLeaseGuard>> {
    let Some(default_role) = allocator_role_for_subcommand(subcommand) else {
        return Ok(None);
    };
    let Some(config) = AllocatorConfig::from_env(default_role)? else {
        return Ok(None);
    };
    acquire(config).await.map(Some)
}

async fn acquire(config: AllocatorConfig) -> anyhow::Result<AllocatorLeaseGuard> {
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("failed to build Codex account allocator client")?;
    let url = allocator_url(&config.endpoint, "/v1/leases");
    let response = client
        .post(url)
        .bearer_auth(&config.token)
        .json(&LeaseRequest {
            consumer_id: config.consumer_id.clone(),
            consumer_kind: config.consumer_kind.clone(),
            run_id: config.run_id.clone(),
            role: config.role.clone(),
            task_id: config.task_id.clone(),
            reason: config.reason.clone(),
            ttl_seconds: config.ttl_seconds,
        })
        .send()
        .await
        .context("Codex account allocator lease request failed")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Codex account allocator lease request failed ({status}): {body}");
    }
    let envelope = response
        .json::<LeaseEnvelope>()
        .await
        .context("Codex account allocator returned invalid JSON")?;
    let auth_bundle = envelope
        .lease
        .auth_bundle
        .context("Codex account allocator lease omitted auth_bundle")?;
    let temp_dir = tempfile::Builder::new()
        .prefix("codex-allocator-lease-")
        .tempdir()
        .context("failed to create temporary Codex allocator auth directory")?;
    let auth_file = materialize_auth_file(temp_dir.path(), &auth_bundle)?;

    // Safety: this runs during CLI bootstrap before Codex creates its long-lived
    // auth/config managers. The override keeps normal CODEX_HOME config intact
    // while routing auth reads and token refresh writes to this lease file.
    unsafe { std::env::set_var(AUTH_FILE_ENV, &auth_file) };

    Ok(AllocatorLeaseGuard {
        endpoint: config.endpoint,
        token: config.token,
        lease_id: envelope.lease.lease_id,
        _temp_dir: temp_dir,
    })
}

fn materialize_auth_file(base_dir: &Path, auth_bundle: &Value) -> anyhow::Result<PathBuf> {
    let codex_home = base_dir.join("codex-home");
    std::fs::create_dir_all(&codex_home).context("failed to create allocator Codex home")?;
    let auth_file = codex_home.join("auth.json");
    let tmp_file = codex_home.join("auth.json.tmp");
    let auth_json = serde_json::to_string_pretty(auth_bundle)
        .context("failed to serialize allocator auth bundle")?;
    std::fs::write(&tmp_file, format!("{auth_json}\n"))
        .context("failed to write allocator auth bundle")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp_file, std::fs::Permissions::from_mode(0o600))
            .context("failed to chmod allocator auth bundle")?;
    }
    std::fs::rename(&tmp_file, &auth_file).context("failed to install allocator auth bundle")?;
    Ok(auth_file)
}

fn allocator_role_for_subcommand(subcommand: &Option<Subcommand>) -> Option<&'static str> {
    match subcommand {
        None => Some("interactive"),
        Some(Subcommand::Exec(_)) => Some("exec"),
        Some(Subcommand::Review(_)) => Some("review"),
        Some(Subcommand::Resume(_)) => Some("resume"),
        Some(Subcommand::Fork(_)) => Some("fork"),
        Some(Subcommand::Cloud(_)) => Some("cloud"),
        Some(Subcommand::Responses(_)) => Some("responses"),
        _ => None,
    }
}

fn allocator_url(endpoint: &str, path: &str) -> String {
    format!("{}{}", endpoint.trim_end_matches('/'), path)
}

impl AllocatorConfig {
    fn from_env(default_role: &str) -> anyhow::Result<Option<Self>> {
        if truthy_env(DISABLE_ENV) {
            return Ok(None);
        }
        let Some(endpoint) = env_string(ENDPOINT_ENV) else {
            return Ok(None);
        };
        let token_env = env_string(TOKEN_ENV_ENV).unwrap_or_else(|| TOKEN_ENV.to_string());
        let token = env_string(&token_env).with_context(|| {
            format!("Codex account allocator token env var `{token_env}` is not set")
        })?;
        let ttl_seconds = match env_string(TTL_SECONDS_ENV) {
            Some(raw) => raw
                .parse::<u64>()
                .with_context(|| format!("{TTL_SECONDS_ENV} must be an integer"))?
                .clamp(300, DEFAULT_TTL_SECONDS),
            None => DEFAULT_TTL_SECONDS,
        };
        Ok(Some(Self {
            endpoint,
            token,
            consumer_id: env_string(CONSUMER_ID_ENV).unwrap_or_else(default_consumer_id),
            consumer_kind: env_string(CONSUMER_KIND_ENV)
                .unwrap_or_else(|| "codex-fork".to_string()),
            run_id: env_string(RUN_ID_ENV).unwrap_or_else(default_run_id),
            role: env_string(ROLE_ENV).unwrap_or_else(|| default_role.to_string()),
            task_id: env_string(TASK_ID_ENV),
            reason: env_string(REASON_ENV),
            ttl_seconds,
        }))
    }
}

impl Drop for AllocatorLeaseGuard {
    fn drop(&mut self) {
        let endpoint = self.endpoint.clone();
        let token = self.token.clone();
        let lease_id = self.lease_id.clone();
        let thread = std::thread::spawn(move || release_lease_blocking(endpoint, token, lease_id));
        if thread.join().is_err() {
            eprintln!("WARNING: failed to join Codex account allocator lease release thread");
        }
    }
}

fn release_lease_blocking(endpoint: String, token: String, lease_id: String) {
    let url = allocator_url(&endpoint, &format!("/v1/leases/{lease_id}/release"));
    let result = reqwest::blocking::Client::builder()
        .timeout(RELEASE_TIMEOUT)
        .build()
        .and_then(|client| {
            client
                .post(url)
                .bearer_auth(&token)
                .json(&serde_json::json!({}))
                .send()
        });
    match result {
        Ok(response) if response.status().is_success() => {}
        Ok(response) => {
            eprintln!(
                "WARNING: failed to release Codex account allocator lease `{}`: {}",
                lease_id,
                response.status()
            );
        }
        Err(err) => {
            eprintln!(
                "WARNING: failed to release Codex account allocator lease `{lease_id}`: {err}"
            );
        }
    }
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn truthy_env(key: &str) -> bool {
    matches!(
        env_string(key)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn default_consumer_id() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "codex-cli".to_string())
}

fn default_run_id() -> String {
    let pid = std::process::id();
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    format!("codex-{pid}-{epoch}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::ffi::OsString;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;
    use wiremock::matchers::body_json;
    use wiremock::matchers::header;
    use wiremock::matchers::method;
    use wiremock::matchers::path;

    struct EnvVarRestore {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarRestore {
        fn new(key: &'static str) -> Self {
            Self {
                key,
                previous: std::env::var_os(key),
            }
        }
    }

    impl Drop for EnvVarRestore {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => {
                    // Safety: test cleanup restores this process-wide variable
                    // after the allocator bootstrap path under test changed it.
                    unsafe { std::env::set_var(self.key, value) };
                }
                None => {
                    // Safety: test cleanup restores the prior absent state.
                    unsafe { std::env::remove_var(self.key) };
                }
            }
        }
    }

    #[test]
    fn allocator_url_trims_endpoint_slash() {
        assert_eq!(
            "https://allocator.example/v1/leases",
            allocator_url("https://allocator.example/", "/v1/leases")
        );
    }

    #[test]
    fn materialize_auth_file_writes_pretty_auth_json() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let auth_file = materialize_auth_file(
            temp_dir.path(),
            &json!({"auth_mode": "chatgpt", "tokens": {"id_token": "test"}}),
        )
        .expect("auth file");

        let auth = std::fs::read_to_string(&auth_file).expect("read auth");
        assert!(auth.contains("\"auth_mode\": \"chatgpt\""));
        assert_eq!(
            temp_dir.path().join("codex-home").join("auth.json"),
            auth_file
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn acquire_requests_lease_and_releases_on_drop() {
        let _auth_file_env = EnvVarRestore::new(AUTH_FILE_ENV);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/leases"))
            .and(header("authorization", "Bearer test-token"))
            .and(body_json(json!({
                "consumer_id": "consumer",
                "consumer_kind": "codex-fork",
                "run_id": "run-1",
                "role": "exec",
                "task_id": null,
                "reason": null,
                "ttl_seconds": 18000
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "lease": {
                    "lease_id": "lease-1",
                    "account": {"account_id": "account-1", "label": "Account 1"},
                    "auth_bundle": {"auth_mode": "chatgpt", "tokens": {"id_token": "test"}}
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/leases/lease-1/release"))
            .and(header("authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"lease": {}})))
            .expect(1)
            .mount(&server)
            .await;

        let guard = acquire(AllocatorConfig {
            endpoint: server.uri(),
            token: "test-token".to_string(),
            consumer_id: "consumer".to_string(),
            consumer_kind: "codex-fork".to_string(),
            run_id: "run-1".to_string(),
            role: "exec".to_string(),
            task_id: None,
            reason: None,
            ttl_seconds: DEFAULT_TTL_SECONDS,
        })
        .await
        .expect("acquire");

        let auth_file = std::env::var(AUTH_FILE_ENV).expect("auth env");
        assert!(PathBuf::from(auth_file).exists());
        drop(guard);
    }
}
