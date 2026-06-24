use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::Parser;
use codex_utils_rustls_provider::ensure_rustls_crypto_provider;
use futures::SinkExt;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use uuid::Uuid;

const BRIDGE_SCHEMA_VERSION: i32 = 1;
const DEFAULT_HEARTBEAT_SECONDS: u64 = 30;
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_TURN_TIMEOUT_SECONDS: u64 = 1200;
const DEFAULT_CODEX_APP_SERVER_WS: &str = "ws://127.0.0.1:43113";
const DEFAULT_MODEL: &str = "gpt-5-codex";
const TURN_RESULT_MARKER: &str = "SEMAPHORE_CODEX_APP_SERVER_TURN_RESULT_V1";

#[derive(Debug, Parser)]
#[command(version, about = "Semaphore sandbox bridge event forwarder")]
struct Args {
    #[arg(long, env = "SEMAPHORE_API_BASE_URL")]
    api_base_url: String,
    #[arg(long, env = "SEMAPHORE_PRODUCT_SESSION_ID")]
    session_id: Uuid,
    #[arg(long, env = "SEMAPHORE_ORGANIZATION_ID")]
    organization_id: Uuid,
    #[arg(long, env = "SEMAPHORE_RUNTIME_ID")]
    runtime_id: Uuid,
    #[arg(long, env = "SEMAPHORE_SANDBOX_BRIDGE_TOKEN", hide_env_values = true)]
    bridge_token: String,
    #[arg(long)]
    bridge_epoch: Option<String>,
    #[arg(long, default_value_t = DEFAULT_HEARTBEAT_SECONDS)]
    heartbeat_seconds: u64,
    #[arg(long, default_value_t = DEFAULT_REQUEST_TIMEOUT_SECONDS)]
    request_timeout_seconds: u64,
    #[arg(long)]
    once: bool,
    #[arg(long, env = "SEMAPHORE_SANDBOX_BRIDGE_DRAIN_COMMANDS")]
    drain_commands: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeEventBody {
    organization_id: Uuid,
    runtime_id: Uuid,
    schema_version: i32,
    bridge_epoch: String,
    sequence: i64,
    idempotency_key: String,
    #[serde(rename = "type")]
    event_type: String,
    payload: Value,
    occurred_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BridgeAck {
    accepted: bool,
    duplicate: bool,
    acknowledged_sequence: i64,
    product_event_id: Option<i64>,
    #[serde(default)]
    commands: Vec<BridgeCommand>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct BridgeCommand {
    id: Uuid,
    command_type: String,
    payload: Value,
}

#[derive(Debug)]
struct BridgeSendResult {
    ack: BridgeAck,
    transport: &'static str,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let endpoint = bridge_events_ws_url(&args.api_base_url, args.session_id)?;
    let bridge_epoch = args
        .bridge_epoch
        .clone()
        .unwrap_or_else(|| default_bridge_epoch(args.runtime_id));

    let mut sequence = 1_i64;
    loop {
        let event = heartbeat_event(
            args.organization_id,
            args.runtime_id,
            &bridge_epoch,
            sequence,
            Utc::now(),
            args.drain_commands,
        );
        match send_bridge_event(
            &endpoint,
            &args.bridge_token,
            &event,
            args.request_timeout_seconds,
        )
        .await
        {
            Ok(result) => {
                println!(
                    "SEMAPHORE_SANDBOX_BRIDGE_ACK_V1 sequence={} acknowledgedSequence={} duplicate={} accepted={} productEventId={} transport={}",
                    sequence,
                    result.ack.acknowledged_sequence,
                    result.ack.duplicate,
                    result.ack.accepted,
                    result
                        .ack
                        .product_event_id
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                    result.transport
                );
                let commands = result.ack.commands.clone();
                sequence += 1;
                if args.drain_commands {
                    for command in commands {
                        if let Err(error) = handle_bridge_command(
                            &args,
                            &endpoint,
                            &bridge_epoch,
                            &mut sequence,
                            command,
                        )
                        .await
                        {
                            eprintln!("Sandbox bridge command failed: {error:#}");
                        }
                    }
                }
                if args.once {
                    return Ok(());
                }
            }
            Err(error) => {
                eprintln!("Sandbox bridge heartbeat failed: {error:#}");
                if args.once {
                    return Err(error);
                }
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(args.heartbeat_seconds)) => {}
        }
    }
}

fn validate_args(args: &Args) -> Result<()> {
    if args.api_base_url.trim().is_empty() {
        return Err(anyhow!("SEMAPHORE_API_BASE_URL is required"));
    }
    if args.bridge_token.trim().is_empty() {
        return Err(anyhow!("SEMAPHORE_SANDBOX_BRIDGE_TOKEN is required"));
    }
    if args.heartbeat_seconds == 0 {
        return Err(anyhow!("heartbeat seconds must be positive"));
    }
    if args.request_timeout_seconds == 0 {
        return Err(anyhow!("request timeout seconds must be positive"));
    }
    if let Some(epoch) = &args.bridge_epoch {
        validate_bridge_epoch(epoch)?;
    }
    Ok(())
}

fn bridge_events_url(api_base_url: &str, session_id: Uuid) -> String {
    format!(
        "{}/api/sessions/{}/bridge/events",
        api_base_url.trim_end_matches('/'),
        session_id
    )
}

fn bridge_events_ws_url(api_base_url: &str, session_id: Uuid) -> Result<String> {
    let base = api_base_url.trim_end_matches('/');
    let websocket_base = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if base.starts_with("ws://") || base.starts_with("wss://") {
        base.to_string()
    } else {
        return Err(anyhow!(
            "SEMAPHORE_API_BASE_URL must start with http://, https://, ws://, or wss://"
        ));
    };
    Ok(format!(
        "{websocket_base}/api/sessions/{session_id}/bridge/events/ws"
    ))
}

fn default_bridge_epoch(runtime_id: Uuid) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or_default();
    format!("runtime-{runtime_id}:pid-{}:{millis}", std::process::id())
}

fn validate_bridge_epoch(value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 160
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':')
        });
    if valid {
        Ok(())
    } else {
        Err(anyhow!("bridge epoch contains unsupported characters"))
    }
}

fn heartbeat_event(
    organization_id: Uuid,
    runtime_id: Uuid,
    bridge_epoch: &str,
    sequence: i64,
    occurred_at: DateTime<Utc>,
    accepts_commands: bool,
) -> BridgeEventBody {
    BridgeEventBody {
        organization_id,
        runtime_id,
        schema_version: BRIDGE_SCHEMA_VERSION,
        bridge_epoch: bridge_epoch.to_string(),
        sequence,
        idempotency_key: format!("{bridge_epoch}:{sequence}"),
        event_type: "heartbeat".to_string(),
        payload: json!({
            "source": "semaphore-sandbox-bridge",
            "pid": std::process::id(),
            "acceptsCommands": accepts_commands,
        }),
        occurred_at,
    }
}

fn bridge_event(
    organization_id: Uuid,
    runtime_id: Uuid,
    bridge_epoch: &str,
    sequence: i64,
    event_type: &str,
    payload: Value,
    occurred_at: DateTime<Utc>,
) -> BridgeEventBody {
    BridgeEventBody {
        organization_id,
        runtime_id,
        schema_version: BRIDGE_SCHEMA_VERSION,
        bridge_epoch: bridge_epoch.to_string(),
        sequence,
        idempotency_key: format!("{bridge_epoch}:{sequence}"),
        event_type: event_type.to_string(),
        payload,
        occurred_at,
    }
}

async fn send_bridge_event(
    endpoint: &str,
    bridge_token: &str,
    event: &BridgeEventBody,
    request_timeout_seconds: u64,
) -> Result<BridgeSendResult> {
    let ack = send_bridge_event_websocket(
        endpoint,
        bridge_token,
        event,
        Duration::from_secs(request_timeout_seconds),
    )
    .await?;
    Ok(BridgeSendResult {
        ack,
        transport: "websocket",
    })
}

async fn send_bridge_event_websocket(
    endpoint: &str,
    bridge_token: &str,
    event: &BridgeEventBody,
    timeout_duration: Duration,
) -> Result<BridgeAck> {
    let mut request = endpoint
        .into_client_request()
        .with_context(|| format!("invalid bridge websocket URL `{endpoint}`"))?;
    let header_value = HeaderValue::from_str(&format!("Bearer {bridge_token}"))
        .context("invalid bridge authorization header value")?;
    request.headers_mut().insert(AUTHORIZATION, header_value);

    ensure_rustls_crypto_provider();
    let (mut websocket, _response) = tokio::time::timeout(timeout_duration, connect_async(request))
        .await
        .context("timed out connecting to bridge websocket")?
        .context("failed to connect to bridge websocket")?;
    let payload = serde_json::to_string(event).context("failed to encode bridge event")?;
    tokio::time::timeout(
        timeout_duration,
        websocket.send(WebSocketMessage::Text(payload.into())),
    )
    .await
    .context("timed out sending bridge event")?
    .context("failed to send bridge event")?;

    loop {
        let message = tokio::time::timeout(timeout_duration, websocket.next())
            .await
            .context("timed out waiting for bridge ack")?
            .ok_or_else(|| anyhow!("bridge websocket closed before ack"))?
            .context("failed to read bridge ack")?;
        match message {
            WebSocketMessage::Text(text) => {
                return serde_json::from_str(&text).context("bridge ack was not valid JSON");
            }
            WebSocketMessage::Binary(bytes) => {
                return serde_json::from_slice(&bytes).context("bridge ack was not valid JSON");
            }
            WebSocketMessage::Ping(bytes) => {
                websocket
                    .send(WebSocketMessage::Pong(bytes))
                    .await
                    .context("failed to respond to bridge websocket ping")?;
            }
            WebSocketMessage::Pong(_) | WebSocketMessage::Frame(_) => {}
            WebSocketMessage::Close(frame) => {
                return Err(anyhow!("bridge websocket closed before ack: {frame:?}"));
            }
        }
    }
}

async fn handle_bridge_command(
    args: &Args,
    endpoint: &str,
    bridge_epoch: &str,
    sequence: &mut i64,
    command: BridgeCommand,
) -> Result<()> {
    send_bridge_status_event(
        args,
        endpoint,
        bridge_epoch,
        sequence,
        "bridge.command_ack",
        json!({
            "source": "semaphore-sandbox-bridge",
            "message": format!("Bridge command accepted: {}", command.command_type),
            "bridgeCommandId": command.id,
            "commandType": command.command_type,
            "accepted": true,
        }),
    )
    .await?;

    match command.command_type.as_str() {
        "turn.start" => match execute_turn_start_command(args, &command).await {
            Ok(result) => {
                let product_turn_id = command_payload_string(&command, "productTurnId")
                    .context("turn.start command is missing productTurnId")?;
                send_bridge_status_event(
                    args,
                    endpoint,
                    bridge_epoch,
                    sequence,
                    "turn.completed",
                    json!({
                        "source": "semaphore-sandbox-bridge",
                        "message": "Codex turn completed",
                        "bridgeCommandId": command.id,
                        "productTurnId": product_turn_id,
                        "response": result.get("response").cloned().unwrap_or(Value::Null),
                        "threadId": result.get("threadId").cloned().unwrap_or(Value::Null),
                        "codexSessionId": result.get("codexSessionId").cloned().unwrap_or(Value::Null),
                        "codexTurnId": result.get("turnId").cloned().unwrap_or(Value::Null),
                        "model": result.get("model").cloned().unwrap_or(Value::Null),
                        "result": result,
                    }),
                )
                .await
                .map(|_| ())
            }
            Err(error) => {
                let product_turn_id = command_payload_string(&command, "productTurnId");
                send_bridge_status_event(
                    args,
                    endpoint,
                    bridge_epoch,
                    sequence,
                    "turn.failed",
                    json!({
                        "source": "semaphore-sandbox-bridge",
                        "message": "Codex turn failed",
                        "bridgeCommandId": command.id,
                        "productTurnId": product_turn_id,
                        "error": error.to_string(),
                    }),
                )
                .await?;
                Err(error)
            }
        },
        other => {
            send_bridge_status_event(
                args,
                endpoint,
                bridge_epoch,
                sequence,
                "bridge.command_failed",
                json!({
                    "source": "semaphore-sandbox-bridge",
                    "message": format!("Unsupported bridge command: {other}"),
                    "bridgeCommandId": command.id,
                    "commandType": other,
                }),
            )
            .await?;
            Err(anyhow!("unsupported bridge command `{other}`"))
        }
    }
}

async fn send_bridge_status_event(
    args: &Args,
    endpoint: &str,
    bridge_epoch: &str,
    sequence: &mut i64,
    event_type: &str,
    payload: Value,
) -> Result<BridgeSendResult> {
    let current_sequence = *sequence;
    *sequence += 1;
    let event = bridge_event(
        args.organization_id,
        args.runtime_id,
        bridge_epoch,
        current_sequence,
        event_type,
        payload,
        Utc::now(),
    );
    send_bridge_event(
        endpoint,
        &args.bridge_token,
        &event,
        args.request_timeout_seconds,
    )
    .await
}

async fn execute_turn_start_command(args: &Args, command: &BridgeCommand) -> Result<Value> {
    let runner = semaphore_codex_runner_bin().context("semaphore-codex-runner is not installed")?;
    let message = command_payload_string(command, "message")
        .context("turn.start command is missing message")?;
    let product_turn_id = command_payload_string(command, "productTurnId")
        .context("turn.start command is missing productTurnId")?;
    let model =
        command_payload_string(command, "model").unwrap_or_else(|| DEFAULT_MODEL.to_string());
    let websocket_url = command_payload_string(command, "appServerWs")
        .unwrap_or_else(|| DEFAULT_CODEX_APP_SERVER_WS.to_string());
    let timeout_seconds = command
        .payload
        .get("timeoutSeconds")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_TURN_TIMEOUT_SECONDS)
        .max(1);
    let cwd = command_payload_string(command, "workspaceDir");
    let codex_home = command_payload_string(command, "codexHome");
    let client_message_id = command_payload_string(command, "clientMessageId");
    let api_base_url = args.api_base_url.trim_end_matches('/').to_string();
    let organization_id = args.organization_id.to_string();
    let session_id = args.session_id.to_string();
    let runtime_id = args.runtime_id.to_string();
    let bridge_token = args.bridge_token.clone();
    let rust_log = env::var("RUST_LOG").unwrap_or_else(|_| "warn".to_string());

    let output = tokio::time::timeout(
        Duration::from_secs(timeout_seconds),
        tokio::task::spawn_blocking(move || {
            let mut process = Command::new(runner);
            process
                .arg("turn")
                .arg("--websocket-url")
                .arg(websocket_url)
                .arg("--model")
                .arg(model)
                .arg("--message")
                .arg(message)
                .env("SEMAPHORE_API_BASE_URL", api_base_url)
                .env("SEMAPHORE_ORGANIZATION_ID", organization_id)
                .env("SEMAPHORE_PRODUCT_SESSION_ID", session_id)
                .env("SEMAPHORE_RUNTIME_ID", runtime_id)
                .env("SEMAPHORE_PRODUCT_TURN_ID", product_turn_id)
                .env("SEMAPHORE_SANDBOX_BRIDGE_TOKEN", bridge_token)
                .env("NO_COLOR", "1")
                .env("RUST_LOG", rust_log);
            if let Some(cwd) = cwd {
                process.arg("--cwd").arg(cwd);
            }
            if let Some(client_message_id) = client_message_id {
                process.arg("--client-message-id").arg(client_message_id);
            }
            if let Some(codex_home) = codex_home {
                process.env("CODEX_HOME", codex_home);
            }
            process.output()
        }),
    )
    .await
    .context("timed out waiting for semaphore-codex-runner")?
    .context("failed to join semaphore-codex-runner task")?
    .context("failed to execute semaphore-codex-runner")?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        bail!(
            "semaphore-codex-runner exited with {}: {}",
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            command_output_summary(&stdout, &stderr)
        );
    }
    parse_turn_result(&stdout)
}

fn semaphore_codex_runner_bin() -> Option<PathBuf> {
    if let Some(path) = env::var("SEMAPHORE_CODEX_RUNNER_BIN")
        .ok()
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path);
    }
    if let Ok(current_exe) = env::current_exe() {
        if let Some(parent) = current_exe.parent() {
            let sibling = parent.join("semaphore-codex-runner");
            if sibling.is_file() {
                return Some(sibling);
            }
        }
    }
    [
        "/opt/semaphore/codex/bin/semaphore-codex-runner",
        "/home/daytona/.semaphore-codex-dist/bin/semaphore-codex-runner",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|path| path.is_file())
}

fn parse_turn_result(stdout: &str) -> Result<Value> {
    let mut lines = stdout.lines();
    while let Some(line) = lines.next() {
        if line.trim() == TURN_RESULT_MARKER {
            let payload = lines
                .next()
                .ok_or_else(|| anyhow!("turn result marker was not followed by JSON"))?;
            return serde_json::from_str(payload).context("turn result JSON was invalid");
        }
    }
    Err(anyhow!("turn result marker was not found"))
}

fn command_payload_string(command: &BridgeCommand, key: &str) -> Option<String> {
    command
        .payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn command_output_summary(stdout: &str, stderr: &str) -> String {
    let summary = if stderr.trim().is_empty() {
        stdout.trim()
    } else {
        stderr.trim()
    };
    if summary.is_empty() {
        return "runner failed without output".to_string();
    }
    summary.chars().take(400).collect()
}

#[allow(dead_code)]
async fn post_bridge_event(
    client: &reqwest::Client,
    endpoint: &str,
    bridge_token: &str,
    event: &BridgeEventBody,
) -> Result<BridgeAck> {
    let response = client
        .post(endpoint)
        .bearer_auth(bridge_token)
        .json(event)
        .send()
        .await
        .context("failed to send bridge event")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "bridge event was rejected with {}: {}",
            status,
            bridge_error_body(status, &body)
        ));
    }
    serde_json::from_str(&body).context("bridge ack was not valid JSON")
}

#[allow(dead_code)]
fn bridge_error_body(status: reqwest::StatusCode, body: &str) -> String {
    let fallback = status.canonical_reason().unwrap_or("request failed");
    let summary = body.trim();
    if summary.is_empty() {
        fallback.to_string()
    } else {
        summary.chars().take(240).collect()
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use reqwest::StatusCode;
    use serde_json::json;

    use super::*;

    fn session_id() -> Uuid {
        Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
    }

    fn organization_id() -> Uuid {
        Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap()
    }

    fn runtime_id() -> Uuid {
        Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()
    }

    #[test]
    fn bridge_events_url_trims_base_url() {
        assert_eq!(
            bridge_events_url("https://api.example.test/", session_id()),
            "https://api.example.test/api/sessions/11111111-1111-1111-1111-111111111111/bridge/events"
        );
    }

    #[test]
    fn bridge_events_ws_url_converts_http_base_url() {
        assert_eq!(
            bridge_events_ws_url("https://api.example.test/", session_id()).unwrap(),
            "wss://api.example.test/api/sessions/11111111-1111-1111-1111-111111111111/bridge/events/ws"
        );
        assert_eq!(
            bridge_events_ws_url("http://localhost:8080", session_id()).unwrap(),
            "ws://localhost:8080/api/sessions/11111111-1111-1111-1111-111111111111/bridge/events/ws"
        );
        assert!(bridge_events_ws_url("api.example.test", session_id()).is_err());
    }

    #[test]
    fn heartbeat_event_matches_product_bridge_envelope() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 6, 24, 12, 0, 0).unwrap();
        let body = heartbeat_event(
            organization_id(),
            runtime_id(),
            "runtime-epoch:pid-1:123",
            4,
            occurred_at,
            true,
        );

        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({
                "organizationId": "99999999-9999-9999-9999-999999999999",
                "runtimeId": "22222222-2222-2222-2222-222222222222",
                "schemaVersion": 1,
                "bridgeEpoch": "runtime-epoch:pid-1:123",
                "sequence": 4,
                "idempotencyKey": "runtime-epoch:pid-1:123:4",
                "type": "heartbeat",
                "payload": {
                    "source": "semaphore-sandbox-bridge",
                    "pid": std::process::id(),
                    "acceptsCommands": true,
                },
                "occurredAt": "2026-06-24T12:00:00Z",
            })
        );
    }

    #[test]
    fn bridge_epoch_validation_matches_api_allowlist() {
        validate_bridge_epoch("runtime-1:pid-2:abc.DEF_3").unwrap();
        assert!(validate_bridge_epoch("").is_err());
        assert!(validate_bridge_epoch("contains slash/value").is_err());
        assert!(validate_bridge_epoch(&"a".repeat(161)).is_err());
    }

    #[test]
    fn bridge_error_summary_is_bounded() {
        let body = "x".repeat(300);
        assert_eq!(
            bridge_error_body(StatusCode::BAD_REQUEST, &body)
                .chars()
                .count(),
            240
        );
    }

    #[test]
    fn turn_result_parser_reads_runner_marker_json() {
        let result = parse_turn_result(
            "log\nSEMAPHORE_CODEX_APP_SERVER_TURN_RESULT_V1\n{\"response\":\"done\",\"threadId\":\"thread-1\"}\n",
        )
        .unwrap();

        assert_eq!(result["response"], "done");
        assert_eq!(result["threadId"], "thread-1");
    }
}
