use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
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
                sequence += 1;
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
        }),
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
}
