use std::env;
use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::Parser;
use codex_utils_rustls_provider::ensure_rustls_crypto_provider;
use futures::SinkExt;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
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
const CONTROL_RESULT_MARKER: &str = "SEMAPHORE_CODEX_APP_SERVER_CONTROL_RESULT_V1";
const DEFAULT_SPOOL_MAX_EVENTS: usize = 256;

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
    #[arg(long, env = "SEMAPHORE_SANDBOX_BRIDGE_SPOOL_PATH")]
    spool_path: Option<PathBuf>,
    #[arg(
        long,
        env = "SEMAPHORE_SANDBOX_BRIDGE_SPOOL_MAX_EVENTS",
        default_value_t = DEFAULT_SPOOL_MAX_EVENTS
    )]
    spool_max_events: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BridgeCommandPush {
    #[serde(default)]
    commands: Vec<BridgeCommand>,
}

#[derive(Debug)]
enum BridgeServerMessage {
    Ack(BridgeAck),
    Commands(Vec<BridgeCommand>),
}

#[derive(Debug)]
struct BridgeSendResult {
    ack: BridgeAck,
    transport: &'static str,
}

struct BridgeTransport {
    endpoint: String,
    bridge_token: String,
    timeout_duration: Duration,
    websocket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

struct EventSpool {
    path: PathBuf,
    max_events: usize,
    events: Vec<BridgeEventBody>,
}

impl EventSpool {
    fn load(path: PathBuf, max_events: usize) -> Result<Self> {
        let mut spool = Self {
            path,
            max_events,
            events: Vec::new(),
        };
        match fs::read_to_string(&spool.path) {
            Ok(raw) => {
                let events = serde_json::from_str::<Vec<BridgeEventBody>>(&raw)
                    .context("bridge event spool was not valid JSON")?;
                spool.events = events;
                spool.enforce_bound();
                spool.persist()?;
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to read bridge event spool {:?}", spool.path)
                });
            }
        }
        Ok(spool)
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn front(&self) -> Option<&BridgeEventBody> {
        self.events.first()
    }

    fn max_sequence(&self) -> i64 {
        self.events
            .iter()
            .map(|event| event.sequence)
            .max()
            .unwrap_or(0)
    }

    fn retain_bridge_epoch(&mut self, bridge_epoch: &str) -> Result<usize> {
        let original_len = self.events.len();
        self.events
            .retain(|event| event.bridge_epoch == bridge_epoch);
        let dropped = original_len - self.events.len();
        if dropped > 0 {
            self.persist()?;
        }
        Ok(dropped)
    }

    fn append(&mut self, event: BridgeEventBody) -> Result<()> {
        self.events.push(event);
        self.enforce_bound();
        self.persist()
    }

    fn remove_front(&mut self) -> Result<()> {
        if !self.events.is_empty() {
            self.events.remove(0);
            self.persist()?;
        }
        Ok(())
    }

    fn enforce_bound(&mut self) {
        if self.events.len() > self.max_events {
            let drop_count = self.events.len() - self.max_events;
            self.events.drain(0..drop_count);
        }
    }

    fn persist(&self) -> Result<()> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create bridge event spool directory {parent:?}")
            })?;
        }
        let tmp_path = self.path.with_extension("tmp");
        let payload = serde_json::to_vec_pretty(&self.events)
            .context("failed to encode bridge event spool")?;
        fs::write(&tmp_path, payload)
            .with_context(|| format!("failed to write bridge event spool {tmp_path:?}"))?;
        fs::rename(&tmp_path, &self.path)
            .with_context(|| format!("failed to replace bridge event spool {:?}", self.path))?;
        Ok(())
    }
}

impl BridgeTransport {
    fn new(endpoint: String, bridge_token: String, timeout_duration: Duration) -> Self {
        Self {
            endpoint,
            bridge_token,
            timeout_duration,
            websocket: None,
        }
    }

    async fn send_event(&mut self, event: &BridgeEventBody) -> Result<BridgeSendResult> {
        let mut last_error = None;
        for _ in 0..2 {
            match self.send_event_once(event).await {
                Ok(ack) => {
                    return Ok(BridgeSendResult {
                        ack,
                        transport: "websocket:persistent",
                    });
                }
                Err(error) => {
                    self.websocket = None;
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("failed to send bridge event")))
    }

    async fn send_event_once(&mut self, event: &BridgeEventBody) -> Result<BridgeAck> {
        self.ensure_connected().await?;
        let websocket = self
            .websocket
            .as_mut()
            .ok_or_else(|| anyhow!("bridge websocket was not connected"))?;
        let payload = serde_json::to_string(event).context("failed to encode bridge event")?;
        tokio::time::timeout(
            self.timeout_duration,
            websocket.send(WebSocketMessage::Text(payload.into())),
        )
        .await
        .context("timed out sending bridge event")?
        .context("failed to send bridge event")?;

        let mut pushed_commands = Vec::new();
        loop {
            match self
                .read_server_message(Some(self.timeout_duration))
                .await?
            {
                BridgeServerMessage::Ack(mut ack) => {
                    if !pushed_commands.is_empty() {
                        pushed_commands.extend(ack.commands);
                        ack.commands = pushed_commands;
                    }
                    return Ok(ack);
                }
                BridgeServerMessage::Commands(commands) => {
                    pushed_commands.extend(commands);
                }
            }
        }
    }

    async fn read_pushed_commands(&mut self) -> Result<Vec<BridgeCommand>> {
        self.ensure_connected().await?;
        loop {
            match self.read_server_message(None).await {
                Ok(BridgeServerMessage::Commands(commands)) if !commands.is_empty() => {
                    return Ok(commands);
                }
                Ok(BridgeServerMessage::Ack(ack)) if !ack.commands.is_empty() => {
                    return Ok(ack.commands);
                }
                Ok(_) => {}
                Err(error) => {
                    self.websocket = None;
                    return Err(error);
                }
            }
        }
    }

    async fn read_server_message(
        &mut self,
        timeout_duration: Option<Duration>,
    ) -> Result<BridgeServerMessage> {
        let websocket = self
            .websocket
            .as_mut()
            .ok_or_else(|| anyhow!("bridge websocket was not connected"))?;
        loop {
            let message = match timeout_duration {
                Some(duration) => tokio::time::timeout(duration, websocket.next())
                    .await
                    .context("timed out waiting for bridge websocket message")?,
                None => websocket.next().await,
            }
            .ok_or_else(|| anyhow!("bridge websocket closed before message"))?
            .context("failed to read bridge websocket message")?;
            match message {
                WebSocketMessage::Text(text) => return decode_bridge_server_message_text(&text),
                WebSocketMessage::Binary(bytes) => {
                    return decode_bridge_server_message_bytes(&bytes);
                }
                WebSocketMessage::Ping(bytes) => {
                    websocket
                        .send(WebSocketMessage::Pong(bytes))
                        .await
                        .context("failed to respond to bridge websocket ping")?;
                }
                WebSocketMessage::Pong(_) | WebSocketMessage::Frame(_) => {}
                WebSocketMessage::Close(frame) => {
                    return Err(anyhow!("bridge websocket closed before message: {frame:?}"));
                }
            }
        }
    }

    async fn ensure_connected(&mut self) -> Result<()> {
        if self.websocket.is_some() {
            return Ok(());
        }
        let mut request = self
            .endpoint
            .as_str()
            .into_client_request()
            .with_context(|| format!("invalid bridge websocket URL `{}`", self.endpoint))?;
        let header_value = HeaderValue::from_str(&format!("Bearer {}", self.bridge_token))
            .context("invalid bridge authorization header value")?;
        request.headers_mut().insert(AUTHORIZATION, header_value);

        ensure_rustls_crypto_provider();
        let (websocket, _response) =
            tokio::time::timeout(self.timeout_duration, connect_async(request))
                .await
                .context("timed out connecting to bridge websocket")?
                .context("failed to connect to bridge websocket")?;
        self.websocket = Some(websocket);
        Ok(())
    }
}

fn decode_bridge_server_message_text(text: &str) -> Result<BridgeServerMessage> {
    let value = serde_json::from_str::<Value>(text).context("bridge message was not valid JSON")?;
    decode_bridge_server_message_value(value)
}

fn decode_bridge_server_message_bytes(bytes: &[u8]) -> Result<BridgeServerMessage> {
    let value =
        serde_json::from_slice::<Value>(bytes).context("bridge message was not valid JSON")?;
    decode_bridge_server_message_value(value)
}

fn decode_bridge_server_message_value(value: Value) -> Result<BridgeServerMessage> {
    if value
        .get("messageType")
        .and_then(Value::as_str)
        .is_some_and(|message_type| message_type == "commands")
    {
        let push = serde_json::from_value::<BridgeCommandPush>(value)
            .context("bridge command push was not valid JSON")?;
        return Ok(BridgeServerMessage::Commands(push.commands));
    }
    let ack =
        serde_json::from_value::<BridgeAck>(value).context("bridge ack was not valid JSON")?;
    Ok(BridgeServerMessage::Ack(ack))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let endpoint = bridge_events_ws_url(&args.api_base_url, args.session_id)?;
    let mut transport = BridgeTransport::new(
        endpoint,
        args.bridge_token.clone(),
        Duration::from_secs(args.request_timeout_seconds),
    );
    let bridge_epoch = args
        .bridge_epoch
        .clone()
        .unwrap_or_else(|| default_bridge_epoch(args.runtime_id));
    let mut spool = EventSpool::load(
        args.spool_path
            .clone()
            .unwrap_or_else(|| default_spool_path(args.runtime_id)),
        args.spool_max_events,
    )?;
    let dropped_spooled_events = spool.retain_bridge_epoch(&bridge_epoch)?;
    if dropped_spooled_events > 0 {
        eprintln!(
            "Sandbox bridge dropped {dropped_spooled_events} stale spooled event(s) from older bridge epochs"
        );
    }

    let mut sequence = (spool.max_sequence() + 1).max(1);
    loop {
        match replay_spooled_events(&args, &mut transport, &mut spool).await {
            Ok(commands) => {
                if args.drain_commands {
                    handle_bridge_commands(
                        &args,
                        &mut transport,
                        &mut spool,
                        &bridge_epoch,
                        &mut sequence,
                        commands,
                    )
                    .await;
                }
            }
            Err(error) => eprintln!("Sandbox bridge spool replay failed: {error:#}"),
        }

        let event = heartbeat_event(
            args.organization_id,
            args.runtime_id,
            &bridge_epoch,
            sequence,
            Utc::now(),
            args.drain_commands,
        );
        let current_sequence = sequence;
        sequence += 1;
        match send_or_spool_event(&mut transport, &mut spool, &event, true).await {
            Ok(result) => {
                println!(
                    "SEMAPHORE_SANDBOX_BRIDGE_ACK_V1 sequence={} acknowledgedSequence={} duplicate={} accepted={} productEventId={} transport={}",
                    current_sequence,
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
                if args.drain_commands {
                    handle_bridge_commands(
                        &args,
                        &mut transport,
                        &mut spool,
                        &bridge_epoch,
                        &mut sequence,
                        commands,
                    )
                    .await;
                }
                if args.once {
                    return Ok(());
                }
            }
            Err(error) => {
                eprintln!("Sandbox bridge heartbeat send failed: {error:#}");
                if args.once {
                    return Err(error);
                }
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(args.heartbeat_seconds)) => {}
            commands = transport.read_pushed_commands(), if args.drain_commands => {
                match commands {
                    Ok(commands) => {
                        handle_bridge_commands(
                            &args,
                            &mut transport,
                            &mut spool,
                            &bridge_epoch,
                            &mut sequence,
                            commands,
                        )
                        .await;
                    }
                    Err(error) => eprintln!("Sandbox bridge command push read failed: {error:#}"),
                }
            }
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
    if args.spool_max_events == 0 {
        return Err(anyhow!("spool max events must be positive"));
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

fn default_spool_path(runtime_id: Uuid) -> PathBuf {
    PathBuf::from(format!(
        "/home/daytona/.semaphore/bridge-spool/{runtime_id}.json"
    ))
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
    transport: &mut BridgeTransport,
    event: &BridgeEventBody,
) -> Result<BridgeSendResult> {
    transport.send_event(event).await
}

async fn send_or_spool_event(
    transport: &mut BridgeTransport,
    spool: &mut EventSpool,
    event: &BridgeEventBody,
    spool_on_failure: bool,
) -> Result<BridgeSendResult> {
    if !spool.is_empty() {
        if spool_on_failure {
            spool.append(event.clone())?;
            return Err(anyhow!(
                "bridge event {}:{} queued behind pending spool",
                event.bridge_epoch,
                event.sequence
            ));
        }
        return Err(anyhow!(
            "bridge event {}:{} not sent because pending spool must replay first",
            event.bridge_epoch,
            event.sequence
        ));
    }
    match send_bridge_event(transport, event).await {
        Ok(result) => Ok(result),
        Err(error) => {
            if spool_on_failure {
                spool.append(event.clone())?;
                Err(error).with_context(|| {
                    format!(
                        "bridge event {}:{} was saved to the local spool",
                        event.bridge_epoch, event.sequence
                    )
                })
            } else {
                Err(error)
            }
        }
    }
}

async fn replay_spooled_events(
    args: &Args,
    transport: &mut BridgeTransport,
    spool: &mut EventSpool,
) -> Result<Vec<BridgeCommand>> {
    let mut commands = Vec::new();
    while let Some(event) = spool.front().cloned() {
        let result = send_bridge_event(transport, &event)
            .await
            .with_context(|| {
                format!(
                    "failed to replay bridge event {}:{} from spool",
                    event.bridge_epoch, event.sequence
                )
            })?;
        println!(
            "SEMAPHORE_SANDBOX_BRIDGE_REPLAY_ACK_V1 sequence={} acknowledgedSequence={} duplicate={} accepted={} productEventId={} transport={}",
            event.sequence,
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
        if args.drain_commands {
            commands.extend(result.ack.commands.clone());
        }
        spool.remove_front()?;
    }
    Ok(commands)
}

async fn handle_bridge_commands(
    args: &Args,
    transport: &mut BridgeTransport,
    spool: &mut EventSpool,
    bridge_epoch: &str,
    sequence: &mut i64,
    commands: Vec<BridgeCommand>,
) {
    for command in commands {
        if let Err(error) =
            handle_bridge_command(args, transport, spool, bridge_epoch, sequence, command).await
        {
            eprintln!("Sandbox bridge command failed: {error:#}");
        }
    }
}

async fn handle_bridge_command(
    args: &Args,
    transport: &mut BridgeTransport,
    spool: &mut EventSpool,
    bridge_epoch: &str,
    sequence: &mut i64,
    command: BridgeCommand,
) -> Result<()> {
    send_bridge_status_event(
        args,
        transport,
        spool,
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
        false,
    )
    .await?;

    match command.command_type.as_str() {
        "turn.start" => match execute_turn_start_command(
            args,
            transport,
            spool,
            bridge_epoch,
            sequence,
            &command,
        )
        .await
        {
            Ok(result) => {
                let product_turn_id = command_payload_string(&command, "productTurnId")
                    .context("turn.start command is missing productTurnId")?;
                let interrupted = result
                    .get("status")
                    .and_then(Value::as_str)
                    .is_some_and(|status| status == "interrupted");
                send_bridge_status_event(
                    args,
                    transport,
                    spool,
                    bridge_epoch,
                    sequence,
                    if interrupted {
                        "turn.interrupted"
                    } else {
                        "turn.completed"
                    },
                    json!({
                        "source": "semaphore-sandbox-bridge",
                        "message": if interrupted { "Codex turn interrupted" } else { "Codex turn completed" },
                        "bridgeCommandId": command.id,
                        "productTurnId": product_turn_id,
                        "response": result.get("response").cloned().unwrap_or(Value::Null),
                        "threadId": result.get("threadId").cloned().unwrap_or(Value::Null),
                        "codexSessionId": result.get("codexSessionId").cloned().unwrap_or(Value::Null),
                        "codexTurnId": result.get("turnId").cloned().unwrap_or(Value::Null),
                        "codexItemId": result.get("assistantItemId").cloned().unwrap_or(Value::Null),
                        "model": result.get("model").cloned().unwrap_or(Value::Null),
                        "result": result,
                    }),
                    true,
                )
                .await
                .map(|_| ())
            }
            Err(error) => {
                let product_turn_id = command_payload_string(&command, "productTurnId");
                send_bridge_status_event(
                    args,
                    transport,
                    spool,
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
                    true,
                )
                .await?;
                Err(error)
            }
        },
        "turn.steer" | "turn.interrupt" | "server_request.respond" => {
            match execute_turn_control_command(&command).await {
                Ok(result) => {
                    let product_turn_id = command_payload_string(&command, "productTurnId");
                    send_bridge_status_event(
                    args,
                    transport,
                    spool,
                    bridge_epoch,
                    sequence,
                    "bridge.command_succeeded",
                    json!({
                        "source": "semaphore-sandbox-bridge",
                        "message": format!("Bridge command completed: {}", command.command_type),
                        "bridgeCommandId": command.id,
                        "commandType": command.command_type,
                        "productTurnId": product_turn_id,
                        "result": result,
                    }),
                    true,
                )
                .await
                .map(|_| ())
                }
                Err(error) => {
                    send_bridge_status_event(
                        args,
                        transport,
                        spool,
                        bridge_epoch,
                        sequence,
                        "bridge.command_failed",
                        json!({
                            "source": "semaphore-sandbox-bridge",
                            "message": format!("Bridge command failed: {}", command.command_type),
                            "bridgeCommandId": command.id,
                            "commandType": command.command_type,
                            "productTurnId": command_payload_string(&command, "productTurnId"),
                            "error": error.to_string(),
                        }),
                        true,
                    )
                    .await?;
                    Err(error)
                }
            }
        }
        other => {
            send_bridge_status_event(
                args,
                transport,
                spool,
                bridge_epoch,
                sequence,
                "bridge.command_failed",
                json!({
                    "source": "semaphore-sandbox-bridge",
                    "message": format!("Unsupported bridge command: {other}"),
                    "bridgeCommandId": command.id,
                    "commandType": other,
                }),
                true,
            )
            .await?;
            Err(anyhow!("unsupported bridge command `{other}`"))
        }
    }
}

async fn send_bridge_status_event(
    args: &Args,
    transport: &mut BridgeTransport,
    spool: &mut EventSpool,
    bridge_epoch: &str,
    sequence: &mut i64,
    event_type: &str,
    payload: Value,
    spool_on_failure: bool,
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
    send_or_spool_event(transport, spool, &event, spool_on_failure).await
}

async fn execute_turn_start_command(
    args: &Args,
    transport: &mut BridgeTransport,
    spool: &mut EventSpool,
    bridge_epoch: &str,
    sequence: &mut i64,
    command: &BridgeCommand,
) -> Result<Value> {
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
    let thread_id = command_payload_string(command, "threadId");
    let composer_context_json = command
        .payload
        .get("composerContext")
        .filter(|value| !value.is_null())
        .map(Value::to_string);
    let api_base_url = args.api_base_url.trim_end_matches('/').to_string();
    let organization_id = args.organization_id.to_string();
    let session_id = args.session_id.to_string();
    let runtime_id = args.runtime_id.to_string();
    let bridge_token = args.bridge_token.clone();
    let rust_log = env::var("RUST_LOG").unwrap_or_else(|_| "warn".to_string());

    let mut process = Command::new(runner);
    process
        .args(turn_start_runner_args(
            &websocket_url,
            &model,
            &message,
            thread_id.as_deref(),
            composer_context_json.as_deref(),
        ))
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
    let output = wait_for_turn_runner_with_control(
        args,
        transport,
        spool,
        bridge_epoch,
        sequence,
        command,
        process,
        timeout_seconds,
    )
    .await?;

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

fn turn_start_runner_args(
    websocket_url: &str,
    model: &str,
    message: &str,
    thread_id: Option<&str>,
    composer_context_json: Option<&str>,
) -> Vec<String> {
    let mut args = vec![
        "turn".to_string(),
        "--websocket-url".to_string(),
        websocket_url.to_string(),
        "--model".to_string(),
        model.to_string(),
        "--message".to_string(),
        message.to_string(),
    ];
    if let Some(thread_id) = thread_id.map(str::trim).filter(|value| !value.is_empty()) {
        args.push("--thread-id".to_string());
        args.push(thread_id.to_string());
    }
    if let Some(context) = composer_context_json
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        args.push("--composer-context-json".to_string());
        args.push(context.to_string());
    }
    args
}

async fn wait_for_turn_runner_with_control(
    args: &Args,
    transport: &mut BridgeTransport,
    spool: &mut EventSpool,
    bridge_epoch: &str,
    sequence: &mut i64,
    active_turn_command: &BridgeCommand,
    mut process: Command,
    timeout_seconds: u64,
) -> Result<std::process::Output> {
    let output = tokio::time::timeout(Duration::from_secs(timeout_seconds), process.output());
    tokio::pin!(output);
    loop {
        tokio::select! {
            result = &mut output => {
                return result
                    .context("timed out waiting for semaphore-codex-runner")?
                    .context("failed to execute semaphore-codex-runner");
            }
            commands = transport.read_pushed_commands(), if args.drain_commands => {
                match commands {
                    Ok(commands) => {
                        handle_active_turn_control_commands(
                            args,
                            transport,
                            spool,
                            bridge_epoch,
                            sequence,
                            active_turn_command,
                            commands,
                        )
                        .await;
                    }
                    Err(error) => eprintln!("Sandbox bridge control command read failed: {error:#}"),
                }
            }
        }
    }
}

async fn handle_active_turn_control_commands(
    args: &Args,
    transport: &mut BridgeTransport,
    spool: &mut EventSpool,
    bridge_epoch: &str,
    sequence: &mut i64,
    active_turn_command: &BridgeCommand,
    commands: Vec<BridgeCommand>,
) {
    for command in commands {
        if let Err(error) = handle_active_turn_control_command(
            args,
            transport,
            spool,
            bridge_epoch,
            sequence,
            active_turn_command,
            command,
        )
        .await
        {
            eprintln!("Sandbox bridge control command failed: {error:#}");
        }
    }
}

async fn handle_active_turn_control_command(
    args: &Args,
    transport: &mut BridgeTransport,
    spool: &mut EventSpool,
    bridge_epoch: &str,
    sequence: &mut i64,
    active_turn_command: &BridgeCommand,
    command: BridgeCommand,
) -> Result<()> {
    send_bridge_status_event(
        args,
        transport,
        spool,
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
        false,
    )
    .await?;
    if !matches!(
        command.command_type.as_str(),
        "turn.steer" | "turn.interrupt" | "server_request.respond"
    ) {
        send_bridge_status_event(
            args,
            transport,
            spool,
            bridge_epoch,
            sequence,
            "bridge.command_failed",
            json!({
                "source": "semaphore-sandbox-bridge",
                "message": format!("Unsupported active-turn bridge command: {}", command.command_type),
                "bridgeCommandId": command.id,
                "commandType": command.command_type,
            }),
            true,
        )
        .await?;
        return Err(anyhow!(
            "unsupported active-turn bridge command `{}`",
            command.command_type
        ));
    }
    let active_product_turn_id = command_payload_string(active_turn_command, "productTurnId")
        .context("active turn command is missing productTurnId")?;
    let product_turn_id = command_payload_string(&command, "productTurnId")
        .context("control command is missing productTurnId")?;
    if product_turn_id != active_product_turn_id {
        send_bridge_status_event(
            args,
            transport,
            spool,
            bridge_epoch,
            sequence,
            "bridge.command_failed",
            json!({
                "source": "semaphore-sandbox-bridge",
                "message": "Control command did not match active turn",
                "bridgeCommandId": command.id,
                "commandType": command.command_type,
                "productTurnId": product_turn_id,
                "activeProductTurnId": active_product_turn_id,
            }),
            true,
        )
        .await?;
        return Err(anyhow!("control command did not match active turn"));
    }
    match execute_turn_control_command(&command).await {
        Ok(result) => {
            send_bridge_status_event(
                args,
                transport,
                spool,
                bridge_epoch,
                sequence,
                "bridge.command_succeeded",
                json!({
                    "source": "semaphore-sandbox-bridge",
                    "message": format!("Bridge command completed: {}", command.command_type),
                    "bridgeCommandId": command.id,
                    "commandType": command.command_type,
                    "productTurnId": product_turn_id,
                    "result": result,
                }),
                true,
            )
            .await?;
            Ok(())
        }
        Err(error) => {
            send_bridge_status_event(
                args,
                transport,
                spool,
                bridge_epoch,
                sequence,
                "bridge.command_failed",
                json!({
                    "source": "semaphore-sandbox-bridge",
                    "message": format!("Bridge command failed: {}", command.command_type),
                    "bridgeCommandId": command.id,
                    "commandType": command.command_type,
                    "productTurnId": product_turn_id,
                    "error": error.to_string(),
                }),
                true,
            )
            .await?;
            Err(error)
        }
    }
}

async fn execute_turn_control_command(command: &BridgeCommand) -> Result<Value> {
    let runner = semaphore_codex_runner_bin().context("semaphore-codex-runner is not installed")?;
    let invocation = turn_control_runner_invocation(command)?;
    let rust_log = env::var("RUST_LOG").unwrap_or_else(|_| "warn".to_string());
    let mut process = Command::new(runner);
    process.args(invocation.args);
    process
        .env("SEMAPHORE_PRODUCT_TURN_ID", invocation.product_turn_id)
        .env("NO_COLOR", "1")
        .env("RUST_LOG", rust_log);
    if let Some(codex_home) = invocation.codex_home {
        process.env("CODEX_HOME", codex_home);
    }
    let output = tokio::time::timeout(Duration::from_secs(30), process.output())
        .await
        .context("timed out waiting for semaphore-codex-runner control command")?
        .context("failed to execute semaphore-codex-runner control command")?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        bail!(
            "semaphore-codex-runner control exited with {}: {}",
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            command_output_summary(&stdout, &stderr)
        );
    }
    parse_control_result(&stdout)
}

struct TurnControlRunnerInvocation {
    product_turn_id: String,
    codex_home: Option<String>,
    args: Vec<String>,
}

fn turn_control_runner_invocation(command: &BridgeCommand) -> Result<TurnControlRunnerInvocation> {
    let product_turn_id = command_payload_string(command, "productTurnId")
        .context("control command is missing productTurnId")?;
    let thread_id = command_payload_string(command, "threadId")
        .context("control command is missing threadId")?;
    let codex_turn_id = command_payload_string(command, "codexTurnId")
        .context("control command is missing codexTurnId")?;
    let websocket_url = command_payload_string(command, "appServerWs")
        .unwrap_or_else(|| DEFAULT_CODEX_APP_SERVER_WS.to_string());
    let codex_home = command_payload_string(command, "codexHome");
    let client_message_id = command_payload_string(command, "clientMessageId");
    let composer_context_json = command
        .payload
        .get("composerContext")
        .filter(|value| !value.is_null())
        .map(Value::to_string);
    let mut args = Vec::new();
    match command.command_type.as_str() {
        "turn.steer" => {
            let message = command_payload_string(command, "message")
                .context("turn.steer command is missing message")?;
            args.extend(["steer".to_string(), "--message".to_string(), message]);
            if let Some(context) = composer_context_json
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                args.extend(["--composer-context-json".to_string(), context.to_string()]);
            }
        }
        "turn.interrupt" => {
            args.push("interrupt".to_string());
        }
        "server_request.respond" => {
            let request_id = command_payload_string(command, "serverRequestId")
                .context("server_request.respond command is missing serverRequestId")?;
            let response = command
                .payload
                .get("response")
                .cloned()
                .context("server_request.respond command is missing response")?;
            args.extend([
                "server-request".to_string(),
                "respond".to_string(),
                "--request-id".to_string(),
                request_id,
                "--response-json".to_string(),
                serde_json::to_string(&response)?,
            ]);
        }
        other => return Err(anyhow!("unsupported control command `{other}`")),
    }
    args.extend([
        "--websocket-url".to_string(),
        websocket_url,
        "--thread-id".to_string(),
        thread_id,
        "--turn-id".to_string(),
        codex_turn_id,
    ]);
    if let Some(client_message_id) = client_message_id {
        args.extend(["--client-message-id".to_string(), client_message_id]);
    }
    Ok(TurnControlRunnerInvocation {
        product_turn_id,
        codex_home,
        args,
    })
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
    parse_marked_json(stdout, TURN_RESULT_MARKER, "turn result")
}

fn parse_control_result(stdout: &str) -> Result<Value> {
    parse_marked_json(stdout, CONTROL_RESULT_MARKER, "control result")
}

fn parse_marked_json(stdout: &str, marker: &str, label: &str) -> Result<Value> {
    let mut lines = stdout.lines();
    while let Some(line) = lines.next() {
        if line.trim() == marker {
            let payload = lines
                .next()
                .ok_or_else(|| anyhow!("{label} marker was not followed by JSON"))?;
            return serde_json::from_str(payload)
                .with_context(|| format!("{label} JSON was invalid"));
        }
    }
    Err(anyhow!("{label} marker was not found"))
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
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

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

    fn temp_spool_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "semaphore-bridge-{name}-{}-{}.json",
            std::process::id(),
            Uuid::new_v4()
        ))
    }

    fn test_event(sequence: i64, event_type: &str) -> BridgeEventBody {
        test_event_with_epoch(sequence, event_type, "runtime-epoch:pid-1:123")
    }

    fn test_event_with_epoch(
        sequence: i64,
        event_type: &str,
        bridge_epoch: &str,
    ) -> BridgeEventBody {
        bridge_event(
            organization_id(),
            runtime_id(),
            bridge_epoch,
            sequence,
            event_type,
            json!({ "source": "test" }),
            Utc.with_ymd_and_hms(2026, 6, 24, 12, 0, 0).unwrap(),
        )
    }

    fn unreachable_transport() -> BridgeTransport {
        BridgeTransport::new(
            "ws://not-used.example.test".to_string(),
            "bridge-token".to_string(),
            Duration::from_secs(1),
        )
    }

    async fn accept_bridge_websocket(listener: &TcpListener) -> WebSocketStream<TcpStream> {
        let (stream, _) = listener.accept().await.expect("accept bridge connection");
        accept_async(stream).await.expect("accept bridge websocket")
    }

    async fn read_bridge_event(websocket: &mut WebSocketStream<TcpStream>) -> BridgeEventBody {
        let message = tokio::time::timeout(Duration::from_secs(2), websocket.next())
            .await
            .expect("bridge event timed out")
            .expect("bridge websocket closed")
            .expect("read bridge event");
        let WebSocketMessage::Text(text) = message else {
            panic!("expected bridge event text frame, got {message:?}");
        };
        serde_json::from_str(&text).expect("decode bridge event")
    }

    async fn send_bridge_ack(websocket: &mut WebSocketStream<TcpStream>, sequence: i64) {
        websocket
            .send(WebSocketMessage::Text(
                json!({
                    "accepted": true,
                    "duplicate": false,
                    "acknowledgedSequence": sequence,
                    "productEventId": sequence,
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send bridge ack");
    }

    async fn send_bridge_command_push(websocket: &mut WebSocketStream<TcpStream>) {
        websocket
            .send(WebSocketMessage::Text(
                json!({
                    "messageType": "commands",
                    "commands": [{
                        "id": "33333333-3333-3333-3333-333333333333",
                        "commandType": "turn.start",
                        "payload": { "productTurnId": "turn-1" },
                    }],
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send bridge command push");
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

    #[test]
    fn control_result_parser_reads_runner_marker_json() {
        let result = parse_control_result(
            "log\nSEMAPHORE_CODEX_APP_SERVER_CONTROL_RESULT_V1\n{\"command\":\"turn.interrupt\",\"accepted\":true}\n",
        )
        .unwrap();

        assert_eq!(result["command"], "turn.interrupt");
        assert_eq!(result["accepted"], true);
    }

    #[test]
    fn turn_start_runner_args_resume_existing_thread_when_present() {
        let args = turn_start_runner_args(
            "ws://127.0.0.1:43113",
            "gpt-validation",
            "continue",
            Some("  thread-1  "),
            None,
        );

        assert_eq!(
            args,
            vec![
                "turn",
                "--websocket-url",
                "ws://127.0.0.1:43113",
                "--model",
                "gpt-validation",
                "--message",
                "continue",
                "--thread-id",
                "thread-1",
            ]
        );
        assert!(
            !turn_start_runner_args("ws://x", "model", "message", None, None)
                .contains(&"--thread-id".to_string())
        );
        assert!(
            !turn_start_runner_args("ws://x", "model", "message", Some(" "), None)
                .contains(&"--thread-id".to_string())
        );
    }

    #[test]
    fn turn_start_runner_args_include_composer_context_when_present() {
        let context = r#"{"targetBranch":"feature/session-context"}"#;
        let args = turn_start_runner_args(
            "ws://127.0.0.1:43113",
            "gpt-validation",
            "continue",
            None,
            Some(context),
        );

        assert_eq!(
            args,
            vec![
                "turn",
                "--websocket-url",
                "ws://127.0.0.1:43113",
                "--model",
                "gpt-validation",
                "--message",
                "continue",
                "--composer-context-json",
                context,
            ]
        );
    }

    #[test]
    fn turn_steer_runner_args_include_composer_context_when_present() {
        let command = BridgeCommand {
            id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
            command_type: "turn.steer".to_string(),
            payload: json!({
                "productTurnId": "product-turn-1",
                "threadId": "thread-1",
                "codexTurnId": "turn-1",
                "appServerWs": "ws://127.0.0.1:43113",
                "clientMessageId": "client-message-1",
                "message": "continue",
                "composerContext": {
                    "targetBranch": "feature/session-context"
                }
            }),
        };

        let invocation = turn_control_runner_invocation(&command).unwrap();

        assert_eq!(invocation.product_turn_id, "product-turn-1");
        assert_eq!(
            invocation.args,
            vec![
                "steer",
                "--message",
                "continue",
                "--composer-context-json",
                r#"{"targetBranch":"feature/session-context"}"#,
                "--websocket-url",
                "ws://127.0.0.1:43113",
                "--thread-id",
                "thread-1",
                "--turn-id",
                "turn-1",
                "--client-message-id",
                "client-message-1",
            ]
        );
    }

    #[test]
    fn server_request_respond_runner_args_include_response_payload() {
        let command = BridgeCommand {
            id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
            command_type: "server_request.respond".to_string(),
            payload: json!({
                "productTurnId": "product-turn-1",
                "threadId": "thread-1",
                "codexTurnId": "turn-1",
                "appServerWs": "ws://127.0.0.1:43113",
                "codexHome": "/tmp/codex-home",
                "clientMessageId": "client-message-1",
                "serverRequestId": "7",
                "response": {
                    "answers": [{
                        "questionId": "color",
                        "answers": ["red"]
                    }]
                }
            }),
        };

        let invocation = turn_control_runner_invocation(&command).unwrap();
        let response_json_index = invocation
            .args
            .iter()
            .position(|arg| arg == "--response-json")
            .unwrap()
            + 1;
        let response_json: Value =
            serde_json::from_str(&invocation.args[response_json_index]).unwrap();
        let mut args = invocation.args.clone();
        args[response_json_index] = "<response-json>".to_string();

        assert_eq!(invocation.product_turn_id, "product-turn-1");
        assert_eq!(invocation.codex_home.as_deref(), Some("/tmp/codex-home"));
        assert_eq!(
            response_json,
            json!({
                "answers": [{
                    "questionId": "color",
                    "answers": ["red"]
                }]
            })
        );
        assert_eq!(
            args,
            vec![
                "server-request",
                "respond",
                "--request-id",
                "7",
                "--response-json",
                "<response-json>",
                "--websocket-url",
                "ws://127.0.0.1:43113",
                "--thread-id",
                "thread-1",
                "--turn-id",
                "turn-1",
                "--client-message-id",
                "client-message-1",
            ]
        );
    }

    #[test]
    fn bridge_server_message_decoder_reads_command_pushes() {
        let message = decode_bridge_server_message_value(json!({
            "messageType": "commands",
            "commands": [{
                "id": "33333333-3333-3333-3333-333333333333",
                "commandType": "turn.start",
                "payload": { "productTurnId": "turn-1" },
            }],
        }))
        .unwrap();

        let BridgeServerMessage::Commands(commands) = message else {
            panic!("expected command push");
        };
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, "turn.start");
    }

    #[test]
    fn event_spool_persists_and_bounds_events() {
        let path = temp_spool_path("bounds");
        let mut spool = EventSpool::load(path.clone(), 2).unwrap();

        spool.append(test_event(1, "heartbeat")).unwrap();
        spool.append(test_event(2, "turn.completed")).unwrap();
        spool.append(test_event(3, "turn.failed")).unwrap();

        let reloaded = EventSpool::load(path.clone(), 2).unwrap();
        let sequences = reloaded
            .events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>();

        assert_eq!(sequences, vec![2, 3]);
        assert_eq!(reloaded.max_sequence(), 3);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn event_spool_discards_events_from_old_bridge_epochs() {
        let path = temp_spool_path("bridge-epoch");
        let current_epoch = "runtime-epoch:bridge-current";
        let mut spool = EventSpool::load(path.clone(), 8).unwrap();

        spool
            .append(test_event_with_epoch(
                10,
                "heartbeat",
                "runtime-epoch:bridge-old",
            ))
            .unwrap();
        spool
            .append(test_event_with_epoch(3, "turn.completed", current_epoch))
            .unwrap();
        spool
            .append(test_event_with_epoch(
                11,
                "turn.failed",
                "runtime-epoch:bridge-older",
            ))
            .unwrap();

        let dropped = spool.retain_bridge_epoch(current_epoch).unwrap();

        assert_eq!(dropped, 2);
        assert_eq!(spool.events.len(), 1);
        assert_eq!(spool.events[0].bridge_epoch, current_epoch);
        assert_eq!(spool.max_sequence(), 3);

        let reloaded = EventSpool::load(path.clone(), 8).unwrap();
        assert_eq!(reloaded.events.len(), 1);
        assert_eq!(reloaded.events[0].bridge_epoch, current_epoch);
        assert_eq!(reloaded.max_sequence(), 3);
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn send_or_spool_queues_durable_event_behind_pending_spool() {
        let path = temp_spool_path("durable");
        let mut spool = EventSpool::load(path.clone(), 8).unwrap();
        let mut transport = unreachable_transport();
        spool.append(test_event(1, "turn.completed")).unwrap();

        let result = send_or_spool_event(
            &mut transport,
            &mut spool,
            &test_event(2, "turn.failed"),
            true,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(spool.events.len(), 2);
        assert_eq!(spool.events[1].event_type, "turn.failed");
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn send_or_spool_does_not_queue_pre_execution_command_ack() {
        let path = temp_spool_path("command-ack");
        let mut spool = EventSpool::load(path.clone(), 8).unwrap();
        let mut transport = unreachable_transport();
        spool.append(test_event(1, "turn.completed")).unwrap();

        let result = send_or_spool_event(
            &mut transport,
            &mut spool,
            &test_event(2, "bridge.command_ack"),
            false,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(spool.events.len(), 1);
        assert_eq!(spool.events[0].event_type, "turn.completed");
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn bridge_transport_reuses_websocket_for_multiple_events() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind bridge test listener");
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut websocket = accept_bridge_websocket(&listener).await;
            let mut sequences = Vec::new();
            for _ in 0..2 {
                let event = read_bridge_event(&mut websocket).await;
                sequences.push(event.sequence);
                send_bridge_ack(&mut websocket, event.sequence).await;
            }
            sequences
        });
        let mut transport =
            BridgeTransport::new(endpoint, "bridge-token".to_string(), Duration::from_secs(2));

        let first = transport
            .send_event(&test_event(1, "heartbeat"))
            .await
            .unwrap();
        let second = transport
            .send_event(&test_event(2, "turn.completed"))
            .await
            .unwrap();

        assert_eq!(first.transport, "websocket:persistent");
        assert_eq!(second.transport, "websocket:persistent");
        assert_eq!(first.ack.acknowledged_sequence, 1);
        assert_eq!(second.ack.acknowledged_sequence, 2);
        assert_eq!(server.await.unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn bridge_transport_merges_command_push_received_before_ack() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind bridge test listener");
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut websocket = accept_bridge_websocket(&listener).await;
            let event = read_bridge_event(&mut websocket).await;
            send_bridge_command_push(&mut websocket).await;
            send_bridge_ack(&mut websocket, event.sequence).await;
        });
        let mut transport =
            BridgeTransport::new(endpoint, "bridge-token".to_string(), Duration::from_secs(2));

        let result = transport
            .send_event(&test_event(1, "heartbeat"))
            .await
            .unwrap();

        assert_eq!(result.ack.acknowledged_sequence, 1);
        assert_eq!(result.ack.commands.len(), 1);
        assert_eq!(result.ack.commands[0].command_type, "turn.start");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bridge_transport_reads_idle_command_pushes() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind bridge test listener");
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut websocket = accept_bridge_websocket(&listener).await;
            send_bridge_command_push(&mut websocket).await;
        });
        let mut transport =
            BridgeTransport::new(endpoint, "bridge-token".to_string(), Duration::from_secs(2));

        let commands =
            tokio::time::timeout(Duration::from_secs(2), transport.read_pushed_commands())
                .await
                .expect("read command push timed out")
                .unwrap();

        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, "turn.start");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bridge_transport_reconnects_and_resends_after_dropped_socket() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind bridge test listener");
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut sequences = Vec::new();
            let mut first_connection = accept_bridge_websocket(&listener).await;
            sequences.push(read_bridge_event(&mut first_connection).await.sequence);
            first_connection
                .close(None)
                .await
                .expect("close first socket");

            let mut second_connection = accept_bridge_websocket(&listener).await;
            for _ in 0..2 {
                let event = read_bridge_event(&mut second_connection).await;
                sequences.push(event.sequence);
                send_bridge_ack(&mut second_connection, event.sequence).await;
            }
            sequences
        });
        let mut transport =
            BridgeTransport::new(endpoint, "bridge-token".to_string(), Duration::from_secs(2));

        let first = transport
            .send_event(&test_event(1, "heartbeat"))
            .await
            .unwrap();
        let second = transport
            .send_event(&test_event(2, "turn.completed"))
            .await
            .unwrap();

        assert_eq!(first.ack.acknowledged_sequence, 1);
        assert_eq!(second.ack.acknowledged_sequence, 2);
        assert_eq!(server.await.unwrap(), vec![1, 1, 2]);
    }
}
