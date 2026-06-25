use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use codex_app_server_client::{
    AppServerEvent, RemoteAppServerClient, RemoteAppServerConnectArgs, RemoteAppServerEndpoint,
};
use codex_app_server_protocol::{
    AgentMessageDeltaNotification, ApprovalsReviewer, AskForApproval, ClientRequest,
    CommandExecutionApprovalDecision, CommandExecutionRequestApprovalResponse,
    FileChangeApprovalDecision, FileChangeRequestApprovalResponse, GrantedPermissionProfile,
    JSONRPCErrorError, McpServerElicitationAction, McpServerElicitationRequestResponse,
    PermissionGrantScope, PermissionsRequestApprovalResponse, RequestId, SandboxMode,
    SandboxPolicy, ServerNotification, ServerRequest, ThreadItem, ThreadResumeParams,
    ThreadResumeResponse, ThreadSource, ThreadStartParams, ThreadStartResponse,
    TurnInterruptParams, TurnInterruptResponse, TurnStartParams, TurnStartResponse, TurnStatus,
    TurnSteerParams, TurnSteerResponse, UserInput,
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::time::timeout;

const RESULT_MARKER: &str = "SEMAPHORE_CODEX_APP_SERVER_TURN_RESULT_V1";
const CONTROL_RESULT_MARKER: &str = "SEMAPHORE_CODEX_APP_SERVER_CONTROL_RESULT_V1";
const DEFAULT_WEBSOCKET_URL: &str = "ws://127.0.0.1:43113";
const DEFAULT_MODEL: &str = "gpt-5-codex";
const DEFAULT_TIMEOUT_SECONDS: u64 = 1200;
const BRIDGE_SCHEMA_VERSION: i32 = 1;
const BRIDGE_REQUEST_TIMEOUT_SECONDS: u64 = 2;

#[derive(Debug, Parser)]
#[command(version, about = "Semaphore runner for Codex app-server turns")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Turn(TurnCommand),
    Steer(TurnSteerCommand),
    Interrupt(TurnInterruptCommand),
}

#[derive(Debug, Parser)]
struct TurnCommand {
    #[arg(long, env = "SEMAPHORE_CODEX_APP_SERVER_WS", default_value = DEFAULT_WEBSOCKET_URL)]
    websocket_url: String,
    #[arg(long, env = "SEMAPHORE_CODEX_MODEL", default_value = DEFAULT_MODEL)]
    model: String,
    #[arg(long, env = "SEMAPHORE_CODEX_CWD")]
    cwd: Option<PathBuf>,
    #[arg(long, env = "SEMAPHORE_CODEX_PROMPT")]
    message: Option<String>,
    #[arg(long)]
    message_file: Option<PathBuf>,
    #[arg(long, env = "SEMAPHORE_CODEX_CLIENT_MESSAGE_ID")]
    client_message_id: Option<String>,
    #[arg(long, env = "SEMAPHORE_CODEX_THREAD_ID")]
    thread_id: Option<String>,
    #[arg(long, env = "SEMAPHORE_PRODUCT_TURN_ID")]
    product_turn_id: Option<String>,
    #[arg(long, env = "SEMAPHORE_CODEX_TIMEOUT_SECONDS", default_value_t = DEFAULT_TIMEOUT_SECONDS)]
    timeout_seconds: u64,
}

#[derive(Debug, Parser)]
struct TurnSteerCommand {
    #[arg(long, env = "SEMAPHORE_CODEX_APP_SERVER_WS", default_value = DEFAULT_WEBSOCKET_URL)]
    websocket_url: String,
    #[arg(long)]
    thread_id: String,
    #[arg(long)]
    turn_id: String,
    #[arg(long, env = "SEMAPHORE_CODEX_STEER_MESSAGE")]
    message: Option<String>,
    #[arg(long)]
    message_file: Option<PathBuf>,
    #[arg(long, env = "SEMAPHORE_CODEX_CLIENT_MESSAGE_ID")]
    client_message_id: Option<String>,
    #[arg(long, env = "SEMAPHORE_PRODUCT_TURN_ID")]
    product_turn_id: Option<String>,
}

#[derive(Debug, Parser)]
struct TurnInterruptCommand {
    #[arg(long, env = "SEMAPHORE_CODEX_APP_SERVER_WS", default_value = DEFAULT_WEBSOCKET_URL)]
    websocket_url: String,
    #[arg(long)]
    thread_id: String,
    #[arg(long)]
    turn_id: String,
    #[arg(long, env = "SEMAPHORE_PRODUCT_TURN_ID")]
    product_turn_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnResult {
    schema_version: u8,
    source: &'static str,
    status: &'static str,
    response: String,
    thread_id: String,
    codex_session_id: String,
    turn_id: String,
    assistant_item_id: Option<String>,
    model: String,
    workspace_dir: Option<String>,
    thread_reused: bool,
    thread_mode: &'static str,
    server_version: Option<String>,
    codex_home: Option<String>,
    event_count: u64,
    assistant_delta_count: u64,
    item_completed_count: u64,
    server_request_count: u64,
    auto_approved_request_count: u64,
    timeout_seconds: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ControlResult {
    schema_version: u8,
    source: &'static str,
    command: &'static str,
    thread_id: String,
    turn_id: String,
    product_turn_id: Option<String>,
    accepted: bool,
}

#[derive(Debug, Default)]
struct TurnAccumulator {
    response_delta: String,
    completed_agent_message: Option<String>,
    assistant_item_id: Option<String>,
    event_count: u64,
    assistant_delta_count: u64,
    item_completed_count: u64,
    server_request_count: u64,
    auto_approved_request_count: u64,
    interrupted: bool,
}

struct RequestIds {
    next: i64,
}

impl RequestIds {
    fn new() -> Self {
        Self { next: 1 }
    }

    fn next(&mut self) -> RequestId {
        let value = self.next;
        self.next += 1;
        RequestId::Integer(value)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Turn(command) => run_turn(command).await,
        Command::Steer(command) => run_steer(command).await,
        Command::Interrupt(command) => run_interrupt(command).await,
    }
}

async fn run_turn(command: TurnCommand) -> Result<()> {
    let message = read_message(&command)?;
    let timeout_duration = Duration::from_secs(command.timeout_seconds.max(1));
    let cwd_string = command
        .cwd
        .as_ref()
        .map(|path| path.to_string_lossy().to_string());

    let mut client = connect_app_server(&command.websocket_url).await?;

    let server_version = client.server_version().map(ToOwned::to_owned);
    let codex_home = client.codex_home().map(ToOwned::to_owned);
    let mut request_ids = RequestIds::new();
    let mut bridge = match BridgeForwarder::from_env(command.product_turn_id.clone()) {
        Ok(bridge) => bridge,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "Codex runner bridge forwarding is disabled"
            );
            None
        }
    };

    let thread = ensure_thread(&client, &mut request_ids, &command, cwd_string.clone()).await?;

    let mut responsesapi_client_metadata = HashMap::from([
        (
            "origin".to_string(),
            "semaphore_daytona_operator".to_string(),
        ),
        ("runner".to_string(), "semaphore-codex-runner".to_string()),
    ]);
    if let Some(product_turn_id) = command.product_turn_id.as_ref() {
        responsesapi_client_metadata
            .insert("semaphore_turn_id".to_string(), product_turn_id.clone());
    }

    let turn: TurnStartResponse = client
        .request_typed(ClientRequest::TurnStart {
            request_id: request_ids.next(),
            params: TurnStartParams {
                thread_id: thread.id.clone(),
                client_user_message_id: command.client_message_id.clone(),
                input: vec![UserInput::Text {
                    text: message,
                    text_elements: Vec::new(),
                }],
                responsesapi_client_metadata: Some(responsesapi_client_metadata),
                cwd: command.cwd.clone(),
                approval_policy: Some(AskForApproval::Never),
                approvals_reviewer: Some(ApprovalsReviewer::AutoReview),
                sandbox_policy: Some(SandboxPolicy::DangerFullAccess),
                model: Some(command.model.clone()),
                ..TurnStartParams::default()
            },
        })
        .await
        .context("turn/start failed")?;

    let mut accumulator = TurnAccumulator::default();
    let deadline = Instant::now() + timeout_duration;
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            bail!("timed out waiting for Codex turn/completed");
        };
        let event = timeout(remaining, client.next_event())
            .await
            .context("timed out waiting for Codex app-server event")?
            .ok_or_else(|| anyhow!("Codex app-server event stream closed before turn completed"))?;
        accumulator.event_count += 1;
        if handle_event(
            &client,
            event,
            &thread.id,
            &turn.turn.id,
            &mut accumulator,
            bridge.as_mut(),
        )
        .await?
        {
            break;
        }
    }

    client
        .shutdown()
        .await
        .context("failed to shutdown Codex app-server client")?;

    let response = accumulator.completed_agent_message.or_else(|| {
        let response = accumulator.response_delta.trim().to_string();
        (!response.is_empty()).then_some(response)
    });
    let response = if accumulator.interrupted {
        response.unwrap_or_default()
    } else {
        response.ok_or_else(|| anyhow!("Codex turn completed without an assistant response"))?
    };

    let result = TurnResult {
        schema_version: 1,
        source: "codex_app_server_remote_client",
        status: if accumulator.interrupted {
            "interrupted"
        } else {
            "completed"
        },
        response,
        thread_id: thread.id.clone(),
        codex_session_id: thread.session_id.clone(),
        turn_id: turn.turn.id.clone(),
        assistant_item_id: accumulator.assistant_item_id,
        model: command.model,
        workspace_dir: cwd_string,
        thread_reused: thread.reused,
        thread_mode: thread.mode,
        server_version,
        codex_home,
        event_count: accumulator.event_count,
        assistant_delta_count: accumulator.assistant_delta_count,
        item_completed_count: accumulator.item_completed_count,
        server_request_count: accumulator.server_request_count,
        auto_approved_request_count: accumulator.auto_approved_request_count,
        timeout_seconds: command.timeout_seconds,
    };

    println!("{RESULT_MARKER}");
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

struct PreparedThread {
    id: String,
    session_id: String,
    reused: bool,
    mode: &'static str,
}

async fn ensure_thread(
    client: &RemoteAppServerClient,
    request_ids: &mut RequestIds,
    command: &TurnCommand,
    cwd_string: Option<String>,
) -> Result<PreparedThread> {
    if let Some(thread_id) = clean_optional_string(command.thread_id.as_deref()) {
        let resumed: ThreadResumeResponse = client
            .request_typed(ClientRequest::ThreadResume {
                request_id: request_ids.next(),
                params: ThreadResumeParams {
                    thread_id,
                    model: Some(command.model.clone()),
                    cwd: cwd_string,
                    approval_policy: Some(AskForApproval::Never),
                    approvals_reviewer: Some(ApprovalsReviewer::AutoReview),
                    sandbox: Some(SandboxMode::DangerFullAccess),
                    ..ThreadResumeParams::default()
                },
            })
            .await
            .context("thread/resume failed")?;
        return Ok(PreparedThread {
            id: resumed.thread.id,
            session_id: resumed.thread.session_id,
            reused: true,
            mode: "resume_existing",
        });
    }

    let started: ThreadStartResponse = client
        .request_typed(ClientRequest::ThreadStart {
            request_id: request_ids.next(),
            params: ThreadStartParams {
                model: Some(command.model.clone()),
                model_provider: Some("openai".to_string()),
                cwd: cwd_string,
                approval_policy: Some(AskForApproval::Never),
                approvals_reviewer: Some(ApprovalsReviewer::AutoReview),
                sandbox: Some(SandboxMode::DangerFullAccess),
                ephemeral: Some(false),
                thread_source: Some(ThreadSource::Feature("semaphore_operator".to_string())),
                experimental_raw_events: true,
                ..ThreadStartParams::default()
            },
        })
        .await
        .context("thread/start failed")?;
    Ok(PreparedThread {
        id: started.thread.id,
        session_id: started.thread.session_id,
        reused: false,
        mode: "start_new",
    })
}

async fn run_steer(command: TurnSteerCommand) -> Result<()> {
    let message = read_steer_message(&command)?;
    let client = connect_app_server(&command.websocket_url).await?;
    let mut request_ids = RequestIds::new();
    let _: TurnSteerResponse = client
        .request_typed(ClientRequest::TurnSteer {
            request_id: request_ids.next(),
            params: TurnSteerParams {
                thread_id: command.thread_id.clone(),
                client_user_message_id: command.client_message_id.clone(),
                input: vec![UserInput::Text {
                    text: message,
                    text_elements: Vec::new(),
                }],
                responsesapi_client_metadata: None,
                additional_context: None,
                expected_turn_id: command.turn_id.clone(),
            },
        })
        .await
        .context("turn/steer failed")?;
    client
        .shutdown()
        .await
        .context("failed to shutdown Codex app-server client")?;
    print_control_result(ControlResult {
        schema_version: 1,
        source: "codex_app_server_remote_client",
        command: "turn.steer",
        thread_id: command.thread_id,
        turn_id: command.turn_id,
        product_turn_id: command.product_turn_id,
        accepted: true,
    })
}

async fn run_interrupt(command: TurnInterruptCommand) -> Result<()> {
    let client = connect_app_server(&command.websocket_url).await?;
    let mut request_ids = RequestIds::new();
    let _: TurnInterruptResponse = client
        .request_typed(ClientRequest::TurnInterrupt {
            request_id: request_ids.next(),
            params: TurnInterruptParams {
                thread_id: command.thread_id.clone(),
                turn_id: command.turn_id.clone(),
            },
        })
        .await
        .context("turn/interrupt failed")?;
    client
        .shutdown()
        .await
        .context("failed to shutdown Codex app-server client")?;
    print_control_result(ControlResult {
        schema_version: 1,
        source: "codex_app_server_remote_client",
        command: "turn.interrupt",
        thread_id: command.thread_id,
        turn_id: command.turn_id,
        product_turn_id: command.product_turn_id,
        accepted: true,
    })
}

async fn connect_app_server(websocket_url: &str) -> Result<RemoteAppServerClient> {
    RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url: websocket_url.to_string(),
            auth_token: None,
        },
        client_name: "semaphore-codex-runner".to_string(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        experimental_api: true,
        mcp_server_openai_form_elicitation: true,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: 512,
    })
    .await
    .with_context(|| format!("failed to connect to Codex app-server at `{websocket_url}`"))
}

fn print_control_result(result: ControlResult) -> Result<()> {
    println!("{CONTROL_RESULT_MARKER}");
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn read_message(command: &TurnCommand) -> Result<String> {
    if command.message.is_some() && command.message_file.is_some() {
        bail!("provide either --message or --message-file, not both");
    }
    if let Some(message) = command.message.as_ref() {
        return Ok(message.clone());
    }
    if let Some(path) = command.message_file.as_ref() {
        return std::fs::read_to_string(path)
            .with_context(|| format!("failed to read message file `{}`", path.display()));
    }
    bail!("missing turn message; set --message, --message-file, or SEMAPHORE_CODEX_PROMPT")
}

fn read_steer_message(command: &TurnSteerCommand) -> Result<String> {
    if command.message.is_some() && command.message_file.is_some() {
        bail!("provide either --message or --message-file, not both");
    }
    if let Some(message) = command.message.as_ref() {
        return Ok(message.clone());
    }
    if let Some(path) = command.message_file.as_ref() {
        return std::fs::read_to_string(path)
            .with_context(|| format!("failed to read message file `{}`", path.display()));
    }
    bail!("missing steer message; set --message, --message-file, or SEMAPHORE_CODEX_STEER_MESSAGE")
}

async fn handle_event(
    client: &RemoteAppServerClient,
    event: AppServerEvent,
    thread_id: &str,
    turn_id: &str,
    accumulator: &mut TurnAccumulator,
    bridge: Option<&mut BridgeForwarder>,
) -> Result<bool> {
    match event {
        AppServerEvent::Lagged { skipped } => {
            tracing::warn!(skipped, "Codex app-server event stream lagged");
        }
        AppServerEvent::Disconnected { message } => {
            bail!("Codex app-server disconnected: {message}");
        }
        AppServerEvent::ServerRequest(request) => {
            accumulator.server_request_count += 1;
            let method = server_request_method_name(&request);
            let server_request_id = server_request_id_string(&request);
            if auto_resolve_server_request(client, request).await? {
                accumulator.auto_approved_request_count += 1;
                if let Some(bridge) = bridge {
                    bridge
                        .forward_server_request(
                            &method,
                            &server_request_id,
                            turn_id,
                            true,
                            accumulator.server_request_count,
                            accumulator.event_count,
                        )
                        .await;
                }
            } else if let Some(bridge) = bridge {
                bridge
                    .forward_server_request(
                        &method,
                        &server_request_id,
                        turn_id,
                        false,
                        accumulator.server_request_count,
                        accumulator.event_count,
                    )
                    .await;
            }
        }
        AppServerEvent::ServerNotification(notification) => {
            if let Some(bridge) = bridge {
                bridge
                    .forward_notification(&notification, accumulator.event_count)
                    .await;
            }
            if handle_notification(notification, thread_id, turn_id, accumulator)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

struct BridgeForwarder {
    client: reqwest::Client,
    endpoint: String,
    bridge_token: String,
    organization_id: String,
    runtime_id: String,
    bridge_epoch: String,
    sequence: i64,
    product_turn_id: Option<String>,
}

impl BridgeForwarder {
    fn from_env(product_turn_id: Option<String>) -> Result<Option<Self>> {
        let api_base_url = optional_env("SEMAPHORE_API_BASE_URL");
        let organization_id = optional_env("SEMAPHORE_ORGANIZATION_ID");
        let product_session_id = optional_env("SEMAPHORE_PRODUCT_SESSION_ID");
        let runtime_id = optional_env("SEMAPHORE_RUNTIME_ID");
        let bridge_token = optional_env("SEMAPHORE_SANDBOX_BRIDGE_TOKEN");
        if api_base_url.is_none()
            && organization_id.is_none()
            && product_session_id.is_none()
            && runtime_id.is_none()
            && bridge_token.is_none()
        {
            return Ok(None);
        }
        let api_base_url =
            api_base_url.ok_or_else(|| anyhow!("SEMAPHORE_API_BASE_URL is required"))?;
        let organization_id =
            organization_id.ok_or_else(|| anyhow!("SEMAPHORE_ORGANIZATION_ID is required"))?;
        let product_session_id = product_session_id
            .ok_or_else(|| anyhow!("SEMAPHORE_PRODUCT_SESSION_ID is required"))?;
        let runtime_id = runtime_id.ok_or_else(|| anyhow!("SEMAPHORE_RUNTIME_ID is required"))?;
        let bridge_token =
            bridge_token.ok_or_else(|| anyhow!("SEMAPHORE_SANDBOX_BRIDGE_TOKEN is required"))?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(BRIDGE_REQUEST_TIMEOUT_SECONDS))
            .build()
            .context("failed to build bridge HTTP client")?;
        Ok(Some(Self {
            client,
            endpoint: bridge_events_url(&api_base_url, &product_session_id),
            bridge_token,
            organization_id,
            bridge_epoch: default_bridge_epoch(&runtime_id),
            runtime_id,
            sequence: 1,
            product_turn_id,
        }))
    }

    async fn forward_notification(&mut self, notification: &ServerNotification, event_count: u64) {
        let event_type = notification_bridge_event_type(notification);
        let payload =
            notification_bridge_payload(notification, self.product_turn_id.as_deref(), event_count);
        if let Err(error) = self.post_event(event_type, payload).await {
            tracing::warn!(
                event_type,
                error = %error,
                "failed to forward Codex notification through sandbox bridge"
            );
        }
    }

    async fn forward_server_request(
        &mut self,
        method: &str,
        server_request_id: &str,
        codex_turn_id: &str,
        auto_approved: bool,
        server_request_count: u64,
        event_count: u64,
    ) {
        let payload = server_request_bridge_payload(
            method,
            server_request_id,
            codex_turn_id,
            auto_approved,
            self.product_turn_id.as_deref(),
            server_request_count,
            event_count,
        );
        if let Err(error) = self.post_event("codex.notification", payload).await {
            tracing::warn!(
                method,
                error = %error,
                "failed to forward Codex server request through sandbox bridge"
            );
        }
    }

    async fn post_event(&mut self, event_type: &str, payload: Value) -> Result<()> {
        let sequence = self.sequence;
        self.sequence += 1;
        let body = bridge_event_body(
            &self.organization_id,
            &self.runtime_id,
            &self.bridge_epoch,
            sequence,
            event_type,
            payload,
        );
        let response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.bridge_token)
            .json(&body)
            .send()
            .await
            .context("failed to send bridge event")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "bridge event was rejected with {}: {}",
                status,
                bridge_error_body(&body)
            ));
        }
        Ok(())
    }
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn clean_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn bridge_events_url(api_base_url: &str, product_session_id: &str) -> String {
    format!(
        "{}/api/sessions/{}/bridge/events",
        api_base_url.trim_end_matches('/'),
        product_session_id
    )
}

fn default_bridge_epoch(runtime_id: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or_default();
    format!("runner-{runtime_id}:pid-{}:{millis}", std::process::id())
}

fn bridge_event_body(
    organization_id: &str,
    runtime_id: &str,
    bridge_epoch: &str,
    sequence: i64,
    event_type: &str,
    payload: Value,
) -> Value {
    json!({
        "organizationId": organization_id,
        "runtimeId": runtime_id,
        "schemaVersion": BRIDGE_SCHEMA_VERSION,
        "bridgeEpoch": bridge_epoch,
        "sequence": sequence,
        "idempotencyKey": format!("{bridge_epoch}:{sequence}"),
        "type": event_type,
        "payload": payload,
    })
}

fn notification_bridge_event_type(notification: &ServerNotification) -> &'static str {
    match notification {
        ServerNotification::CommandExecutionOutputDelta(_) => "command.output",
        ServerNotification::FileChangePatchUpdated(_) => "file.changed",
        _ => "codex.notification",
    }
}

fn notification_bridge_payload(
    notification: &ServerNotification,
    product_turn_id: Option<&str>,
    event_count: u64,
) -> Value {
    let method = notification_method_name(notification);
    let value = serde_json::to_value(notification).unwrap_or_else(|_| json!({}));
    json!({
        "source": "semaphore-codex-runner",
        "sourceKind": "server_notification",
        "message": format!("Codex notification: {method}"),
        "notificationMethod": method,
        "productTurnId": product_turn_id,
        "threadId": notification_param_string(&value, "threadId"),
        "turnId": notification_turn_id(&value),
        "itemId": notification_param_string(&value, "itemId"),
        "eventCount": event_count,
        "payloadSummary": notification_payload_summary(&value),
    })
}

fn server_request_bridge_payload(
    method: &str,
    server_request_id: &str,
    codex_turn_id: &str,
    auto_approved: bool,
    product_turn_id: Option<&str>,
    server_request_count: u64,
    event_count: u64,
) -> Value {
    json!({
        "source": "semaphore-codex-runner",
        "sourceKind": "server_request",
        "message": format!("Codex server request: {method}"),
        "notificationMethod": "serverRequest/resolved",
        "serverRequestId": server_request_id,
        "serverRequestMethod": method,
        "productTurnId": product_turn_id,
        "codexTurnId": codex_turn_id,
        "autoApprovedByProductPolicy": auto_approved,
        "serverRequestCount": server_request_count,
        "eventCount": event_count,
    })
}

fn notification_method_name(notification: &ServerNotification) -> String {
    serde_json::to_value(notification)
        .ok()
        .and_then(|value| {
            value
                .get("method")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn notification_param_string(value: &Value, key: &str) -> Option<String> {
    value
        .get("params")
        .and_then(|params| params.get(key))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn notification_turn_id(value: &Value) -> Option<String> {
    notification_param_string(value, "turnId").or_else(|| {
        value
            .get("params")
            .and_then(|params| params.get("turn"))
            .and_then(|turn| turn.get("id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    })
}

fn notification_payload_summary(value: &Value) -> Value {
    let Some(params) = value.get("params") else {
        return json!({});
    };
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut summary = serde_json::Map::new();
    for key in [
        "status",
        "stream",
        "changeType",
        "path",
        "kind",
        "type",
        "name",
    ] {
        if let Some(value) = params.get(key).and_then(Value::as_str) {
            summary.insert(key.to_string(), json!(value));
        }
    }
    for key in ["exitCode", "byteCount", "sequenceNumber"] {
        if let Some(value) = params.get(key).and_then(Value::as_i64) {
            summary.insert(key.to_string(), json!(value));
        }
    }
    match method {
        "item/agentMessage/delta" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "delta",
                "assistantTextDelta",
                "assistantTextDeltaTruncated",
                4_096,
            );
        }
        "item/plan/delta" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "delta",
                "planTextDelta",
                "planTextDeltaTruncated",
                4_096,
            );
        }
        "item/reasoning/summaryTextDelta" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "delta",
                "reasoningSummaryTextDelta",
                "reasoningSummaryTextDeltaTruncated",
                4_096,
            );
            insert_i64_field(&mut summary, params, "summaryIndex", "summaryIndex");
        }
        "item/reasoning/textDelta" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "delta",
                "reasoningTextDelta",
                "reasoningTextDeltaTruncated",
                4_096,
            );
            insert_i64_field(&mut summary, params, "contentIndex", "contentIndex");
        }
        "item/reasoning/summaryPartAdded" => {
            insert_i64_field(&mut summary, params, "summaryIndex", "summaryIndex");
        }
        "thread/status/changed" => {
            insert_thread_status_summary(&mut summary, params.get("status"));
        }
        "thread/name/updated" => {
            copy_string_field(&mut summary, params, "threadName", "threadName", 160);
        }
        "thread/goal/updated" => {
            insert_thread_goal_summary(&mut summary, params.get("goal"));
        }
        "thread/goal/cleared" => {
            summary.insert("goalCleared".to_string(), json!(true));
        }
        "item/autoApprovalReview/started" => {
            insert_auto_review_summary(&mut summary, params, false);
        }
        "item/autoApprovalReview/completed" => {
            insert_auto_review_summary(&mut summary, params, true);
        }
        "item/commandExecution/outputDelta" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "delta",
                "commandOutputDelta",
                "commandOutputDeltaTruncated",
                4_096,
            );
            if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                summary.insert(
                    "commandOutputDeltaByteCount".to_string(),
                    json!(delta.len()),
                );
                summary.insert(
                    "commandOutputDeltaLineCount".to_string(),
                    json!(delta.lines().count()),
                );
            }
        }
        "item/commandExecution/terminalInteraction" => {
            copy_string_field(&mut summary, params, "processId", "processId", 160);
            if let Some(stdin) = params.get("stdin").and_then(Value::as_str) {
                summary.insert("stdinByteCount".to_string(), json!(stdin.len()));
                summary.insert("stdinLineCount".to_string(), json!(stdin.lines().count()));
            }
        }
        "item/fileChange/outputDelta" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "delta",
                "fileChangeOutputDelta",
                "fileChangeOutputDeltaTruncated",
                4_096,
            );
            if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                summary.insert(
                    "fileChangeOutputDeltaByteCount".to_string(),
                    json!(delta.len()),
                );
                summary.insert(
                    "fileChangeOutputDeltaLineCount".to_string(),
                    json!(delta.lines().count()),
                );
            }
        }
        "item/mcpToolCall/progress" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "message",
                "progressMessage",
                "progressMessageTruncated",
                1_024,
            );
        }
        "mcpServer/startupStatus/updated" => {
            copy_string_field(&mut summary, params, "name", "serverName", 160);
            copy_string_field(&mut summary, params, "status", "serverStatus", 80);
            insert_bounded_optional_string(
                &mut summary,
                params,
                "error",
                "errorPreview",
                "errorPreviewTruncated",
                1_024,
            );
        }
        "fs/changed" => {
            insert_changed_paths_summary(&mut summary, params);
        }
        "model/rerouted" => {
            copy_string_field(&mut summary, params, "fromModel", "fromModel", 160);
            copy_string_field(&mut summary, params, "toModel", "toModel", 160);
            copy_string_field(&mut summary, params, "reason", "reason", 160);
        }
        "model/verification" => {
            insert_string_array_summary(
                &mut summary,
                params,
                "verifications",
                "verifications",
                "verificationCount",
                20,
                160,
            );
        }
        "model/safetyBuffering/updated" => {
            copy_string_field(&mut summary, params, "model", "model", 160);
            insert_string_array_summary(
                &mut summary,
                params,
                "useCases",
                "useCases",
                "useCaseCount",
                20,
                160,
            );
            insert_string_array_summary(
                &mut summary,
                params,
                "reasons",
                "reasons",
                "reasonCount",
                20,
                240,
            );
        }
        "warning" | "guardianWarning" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "message",
                "message",
                "messageTruncated",
                1_024,
            );
        }
        "configWarning" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "summary",
                "summary",
                "summaryTruncated",
                1_024,
            );
            insert_bounded_optional_string(
                &mut summary,
                params,
                "details",
                "detailsPreview",
                "detailsPreviewTruncated",
                1_024,
            );
            copy_string_field(&mut summary, params, "path", "configPath", 512);
        }
        "error" => {
            insert_error_summary(&mut summary, params);
        }
        "thread/tokenUsage/updated" => {
            insert_token_usage_summary(&mut summary, params);
        }
        "turn/diff/updated" => {
            insert_bounded_text_summary(
                &mut summary,
                params,
                "diff",
                "diffPreview",
                "diffPreviewTruncated",
                4_096,
            );
            if let Some(diff) = params.get("diff").and_then(Value::as_str) {
                summary.insert("diffLineCount".to_string(), json!(diff.lines().count()));
                summary.insert("diffByteCount".to_string(), json!(diff.len()));
            }
        }
        "item/fileChange/patchUpdated" => {
            if let Some(changes) = params.get("changes").and_then(Value::as_array) {
                summary.insert("changeCount".to_string(), json!(changes.len()));
            }
        }
        "turn/plan/updated" => {
            if let Some(explanation) = params.get("explanation").and_then(Value::as_str) {
                let (value, truncated) = bounded_text(explanation, 2_048);
                summary.insert("explanation".to_string(), json!(value));
                summary.insert("explanationTruncated".to_string(), json!(truncated));
            }
            if let Some(plan) = summarize_plan_steps(params.get("plan")) {
                summary.insert("plan".to_string(), plan);
            }
        }
        "item/started" | "item/completed" => {
            if let Some(item) = params.get("item") {
                let item_summary = summarize_thread_item(item);
                if let Some(kind) = item_summary.get("kind").cloned() {
                    summary.insert("itemKind".to_string(), kind);
                }
                if let Some(status) = item_summary.get("status").cloned() {
                    summary.insert("itemStatus".to_string(), status);
                }
                if let Some(outcome) = item_summary.get("outcome").cloned() {
                    summary.insert("itemOutcome".to_string(), outcome);
                }
                summary.insert("item".to_string(), item_summary);
            }
        }
        _ => {}
    }
    Value::Object(summary)
}

fn insert_token_usage_summary(summary: &mut serde_json::Map<String, Value>, params: &Value) {
    let Some(token_usage) = params.get("tokenUsage") else {
        return;
    };
    insert_token_breakdown(summary, token_usage.get("total"), "");
    insert_token_breakdown(summary, token_usage.get("last"), "last");
    insert_i64_field(
        summary,
        token_usage,
        "modelContextWindow",
        "modelContextWindow",
    );
}

fn insert_token_breakdown(
    summary: &mut serde_json::Map<String, Value>,
    value: Option<&Value>,
    prefix: &str,
) {
    let Some(value) = value else {
        return;
    };
    let fields = [
        ("totalTokens", "totalTokens"),
        ("inputTokens", "inputTokens"),
        ("cachedInputTokens", "cachedInputTokens"),
        ("outputTokens", "outputTokens"),
        ("reasoningOutputTokens", "reasoningOutputTokens"),
    ];
    for (source_key, output_key) in fields {
        let output_key = if prefix.is_empty() {
            output_key.to_string()
        } else {
            format!("{prefix}{}", uppercase_first(output_key))
        };
        insert_i64_field(summary, value, source_key, &output_key);
    }
}

fn uppercase_first(value: &str) -> String {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    format!("{}{}", first.to_ascii_uppercase(), chars.as_str())
}

fn insert_thread_status_summary(
    summary: &mut serde_json::Map<String, Value>,
    status: Option<&Value>,
) {
    let Some(status) = status else {
        return;
    };
    if let Some(status) = status.as_str() {
        summary.insert("threadStatus".to_string(), json!(status));
        return;
    }
    if let Some(status_type) = status.get("type").and_then(Value::as_str) {
        summary.insert("threadStatus".to_string(), json!(status_type));
    }
    insert_string_array_summary(
        summary,
        status,
        "activeFlags",
        "activeFlags",
        "activeFlagCount",
        10,
        80,
    );
}

fn insert_thread_goal_summary(summary: &mut serde_json::Map<String, Value>, goal: Option<&Value>) {
    let Some(goal) = goal else {
        return;
    };
    copy_string_field(summary, goal, "status", "goalStatus", 80);
    copy_string_field(summary, goal, "objective", "objective", 2_048);
    insert_i64_field(summary, goal, "tokenBudget", "tokenBudget");
    insert_i64_field(summary, goal, "tokensUsed", "tokensUsed");
    insert_i64_field(summary, goal, "timeUsedSeconds", "timeUsedSeconds");
}

fn insert_auto_review_summary(
    summary: &mut serde_json::Map<String, Value>,
    params: &Value,
    completed: bool,
) {
    copy_string_field(summary, params, "reviewId", "reviewId", 160);
    copy_string_field(summary, params, "targetItemId", "targetItemId", 160);
    if completed {
        copy_string_field(summary, params, "decisionSource", "decisionSource", 80);
    }
    insert_i64_field(summary, params, "startedAtMs", "startedAtMs");
    insert_i64_field(summary, params, "completedAtMs", "completedAtMs");
    if let (Some(started), Some(completed_at)) = (
        params.get("startedAtMs").and_then(Value::as_i64),
        params.get("completedAtMs").and_then(Value::as_i64),
    ) {
        summary.insert(
            "durationMs".to_string(),
            json!(completed_at.saturating_sub(started)),
        );
    }
    if let Some(review) = params.get("review") {
        copy_string_field(summary, review, "status", "reviewStatus", 80);
        copy_string_field(summary, review, "riskLevel", "riskLevel", 80);
        copy_string_field(summary, review, "userAuthorization", "reviewUserLevel", 80);
        insert_bounded_optional_string(
            summary,
            review,
            "rationale",
            "rationalePreview",
            "rationalePreviewTruncated",
            1_024,
        );
    }
    if let Some(action) = params.get("action") {
        insert_auto_review_action_summary(summary, action);
    }
}

fn insert_auto_review_action_summary(summary: &mut serde_json::Map<String, Value>, action: &Value) {
    copy_string_field(summary, action, "type", "actionType", 80);
    copy_string_field(summary, action, "source", "actionSource", 80);
    match action
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "command" => {
            copy_string_field(summary, action, "command", "actionPreview", 512);
            copy_string_field(summary, action, "cwd", "cwd", 512);
        }
        "execve" => {
            copy_string_field(summary, action, "program", "actionPreview", 512);
            copy_string_field(summary, action, "cwd", "cwd", 512);
            if let Some(argv) = action.get("argv").and_then(Value::as_array) {
                summary.insert("argvCount".to_string(), json!(argv.len()));
            }
        }
        "applyPatch" => {
            copy_string_field(summary, action, "cwd", "cwd", 512);
            insert_string_array_summary(
                summary,
                action,
                "files",
                "filePaths",
                "fileCount",
                20,
                512,
            );
        }
        "networkAccess" => {
            copy_string_field(summary, action, "host", "host", 255);
            copy_string_field(summary, action, "protocol", "protocol", 40);
            copy_string_field(summary, action, "target", "actionPreview", 512);
            insert_i64_field(summary, action, "port", "port");
        }
        "mcpToolCall" => {
            copy_string_field(summary, action, "server", "server", 160);
            copy_string_field(summary, action, "toolName", "toolName", 160);
            copy_string_field(summary, action, "toolTitle", "actionPreview", 240);
            copy_string_field(summary, action, "connectorName", "connectorName", 160);
        }
        "requestPermissions" => {
            summary.insert("permissionProfileRequested".to_string(), json!(true));
            insert_bounded_optional_string(
                summary,
                action,
                "reason",
                "actionPreview",
                "actionPreviewTruncated",
                512,
            );
        }
        _ => {}
    }
}

fn insert_string_array_summary(
    summary: &mut serde_json::Map<String, Value>,
    value: &Value,
    source_key: &str,
    output_key: &str,
    count_key: &str,
    max_items: usize,
    max_chars: usize,
) {
    let Some(items) = value.get(source_key).and_then(Value::as_array) else {
        return;
    };
    summary.insert(count_key.to_string(), json!(items.len()));
    let preview = items
        .iter()
        .filter_map(Value::as_str)
        .take(max_items)
        .map(|value| {
            let (value, truncated) = bounded_text(value, max_chars);
            json!({
                "value": value,
                "truncated": truncated,
            })
        })
        .collect::<Vec<_>>();
    summary.insert(output_key.to_string(), json!(preview));
    summary.insert(
        format!("{output_key}Truncated"),
        json!(items.len() > max_items),
    );
}

fn insert_changed_paths_summary(summary: &mut serde_json::Map<String, Value>, params: &Value) {
    copy_string_field(summary, params, "watchId", "watchId", 160);
    insert_string_array_summary(
        summary,
        params,
        "changedPaths",
        "changedPaths",
        "changedPathCount",
        20,
        512,
    );
}

fn insert_error_summary(summary: &mut serde_json::Map<String, Value>, params: &Value) {
    if let Some(will_retry) = params.get("willRetry").and_then(Value::as_bool) {
        summary.insert("willRetry".to_string(), json!(will_retry));
    }
    let Some(error) = params.get("error") else {
        return;
    };
    if let Some(message) = error.as_str() {
        let (message, truncated) = bounded_text(message, 1_024);
        summary.insert("message".to_string(), json!(message));
        summary.insert("messageTruncated".to_string(), json!(truncated));
        return;
    }
    insert_bounded_optional_string(
        summary,
        error,
        "message",
        "message",
        "messageTruncated",
        1_024,
    );
    insert_bounded_optional_string(
        summary,
        error,
        "additionalDetails",
        "detailsPreview",
        "detailsPreviewTruncated",
        1_024,
    );
}

fn insert_bounded_text_summary(
    summary: &mut serde_json::Map<String, Value>,
    params: &Value,
    source_key: &str,
    value_key: &str,
    truncated_key: &str,
    max_chars: usize,
) {
    let Some(value) = params.get(source_key).and_then(Value::as_str) else {
        return;
    };
    let (value, truncated) = bounded_text(value, max_chars);
    summary.insert(value_key.to_string(), json!(value));
    summary.insert(truncated_key.to_string(), json!(truncated));
}

fn insert_bounded_optional_string(
    summary: &mut serde_json::Map<String, Value>,
    params: &Value,
    source_key: &str,
    value_key: &str,
    truncated_key: &str,
    max_chars: usize,
) {
    if params.get(source_key).is_some_and(Value::is_null) {
        return;
    }
    insert_bounded_text_summary(
        summary,
        params,
        source_key,
        value_key,
        truncated_key,
        max_chars,
    );
}

fn insert_i64_field(
    summary: &mut serde_json::Map<String, Value>,
    value: &Value,
    source_key: &str,
    output_key: &str,
) {
    if let Some(number) = value.get(source_key).and_then(Value::as_i64) {
        summary.insert(output_key.to_string(), json!(number));
    }
}

fn summarize_plan_steps(value: Option<&Value>) -> Option<Value> {
    let steps = value?.as_array()?;
    let steps = steps
        .iter()
        .take(20)
        .map(|step| {
            let mut summary = serde_json::Map::new();
            if let Some(text) = step.get("step").and_then(Value::as_str) {
                let (text, truncated) = bounded_text(text, 512);
                summary.insert("step".to_string(), json!(text));
                summary.insert("stepTruncated".to_string(), json!(truncated));
            }
            if let Some(status) = step.get("status").and_then(Value::as_str) {
                summary.insert("status".to_string(), json!(status));
            }
            Value::Object(summary)
        })
        .collect::<Vec<_>>();
    Some(json!({
        "steps": steps,
        "totalCount": value.and_then(Value::as_array).map_or(0, Vec::len),
        "truncated": value.and_then(Value::as_array).is_some_and(|value| value.len() > 20),
    }))
}

fn summarize_thread_item(item: &Value) -> Value {
    let mut summary = serde_json::Map::new();
    copy_string_field(&mut summary, item, "type", "kind", 80);
    copy_string_field(&mut summary, item, "id", "id", 160);
    copy_string_field(&mut summary, item, "status", "status", 80);
    copy_string_field(&mut summary, item, "server", "server", 160);
    copy_string_field(&mut summary, item, "tool", "tool", 160);
    copy_string_field(&mut summary, item, "namespace", "namespace", 160);
    copy_string_field(&mut summary, item, "source", "source", 80);
    copy_string_field(&mut summary, item, "cwd", "cwd", 512);
    copy_string_field(&mut summary, item, "path", "path", 512);
    copy_string_field(&mut summary, item, "savedPath", "savedPath", 512);
    copy_string_field(&mut summary, item, "command", "commandPreview", 512);
    insert_web_search_summary(&mut summary, item);
    insert_collab_agent_summary(&mut summary, item);
    insert_sub_agent_activity_summary(&mut summary, item);
    insert_review_mode_summary(&mut summary, item);
    insert_context_compaction_summary(&mut summary, item);
    insert_bounded_optional_string(
        &mut summary,
        item,
        "revisedPrompt",
        "revisedPromptPreview",
        "revisedPromptPreviewTruncated",
        1_024,
    );
    insert_image_generation_result_summary(&mut summary, item);
    insert_i64_field(&mut summary, item, "exitCode", "exitCode");
    insert_i64_field(&mut summary, item, "durationMs", "durationMs");
    if let Some(success) = item.get("success").and_then(Value::as_bool) {
        summary.insert("success".to_string(), json!(success));
    }
    if let Some(changes) = item.get("changes").and_then(Value::as_array) {
        summary.insert("changeCount".to_string(), json!(changes.len()));
    }
    if let Some(outcome) = thread_item_outcome(item) {
        summary.insert("outcome".to_string(), json!(outcome));
    }
    for key in [
        "arguments",
        "result",
        "error",
        "aggregatedOutput",
        "contentItems",
    ] {
        if item.get(key).is_some() {
            summary.insert(format!("{key}Present"), json!(true));
        }
    }
    Value::Object(summary)
}

fn insert_collab_agent_summary(summary: &mut serde_json::Map<String, Value>, item: &Value) {
    if item.get("type").and_then(Value::as_str) != Some("collabAgentToolCall") {
        return;
    }
    copy_string_field(summary, item, "senderThreadId", "senderThreadId", 160);
    copy_string_field(summary, item, "model", "model", 160);
    copy_string_field(summary, item, "reasoningEffort", "reasoningEffort", 80);
    insert_bounded_optional_string(
        summary,
        item,
        "prompt",
        "promptPreview",
        "promptPreviewTruncated",
        1_024,
    );
    if let Some(receivers) = item.get("receiverThreadIds").and_then(Value::as_array) {
        summary.insert("receiverThreadCount".to_string(), json!(receivers.len()));
    }
    if let Some(states) = item.get("agentsStates").and_then(Value::as_object) {
        summary.insert("agentStateCount".to_string(), json!(states.len()));
    }
}

fn insert_sub_agent_activity_summary(summary: &mut serde_json::Map<String, Value>, item: &Value) {
    if item.get("type").and_then(Value::as_str) != Some("subAgentActivity") {
        return;
    }
    copy_string_field(summary, item, "kind", "subAgentKind", 80);
    copy_string_field(summary, item, "agentThreadId", "agentThreadId", 160);
    copy_string_field(summary, item, "agentPath", "agentPath", 512);
}

fn insert_review_mode_summary(summary: &mut serde_json::Map<String, Value>, item: &Value) {
    if !matches!(
        item.get("type").and_then(Value::as_str),
        Some("enteredReviewMode" | "exitedReviewMode")
    ) {
        return;
    }
    insert_bounded_optional_string(
        summary,
        item,
        "review",
        "reviewPreview",
        "reviewPreviewTruncated",
        1_024,
    );
}

fn insert_context_compaction_summary(summary: &mut serde_json::Map<String, Value>, item: &Value) {
    if item.get("type").and_then(Value::as_str) == Some("contextCompaction") {
        summary.insert("contextCompaction".to_string(), json!(true));
    }
}

fn insert_web_search_summary(summary: &mut serde_json::Map<String, Value>, item: &Value) {
    if !matches!(
        item.get("type").and_then(Value::as_str),
        Some("webSearch" | "web_search")
    ) {
        return;
    }

    insert_bounded_optional_string(
        summary,
        item,
        "query",
        "queryPreview",
        "queryPreviewTruncated",
        512,
    );

    let Some(action) = item.get("action").filter(|value| !value.is_null()) else {
        return;
    };
    summary.insert("actionPresent".to_string(), json!(true));
    copy_string_field(summary, action, "type", "actionType", 80);

    match action.get("type").and_then(Value::as_str) {
        Some("search") => {
            insert_bounded_optional_string(
                summary,
                action,
                "query",
                "actionQueryPreview",
                "actionQueryPreviewTruncated",
                512,
            );
            insert_string_array_summary(
                summary,
                action,
                "queries",
                "actionQueries",
                "actionQueryCount",
                10,
                256,
            );
        }
        Some("openPage") => {
            insert_safe_url_preview(summary, action, "url", "actionUrlPreview", 512);
        }
        Some("findInPage") => {
            insert_safe_url_preview(summary, action, "url", "actionUrlPreview", 512);
            insert_bounded_optional_string(
                summary,
                action,
                "pattern",
                "actionPatternPreview",
                "actionPatternPreviewTruncated",
                256,
            );
        }
        _ => {}
    }
}

fn insert_safe_url_preview(
    summary: &mut serde_json::Map<String, Value>,
    value: &Value,
    source_key: &str,
    output_key: &str,
    max_chars: usize,
) {
    let Some(url) = value.get(source_key).and_then(Value::as_str) else {
        return;
    };
    let Some((preview, truncated)) = safe_url_preview(url, max_chars) else {
        return;
    };
    summary.insert(output_key.to_string(), json!(preview));
    if truncated {
        summary.insert(format!("{output_key}Truncated"), json!(true));
    }
}

fn safe_url_preview(value: &str, max_chars: usize) -> Option<(String, bool)> {
    let value = value.trim();
    if value.is_empty() || value.chars().any(char::is_control) {
        return None;
    }
    let lower = value.to_ascii_lowercase();
    if !lower.starts_with("http://") && !lower.starts_with("https://") {
        return None;
    }
    let after_scheme = value.split_once("://")?.1;
    let authority = after_scheme.split('/').next().unwrap_or_default();
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    let without_fragment = value.split('#').next().unwrap_or(value);
    let without_query = without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment);
    let preview = without_query.trim_end_matches('/');
    if preview.is_empty() {
        return None;
    }
    Some(bounded_text(preview, max_chars))
}

fn insert_image_generation_result_summary(
    summary: &mut serde_json::Map<String, Value>,
    item: &Value,
) {
    if item.get("type").and_then(Value::as_str) != Some("imageGeneration") {
        return;
    }
    let Some(result) = item.get("result").and_then(Value::as_str) else {
        return;
    };
    summary.insert("resultByteCount".to_string(), json!(result.len()));
    let result_kind = image_generation_result_kind(result);
    summary.insert("resultKind".to_string(), json!(result_kind));
    if let Some((preview, truncated)) = safe_image_generation_result_preview(result, result_kind) {
        summary.insert("resultPreview".to_string(), json!(preview));
        summary.insert("resultPreviewTruncated".to_string(), json!(truncated));
    }
}

fn image_generation_result_kind(value: &str) -> &'static str {
    let value = value.trim();
    if value.is_empty() {
        "empty"
    } else if value.starts_with("data:") {
        "data_url"
    } else if value.starts_with("http://") || value.starts_with("https://") {
        "url"
    } else if value.starts_with('/') {
        "sandbox_path"
    } else if looks_like_base64_payload(value) {
        "base64_payload"
    } else {
        "text"
    }
}

fn looks_like_base64_payload(value: &str) -> bool {
    value.len() >= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=' | b'\r' | b'\n')
        })
}

fn safe_image_generation_result_preview(value: &str, result_kind: &str) -> Option<(String, bool)> {
    match result_kind {
        "sandbox_path" if safe_result_path(value) => Some(bounded_text(value, 512)),
        "text" => Some(bounded_text(value, 256)),
        _ => None,
    }
}

fn safe_result_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value.starts_with('/')
        && !value.chars().any(char::is_control)
}

fn thread_item_outcome(item: &Value) -> Option<&'static str> {
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_lowercase());
    if matches!(
        status.as_deref(),
        Some("failed" | "error" | "errored" | "timedout" | "timed_out")
    ) || item.get("error").is_some()
        || item.get("success").and_then(Value::as_bool) == Some(false)
        || item
            .get("exitCode")
            .and_then(Value::as_i64)
            .is_some_and(|exit_code| exit_code != 0)
    {
        return Some("failed");
    }
    if matches!(
        status.as_deref(),
        Some("declined" | "canceled" | "cancelled" | "interrupted" | "aborted")
    ) {
        return Some("canceled");
    }
    if matches!(status.as_deref(), Some("redacted")) {
        return Some("redacted");
    }
    if matches!(
        status.as_deref(),
        Some("inprogress" | "in_progress" | "running" | "started" | "pending")
    ) {
        return Some("running");
    }
    if item.get("success").and_then(Value::as_bool) == Some(true)
        || item.get("exitCode").and_then(Value::as_i64) == Some(0)
        || matches!(
            status.as_deref(),
            Some("completed" | "succeeded" | "success")
        )
    {
        return Some("success");
    }
    None
}

fn copy_string_field(
    summary: &mut serde_json::Map<String, Value>,
    value: &Value,
    source_key: &str,
    output_key: &str,
    max_chars: usize,
) {
    let Some(text) = value.get(source_key).and_then(Value::as_str) else {
        return;
    };
    let (text, truncated) = bounded_text(text, max_chars);
    summary.insert(output_key.to_string(), json!(text));
    if truncated {
        summary.insert(format!("{output_key}Truncated"), json!(true));
    }
}

fn bounded_text(value: &str, max_chars: usize) -> (String, bool) {
    let mut chars = value.chars();
    let text = chars.by_ref().take(max_chars).collect::<String>();
    let truncated = chars.next().is_some();
    (text, truncated)
}

fn bridge_error_body(body: &str) -> String {
    let summary = body.trim();
    if summary.is_empty() {
        "request failed".to_string()
    } else {
        summary.chars().take(240).collect()
    }
}

fn handle_notification(
    notification: ServerNotification,
    thread_id: &str,
    turn_id: &str,
    accumulator: &mut TurnAccumulator,
) -> Result<bool> {
    match notification {
        ServerNotification::AgentMessageDelta(notification)
            if notification_matches(&notification, thread_id, turn_id) =>
        {
            accumulator.assistant_delta_count += 1;
            accumulator.assistant_item_id = Some(notification.item_id.clone());
            accumulator.response_delta.push_str(&notification.delta);
        }
        ServerNotification::ItemCompleted(notification)
            if notification.thread_id == thread_id && notification.turn_id == turn_id =>
        {
            accumulator.item_completed_count += 1;
            if let ThreadItem::AgentMessage { id, text, .. } = notification.item {
                accumulator.assistant_item_id = Some(id);
                accumulator.completed_agent_message = Some(text);
            }
        }
        ServerNotification::TurnCompleted(notification)
            if notification.thread_id == thread_id && notification.turn.id == turn_id =>
        {
            for item in notification.turn.items {
                if let ThreadItem::AgentMessage { id, text, .. } = item {
                    accumulator.assistant_item_id = Some(id);
                    accumulator.completed_agent_message = Some(text);
                }
            }
            match notification.turn.status {
                TurnStatus::Completed => return Ok(true),
                TurnStatus::Failed => {
                    let message = notification
                        .turn
                        .error
                        .map(|error| error.message)
                        .unwrap_or_else(|| "Codex turn failed".to_string());
                    bail!("{message}");
                }
                TurnStatus::Interrupted => {
                    accumulator.interrupted = true;
                    return Ok(true);
                }
                TurnStatus::InProgress => {}
            }
        }
        _ => {}
    }
    Ok(false)
}

fn notification_matches(
    notification: &AgentMessageDeltaNotification,
    thread_id: &str,
    turn_id: &str,
) -> bool {
    notification.thread_id == thread_id && notification.turn_id == turn_id
}

async fn auto_resolve_server_request(
    client: &RemoteAppServerClient,
    request: ServerRequest,
) -> Result<bool> {
    let method = server_request_method_name(&request);
    match request {
        ServerRequest::CommandExecutionRequestApproval { request_id, .. } => {
            resolve_server_request(
                client,
                request_id,
                &method,
                CommandExecutionRequestApprovalResponse {
                    decision: CommandExecutionApprovalDecision::AcceptForSession,
                },
            )
            .await?;
            Ok(true)
        }
        ServerRequest::FileChangeRequestApproval { request_id, .. } => {
            resolve_server_request(
                client,
                request_id,
                &method,
                FileChangeRequestApprovalResponse {
                    decision: FileChangeApprovalDecision::AcceptForSession,
                },
            )
            .await?;
            Ok(true)
        }
        ServerRequest::PermissionsRequestApproval { request_id, params } => {
            resolve_server_request(
                client,
                request_id,
                &method,
                PermissionsRequestApprovalResponse {
                    permissions: GrantedPermissionProfile {
                        network: params.permissions.network,
                        file_system: params.permissions.file_system,
                    },
                    scope: PermissionGrantScope::Session,
                    strict_auto_review: None,
                },
            )
            .await?;
            Ok(true)
        }
        ServerRequest::ApplyPatchApproval { request_id, .. }
        | ServerRequest::ExecCommandApproval { request_id, .. } => {
            client
                .resolve_server_request(request_id, json!({ "decision": "approved_for_session" }))
                .await
                .with_context(|| format!("failed to resolve `{method}` server request"))?;
            Ok(true)
        }
        ServerRequest::McpServerElicitationRequest { request_id, .. } => {
            resolve_server_request(
                client,
                request_id,
                &method,
                McpServerElicitationRequestResponse {
                    action: McpServerElicitationAction::Cancel,
                    content: None,
                    meta: None,
                },
            )
            .await?;
            Ok(false)
        }
        ServerRequest::ToolRequestUserInput { request_id, .. }
        | ServerRequest::DynamicToolCall { request_id, .. }
        | ServerRequest::ChatgptAuthTokensRefresh { request_id, .. }
        | ServerRequest::AttestationGenerate { request_id, .. }
        | ServerRequest::CurrentTimeRead { request_id, .. } => {
            client
                .reject_server_request(
                    request_id,
                    JSONRPCErrorError {
                        code: -32000,
                        message: format!("`{method}` is not supported by semaphore-codex-runner"),
                        data: None,
                    },
                )
                .await
                .with_context(|| format!("failed to reject `{method}` server request"))?;
            Ok(false)
        }
    }
}

async fn resolve_server_request<T: Serialize>(
    client: &RemoteAppServerClient,
    request_id: RequestId,
    method: &str,
    response: T,
) -> Result<()> {
    let value = serde_json::to_value(response)
        .with_context(|| format!("failed to encode `{method}` server request response"))?;
    client
        .resolve_server_request(request_id, value)
        .await
        .with_context(|| format!("failed to resolve `{method}` server request"))
}

fn server_request_method_name(request: &ServerRequest) -> String {
    serde_json::to_value(request)
        .ok()
        .and_then(|value: Value| {
            value
                .get("method")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn server_request_id_string(request: &ServerRequest) -> String {
    request_id_string(request.id())
}

fn request_id_string(request_id: &RequestId) -> String {
    match serde_json::to_value(request_id).unwrap_or(Value::Null) {
        Value::String(value) => value,
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use codex_app_server_protocol::{
        CommandExecutionOutputDeltaNotification, CurrentTimeReadParams, Turn, TurnItemsView,
        TurnStartedNotification,
    };

    use super::*;

    fn agent_delta(delta: &str) -> ServerNotification {
        ServerNotification::AgentMessageDelta(AgentMessageDeltaNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item_id: "item-1".to_string(),
            delta: delta.to_string(),
        })
    }

    #[test]
    fn bridge_event_body_matches_product_ingress_envelope() {
        let body = bridge_event_body(
            "99999999-9999-9999-9999-999999999999",
            "22222222-2222-2222-2222-222222222222",
            "runner-epoch:pid-1:123",
            3,
            "codex.notification",
            json!({"message": "Codex notification: agentMessageDelta"}),
        );

        assert_eq!(
            body,
            json!({
                "organizationId": "99999999-9999-9999-9999-999999999999",
                "runtimeId": "22222222-2222-2222-2222-222222222222",
                "schemaVersion": 1,
                "bridgeEpoch": "runner-epoch:pid-1:123",
                "sequence": 3,
                "idempotencyKey": "runner-epoch:pid-1:123:3",
                "type": "codex.notification",
                "payload": {
                    "message": "Codex notification: agentMessageDelta",
                },
            })
        );
    }

    #[test]
    fn turn_result_records_thread_resume_mode() {
        let result = TurnResult {
            schema_version: 1,
            source: "codex_app_server_remote_client",
            status: "completed",
            response: "done".to_string(),
            thread_id: "thread-1".to_string(),
            codex_session_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            assistant_item_id: None,
            model: "gpt-validation".to_string(),
            workspace_dir: Some("/workspace/project".to_string()),
            thread_reused: true,
            thread_mode: "resume_existing",
            server_version: Some("0.0.0".to_string()),
            codex_home: Some("/home/daytona/.semaphore-codex-home".to_string()),
            event_count: 4,
            assistant_delta_count: 1,
            item_completed_count: 1,
            server_request_count: 0,
            auto_approved_request_count: 0,
            timeout_seconds: 1200,
        };

        let value = serde_json::to_value(result).unwrap();

        assert_eq!(value["threadReused"], true);
        assert_eq!(value["threadMode"], "resume_existing");
        assert_eq!(
            clean_optional_string(Some("  thread-1  ")).as_deref(),
            Some("thread-1")
        );
        assert_eq!(clean_optional_string(Some("   ")), None);
    }

    #[test]
    fn notification_bridge_payload_keeps_ids_with_bounded_assistant_delta_summary() {
        let notification = agent_delta("assistant delta");

        let payload = notification_bridge_payload(&notification, Some("product-turn-1"), 7);

        assert_eq!(payload["source"], "semaphore-codex-runner");
        assert_eq!(payload["sourceKind"], "server_notification");
        assert_eq!(payload["notificationMethod"], "item/agentMessage/delta");
        assert_eq!(payload["productTurnId"], "product-turn-1");
        assert_eq!(payload["threadId"], "thread-1");
        assert_eq!(payload["turnId"], "turn-1");
        assert_eq!(payload["itemId"], "item-1");
        assert_eq!(payload["eventCount"], 7);
        assert_eq!(
            payload["payloadSummary"]["assistantTextDelta"],
            "assistant delta"
        );
        assert_eq!(
            payload["payloadSummary"]["assistantTextDeltaTruncated"],
            false
        );
        assert!(payload.get("delta").is_none());
    }

    #[test]
    fn assistant_delta_summary_is_bounded() {
        let notification = agent_delta(&"x".repeat(4_100));

        let payload = notification_bridge_payload(&notification, Some("product-turn-1"), 7);

        assert_eq!(
            payload["payloadSummary"]["assistantTextDelta"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            4_096
        );
        assert_eq!(
            payload["payloadSummary"]["assistantTextDeltaTruncated"],
            true
        );
    }

    #[test]
    fn item_lifecycle_summary_keeps_safe_tool_shape_without_raw_arguments() {
        let summary = notification_payload_summary(&json!({
            "method": "item/started",
            "params": {
                "item": {
                    "type": "mcpToolCall",
                    "id": "item-1",
                    "server": "github",
                    "tool": "create_pr",
                    "status": "inProgress",
                    "arguments": {"token": "secret", "title": "ship it"},
                    "result": {"body": "large raw result"}
                }
            }
        }));

        assert_eq!(summary["itemKind"], "mcpToolCall");
        assert_eq!(summary["itemStatus"], "inProgress");
        assert_eq!(summary["itemOutcome"], "running");
        assert_eq!(summary["item"]["id"], "item-1");
        assert_eq!(summary["item"]["server"], "github");
        assert_eq!(summary["item"]["tool"], "create_pr");
        assert_eq!(summary["item"]["outcome"], "running");
        assert_eq!(summary["item"]["argumentsPresent"], true);
        assert_eq!(summary["item"]["resultPresent"], true);
        assert!(summary["item"].get("arguments").is_none());
        assert!(summary["item"].get("result").is_none());
    }

    #[test]
    fn item_lifecycle_summary_marks_failed_commands_without_raw_output() {
        let summary = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "commandExecution",
                    "id": "item-2",
                    "command": "cargo test -p api",
                    "cwd": "/workspace/project",
                    "status": "completed",
                    "exitCode": 101,
                    "durationMs": 3200,
                    "aggregatedOutput": "long raw test failure",
                    "error": {"message": "tests failed"}
                }
            }
        }));

        assert_eq!(summary["itemKind"], "commandExecution");
        assert_eq!(summary["itemStatus"], "completed");
        assert_eq!(summary["itemOutcome"], "failed");
        assert_eq!(summary["item"]["commandPreview"], "cargo test -p api");
        assert_eq!(summary["item"]["cwd"], "/workspace/project");
        assert_eq!(summary["item"]["exitCode"], 101);
        assert_eq!(summary["item"]["durationMs"], 3200);
        assert_eq!(summary["item"]["outcome"], "failed");
        assert_eq!(summary["item"]["aggregatedOutputPresent"], true);
        assert_eq!(summary["item"]["errorPresent"], true);
        assert!(summary["item"].get("aggregatedOutput").is_none());
        assert!(summary["item"].get("error").is_none());
    }

    #[test]
    fn item_lifecycle_summary_keeps_image_reference_fields_without_raw_result() {
        let summary = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "imageGeneration",
                    "id": "image-1",
                    "status": "completed",
                    "revisedPrompt": format!("{}{}", "paint ", "x".repeat(2_000)),
                    "result": "a".repeat(256),
                    "savedPath": "/home/daytona/workspace/.codex/images/generated.png"
                }
            }
        }));

        assert_eq!(summary["itemKind"], "imageGeneration");
        assert_eq!(summary["itemStatus"], "completed");
        assert_eq!(summary["item"]["id"], "image-1");
        assert_eq!(
            summary["item"]["savedPath"],
            "/home/daytona/workspace/.codex/images/generated.png"
        );
        assert_eq!(
            summary["item"]["revisedPromptPreview"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            1_024
        );
        assert_eq!(summary["item"]["revisedPromptPreviewTruncated"], true);
        assert_eq!(summary["item"]["resultPresent"], true);
        assert_eq!(summary["item"]["resultKind"], "base64_payload");
        assert_eq!(summary["item"]["resultByteCount"], 256);
        assert!(summary["item"].get("resultPreview").is_none());
        assert!(summary["item"].get("result").is_none());
        assert!(summary["item"].get("revisedPrompt").is_none());
    }

    #[test]
    fn image_generation_result_summary_allows_safe_handles_only() {
        let summary = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "imageGeneration",
                    "id": "image-2",
                    "status": "completed",
                    "result": "/home/daytona/workspace/.codex/images/generated.png"
                }
            }
        }));

        assert_eq!(summary["item"]["resultKind"], "sandbox_path");
        assert_eq!(
            summary["item"]["resultPreview"],
            "/home/daytona/workspace/.codex/images/generated.png"
        );
        assert_eq!(summary["item"]["resultPreviewTruncated"], false);

        let summary = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "imageGeneration",
                    "id": "image-3",
                    "status": "completed",
                    "result": "data:image/png;base64,abcd"
                }
            }
        }));

        assert_eq!(summary["item"]["resultKind"], "data_url");
        assert!(summary["item"].get("resultPreview").is_none());
    }

    #[test]
    fn item_lifecycle_summary_keeps_web_search_evidence_without_raw_action() {
        let summary = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "webSearch",
                    "id": "search-1",
                    "query": format!("{}{}", "release notes ", "x".repeat(600)),
                    "action": {
                        "type": "search",
                        "query": "latest release notes",
                        "queries": [
                            "latest release notes",
                            format!("{}{}", "related ", "y".repeat(400))
                        ]
                    }
                }
            }
        }));

        assert_eq!(summary["itemKind"], "webSearch");
        assert_eq!(summary["item"]["id"], "search-1");
        assert_eq!(
            summary["item"]["queryPreview"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            512
        );
        assert_eq!(summary["item"]["queryPreviewTruncated"], true);
        assert_eq!(summary["item"]["actionPresent"], true);
        assert_eq!(summary["item"]["actionType"], "search");
        assert_eq!(
            summary["item"]["actionQueryPreview"],
            "latest release notes"
        );
        assert_eq!(summary["item"]["actionQueryCount"], 2);
        assert_eq!(
            summary["item"]["actionQueries"][1]["value"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            256
        );
        assert_eq!(summary["item"]["actionQueries"][1]["truncated"], true);
        assert!(summary["item"].get("action").is_none());
    }

    #[test]
    fn web_search_url_previews_drop_credentials_queries_and_fragments() {
        let summary = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "webSearch",
                    "id": "search-2",
                    "query": "open docs",
                    "action": {
                        "type": "openPage",
                        "url": "https://example.test/docs/page?token=secret#section"
                    }
                }
            }
        }));

        assert_eq!(
            summary["item"]["actionUrlPreview"],
            "https://example.test/docs/page"
        );

        let unsafe_summary = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "webSearch",
                    "id": "search-3",
                    "query": "open docs",
                    "action": {
                        "type": "openPage",
                        "url": "https://user:secret@example.test/docs"
                    }
                }
            }
        }));

        assert!(unsafe_summary["item"].get("actionUrlPreview").is_none());
        assert_eq!(unsafe_summary["item"]["actionPresent"], true);
    }

    #[test]
    fn item_lifecycle_summary_keeps_collab_agent_shape_without_raw_prompt() {
        let summary = notification_payload_summary(&json!({
            "method": "item/started",
            "params": {
                "item": {
                    "type": "collabAgentToolCall",
                    "id": "collab-1",
                    "tool": "spawn",
                    "status": "inProgress",
                    "senderThreadId": "thread-parent",
                    "receiverThreadIds": ["thread-child-1", "thread-child-2"],
                    "prompt": format!("{}{}", "inspect failing test ", "x".repeat(1_200)),
                    "model": "gpt-5-codex",
                    "reasoningEffort": "high",
                    "agentsStates": {
                        "thread-child-1": {"status": "running"},
                        "thread-child-2": {"status": "queued"}
                    }
                }
            }
        }));

        assert_eq!(summary["itemKind"], "collabAgentToolCall");
        assert_eq!(summary["itemStatus"], "inProgress");
        assert_eq!(summary["item"]["tool"], "spawn");
        assert_eq!(summary["item"]["senderThreadId"], "thread-parent");
        assert_eq!(summary["item"]["receiverThreadCount"], 2);
        assert_eq!(summary["item"]["agentStateCount"], 2);
        assert_eq!(summary["item"]["model"], "gpt-5-codex");
        assert_eq!(summary["item"]["reasoningEffort"], "high");
        assert_eq!(
            summary["item"]["promptPreview"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            1_024
        );
        assert_eq!(summary["item"]["promptPreviewTruncated"], true);
        assert!(summary["item"].get("prompt").is_none());
        assert!(summary["item"].get("receiverThreadIds").is_none());
        assert!(summary["item"].get("agentsStates").is_none());
    }

    #[test]
    fn item_lifecycle_summary_keeps_sub_agent_review_and_compaction_shape() {
        let sub_agent = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "subAgentActivity",
                    "id": "activity-1",
                    "kind": "handoff",
                    "agentThreadId": "thread-child",
                    "agentPath": "/agents/runtime-reviewer"
                }
            }
        }));

        assert_eq!(sub_agent["itemKind"], "subAgentActivity");
        assert_eq!(sub_agent["item"]["subAgentKind"], "handoff");
        assert_eq!(sub_agent["item"]["agentThreadId"], "thread-child");
        assert_eq!(sub_agent["item"]["agentPath"], "/agents/runtime-reviewer");

        let review = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "enteredReviewMode",
                    "id": "review-1",
                    "review": format!("{}{}", "review summary ", "r".repeat(1_300))
                }
            }
        }));

        assert_eq!(review["itemKind"], "enteredReviewMode");
        assert_eq!(
            review["item"]["reviewPreview"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            1_024
        );
        assert_eq!(review["item"]["reviewPreviewTruncated"], true);
        assert!(review["item"].get("review").is_none());

        let compaction = notification_payload_summary(&json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "contextCompaction",
                    "id": "compact-1"
                }
            }
        }));

        assert_eq!(compaction["itemKind"], "contextCompaction");
        assert_eq!(compaction["item"]["contextCompaction"], true);
    }

    #[test]
    fn plan_and_reasoning_summaries_are_bounded() {
        let plan_summary = notification_payload_summary(&json!({
            "method": "turn/plan/updated",
            "params": {
                "explanation": "x".repeat(2_100),
                "plan": [
                    {"step": "inspect bridge", "status": "completed"},
                    {"step": "wire events", "status": "inProgress"}
                ]
            }
        }));
        assert_eq!(
            plan_summary["explanation"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            2_048
        );
        assert_eq!(plan_summary["explanationTruncated"], true);
        assert_eq!(plan_summary["plan"]["steps"][0]["step"], "inspect bridge");
        assert_eq!(plan_summary["plan"]["steps"][1]["status"], "inProgress");

        let reasoning_summary = notification_payload_summary(&json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {
                "itemId": "reasoning-1",
                "summaryIndex": 2,
                "delta": "r".repeat(4_100)
            }
        }));
        assert_eq!(
            reasoning_summary["reasoningSummaryTextDelta"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            4_096
        );
        assert_eq!(
            reasoning_summary["reasoningSummaryTextDeltaTruncated"],
            true
        );
        assert_eq!(reasoning_summary["summaryIndex"], 2);
    }

    #[test]
    fn token_usage_and_turn_diff_summaries_are_bounded() {
        let token_summary = notification_payload_summary(&json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "tokenUsage": {
                    "total": {
                        "totalTokens": 1000,
                        "inputTokens": 600,
                        "cachedInputTokens": 200,
                        "outputTokens": 400,
                        "reasoningOutputTokens": 150
                    },
                    "last": {
                        "totalTokens": 120,
                        "inputTokens": 80,
                        "cachedInputTokens": 20,
                        "outputTokens": 40,
                        "reasoningOutputTokens": 10
                    },
                    "modelContextWindow": 200000
                }
            }
        }));

        assert_eq!(token_summary["totalTokens"], 1000);
        assert_eq!(token_summary["inputTokens"], 600);
        assert_eq!(token_summary["cachedInputTokens"], 200);
        assert_eq!(token_summary["outputTokens"], 400);
        assert_eq!(token_summary["reasoningOutputTokens"], 150);
        assert_eq!(token_summary["lastTotalTokens"], 120);
        assert_eq!(token_summary["lastInputTokens"], 80);
        assert_eq!(token_summary["lastCachedInputTokens"], 20);
        assert_eq!(token_summary["lastOutputTokens"], 40);
        assert_eq!(token_summary["lastReasoningOutputTokens"], 10);
        assert_eq!(token_summary["modelContextWindow"], 200000);

        let diff_summary = notification_payload_summary(&json!({
            "method": "turn/diff/updated",
            "params": {
                "diff": format!("{}\n{}", "+".repeat(4_200), "-changed")
            }
        }));

        assert_eq!(
            diff_summary["diffPreview"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            4_096
        );
        assert_eq!(diff_summary["diffPreviewTruncated"], true);
        assert_eq!(diff_summary["diffLineCount"], 2);
        assert_eq!(diff_summary["diffByteCount"], 4209);
    }

    #[test]
    fn compatibility_notification_summaries_are_bounded_and_redacted() {
        let status_summary = notification_payload_summary(&json!({
            "method": "thread/status/changed",
            "params": {
                "threadId": "thread-1",
                "status": {
                    "type": "active",
                    "activeFlags": ["waitingOnApproval", "waitingOnUserInput"]
                }
            }
        }));
        assert_eq!(status_summary["threadStatus"], "active");
        assert_eq!(status_summary["activeFlagCount"], 2);
        assert_eq!(
            status_summary["activeFlags"][0]["value"],
            "waitingOnApproval"
        );

        let review_summary = notification_payload_summary(&json!({
            "method": "item/autoApprovalReview/completed",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "reviewId": "review-1",
                "targetItemId": "item-1",
                "startedAtMs": 10,
                "completedAtMs": 30,
                "decisionSource": "agent",
                "review": {
                    "status": "approved",
                    "riskLevel": "medium",
                    "userAuthorization": "high",
                    "rationale": "safe enough"
                },
                "action": {
                    "type": "requestPermissions",
                    "reason": "Need broader access",
                    "permissions": {
                        "raw": "must not be copied"
                    }
                }
            }
        }));
        assert_eq!(review_summary["reviewId"], "review-1");
        assert_eq!(review_summary["reviewStatus"], "approved");
        assert_eq!(review_summary["riskLevel"], "medium");
        assert_eq!(review_summary["reviewUserLevel"], "high");
        assert_eq!(review_summary["durationMs"], 20);
        assert_eq!(review_summary["actionType"], "requestPermissions");
        assert_eq!(review_summary["permissionProfileRequested"], true);
        assert!(review_summary.get("permissions").is_none());

        let terminal_summary = notification_payload_summary(&json!({
            "method": "item/commandExecution/terminalInteraction",
            "params": {
                "processId": "process-1",
                "stdin": "secret-looking input\n"
            }
        }));
        assert_eq!(terminal_summary["processId"], "process-1");
        assert_eq!(terminal_summary["stdinLineCount"], 1);
        assert!(terminal_summary.get("stdin").is_none());

        let fs_summary = notification_payload_summary(&json!({
            "method": "fs/changed",
            "params": {
                "watchId": "watch-1",
                "changedPaths": (0..25)
                    .map(|index| format!("/workspace/file-{index}.rs"))
                    .collect::<Vec<_>>()
            }
        }));
        assert_eq!(fs_summary["changedPathCount"], 25);
        assert_eq!(fs_summary["changedPaths"].as_array().unwrap().len(), 20);
        assert_eq!(fs_summary["changedPathsTruncated"], true);

        let error_summary = notification_payload_summary(&json!({
            "method": "error",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "willRetry": true,
                "error": {
                    "message": "model auth expired",
                    "additionalDetails": "refresh required"
                }
            }
        }));
        assert_eq!(error_summary["message"], "model auth expired");
        assert_eq!(error_summary["willRetry"], true);

        let model_summary = notification_payload_summary(&json!({
            "method": "model/rerouted",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "fromModel": "gpt-5",
                "toModel": "gpt-5-mini",
                "reason": "highRiskCyberActivity"
            }
        }));
        assert_eq!(model_summary["fromModel"], "gpt-5");
        assert_eq!(model_summary["toModel"], "gpt-5-mini");
        assert_eq!(model_summary["reason"], "highRiskCyberActivity");
    }

    #[test]
    fn turn_started_notification_bridge_payload_extracts_nested_turn_id() {
        let notification = ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: "thread-1".to_string(),
            turn: Turn {
                id: "turn-1".to_string(),
                items: Vec::new(),
                items_view: TurnItemsView::NotLoaded,
                status: TurnStatus::InProgress,
                error: None,
                started_at: Some(1),
                completed_at: None,
                duration_ms: None,
            },
        });

        let payload = notification_bridge_payload(&notification, Some("product-turn-1"), 3);

        assert_eq!(payload["notificationMethod"], "turn/started");
        assert_eq!(payload["productTurnId"], "product-turn-1");
        assert_eq!(payload["threadId"], "thread-1");
        assert_eq!(payload["turnId"], "turn-1");
    }

    #[test]
    fn command_output_notification_maps_to_command_output_with_bounded_summary() {
        let notification = ServerNotification::CommandExecutionOutputDelta(
            CommandExecutionOutputDeltaNotification {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "command-1".to_string(),
                delta: "raw command output".to_string(),
            },
        );

        let payload = notification_bridge_payload(&notification, None, 9);

        assert_eq!(
            notification_bridge_event_type(&notification),
            "command.output"
        );
        assert_eq!(
            payload["notificationMethod"],
            "item/commandExecution/outputDelta"
        );
        assert_eq!(payload["itemId"], "command-1");
        assert_eq!(
            payload["payloadSummary"]["commandOutputDelta"],
            "raw command output"
        );
        assert_eq!(
            payload["payloadSummary"]["commandOutputDeltaTruncated"],
            false
        );
        assert_eq!(
            payload["payloadSummary"]["commandOutputDeltaByteCount"],
            "raw command output".len()
        );
        assert_eq!(payload["payloadSummary"]["commandOutputDeltaLineCount"], 1);
        assert!(payload.get("delta").is_none());

        let file_output_summary = notification_payload_summary(&json!({
            "method": "item/fileChange/outputDelta",
            "params": {
                "delta": format!("{}\n{}", "x".repeat(4_200), "done")
            }
        }));
        assert_eq!(
            file_output_summary["fileChangeOutputDelta"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            4_096
        );
        assert_eq!(file_output_summary["fileChangeOutputDeltaTruncated"], true);
        assert_eq!(file_output_summary["fileChangeOutputDeltaByteCount"], 4205);
        assert_eq!(file_output_summary["fileChangeOutputDeltaLineCount"], 2);
    }

    #[test]
    fn server_request_bridge_payload_records_product_policy_outcome() {
        let payload = server_request_bridge_payload(
            "execCommandApproval",
            "42",
            "codex-turn-1",
            true,
            Some("product-turn-1"),
            2,
            11,
        );

        assert_eq!(payload["source"], "semaphore-codex-runner");
        assert_eq!(payload["sourceKind"], "server_request");
        assert_eq!(payload["notificationMethod"], "serverRequest/resolved");
        assert_eq!(payload["serverRequestId"], "42");
        assert_eq!(payload["serverRequestMethod"], "execCommandApproval");
        assert_eq!(payload["codexTurnId"], "codex-turn-1");
        assert_eq!(payload["autoApprovedByProductPolicy"], true);
        assert_eq!(payload["serverRequestCount"], 2);
        assert_eq!(payload["eventCount"], 11);
    }

    #[test]
    fn server_request_method_name_reads_codex_wire_method() {
        let request = ServerRequest::CurrentTimeRead {
            request_id: RequestId::Integer(1),
            params: CurrentTimeReadParams {
                thread_id: "thread-1".to_string(),
            },
        };

        assert_eq!(server_request_method_name(&request), "currentTime/read");
    }

    #[test]
    fn server_request_id_string_reads_codex_request_id() {
        let integer_request = ServerRequest::CurrentTimeRead {
            request_id: RequestId::Integer(7),
            params: CurrentTimeReadParams {
                thread_id: "thread-1".to_string(),
            },
        };
        assert_eq!(server_request_id_string(&integer_request), "7");

        let string_request = ServerRequest::CurrentTimeRead {
            request_id: RequestId::String("request-1".to_string()),
            params: CurrentTimeReadParams {
                thread_id: "thread-1".to_string(),
            },
        };
        assert_eq!(server_request_id_string(&string_request), "request-1");
    }

    #[test]
    fn bridge_url_trims_base_url() {
        assert_eq!(
            bridge_events_url(
                "https://api.example.test/",
                "11111111-1111-1111-1111-111111111111"
            ),
            "https://api.example.test/api/sessions/11111111-1111-1111-1111-111111111111/bridge/events"
        );
    }
}
