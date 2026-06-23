use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

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
    SandboxPolicy, ServerNotification, ServerRequest, ThreadItem, ThreadSource, ThreadStartParams,
    ThreadStartResponse, TurnStartParams, TurnStartResponse, TurnStatus, UserInput,
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::time::timeout;

const RESULT_MARKER: &str = "SEMAPHORE_CODEX_APP_SERVER_TURN_RESULT_V1";
const DEFAULT_WEBSOCKET_URL: &str = "ws://127.0.0.1:43113";
const DEFAULT_MODEL: &str = "gpt-5-codex";
const DEFAULT_TIMEOUT_SECONDS: u64 = 1200;

#[derive(Debug, Parser)]
#[command(version, about = "Semaphore runner for Codex app-server turns")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Turn(TurnCommand),
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
    #[arg(long, env = "SEMAPHORE_PRODUCT_TURN_ID")]
    product_turn_id: Option<String>,
    #[arg(long, env = "SEMAPHORE_CODEX_TIMEOUT_SECONDS", default_value_t = DEFAULT_TIMEOUT_SECONDS)]
    timeout_seconds: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnResult {
    schema_version: u8,
    source: &'static str,
    response: String,
    thread_id: String,
    codex_session_id: String,
    turn_id: String,
    model: String,
    workspace_dir: Option<String>,
    server_version: Option<String>,
    codex_home: Option<String>,
    event_count: u64,
    assistant_delta_count: u64,
    item_completed_count: u64,
    server_request_count: u64,
    auto_approved_request_count: u64,
    timeout_seconds: u64,
}

#[derive(Debug, Default)]
struct TurnAccumulator {
    response_delta: String,
    completed_agent_message: Option<String>,
    event_count: u64,
    assistant_delta_count: u64,
    item_completed_count: u64,
    server_request_count: u64,
    auto_approved_request_count: u64,
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
    }
}

async fn run_turn(command: TurnCommand) -> Result<()> {
    let message = read_message(&command)?;
    let timeout_duration = Duration::from_secs(command.timeout_seconds.max(1));
    let cwd_string = command
        .cwd
        .as_ref()
        .map(|path| path.to_string_lossy().to_string());

    let mut client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url: command.websocket_url.clone(),
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
    .with_context(|| {
        format!(
            "failed to connect to Codex app-server at `{}`",
            command.websocket_url
        )
    })?;

    let server_version = client.server_version().map(ToOwned::to_owned);
    let codex_home = client.codex_home().map(ToOwned::to_owned);
    let mut request_ids = RequestIds::new();

    let thread: ThreadStartResponse = client
        .request_typed(ClientRequest::ThreadStart {
            request_id: request_ids.next(),
            params: ThreadStartParams {
                model: Some(command.model.clone()),
                model_provider: Some("openai".to_string()),
                cwd: cwd_string.clone(),
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
                thread_id: thread.thread.id.clone(),
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
            &thread.thread.id,
            &turn.turn.id,
            &mut accumulator,
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

    let response = accumulator
        .completed_agent_message
        .or_else(|| {
            let response = accumulator.response_delta.trim().to_string();
            (!response.is_empty()).then_some(response)
        })
        .ok_or_else(|| anyhow!("Codex turn completed without an assistant response"))?;

    let result = TurnResult {
        schema_version: 1,
        source: "codex_app_server_remote_client",
        response,
        thread_id: thread.thread.id.clone(),
        codex_session_id: thread.thread.session_id.clone(),
        turn_id: turn.turn.id.clone(),
        model: command.model,
        workspace_dir: cwd_string,
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

async fn handle_event(
    client: &RemoteAppServerClient,
    event: AppServerEvent,
    thread_id: &str,
    turn_id: &str,
    accumulator: &mut TurnAccumulator,
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
            if auto_resolve_server_request(client, request).await? {
                accumulator.auto_approved_request_count += 1;
            }
        }
        AppServerEvent::ServerNotification(notification) => {
            if handle_notification(notification, thread_id, turn_id, accumulator)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
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
            accumulator.response_delta.push_str(&notification.delta);
        }
        ServerNotification::ItemCompleted(notification)
            if notification.thread_id == thread_id && notification.turn_id == turn_id =>
        {
            accumulator.item_completed_count += 1;
            if let ThreadItem::AgentMessage { text, .. } = notification.item {
                accumulator.completed_agent_message = Some(text);
            }
        }
        ServerNotification::TurnCompleted(notification)
            if notification.thread_id == thread_id && notification.turn.id == turn_id =>
        {
            for item in notification.turn.items {
                if let ThreadItem::AgentMessage { text, .. } = item {
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
                TurnStatus::Interrupted => bail!("Codex turn was interrupted"),
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
