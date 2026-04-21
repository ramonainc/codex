use std::fs;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::path::Path;
use std::thread;

use anyhow::Result;
use pretty_assertions::assert_eq;
use serde_json::Value;
use tempfile::TempDir;

const TEST_ID_TOKEN: &str = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJlbWFpbCI6InVzZXJAZXhhbXBsZS5jb20iLCJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF91c2VyX2lkIjoidXNlci0xMjM0NSIsInVzZXJfaWQiOiJ1c2VyLTEyMzQ1IiwiY2hhdGdwdF9wbGFuX3R5cGUiOiJwcm8iLCJjaGF0Z3B0X2FjY291bnRfaWQiOiJ3b3Jrc3BhY2UtMTIzIn19.c2ln";

fn codex_command(codex_home: &Path) -> Result<assert_cmd::Command> {
    let mut cmd = assert_cmd::Command::new(codex_utils_cargo_bin::cargo_bin("codex")?);
    cmd.env("CODEX_HOME", codex_home);
    Ok(cmd)
}

fn write_chatgpt_auth(codex_home: &Path) -> Result<()> {
    fs::write(
        codex_home.join("auth.json"),
        serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": TEST_ID_TOKEN,
                "access_token": "test-access-token",
                "refresh_token": "test-refresh-token",
                "account_id": "workspace-123"
            },
            "last_refresh": "2026-01-01T00:00:00Z"
        })
        .to_string(),
    )?;
    Ok(())
}

#[test]
fn login_status_json_reads_auth_from_codex_home() -> Result<()> {
    let codex_home = TempDir::new()?;
    fs::write(
        codex_home.path().join("config.toml"),
        "cli_auth_credentials_store = \"file\"\n",
    )?;
    write_chatgpt_auth(codex_home.path())?;

    let output = codex_command(codex_home.path())?
        .args(["login", "status", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let payload: Value = serde_json::from_slice(&output)?;

    assert_eq!(payload["authenticated"], Value::Bool(true));
    assert_eq!(payload["auth_mode"], Value::String("chatgpt".to_string()));
    assert_eq!(
        payload["account"]["account_id"],
        Value::String("workspace-123".to_string())
    );
    assert_eq!(
        payload["account"]["plan_type"],
        Value::String("pro".to_string())
    );

    Ok(())
}

#[test]
fn login_status_json_includes_rate_limits() -> Result<()> {
    let codex_home = TempDir::new()?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let server_addr = listener.local_addr()?;
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buffer = [0_u8; 4096];
        let _ = stream.read(&mut buffer).expect("read");
        let body = serde_json::json!({
            "plan_type": "pro",
            "rate_limit": {
                "allowed": true,
                "limit_reached": false,
                "primary_window": {
                    "used_percent": 42,
                    "limit_window_seconds": 300,
                    "reset_after_seconds": 0,
                    "reset_at": 123
                },
                "secondary_window": {
                    "used_percent": 84,
                    "limit_window_seconds": 3600,
                    "reset_after_seconds": 0,
                    "reset_at": 456
                }
            },
            "credits": {
                "has_credits": true,
                "unlimited": false,
                "balance": "9.99"
            },
            "additional_rate_limits": [
                {
                    "limit_name": "codex_other",
                    "metered_feature": "codex_other",
                    "rate_limit": {
                        "allowed": true,
                        "limit_reached": false,
                        "primary_window": {
                            "used_percent": 70,
                            "limit_window_seconds": 900,
                            "reset_after_seconds": 0,
                            "reset_at": 789
                        }
                    }
                }
            ]
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).expect("write");
    });

    fs::write(
        codex_home.path().join("config.toml"),
        format!(
            "cli_auth_credentials_store = \"file\"\nchatgpt_base_url = \"http://{server_addr}/backend-api\"\n"
        ),
    )?;
    write_chatgpt_auth(codex_home.path())?;

    let output = codex_command(codex_home.path())?
        .args(["login", "status", "--json", "--rate-limits"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    server_thread.join().expect("server thread");

    let payload: Value = serde_json::from_slice(&output)?;
    assert_eq!(
        payload["rate_limits"]["limit_id"],
        Value::String("codex".to_string())
    );
    assert_eq!(
        payload["rate_limits"]["plan_type"],
        Value::String("pro".to_string())
    );
    assert_eq!(
        payload["rate_limits_by_limit_id"]["codex_other"]["limit_name"],
        Value::String("codex_other".to_string())
    );
    assert_eq!(payload["rate_limits_error"], Value::Null);

    Ok(())
}
