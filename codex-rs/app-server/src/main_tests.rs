use super::AppServerArgs;
use super::MANAGED_CONFIG_PATH_ENV_VAR;
use super::managed_config_path_from_env;
use super::managed_config_path_from_env_value;
use clap::Parser;
use pretty_assertions::assert_eq;
use std::sync::Mutex;
use toml::Value as TomlValue;

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn app_server_accepts_cli_config_overrides() {
    let args = AppServerArgs::try_parse_from([
        "codex-app-server",
        "-c",
        "model=\"gpt-5-codex\"",
        "--config",
        "sandbox_mode=\"read-only\"",
        "--listen",
        "off",
    ])
    .expect("parse app-server args");

    let parsed_overrides = args
        .config_overrides
        .parse_overrides()
        .expect("parse config overrides");

    assert_eq!(
        parsed_overrides,
        vec![
            (
                "model".to_string(),
                TomlValue::String("gpt-5-codex".to_string()),
            ),
            (
                "sandbox_mode".to_string(),
                TomlValue::String("read-only".to_string()),
            ),
        ]
    );
}

#[test]
fn managed_config_path_env_works_in_release_startup() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    let path = std::env::temp_dir().join("codex-app-server-managed-config.toml");

    // SAFETY: this test serializes access to the process environment with
    // ENV_LOCK and restores the variable before returning.
    unsafe {
        std::env::set_var(MANAGED_CONFIG_PATH_ENV_VAR, &path);
    }

    assert_eq!(managed_config_path_from_env(), Some(path.clone()));

    // SAFETY: see the set_var safety note above.
    unsafe {
        std::env::remove_var(MANAGED_CONFIG_PATH_ENV_VAR);
    }
}

#[test]
fn managed_config_path_env_ignores_empty_values() {
    assert_eq!(managed_config_path_from_env_value(None), None);
    assert_eq!(
        managed_config_path_from_env_value(Some(String::new())),
        None
    );
}
