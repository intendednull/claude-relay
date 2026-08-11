use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;

const CONFIG_ENV_VAR: &str = "RELAY_CONFIG";

#[derive(Debug, Parser)]
#[command(version, about = "Local HTTP proxy for Claude Code")]
pub struct Cli {
    /// Path to the TOML config file. Falls back to the RELAY_CONFIG env var.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Directory to write fixtures of non-2xx Anthropic responses to, for
    /// debugging limit detection. Off unless passed; sensitive header values
    /// are redacted; successful responses are never captured.
    #[arg(long)]
    pub capture_errors: Option<PathBuf>,
}

impl Cli {
    /// Resolves the config file path from `--config`, falling back to the
    /// `RELAY_CONFIG` environment variable.
    pub fn resolve_config_path(&self) -> Result<PathBuf> {
        self.resolve_config_path_with_env(std::env::var(CONFIG_ENV_VAR).ok())
    }

    fn resolve_config_path_with_env(&self, env_value: Option<String>) -> Result<PathBuf> {
        if let Some(path) = &self.config {
            return Ok(path.clone());
        }
        if let Some(path) = env_value.filter(|s| !s.is_empty()) {
            return Ok(PathBuf::from(path));
        }
        bail!(
            "no config file specified: pass --config <PATH> or set the {CONFIG_ENV_VAR} environment variable"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli_without_flag() -> Cli {
        Cli {
            config: None,
            capture_errors: None,
        }
    }

    #[test]
    fn resolves_from_flag_even_when_env_is_set() {
        let cli = Cli {
            config: Some(PathBuf::from("/tmp/from-flag.toml")),
            capture_errors: None,
        };
        let path = cli
            .resolve_config_path_with_env(Some("/tmp/from-env.toml".to_string()))
            .expect("should resolve");
        assert_eq!(path, PathBuf::from("/tmp/from-flag.toml"));
    }

    #[test]
    fn resolves_from_env_when_flag_absent() {
        let cli = cli_without_flag();
        let path = cli
            .resolve_config_path_with_env(Some("/tmp/from-env.toml".to_string()))
            .expect("should resolve");
        assert_eq!(path, PathBuf::from("/tmp/from-env.toml"));
    }

    #[test]
    fn errors_when_neither_flag_nor_env_set() {
        let cli = cli_without_flag();
        let err = cli
            .resolve_config_path_with_env(None)
            .expect_err("should error");
        let message = err.to_string();
        assert!(message.contains("--config"));
        assert!(message.contains("RELAY_CONFIG"));
    }

    #[test]
    fn errors_when_env_is_set_but_empty() {
        let cli = cli_without_flag();
        let err = cli
            .resolve_config_path_with_env(Some(String::new()))
            .expect_err("should error");
        assert!(err.to_string().contains("RELAY_CONFIG"));
    }
}
