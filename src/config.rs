use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: String,
    pub anthropic: AnthropicConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnthropicConfig {
    pub base_url: String,
}

/// A parsed config plus the SHA-256 digest of the exact bytes it was parsed
/// from, for `/status`'s `config_digest` field.
pub struct LoadedConfig {
    pub config: Config,
    pub digest: String,
}

impl Config {
    pub fn from_toml_str(raw: &str) -> std::result::Result<Self, toml::de::Error> {
        toml::from_str(raw)
    }

    pub fn load(path: &Path) -> Result<LoadedConfig> {
        let raw = fs::read(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        let digest = Sha256::digest(&raw)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let text = std::str::from_utf8(&raw)
            .with_context(|| format!("config file is not valid UTF-8: {}", path.display()))?;
        let config = Self::from_toml_str(text)
            .with_context(|| format!("failed to parse config file: {}", path.display()))?;
        Ok(LoadedConfig { config, digest })
    }

    /// Parses `listen` as a socket address, failing fast with a clear error
    /// rather than deferring the failure to the first bind attempt.
    pub fn listen_addr(&self) -> Result<SocketAddr> {
        self.listen
            .parse()
            .with_context(|| format!("invalid `listen` address: {:?}", self.listen))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_config() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"
        "#;

        let config = Config::from_toml_str(raw).expect("should parse");
        assert_eq!(config.listen, "127.0.0.1:8484");
        assert_eq!(config.anthropic.base_url, "https://api.anthropic.com");
    }

    #[test]
    fn missing_anthropic_section_is_a_parse_error() {
        let raw = r#"listen = "127.0.0.1:8484""#;
        let err = Config::from_toml_str(raw).expect_err("should fail to parse");
        assert!(err.to_string().contains("anthropic"));
    }

    #[test]
    fn unknown_top_level_field_is_a_parse_error() {
        let raw = r#"
            listen = "127.0.0.1:8484"
            mystery_field = "oops"

            [anthropic]
            base_url = "https://api.anthropic.com"
        "#;
        let err = Config::from_toml_str(raw).expect_err("should fail to parse");
        assert!(err.to_string().contains("mystery_field"));
    }

    #[test]
    fn unplanned_section_is_a_parse_error() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [some_unplanned_section]
            foo = "bar"
        "#;
        let err = Config::from_toml_str(raw).expect_err("should fail to parse");
        assert!(err.to_string().contains("some_unplanned_section"));
    }

    #[test]
    fn listen_addr_parses_valid_socket_addr() {
        let config = Config {
            listen: "127.0.0.1:8484".to_string(),
            anthropic: AnthropicConfig {
                base_url: "https://api.anthropic.com".to_string(),
            },
        };
        assert_eq!(
            config.listen_addr().expect("should parse"),
            "127.0.0.1:8484".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn listen_addr_rejects_invalid_address() {
        let config = Config {
            listen: "not-an-address".to_string(),
            anthropic: AnthropicConfig {
                base_url: "https://api.anthropic.com".to_string(),
            },
        };
        assert!(config.listen_addr().is_err());
    }
}
