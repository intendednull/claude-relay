use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Context, Result, bail};
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

    /// Same fail-fast reason as `listen_addr`, and a worse failure without it:
    /// reqwest defers a bad URL to `.send()`, where every request 502s with a
    /// bare "builder error" naming neither the field nor the problem.
    ///
    /// These errors name the key but never echo the value: a URL can carry
    /// credentials in its userinfo, the same leak `without_url()` guards against
    /// on the request path.
    pub fn anthropic_base_url(&self) -> Result<reqwest::Url> {
        let url = reqwest::Url::parse(&self.anthropic.base_url)
            .context("invalid `anthropic.base_url`")?;
        if url.host_str().is_none() {
            bail!("`anthropic.base_url` has no host");
        }
        // Any other scheme parses cleanly here and then fails per-request as the
        // opaque "builder error" this check exists to keep out of the hot path.
        if !matches!(url.scheme(), "http" | "https") {
            bail!(
                "`anthropic.base_url` must be http or https, got {:?}",
                url.scheme()
            );
        }
        Ok(url)
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

    fn config_with_base_url(base_url: &str) -> Config {
        Config {
            listen: "127.0.0.1:8484".to_string(),
            anthropic: AnthropicConfig {
                base_url: base_url.to_string(),
            },
        }
    }

    #[test]
    fn anthropic_base_url_parses_a_valid_url() {
        let url = config_with_base_url("https://api.anthropic.com")
            .anthropic_base_url()
            .expect("should parse");
        assert_eq!(url.host_str(), Some("api.anthropic.com"));
    }

    /// The motivating case: valid TOML, a clean startup, and then every single
    /// request 502ing on an unexplained reqwest builder error.
    #[test]
    fn anthropic_base_url_rejects_a_url_without_a_scheme() {
        let err = config_with_base_url("api.anthropic.com")
            .anthropic_base_url()
            .expect_err("a scheme-less URL must not reach the request path");
        assert!(err.to_string().contains("anthropic.base_url"));
    }

    #[test]
    fn anthropic_base_url_rejects_a_url_without_a_host() {
        let err = config_with_base_url("file:///etc/passwd")
            .anthropic_base_url()
            .expect_err("a hostless URL must not reach the request path");
        assert!(err.to_string().contains("no host"));
    }

    /// A non-HTTP scheme parses and has a host, so only an explicit check keeps
    /// it from reaching reqwest and failing there as an opaque builder error.
    #[test]
    fn anthropic_base_url_rejects_a_non_http_scheme() {
        let err = config_with_base_url("ftp://example.com")
            .anthropic_base_url()
            .expect_err("a non-http scheme must not reach the request path");
        assert!(err.to_string().contains("http or https"));
    }

    /// A URL can carry credentials in its userinfo, so these errors name the
    /// config key and never the value — the same rule the request path follows
    /// with `without_url()`.
    #[test]
    fn anthropic_base_url_errors_never_echo_the_value() {
        let secret = "sk-ant-oat01-DO-NOT-ECHO-THIS";
        for base_url in [
            format!("https://user:{secret}@"),
            format!("ftp://user:{secret}@example.com"),
        ] {
            let err = config_with_base_url(&base_url)
                .anthropic_base_url()
                .expect_err("should reject");
            let rendered = format!("{err:?}");
            assert!(
                !rendered.contains(secret),
                "error leaked a credential from base_url: {rendered}"
            );
            assert!(rendered.contains("anthropic.base_url"));
        }
    }
}
