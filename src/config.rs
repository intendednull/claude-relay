use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::detect::DetectConfig;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: String,
    /// Where route state survives a restart; absent means in-memory only,
    /// which is spec §4's default ("optional small JSON file").
    #[serde(default)]
    pub state_file: Option<PathBuf>,
    pub anthropic: AnthropicConfig,
    #[serde(default)]
    pub detect: DetectConfig,
    #[serde(default)]
    pub notify: NotifyConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnthropicConfig {
    pub base_url: String,
}

/// The command run on route state transitions (spec §4). No `command` is the
/// default and means no notifications at all, not an error.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotifyConfig {
    /// Run through `sh -c`, so spec §3's own integration examples
    /// (`notify-send …`, `osascript -e …`, `ntfy publish …`) work as written
    /// instead of each needing a wrapper script. Nothing outside this config
    /// file is ever interpolated into that string: the event reaches the
    /// command through the environment, never as shell text.
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_timeout_secs() -> u64 {
    5
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self {
            command: None,
            timeout_secs: default_timeout_secs(),
        }
    }
}

impl NotifyConfig {
    /// Both ways a configured hook can be silently unable to ever run: a blank
    /// command is a shell that does nothing, and a zero timeout kills the hook
    /// before it can do anything.
    pub fn validate(&self) -> Result<()> {
        let Some(command) = &self.command else {
            return Ok(());
        };
        if command.trim().is_empty() {
            bail!("`notify.command` is empty; remove the key to disable notifications");
        }
        if self.timeout_secs == 0 {
            bail!("`notify.timeout_secs` must be at least 1");
        }
        Ok(())
    }
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

    /// Both ways a state file silently ends up somewhere the operator didn't
    /// mean: spec §8's own example writes `~/.local/state/...` and nothing here
    /// expands `~`, so it would become a directory literally named `~`; and a
    /// relative path resolves against whatever directory the relay happened to
    /// be started from.
    pub fn state_file(&self) -> Result<Option<PathBuf>> {
        let Some(path) = &self.state_file else {
            return Ok(None);
        };
        if !path.is_absolute() {
            bail!(
                "`state_file` must be an absolute path (`~` is not expanded): {}",
                path.display()
            );
        }
        Ok(Some(path.clone()))
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

    /// Milestone 1 configs keep working, and get the documented detection
    /// defaults without naming them.
    #[test]
    fn state_file_and_detect_are_optional() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"
        "#;

        let config = Config::from_toml_str(raw).expect("should parse");
        assert!(config.state_file.is_none());
        assert_eq!(config.detect.status, DetectConfig::default().status);
        assert_eq!(config.state_file().expect("should validate"), None);
        assert!(config.notify.command.is_none());
        assert!(config.notify.validate().is_ok());
    }

    #[test]
    fn parses_a_notify_section_and_defaults_its_timeout() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [notify]
            command = "/path/to/notify-hook"
        "#;

        let config = Config::from_toml_str(raw).expect("should parse");
        assert_eq!(
            config.notify.command.as_deref(),
            Some("/path/to/notify-hook")
        );
        assert_eq!(config.notify.timeout_secs, 5);
        assert!(config.notify.validate().is_ok());
    }

    #[test]
    fn an_unknown_notify_field_is_a_parse_error() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [notify]
            cmd = "/path/to/notify-hook"
        "#;
        let err = Config::from_toml_str(raw).expect_err("should fail to parse");
        assert!(err.to_string().contains("cmd"));
    }

    /// Both configs describe a hook that can never run, and both would do so
    /// silently — the notifier's failures are all warnings by design.
    #[test]
    fn a_notify_hook_that_could_never_run_is_rejected() {
        let blank = NotifyConfig {
            command: Some("   ".to_string()),
            timeout_secs: 5,
        };
        assert!(blank.validate().unwrap_err().to_string().contains("empty"));

        let no_time = NotifyConfig {
            command: Some("notify-send hi".to_string()),
            timeout_secs: 0,
        };
        assert!(
            no_time
                .validate()
                .unwrap_err()
                .to_string()
                .contains("timeout_secs")
        );

        // A zero timeout with no command is still just "no notifications".
        assert!(
            NotifyConfig {
                command: None,
                timeout_secs: 0,
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn parses_state_file_and_a_detect_section() {
        let raw = r#"
            listen = "127.0.0.1:8484"
            state_file = "/var/lib/relay/state.json"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [detect]
            status = 429
            min_reset_horizon_secs = 900
            match_body = { "error.type" = "rate_limit_error" }

            [[detect.reset]]
            from = "header"
            name = "retry-after"
            format = "delta-seconds"
        "#;

        let config = Config::from_toml_str(raw).expect("should parse");
        assert_eq!(
            config.state_file().expect("should validate"),
            Some(PathBuf::from("/var/lib/relay/state.json"))
        );
        assert_eq!(config.detect.min_reset_horizon_secs, 900);
        assert_eq!(config.detect.reset.len(), 1);
    }

    #[test]
    fn an_unknown_detect_field_is_a_parse_error() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [detect]
            jsonpath = "$.error.type"
        "#;
        let err = Config::from_toml_str(raw).expect_err("should fail to parse");
        assert!(err.to_string().contains("jsonpath"));
    }

    /// A tilde path (spec §8's own example) would become a directory named `~`,
    /// and a relative one lands wherever the relay happened to be started —
    /// both are silent, and both put route state somewhere unintended.
    #[test]
    fn a_state_file_that_is_not_absolute_is_rejected() {
        for state_file in [
            "~/.local/state/relay/state.json",
            "state.json",
            "./state.json",
        ] {
            let raw = format!(
                r#"
                listen = "127.0.0.1:8484"
                state_file = "{state_file}"

                [anthropic]
                base_url = "https://api.anthropic.com"
                "#
            );
            let config = Config::from_toml_str(&raw).expect("should parse");
            let err = config.state_file().expect_err(state_file).to_string();
            assert!(err.contains("state_file"), "{state_file}: {err}");
        }
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

    fn config_with_listen(listen: &str) -> Config {
        Config {
            listen: listen.to_string(),
            state_file: None,
            anthropic: AnthropicConfig {
                base_url: "https://api.anthropic.com".to_string(),
            },
            detect: DetectConfig::default(),
            notify: NotifyConfig::default(),
        }
    }

    #[test]
    fn listen_addr_parses_valid_socket_addr() {
        assert_eq!(
            config_with_listen("127.0.0.1:8484")
                .listen_addr()
                .expect("should parse"),
            "127.0.0.1:8484".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn listen_addr_rejects_invalid_address() {
        assert!(config_with_listen("not-an-address").listen_addr().is_err());
    }

    fn config_with_base_url(base_url: &str) -> Config {
        Config {
            listen: "127.0.0.1:8484".to_string(),
            state_file: None,
            anthropic: AnthropicConfig {
                base_url: base_url.to_string(),
            },
            detect: DetectConfig::default(),
            notify: NotifyConfig::default(),
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
