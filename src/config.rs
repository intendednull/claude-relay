use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use indexmap::IndexMap;
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
    /// Fallback targets (spec §8b), keyed by name. An `IndexMap`, not a
    /// `HashMap`: §7d resolves the first `serves` match *in config order*,
    /// which only a map that preserves declaration order can give.
    #[serde(default)]
    pub profiles: IndexMap<String, ProfileConfig>,
    #[serde(default)]
    pub policy: PolicyConfig,
}

/// A named fallback target (spec §8, §8b). Reachable by name-based routing
/// via `serves`, by `policy.active_profile`, or both.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileConfig {
    pub base_url: String,
    /// Name of the env var carrying the profile's API key — read at request
    /// time, never stored in the config file and never logged (spec §7b).
    pub api_key_env: String,
    /// `"anthropic"` (passthrough) or `"openai"` (translated); validated in
    /// `validate`, not by the type, so a bad value names the config key
    /// instead of failing with a generic enum-variant parse error.
    pub format: String,
    /// Model-name prefixes this profile claims (spec §7d); empty means it is
    /// reachable only via `policy.active_profile`.
    #[serde(default)]
    pub serves: Vec<String>,
    /// Prefix-matched against the incoming `model` field when remapping a
    /// failed-over `claude-*` request (spec §7a); `"*"` is the catch-all. A
    /// name-routed request's model name passes through unchanged instead.
    /// An `IndexMap`, not a `HashMap`, for the same reason as
    /// `Config::profiles`: overlapping prefix keys need a deterministic
    /// winner, and only file order can give Task 3's longest/first-match
    /// logic something stable to work from.
    #[serde(default)]
    pub model_map: IndexMap<String, String>,
}

impl ProfileConfig {
    /// A parsed, host-bearing, http(s) `base_url` — same shape as
    /// `Config::anthropic_base_url`, and shared with it, since a profile's
    /// endpoint is exactly as capable of silently 502ing every request as
    /// Anthropic's is if this isn't checked at startup.
    pub fn base_url(&self) -> Result<reqwest::Url> {
        validated_base_url("profiles.*.base_url", &self.base_url)
    }

    /// An unrecognized `format`, a `base_url` that can't route a request
    /// (see `base_url`), carries userinfo (see `reject_userinfo`) or would
    /// carry the profile's key in cleartext (see
    /// `require_encrypted_transport`), a `serves` entry that is an empty
    /// string (which would `starts_with`-match every non-`claude-*` model,
    /// silently shadowing every profile declared after it), and an empty
    /// `model_map` key (which is the same trap one level down: it prefix-
    /// matches every name and, being a match rather than a fallthrough, beats
    /// the explicit `"*"` catch-all) are all startup-time errors rather than
    /// silent misrouting.
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.format.as_str(), "anthropic" | "openai") {
            bail!(
                "`profiles.*.format` must be \"anthropic\" or \"openai\", got {:?}",
                self.format
            );
        }
        let base_url = self.base_url()?;
        reject_userinfo(&base_url)?;
        require_encrypted_transport(&base_url)?;
        if self.serves.iter().any(String::is_empty) {
            bail!(
                "`profiles.*.serves` entries must not be empty — an empty prefix matches every model name"
            );
        }
        if self.model_map.keys().any(String::is_empty) {
            bail!(
                "`profiles.*.model_map` keys must not be empty — an empty prefix matches every model name and shadows the \"*\" catch-all"
            );
        }
        Ok(())
    }
}

/// A credential written here does not authenticate anything and never reaches
/// the wire: reqwest strips userinfo out of the URI at request-build time and
/// only synthesizes a `Basic` header when `Authorization` is unset, which
/// `fallback::outgoing_headers` always sets. It was probed on the wire, and it
/// does not surface through a `reqwest::Error` either. So this closes no
/// disclosure — it removes a field that silently swallows a secret and leaves
/// it in the config file, where the only thing standing between it and a log
/// line is every future log line remembering not to print `base_url`.
///
/// Refusing it cannot break a working deployment, precisely because the feature
/// provably never worked.
fn reject_userinfo(url: &reqwest::Url) -> Result<()> {
    if !url.username().is_empty() || url.password().is_some() {
        bail!(
            "`profiles.*.base_url` must not carry userinfo — a credential there is silently \
             discarded; put the key in the env var named by `api_key_env`"
        );
    }
    Ok(())
}

/// A profile's `base_url` is where its own API key travels, so plaintext
/// `http` to anywhere but this machine is refused at startup rather than
/// leaked once per request. Loopback stays allowed: that is a local mock or a
/// sidecar, and the key never leaves the host.
///
/// Deliberately not applied to `anthropic.base_url`: that one carries the
/// client's own credentials to a fixed, always-HTTPS endpoint, and Milestone 1
/// settled its rules. Host resolution is textual — no DNS lookup happens at
/// startup, so a name that merely *resolves* to loopback is still refused.
fn require_encrypted_transport(url: &reqwest::Url) -> Result<()> {
    if url.scheme() == "https" {
        return Ok(());
    }
    // `validated_base_url` already refused a URL with no host.
    let host = url.host_str().unwrap_or_default();
    let loopback = match host.trim_start_matches('[').trim_end_matches(']').parse() {
        Ok(std::net::IpAddr::V4(ip)) => ip.is_loopback(),
        Ok(std::net::IpAddr::V6(ip)) => ip.is_loopback(),
        Err(_) => host == "localhost" || host.ends_with(".localhost"),
    };
    if !loopback {
        bail!(
            "`profiles.*.base_url` must be https for a non-loopback host — a profile's API key must not travel in cleartext"
        );
    }
    Ok(())
}

/// Failover policy and the moved-from-`[detect]` horizon/jitter settings
/// (spec §8; see `docs/decisions.md` for why they now live here rather than
/// in `[detect]`, where Milestone 2 put them).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    /// Which `claude-*` requests fail over to `active_profile` while
    /// Anthropic is `Limited` — `new-sessions` (default) | `all` |
    /// `notify-only`. Only the field is validated here; what each mode does
    /// is Milestone 3's later routing-wiring task, not this one.
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Startup default; a runtime switch via `/control/profile` overrides it
    /// for new requests only and is never written back here (spec §8b).
    #[serde(default)]
    pub active_profile: Option<String>,
    /// Without a marker, a reset horizon must exceed this to count as the
    /// subscription limit rather than a burst 429 (spec §5). Also the floor
    /// every classified window gets, so no match ever produces one that has
    /// already expired.
    #[serde(default = "default_min_reset_horizon_secs")]
    pub min_reset_horizon_secs: u64,
    /// The ceiling on a classified window — a units/format sanity check, not
    /// a claim about how long Anthropic's windows are: a reset read in the
    /// wrong unit (epoch *milliseconds* through a rule expecting seconds)
    /// lands ~55,000 years out, and without a ceiling that window is
    /// persisted, survives every restart, and never elapses.
    #[serde(default = "default_max_reset_horizon_secs")]
    pub max_reset_horizon_secs: u64,
    /// `[min, max]` inclusive: random slack added past a reported reset time
    /// before transitioning to `Limited`, so the first probe after the
    /// window doesn't race the upstream reset boundary (spec §4).
    #[serde(default = "default_reset_jitter_secs")]
    pub reset_jitter_secs: [u64; 2],
}

fn default_mode() -> String {
    "new-sessions".to_string()
}

fn default_min_reset_horizon_secs() -> u64 {
    300
}

/// A week. Claude subscription limits include weekly windows, so a tighter
/// ceiling would reject a legitimate reset — and it costs nothing against what
/// the ceiling is actually for, since every wrong-unit or garbage value is
/// orders of magnitude past it.
fn default_max_reset_horizon_secs() -> u64 {
    7 * 24 * 60 * 60
}

fn default_reset_jitter_secs() -> [u64; 2] {
    [15, 60]
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            active_profile: None,
            min_reset_horizon_secs: default_min_reset_horizon_secs(),
            max_reset_horizon_secs: default_max_reset_horizon_secs(),
            reset_jitter_secs: default_reset_jitter_secs(),
        }
    }
}

/// The ceiling on `max_reset_horizon_secs` itself. Written in the wrong unit
/// (milliseconds for seconds) it stops bounding anything: large enough and
/// `detect::bounded`'s `checked_add` returns `None`, silently killing every
/// marked classification; merely huge and it yields a `Limited` window
/// `/status` cannot render. 10 years is far past any real subscription window
/// and far short of either failure.
pub(crate) const MAX_RESET_HORIZON_CEILING_SECS: u64 = 10 * 365 * 24 * 60 * 60;

/// The ceiling on `reset_jitter_secs[1]`. Jitter is added *on top of* the
/// already-bounded window `detect::bounded` produces (`route_state::
/// add_jitter`), so it is not covered by `MAX_RESET_HORIZON_CEILING_SECS` at
/// all — a units mistake here (milliseconds for seconds, or an operator
/// meaning "a day" and writing `86400`) pushes `Limited.until` unboundedly far
/// past a validated, sane window. Jitter exists only to avoid racing the
/// upstream reset boundary, so it needs seconds to minutes, never hours: one
/// hour is already generous for that purpose and small next to
/// `min_reset_horizon_secs`'s 5-minute default.
const MAX_JITTER_SECS: u64 = 60 * 60;

impl PolicyConfig {
    /// An unrecognized `mode`, an `active_profile` naming a profile that
    /// isn't configured, crossed or absurd horizon bounds, and jitter bounds
    /// that are inverted or exceed `MAX_JITTER_SECS` are all startup-time
    /// errors rather than silent misbehavior. Inverted jitter bounds would
    /// panic `rand::random_range` inside `RouteStateMachine::on_limit_detected`
    /// — caught by the applier thread's `catch_unwind`
    /// (`route_updates.rs`), so the process survives, but that outcome is
    /// lost: the route never transitions to `Limited` for *any* detected
    /// limit, silently, for the life of the process, and `/status` keeps
    /// reporting `ACTIVE`.
    pub fn validate(&self, profiles: &IndexMap<String, ProfileConfig>) -> Result<()> {
        if !matches!(self.mode.as_str(), "new-sessions" | "all" | "notify-only") {
            bail!(
                "`policy.mode` must be one of new-sessions, all, notify-only, got {:?}",
                self.mode
            );
        }
        if let Some(name) = &self.active_profile
            && !profiles.contains_key(name)
        {
            bail!("`policy.active_profile` names an unconfigured profile: {name:?}");
        }
        if self.max_reset_horizon_secs > MAX_RESET_HORIZON_CEILING_SECS {
            bail!(
                "`policy.max_reset_horizon_secs` must be at most {MAX_RESET_HORIZON_CEILING_SECS} (10 years), got {}",
                self.max_reset_horizon_secs
            );
        }
        if self.min_reset_horizon_secs > self.max_reset_horizon_secs {
            bail!(
                "`policy.min_reset_horizon_secs` ({}) must not exceed `policy.max_reset_horizon_secs` ({})",
                self.min_reset_horizon_secs,
                self.max_reset_horizon_secs
            );
        }
        let [jitter_min, jitter_max] = self.reset_jitter_secs;
        if jitter_min > jitter_max {
            bail!(
                "`policy.reset_jitter_secs` min ({jitter_min}) must not exceed max ({jitter_max})"
            );
        }
        if jitter_max > MAX_JITTER_SECS {
            bail!(
                "`policy.reset_jitter_secs` max must be at most {MAX_JITTER_SECS} (1 hour), got {jitter_max}"
            );
        }
        Ok(())
    }
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

/// A notification is fire-and-forget; anything still running a minute later has
/// stopped being one. The ceiling also keeps the deadline arithmetic in
/// `notify` inside `Instant`'s representable range, and bounds how long one
/// wedged hook can hold up the next notification.
const MAX_TIMEOUT_SECS: u64 = 60;

impl Default for NotifyConfig {
    fn default() -> Self {
        Self {
            command: None,
            timeout_secs: default_timeout_secs(),
        }
    }
}

impl NotifyConfig {
    /// The ways a configured hook can be silently unable to work: a blank
    /// command is a shell that does nothing, a zero timeout kills the hook
    /// before it can do anything, and a timeout large enough to overflow the
    /// deadline arithmetic panics the notifier *after* it has spawned the hook
    /// — losing the notification and leaving the child unreaped.
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
        if self.timeout_secs > MAX_TIMEOUT_SECS {
            bail!(
                "`notify.timeout_secs` must be at most {MAX_TIMEOUT_SECS}, got {}",
                self.timeout_secs
            );
        }
        Ok(())
    }
}

/// Shared shape for `anthropic.base_url` and `profiles.*.base_url`: parses
/// the URL, requires a host, requires `http`/`https` — and never echoes the
/// raw value in errors, since a URL can carry credentials in its userinfo
/// (the same leak `without_url()` guards against on the request path).
/// `field` names the config key in the error, never the value.
fn validated_base_url(field: &str, raw: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw).with_context(|| format!("invalid `{field}`"))?;
    if url.host_str().is_none() {
        bail!("`{field}` has no host");
    }
    // Any other scheme parses cleanly here and then fails per-request as the
    // opaque "builder error" this check exists to keep out of the hot path.
    if !matches!(url.scheme(), "http" | "https") {
        bail!("`{field}` must be http or https, got {:?}", url.scheme());
    }
    Ok(url)
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
        validated_base_url("anthropic.base_url", &self.anthropic.base_url)
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

    /// These values are ordinary TOML integers, and every one of them makes
    /// the notifier's `Instant::now() + timeout` overflow and panic — which
    /// happens *after* the hook has been spawned, so the notification is lost
    /// and the child is never reaped. Rejecting them at load keeps that
    /// arithmetic unreachable.
    #[test]
    fn a_notify_timeout_that_would_overflow_the_deadline_is_rejected() {
        for timeout_secs in [MAX_TIMEOUT_SECS + 1, i64::MAX as u64, u64::MAX] {
            let config = NotifyConfig {
                command: Some("notify-send hi".to_string()),
                timeout_secs,
            };
            let err = config
                .validate()
                .expect_err("{timeout_secs} must not reach the deadline calculation")
                .to_string();
            assert!(err.contains("timeout_secs"), "{timeout_secs}: {err}");
        }
        assert!(
            NotifyConfig {
                command: Some("notify-send hi".to_string()),
                timeout_secs: MAX_TIMEOUT_SECS,
            }
            .validate()
            .is_ok(),
            "the ceiling itself is a valid timeout"
        );
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
            match_body = { "error.type" = "rate_limit_error" }

            [[detect.reset]]
            from = "header"
            name = "retry-after"
            format = "delta-seconds"

            [policy]
            min_reset_horizon_secs = 900
        "#;

        let config = Config::from_toml_str(raw).expect("should parse");
        assert_eq!(
            config.state_file().expect("should validate"),
            Some(PathBuf::from("/var/lib/relay/state.json"))
        );
        assert_eq!(config.policy.min_reset_horizon_secs, 900);
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
            profiles: IndexMap::new(),
            policy: PolicyConfig::default(),
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
            profiles: IndexMap::new(),
            policy: PolicyConfig::default(),
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

    // --- [profiles.*] ---

    fn profile(format: &str) -> ProfileConfig {
        ProfileConfig {
            base_url: "https://api.together.ai".to_string(),
            api_key_env: "RELAY_TOGETHER_KEY".to_string(),
            format: format.to_string(),
            serves: vec!["deepseek-ai/".to_string()],
            model_map: IndexMap::new(),
        }
    }

    #[test]
    fn profile_format_accepts_anthropic_and_openai() {
        assert!(profile("anthropic").validate().is_ok());
        assert!(profile("openai").validate().is_ok());
    }

    #[test]
    fn profile_format_rejects_anything_else() {
        let err = profile("claude").validate().unwrap_err().to_string();
        assert!(err.contains("format"), "{err}");
    }

    fn profile_with_base_url(base_url: &str) -> ProfileConfig {
        ProfileConfig {
            base_url: base_url.to_string(),
            ..profile("openai")
        }
    }

    #[test]
    fn profile_base_url_parses_a_valid_url() {
        let url = profile_with_base_url("https://api.together.ai")
            .base_url()
            .expect("should parse");
        assert_eq!(url.host_str(), Some("api.together.ai"));
    }

    #[test]
    fn profile_base_url_rejects_a_url_without_a_scheme() {
        let err = profile_with_base_url("api.together.ai")
            .base_url()
            .expect_err("a scheme-less URL must not reach the request path");
        assert!(err.to_string().contains("profiles.*.base_url"));
    }

    #[test]
    fn profile_base_url_rejects_a_url_without_a_host() {
        let err = profile_with_base_url("file:///etc/passwd")
            .base_url()
            .expect_err("a hostless URL must not reach the request path");
        assert!(err.to_string().contains("no host"));
    }

    #[test]
    fn profile_base_url_rejects_a_non_http_scheme() {
        let err = profile_with_base_url("ftp://example.com")
            .base_url()
            .expect_err("a non-http scheme must not reach the request path");
        assert!(err.to_string().contains("http or https"));
    }

    /// A profile's `base_url` is where its own API key travels, so plaintext
    /// off-host is refused. Loopback stays allowed — that is a local mock or a
    /// sidecar, and every test fixture in this repo depends on it.
    #[test]
    fn a_profiles_base_url_must_be_https_unless_it_is_loopback() {
        for allowed in [
            "https://api.together.ai",
            "http://127.0.0.1:8080",
            "http://[::1]:8080",
            "http://localhost:8080",
        ] {
            profile_with_base_url(allowed)
                .validate()
                .unwrap_or_else(|err| panic!("{allowed} should be accepted: {err}"));
        }
        for refused in [
            "http://api.together.ai",
            // Not loopback by name, whatever it might resolve to: no DNS
            // lookup happens at startup, so this is the only honest answer.
            "http://127.0.0.1.example.com",
            "http://10.0.0.5:8080",
        ] {
            let err = profile_with_base_url(refused)
                .validate()
                .expect_err("a profile key must not travel in cleartext off this host");
            assert!(err.to_string().contains("https"), "{err}");
        }
    }

    /// The same trap `serves` already guards against, one level down: an empty
    /// prefix matches every name, and because it is a *match* it returns
    /// before the explicit `"*"` catch-all is ever consulted.
    #[test]
    fn an_empty_model_map_key_is_rejected() {
        let mut profile = profile("openai");
        profile.model_map = [("", "shadow/Model"), ("*", "intended/Model")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let err = profile
            .validate()
            .expect_err("an empty model_map key silently shadows the catch-all");
        assert!(err.to_string().contains("model_map"), "{err}");
    }

    #[test]
    fn profile_base_url_errors_never_echo_the_value() {
        let secret = "sk-together-DO-NOT-ECHO-THIS";
        for base_url in [
            format!("https://user:{secret}@"),
            format!("ftp://user:{secret}@example.com"),
        ] {
            let err = profile_with_base_url(&base_url)
                .base_url()
                .expect_err("should reject");
            let rendered = format!("{err:?}");
            assert!(
                !rendered.contains(secret),
                "error leaked a credential from base_url: {rendered}"
            );
            assert!(rendered.contains("profiles.*.base_url"));
        }
    }

    /// Not a disclosure fix: the userinfo never reaches the wire (reqwest
    /// strips it, and `outgoing_headers` always sets `Authorization`, so no
    /// `Basic` header is ever synthesized). What it removes is a config field
    /// that accepts a secret, silently discards it, and leaves it in the file
    /// for a future log line to find.
    #[test]
    fn a_profiles_base_url_must_not_carry_userinfo() {
        let secret = "sk-together-DO-NOT-ECHO-THIS";
        for base_url in [
            format!("https://user:{secret}@api.together.ai"),
            format!("https://{secret}@api.together.ai"),
            format!("http://user:{secret}@127.0.0.1:8080"),
        ] {
            let err = profile_with_base_url(&base_url)
                .validate()
                .expect_err("userinfo in a base_url must not pass startup validation");
            let rendered = format!("{err:?}");
            assert!(
                rendered.contains("userinfo") && rendered.contains("api_key_env"),
                "the error must say what to do instead: {rendered}"
            );
            assert!(
                !rendered.contains(secret),
                "the error echoed the discarded credential: {rendered}"
            );
        }
        // The same hosts without userinfo stay valid, so this rejects the
        // userinfo and not the URL.
        for base_url in ["https://api.together.ai", "http://127.0.0.1:8080"] {
            profile_with_base_url(base_url)
                .validate()
                .unwrap_or_else(|err| panic!("{base_url} should be accepted: {err}"));
        }
    }

    #[test]
    fn profile_validate_surfaces_a_bad_base_url() {
        let err = profile_with_base_url("api.together.ai")
            .validate()
            .expect_err("a bad base_url must not pass startup validation");
        assert!(err.to_string().contains("profiles.*.base_url"));
    }

    /// An empty `serves` entry `starts_with`-matches every model name, so it
    /// would silently claim every non-`claude-*` request and shadow every
    /// profile declared after it.
    #[test]
    fn an_empty_serves_entry_is_rejected() {
        let bad = ProfileConfig {
            serves: vec!["deepseek-ai/".to_string(), String::new()],
            ..profile("openai")
        };
        let err = bad.validate().unwrap_err().to_string();
        assert!(err.contains("serves"), "{err}");
    }

    #[test]
    fn profiles_parse_from_toml_in_declaration_order() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [profiles.kimi]
            base_url = "https://kimi.example"
            api_key_env = "RELAY_MOONSHOT_KEY"
            format = "anthropic"
            serves = ["moonshotai/"]
            model_map = { "*" = "moonshotai/Kimi-K3" }

            [profiles.deepseek]
            base_url = "https://deepseek.example"
            api_key_env = "RELAY_TOGETHER_KEY"
            format = "openai"
            serves = ["deepseek-ai/", "Qwen/"]
            model_map = { "claude-opus" = "deepseek-ai/DeepSeek-V4" }
        "#;

        let config = Config::from_toml_str(raw).expect("should parse");
        assert_eq!(config.profiles.len(), 2);
        // §7d resolves the first `serves` match *in config order* — this is
        // only meaningful if the map preserves the file's declaration order
        // rather than a hash-based one.
        assert_eq!(
            config.profiles.keys().collect::<Vec<_>>(),
            vec!["kimi", "deepseek"]
        );
        let deepseek = &config.profiles["deepseek"];
        assert_eq!(deepseek.format, "openai");
        assert_eq!(deepseek.serves, vec!["deepseek-ai/", "Qwen/"]);
        assert_eq!(
            deepseek.model_map.get("claude-opus").map(String::as_str),
            Some("deepseek-ai/DeepSeek-V4")
        );
    }

    #[test]
    fn zero_profiles_is_valid() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"
        "#;
        let config = Config::from_toml_str(raw).expect("should parse");
        assert!(config.profiles.is_empty());
    }

    #[test]
    fn an_unknown_profile_field_is_a_parse_error() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [profiles.deepseek]
            base_url = "https://deepseek.example"
            api_key_env = "RELAY_TOGETHER_KEY"
            format = "openai"
            region = "us-east"
        "#;
        let err = Config::from_toml_str(raw).expect_err("should fail to parse");
        assert!(err.to_string().contains("region"));
    }

    // --- [policy] ---

    #[test]
    fn policy_defaults_match_the_documented_shape() {
        let policy = PolicyConfig::default();
        assert_eq!(policy.mode, "new-sessions");
        assert_eq!(policy.active_profile, None);
        assert_eq!(policy.min_reset_horizon_secs, 300);
        assert_eq!(policy.max_reset_horizon_secs, 7 * 24 * 60 * 60);
        assert_eq!(policy.reset_jitter_secs, [15, 60]);
        assert!(policy.validate(&IndexMap::new()).is_ok());
    }

    #[test]
    fn policy_mode_rejects_anything_but_the_three_documented_values() {
        for mode in ["new-sessions", "all", "notify-only"] {
            let policy = PolicyConfig {
                mode: mode.to_string(),
                ..PolicyConfig::default()
            };
            assert!(policy.validate(&IndexMap::new()).is_ok(), "{mode}");
        }
        let policy = PolicyConfig {
            mode: "always".to_string(),
            ..PolicyConfig::default()
        };
        let err = policy.validate(&IndexMap::new()).unwrap_err().to_string();
        assert!(err.contains("policy.mode"), "{err}");
    }

    #[test]
    fn active_profile_naming_a_nonexistent_profile_is_rejected() {
        let policy = PolicyConfig {
            active_profile: Some("ghost".to_string()),
            ..PolicyConfig::default()
        };
        let err = policy.validate(&IndexMap::new()).unwrap_err().to_string();
        assert!(err.contains("active_profile"), "{err}");
    }

    #[test]
    fn active_profile_naming_a_configured_profile_is_accepted() {
        let mut profiles = IndexMap::new();
        profiles.insert("deepseek".to_string(), profile("openai"));
        let policy = PolicyConfig {
            active_profile: Some("deepseek".to_string()),
            ..PolicyConfig::default()
        };
        assert!(policy.validate(&profiles).is_ok());
    }

    #[test]
    fn no_active_profile_configured_is_valid() {
        assert!(PolicyConfig::default().validate(&IndexMap::new()).is_ok());
    }

    /// Crossed bounds would make every classified window collapse onto the
    /// ceiling; catching it at startup keeps `detect::bounded` out of that
    /// situation. (Moved from `DetectConfig::validate` with the fields.)
    #[test]
    fn validate_rejects_crossed_horizon_bounds() {
        let policy = PolicyConfig {
            min_reset_horizon_secs: 900,
            max_reset_horizon_secs: 300,
            ..PolicyConfig::default()
        };
        let err = policy.validate(&IndexMap::new()).unwrap_err().to_string();
        assert!(err.contains("max_reset_horizon_secs"));
    }

    /// A `max_reset_horizon_secs` written in the wrong unit is not a bound at
    /// all: large enough and `bounded`'s `checked_add` returns `None`,
    /// silently killing every marked classification; merely huge and
    /// `/status` renders a `Limited` window it cannot express.
    #[test]
    fn validate_rejects_a_max_reset_horizon_that_is_not_a_bound_at_all() {
        for max_reset_horizon_secs in [
            7 * 24 * 60 * 60 * 1000, // 7 days written in milliseconds
            i64::MAX as u64,
            u64::MAX,
        ] {
            let policy = PolicyConfig {
                max_reset_horizon_secs,
                ..PolicyConfig::default()
            };
            let err = policy
                .validate(&IndexMap::new())
                .expect_err("an unbounded ceiling bounds nothing")
                .to_string();
            assert!(
                err.contains("max_reset_horizon_secs"),
                "{max_reset_horizon_secs}: {err}"
            );
        }

        assert!(
            PolicyConfig {
                max_reset_horizon_secs: MAX_RESET_HORIZON_CEILING_SECS,
                ..PolicyConfig::default()
            }
            .validate(&IndexMap::new())
            .is_ok(),
            "the ceiling itself is a valid configuration"
        );
    }

    /// Inverted jitter bounds would panic `rand::random_range` inside the
    /// route-state applier thread — caught by its `catch_unwind`, so the
    /// process survives, but silently: the route never transitions to
    /// `Limited` again for the life of the process.
    #[test]
    fn validate_rejects_inverted_jitter_bounds() {
        let policy = PolicyConfig {
            reset_jitter_secs: [60, 15],
            ..PolicyConfig::default()
        };
        let err = policy.validate(&IndexMap::new()).unwrap_err().to_string();
        assert!(err.contains("reset_jitter_secs"), "{err}");
    }

    /// Jitter is added *on top of* the already-bounded window `bounded`
    /// produces, so it is not covered by `MAX_RESET_HORIZON_CEILING_SECS` —
    /// a units mistake here can push `Limited.until` unboundedly far out
    /// unless it has its own ceiling.
    #[test]
    fn validate_rejects_a_jitter_max_past_its_own_ceiling() {
        let policy = PolicyConfig {
            reset_jitter_secs: [15, 24 * 60 * 60], // a day, written as seconds
            ..PolicyConfig::default()
        };
        let err = policy.validate(&IndexMap::new()).unwrap_err().to_string();
        assert!(err.contains("reset_jitter_secs"), "{err}");

        assert!(
            PolicyConfig {
                reset_jitter_secs: [15, MAX_JITTER_SECS],
                ..PolicyConfig::default()
            }
            .validate(&IndexMap::new())
            .is_ok(),
            "the ceiling itself is a valid configuration"
        );
    }

    #[test]
    fn policy_parses_from_toml_and_moved_fields_are_gone_from_detect() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [policy]
            mode = "all"
            active_profile = "deepseek"
            min_reset_horizon_secs = 900
            max_reset_horizon_secs = 3600
            reset_jitter_secs = [10, 30]

            [profiles.deepseek]
            base_url = "https://deepseek.example"
            api_key_env = "RELAY_TOGETHER_KEY"
            format = "openai"
        "#;
        let config = Config::from_toml_str(raw).expect("should parse");
        assert_eq!(config.policy.mode, "all");
        assert_eq!(config.policy.active_profile.as_deref(), Some("deepseek"));
        assert_eq!(config.policy.min_reset_horizon_secs, 900);
        assert_eq!(config.policy.max_reset_horizon_secs, 3600);
        assert_eq!(config.policy.reset_jitter_secs, [10, 30]);
        assert!(config.policy.validate(&config.profiles).is_ok());
    }

    #[test]
    fn an_unknown_policy_field_is_a_parse_error() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [policy]
            hot_reload = true
        "#;
        let err = Config::from_toml_str(raw).expect_err("should fail to parse");
        assert!(err.to_string().contains("hot_reload"));
    }

    /// Milestone-2-era configs that put these fields under `[detect]` (per
    /// `docs/decisions.md`'s recorded divergence) fail to parse now that
    /// `[detect]` is `deny_unknown_fields` and no longer has them — this is
    /// the deliberate breaking move `docs/decisions.md` resolves, not a bug.
    #[test]
    fn a_milestone_2_style_detect_section_with_the_moved_fields_is_a_parse_error() {
        let raw = r#"
            listen = "127.0.0.1:8484"

            [anthropic]
            base_url = "https://api.anthropic.com"

            [detect]
            min_reset_horizon_secs = 900
        "#;
        let err = Config::from_toml_str(raw).expect_err("should fail to parse");
        assert!(err.to_string().contains("min_reset_horizon_secs"));
    }

    /// The shipped example doubled in size this milestone (`[profiles.*]`,
    /// `[policy]`); nothing else pins it against silently going stale or
    /// unparseable the next time a field moves or gets renamed.
    #[test]
    fn relay_example_toml_parses_and_validates() {
        let raw = include_str!("../relay.example.toml");
        let config = Config::from_toml_str(raw).expect("relay.example.toml should parse");
        config
            .detect
            .validate()
            .expect("relay.example.toml's [detect] should validate");
        config
            .notify
            .validate()
            .expect("relay.example.toml's [notify] should validate");
        for profile in config.profiles.values() {
            profile
                .validate()
                .expect("relay.example.toml's profiles should validate");
        }
        config
            .policy
            .validate(&config.profiles)
            .expect("relay.example.toml's [policy] should validate");
        config
            .anthropic_base_url()
            .expect("relay.example.toml's anthropic.base_url should validate");
    }
}
