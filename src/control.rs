//! `/control/*` (spec §8b): the runtime knobs for which profile new requests
//! fail over to. This is the one surface in the relay that can redirect where
//! a client's traffic — and a profile's API key — goes, so it carries its own
//! rules beyond the usual review: it must not exist at all on a non-loopback
//! bind (`enabled`), and it must never read, let alone return, an API key
//! value (only a profile's `api_key_env` *name* is ever touched here).

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::Config;
use crate::notify::NotifyEvent;
use crate::state::AppState;

/// Whether `/control/*` may be registered at all (spec §8b, and the risk
/// table's own line for it): code-enforced, not documentation — a `listen`
/// address is operator config like any other and gets no special trust.
///
/// A `listen` that fails to parse is treated as not loopback: it has not been
/// *proven* safe, and the real binary already refuses to start on one
/// (`main`'s `Config::listen_addr` call happens before `build_router`), so
/// this path is only ever exercised by an embedder that skipped that check.
pub(crate) fn enabled(config: &Config) -> bool {
    config
        .listen_addr()
        .is_ok_and(|addr| addr.ip().is_loopback())
}

#[derive(Serialize)]
struct ProfileView<'a> {
    name: &'a str,
    format: &'a str,
    serves: &'a [String],
    model_map: &'a IndexMap<String, String>,
    /// The env var's *name*, never its value — this response must never
    /// carry a credential (Global Constraint 2).
    api_key_env: &'a str,
    active: bool,
}

/// `GET /control/profiles` (spec §8b): every configured profile, marking
/// which one new requests currently route against.
pub async fn list_profiles(State(state): State<AppState>) -> Json<serde_json::Value> {
    let active = state.active_profile();
    let profiles: Vec<_> = state
        .config
        .profiles
        .iter()
        .map(|(name, profile)| ProfileView {
            name,
            format: &profile.format,
            serves: &profile.serves,
            model_map: &profile.model_map,
            api_key_env: &profile.api_key_env,
            active: active.as_deref() == Some(name.as_str()),
        })
        .collect();
    Json(json!({ "profiles": profiles }))
}

#[derive(Deserialize)]
pub struct SwitchProfile {
    name: String,
}

/// `POST /control/profile` (spec §8b): switches the profile new requests
/// route against. 404 on a name nothing configured claims — checked here,
/// once, so `AppState::set_active_profile` never has to; an in-flight request
/// already read `active_profile()` before this ever runs and is unaffected
/// either way (`proxy::forward`'s doc comment on that read).
pub async fn switch_profile(
    State(state): State<AppState>,
    Json(body): Json<SwitchProfile>,
) -> Response {
    if !state.config.profiles.contains_key(&body.name) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown_profile" })),
        )
            .into_response();
    }
    state.set_active_profile(body.name.clone());
    state.notifier.notify_event(NotifyEvent::ProfileSwitched {
        name: body.name.clone(),
    });
    Json(json!({ "active_profile": body.name })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AnthropicConfig, NotifyConfig, PolicyConfig};
    use crate::detect::DetectConfig;
    use indexmap::IndexMap as Map;

    fn config_with_listen(listen: &str) -> Config {
        Config {
            listen: listen.to_string(),
            state_file: None,
            anthropic: AnthropicConfig {
                base_url: "https://api.anthropic.com".to_string(),
            },
            detect: DetectConfig::default(),
            notify: NotifyConfig::default(),
            profiles: Map::new(),
            policy: PolicyConfig::default(),
        }
    }

    #[test]
    fn enabled_on_loopback_binds() {
        assert!(enabled(&config_with_listen("127.0.0.1:8484")));
        assert!(enabled(&config_with_listen("[::1]:8484")));
    }

    #[test]
    fn disabled_on_a_non_loopback_bind() {
        for listen in ["0.0.0.0:8484", "10.0.0.5:8484", "[::]:8484"] {
            assert!(
                !enabled(&config_with_listen(listen)),
                "{listen} must not enable /control/*"
            );
        }
    }

    /// Not proven loopback is not the same as proven non-loopback, but this
    /// path treats them the same — fail closed, since nothing downstream can
    /// tell the two apart once `listen_addr()` has already refused to parse.
    #[test]
    fn disabled_when_listen_does_not_even_parse() {
        assert!(!enabled(&config_with_listen("not-an-address")));
        assert!(!enabled(&config_with_listen("localhost:8484")));
    }
}
