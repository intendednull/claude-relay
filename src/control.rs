//! `/control/*` (spec §8b): the runtime knobs for which profile new requests
//! fail over to. This is the one surface in the relay that can redirect where
//! a client's traffic — and a profile's API key — goes, so it carries its own
//! rules beyond the usual review:
//! - it must not exist at all on a non-loopback bind (`enabled`);
//! - a loopback *bind* is not enough on its own — DNS rebinding lets an
//!   attacker's own domain resolve to 127.0.0.1, so every request is also
//!   checked for a loopback `Host` header (`require_loopback_host`);
//! - it must never read, let alone return, an API key value (only a
//!   profile's `api_key_env` *name* is ever touched here), and never return
//!   `base_url` at all, since that can carry a credential of its own.
//!
//! `routes` is the only place the two endpoints below are ever wired up —
//! both handlers are private — so neither gate can be bypassed by a route
//! registered some other way.

use std::net::IpAddr;

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::uri::Authority;
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::Config;
use crate::notify::NotifyEvent;
use crate::state::AppState;

/// Whether `/control/*` may exist at all (spec §8b, and the risk table's own
/// line for it): code-enforced, not documentation — a `listen` address is
/// operator config like any other and gets no special trust.
///
/// A `listen` that fails to parse is treated as not loopback: it has not been
/// *proven* safe, and the real binary already refuses to start on one
/// (`main`'s `Config::listen_addr` call happens before `build_router`), so
/// this path is only ever exercised by an embedder that skipped that check.
pub(crate) fn enabled(config: &Config) -> bool {
    config
        .listen_addr()
        .is_ok_and(|addr| is_loopback_ip(addr.ip()))
}

fn is_loopback_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        // A v4-mapped IPv6 literal (`::ffff:127.0.0.1`) is still loopback:
        // without this, a genuinely loopback-only bind written that way would
        // get no control surface at all (fail closed, so a usability wart
        // rather than a hole, but still wrong).
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

/// `/control/*`'s complete sub-router: the loopback-bind gate (`enabled`),
/// the loopback-`Host` gate (`require_loopback_host`), and both routes,
/// assembled in the one place that ever does so. Returns an empty router when
/// disabled — `build_router` merges this in unconditionally, so a
/// non-loopback bind gets axum's ordinary 404 for `/control/*`, the same as
/// any other unmatched path, rather than a handler-shaped place that could
/// accidentally say more.
pub(crate) fn routes(config: &Config) -> Router<AppState> {
    if !enabled(config) {
        return Router::new();
    }
    Router::new()
        .route("/control/profiles", get(list_profiles))
        .route("/control/profile", post(switch_profile))
        .layer(middleware::from_fn(require_loopback_host))
}

/// DNS rebinding defeats "loopback bind implies local operator only" unless
/// this is also checked: an attacker's own domain can be made to resolve to
/// 127.0.0.1, so a page served from that domain reaches this port as a
/// same-origin request whose `Host` the browser still shows as the
/// attacker's domain, not "localhost". The TCP peer being loopback says
/// nothing about that. Rejects with 404, matching `enabled`'s own choice, so
/// a rejected request looks identical to a route that was never registered.
async fn require_loopback_host(request: Request, next: Next) -> Response {
    if is_loopback_host(request.headers()) {
        next.run(request).await
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

fn is_loopback_host(headers: &HeaderMap) -> bool {
    headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(host_str_is_loopback)
}

/// `Authority` parses `host[:port]`, including a bracketed IPv6 literal with
/// or without a port — a manual split on the last `:` mishandles a bracketed
/// IPv6 host with no port, since the address itself contains colons.
/// `.host()` leaves the brackets on (e.g. `"[::1]"`), so they're trimmed here
/// the same way `config.rs`'s `require_encrypted_transport` does.
fn host_str_is_loopback(host: &str) -> bool {
    let Ok(authority) = host.parse::<Authority>() else {
        return false;
    };
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    match host.parse::<IpAddr>() {
        Ok(ip) => is_loopback_ip(ip),
        Err(_) => host.eq_ignore_ascii_case("localhost"),
    }
}

#[derive(Serialize)]
struct ProfileView<'a> {
    name: &'a str,
    format: &'a str,
    serves: &'a [String],
    model_map: &'a IndexMap<String, String>,
    /// The env var's *name*, never its value — this response must never
    /// carry a credential (Global Constraint 2). `base_url` is deliberately
    /// not a field here at all: a configured `base_url` can carry a
    /// credential in its own userinfo (`docs/decisions.md`), a second,
    /// independent way this response must not leak one.
    api_key_env: &'a str,
    active: bool,
}

/// `GET /control/profiles` (spec §8b): every configured profile, marking
/// which one new requests currently route against.
async fn list_profiles(State(state): State<AppState>) -> Json<serde_json::Value> {
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
#[serde(deny_unknown_fields)]
struct SwitchProfile {
    name: String,
}

/// `POST /control/profile` (spec §8b): switches the profile new requests
/// route against. 404 on a name nothing configured claims — checked here,
/// once, so `AppState::set_active_profile` never has to; an in-flight request
/// already read `active_profile()` before this ever runs and is unaffected
/// either way (`proxy::forward`'s doc comment on that read).
///
/// Parses the body by hand (`Bytes`, not axum's `Json` extractor) so a
/// malformed or unrecognized-field body gets this endpoint's own
/// `{"error": ...}` envelope instead of axum's default plain-text rejection.
async fn switch_profile(State(state): State<AppState>, body: Bytes) -> Response {
    let request: SwitchProfile = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "invalid_request_body" })),
            )
                .into_response();
        }
    };
    if !state.config.profiles.contains_key(&request.name) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown_profile" })),
        )
            .into_response();
    }
    // A switch to the name already active is not a real change, and must not
    // queue a notification — matches `notify.rs`'s existing "only real
    // changes are reported" rule for route transitions.
    if state.set_active_profile(request.name.clone()) {
        state.notifier.notify_event(NotifyEvent::ProfileSwitched {
            name: request.name.clone(),
        });
    }
    Json(json!({ "active_profile": request.name })).into_response()
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

    /// M5: a v4-mapped IPv6 literal is genuinely loopback and must not be
    /// fail-closed out of the control surface just because
    /// `Ipv6Addr::is_loopback` alone says no.
    #[test]
    fn enabled_on_a_v4_mapped_loopback_bind() {
        assert!(enabled(&config_with_listen("[::ffff:127.0.0.1]:8484")));
    }

    #[test]
    fn host_header_loopback_classification() {
        for host in [
            "127.0.0.1:8484",
            "127.0.0.1",
            "[::1]:8484",
            "[::1]",
            "[::ffff:127.0.0.1]:8484",
            "localhost:8484",
            "localhost",
            "LOCALHOST",
        ] {
            assert!(host_str_is_loopback(host), "{host} should be loopback");
        }
        for host in [
            "evil.example",
            "evil.example:8484",
            "10.0.0.5",
            "10.0.0.5:8484",
            "0.0.0.0:8484",
            "127.0.0.1.evil.example",
            "",
        ] {
            assert!(!host_str_is_loopback(host), "{host} must not be loopback");
        }
    }
}
