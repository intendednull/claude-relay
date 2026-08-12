//! `/control/*` (spec §8b): the runtime knobs for which profile new requests
//! fail over to. This is the one surface in the relay that can redirect where
//! a client's traffic — and a profile's API key — goes, so it carries its own
//! rules beyond the usual review:
//! - it must not exist at all on a non-loopback bind (`enabled`);
//! - a loopback *bind* is not enough on its own — DNS rebinding lets an
//!   attacker's own domain resolve to 127.0.0.1, so every request under
//!   `/control` is also checked for a loopback `Host` header;
//! - a loopback `Host` is not enough on its own either — a page loaded
//!   directly from `http://127.0.0.1:<port>` has an honestly loopback `Host`
//!   with no rebinding involved, so a state-changing request also has to
//!   look like it came from something other than a cross-origin browser tab
//!   (`Sec-Fetch-Site`/`Origin`, and `POST /control/profile` requiring
//!   `content-type: application/json`, which forces a CORS preflight);
//! - it must never read, let alone return, an API key value (only a
//!   profile's `api_key_env` *name* is ever touched here), and never return
//!   `base_url` at all, since that can carry a credential of its own.
//!
//! The `Sec-Fetch-Site`/`Origin` half of that is *not* `/control`-specific and
//! is applied to every path the relay serves — see `install_gate`, which is
//! where this module's gate stopped being only about its own routes.
//!
//! **The gate is applied by path, over the whole application router
//! (`install_gate`, called from `build_router`), not by which sub-router a
//! route happens to be registered on.** An earlier version gated only the
//! router built by `routes()` below, which two things fell through: a route
//! `.layer()`-ed after inside the same router escaped it, and — as far as
//! this module could ever know — a control route registered somewhere else
//! entirely (a different `.route()` call, in `lib.rs` or a future module)
//! never went through this module at all, so it inherited nothing. Gating by
//! *path*, on the fully assembled router, closes both: it doesn't matter
//! where `/control/...` was registered, or in what order, only that the path
//! matches — proven in this module's own tests by registering a probe route
//! outside `routes()` entirely and confirming it is still gated.
//!
//! This does not make the guarantee unconditional: `install_gate` still has
//! to be the *last* thing `build_router` does to the router before
//! `.with_state`, since a route chained on *after* that call would, once
//! again, never pass through it. That is one call site to get right rather
//! than every future control-route addition anywhere in the crate, which is
//! the actual improvement — not "gated regardless of what `build_router`
//! does henceforth".

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::json;

// D-2: "is this host loopback" is one function, in `config`, used by both the
// `base_url` transport check there and this module's `Host`/`Origin` gate. The
// two used to answer it separately and disagreed.
use crate::config::{Config, host_str_is_loopback, is_loopback_ip};
use crate::notify::NotifyEvent;
use crate::state::AppState;

const CONTROL_PATH_PREFIX: &str = "/control";

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

/// `/control/*`'s two routes. Registering them buys nothing on its own —
/// unlike the previous design, this no longer checks `enabled` or attaches
/// any gate — the gate is `install_gate`, applied once to the *whole*
/// composed application router in `build_router`. Kept as a separate
/// function anyway, so `lib.rs` reads as "the routes" and "the gate" rather
/// than one undifferentiated block.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/control/profiles", get(list_profiles))
        .route("/control/profile", post(switch_profile))
}

/// The relay's one request gate, in two halves.
///
/// **Every path**, `/v1/*` included, must have a trustworthy origin: if a
/// browser sent this, it must not be a cross-origin page
/// (`Sec-Fetch-Site`/`Origin`, when present). `/v1/messages` decides its route
/// from the JSON *body* regardless of content type, and `text/plain` is
/// CORS-safelisted — so without this, a cross-origin page could POST a body
/// naming a profile's model and make the relay spend that profile's API key,
/// with no preflight and no `/control` request anywhere in it (F1,
/// `docs/decisions.md`). It costs the real client nothing: Claude Code is not
/// a browser and sends neither header, which `header_is_trustworthy` treats as
/// acceptable.
///
/// **Under `/control` only**, additionally:
/// - the bind must be loopback (`enabled`);
/// - `Host` must be a loopback literal or `localhost`.
///
/// Matched on `request.uri().path()`, computed once from `config` (there is
/// no hot-reload this milestone, so the bind check never changes after
/// startup) and evaluated fresh per request for the header checks. A `/control`
/// refusal is a bare 404: an attacker probing for this surface must not be
/// able to tell "wrong `Host`" apart from "no such route". Off `/control` that
/// reasoning does not apply — `/v1/messages` is the relay's whole public
/// purpose and its existence is not a secret — so that refusal is an honest
/// 403 in the same error envelope the proxy's own refusals use.
pub(crate) fn install_gate(router: Router<AppState>, config: &Config) -> Router<AppState> {
    let bind_is_loopback = enabled(config);
    router.layer(middleware::from_fn(move |request: Request, next: Next| {
        let bind_is_loopback = bind_is_loopback;
        async move {
            let is_control = request.uri().path().starts_with(CONTROL_PATH_PREFIX);
            if let Some(refused_by) = untrustworthy_origin_header(request.headers()) {
                let status = if is_control {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::FORBIDDEN
                };
                // Otherwise this gate — the likeliest thing here to refuse a
                // legitimate client nobody has tested — returns a bare 403 from
                // the user's own proxy with no explanation anywhere.
                //
                // The refusing header's *name* and the status, never a value:
                // an `Origin` is attacker-controlled, `tracing`'s `%` renders
                // unescaped, and a client-controlled field has already forged a
                // whole record in this codebase (`log_safety::safe_identifier`).
                tracing::warn!(
                    refused_by,
                    status = status.as_u16(),
                    "cross-origin request refused"
                );
                return if is_control {
                    status.into_response()
                } else {
                    (
                        status,
                        Json(json!({ "error": "cross_origin_request_refused" })),
                    )
                        .into_response()
                };
            }
            if !is_control {
                return next.run(request).await;
            }
            if !bind_is_loopback || !is_loopback_host(request.headers()) {
                return StatusCode::NOT_FOUND.into_response();
            }
            next.run(request).await
        }
    }))
}

fn is_loopback_host(headers: &HeaderMap) -> bool {
    let mut hosts = headers.get_all(header::HOST).iter();
    let Some(host) = hosts.next() else {
        return false;
    };
    // RFC 9112 requires exactly one `Host` header; treating a second one as
    // authoritative (or silently preferring the first) is exactly the kind
    // of request-smuggling-adjacent ambiguity a `Host`-based trust decision
    // must not paper over.
    if hosts.next().is_some() {
        return false;
    }
    let Ok(host) = host.to_str() else {
        return false;
    };
    host_str_is_loopback(host)
}

/// `Sec-Fetch-Site`/`Origin` are attached by every current browser and
/// cannot be overridden by page script (`fetch`/XHR silently ignore attempts
/// to set them) — unlike `Host`, no DNS trick lets a page forge these into
/// claiming same-origin. A request carrying neither header at all (`curl`,
/// `relay ctl`, this project's own tests) is not a browser request and is
/// unaffected: `header_is_trustworthy` treats *absent* as fine and everything
/// else it cannot positively validate — duplicated, not valid UTF-8, or
/// present-but-rejected-by-`valid` — as a rejection, never a silent skip.
///
/// Returns the *name* of the header that refused (`None` when both are
/// trustworthy) rather than a bool, so the refusal's WARN can say which check
/// fired without going anywhere near either value.
fn untrustworthy_origin_header(headers: &HeaderMap) -> Option<&'static str> {
    const SEC_FETCH_SITE: &str = "sec-fetch-site";
    if !header_is_trustworthy(headers, SEC_FETCH_SITE, |site| {
        matches!(site, "same-origin" | "none")
    }) {
        return Some(SEC_FETCH_SITE);
    }
    if !header_is_trustworthy(headers, header::ORIGIN.as_str(), is_loopback_origin) {
        return Some("origin");
    }
    None
}

/// Same discipline `is_loopback_host` already applies to `Host`, generalized
/// to a header that's optional rather than required: absent is fine (this
/// might not be a browser request at all), but present-and-duplicated,
/// present-and-not-valid-UTF-8, or present-and-failing `valid` are all
/// rejections. A rejection indistinguishable from "not present" — the
/// `and_then(...).ok()` shape this replaces — defeats the point of checking
/// at all: a non-ASCII byte or a second header value would previously fall
/// through `and_then` as `None` and the request would pass.
fn header_is_trustworthy(headers: &HeaderMap, name: &str, valid: impl Fn(&str) -> bool) -> bool {
    let mut values = headers.get_all(name).iter();
    let Some(first) = values.next() else {
        return true;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(value) = first.to_str() else {
        return false;
    };
    valid(value)
}

/// An `Origin` header is `scheme "://" host [":" port]` — no path, no
/// userinfo — so the host half reuses `host_str_is_loopback` once the scheme
/// is checked and stripped. An opaque origin (`"null"`, from a sandboxed
/// frame or a `data:` context) has no scheme or host to check and is
/// rejected outright, the same as any other scheme this doesn't recognize as
/// carrying a trustworthy host at all (`evil://localhost`'s host half being
/// loopback proves nothing about what `evil://` actually means).
fn is_loopback_origin(origin: &str) -> bool {
    let Some((scheme, host)) = origin.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    host_str_is_loopback(host)
}

#[derive(Serialize)]
struct ProfileView<'a> {
    name: &'a str,
    format: &'a str,
    serves: &'a [String],
    model_map: &'a IndexMap<String, String>,
    /// The env var's *name*, never its value — this response must never
    /// carry a credential (Global Constraint 2). `base_url` is deliberately
    /// not a field here at all: a configured `base_url` can carry a secret of
    /// its own in its path or query (userinfo is refused at startup — see
    /// `config::reject_userinfo`), a second, independent way this response
    /// must not leak one.
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

fn has_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            // Parameters (`; charset=...`) ride along on a legitimate JSON
            // body and are not part of what's being checked here.
            value
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .eq_ignore_ascii_case("application/json")
        })
}

/// `POST /control/profile` (spec §8b): switches the profile new requests
/// route against. 404 on a name nothing configured claims — checked here,
/// once, so `AppState::set_active_profile` never has to; an in-flight request
/// already read `active_profile()` before this ever runs and is unaffected
/// either way (`proxy::forward`'s doc comment on that read).
///
/// Requires `content-type: application/json`, rejecting anything else —
/// missing, or one of the three CORS-simple types a `<form>` can send
/// without a preflight — with 415. This one is not folded into the
/// path-based gate above: it is a property of *this* endpoint's body, not of
/// `/control` as a whole, and `install_gate`'s refusals are deliberately a
/// bare 404 to stay indistinguishable from a nonexistent route, which 415
/// does not need to be — by the time content-type is checked, the request
/// has already passed the loopback-`Host`/`Origin` gate, so revealing "wrong
/// content type" tells an already-trusted caller something, not an attacker
/// who was rejected earlier.
///
/// Parses the body by hand (`Bytes`, not axum's `Json` extractor) so a
/// malformed or unrecognized-field body gets this endpoint's own
/// `{"error": ...}` envelope instead of axum's default plain-text rejection.
async fn switch_profile(State(state): State<AppState>, request: Request) -> Response {
    if !has_json_content_type(request.headers()) {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(json!({ "error": "unsupported_content_type" })),
        )
            .into_response();
    }
    // A legitimate body here is a few dozen bytes (`{"name": "..."}`); the
    // cap is generous headroom, not a real limit, and going through
    // `Request`/`to_bytes` by hand (rather than the `Bytes` extractor) means
    // this has to set one explicitly instead of inheriting axum's default.
    let body = match axum::body::to_bytes(request.into_body(), 64 * 1024).await {
        Ok(body) => body,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "invalid_request_body" })),
            )
                .into_response();
        }
    };
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
    use std::sync::Arc;

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
            // R7: no legitimate `Host` value ever carries userinfo.
            "evil.tld@localhost",
            "evil.tld@127.0.0.1",
            "",
        ] {
            assert!(!host_str_is_loopback(host), "{host} must not be loopback");
        }
    }

    #[test]
    fn origin_header_loopback_classification() {
        for origin in [
            "http://127.0.0.1",
            "http://127.0.0.1:8484",
            "http://localhost:8484",
            "http://[::1]:8484",
            "https://127.0.0.1",
        ] {
            assert!(is_loopback_origin(origin), "{origin} should be loopback");
        }
        for origin in [
            "http://evil.example",
            "https://evil.example:8484",
            "null",
            "",
            // Y8: the scheme must be http(s) — a loopback host half proves
            // nothing about what a non-http(s) scheme actually means.
            "evil://localhost",
            "evil://127.0.0.1",
        ] {
            assert!(!is_loopback_origin(origin), "{origin} must not be loopback");
        }
    }

    /// Y8: a header this module cannot positively validate — not valid UTF-8,
    /// or duplicated — must be a rejection, not a silently-skipped check. The
    /// `and_then(...).ok()` shape this replaced let both fall through as "not
    /// present" and pass.
    #[test]
    fn origin_and_sec_fetch_site_fail_closed_on_what_they_cannot_validate() {
        // A non-ASCII byte: `to_str()` fails.
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ORIGIN,
            axum::http::HeaderValue::from_bytes(b"http://\xff").expect("valid header bytes"),
        );
        assert_eq!(untrustworthy_origin_header(&headers), Some("origin"));

        // Duplicate Origin, loopback first: taking "the first" would pass this.
        let mut headers = HeaderMap::new();
        headers.append(header::ORIGIN, "http://127.0.0.1".parse().unwrap());
        headers.append(header::ORIGIN, "http://evil.example".parse().unwrap());
        assert_eq!(untrustworthy_origin_header(&headers), Some("origin"));

        // Duplicate Sec-Fetch-Site, same-origin first.
        let mut headers = HeaderMap::new();
        headers.append("sec-fetch-site", "same-origin".parse().unwrap());
        headers.append("sec-fetch-site", "cross-site".parse().unwrap());
        assert_eq!(
            untrustworthy_origin_header(&headers),
            Some("sec-fetch-site")
        );

        // Sanity: a single, valid header of each kind still passes.
        let mut headers = HeaderMap::new();
        headers.insert(header::ORIGIN, "http://127.0.0.1".parse().unwrap());
        headers.insert("sec-fetch-site", "same-origin".parse().unwrap());
        assert_eq!(untrustworthy_origin_header(&headers), None);
    }

    #[test]
    fn json_content_type_classification() {
        let mut ok = HeaderMap::new();
        ok.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        assert!(has_json_content_type(&ok));

        let mut ok_with_charset = HeaderMap::new();
        ok_with_charset.insert(
            header::CONTENT_TYPE,
            "application/json; charset=utf-8".parse().unwrap(),
        );
        assert!(has_json_content_type(&ok_with_charset));

        for bad in [
            "text/plain",
            "text/plain;charset=UTF-8",
            "application/x-www-form-urlencoded",
            "multipart/form-data",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, bad.parse().unwrap());
            assert!(!has_json_content_type(&headers), "{bad} must be rejected");
        }
        assert!(!has_json_content_type(&HeaderMap::new()));
    }

    async fn serve_test_router(router: Router) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind ephemeral port");
        let addr = listener.local_addr().expect("failed to read local addr");
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("server error");
        });
        addr
    }

    /// R2, pinned directly against `install_gate` rather than against
    /// `build_router`: a control route registered *outside* `routes()`
    /// entirely — the way Milestone 4's own `POST /control/mode` plausibly
    /// will be — must still inherit both the bind check and the `Host`
    /// check. This is the property the module doc above claims; this test
    /// is what makes that claim checked rather than asserted.
    #[tokio::test]
    async fn a_control_route_registered_outside_routes_still_inherits_the_gate() {
        async fn probe() -> &'static str {
            "reached"
        }

        // Non-loopback bind: the probe must be exactly as absent as a real
        // control route.
        let non_loopback = config_with_listen("0.0.0.0:8484");
        let router: Router<AppState> = Router::new().route("/control/probe", get(probe));
        let gated = install_gate(router, &non_loopback);
        let state = AppState::new(Arc::new(non_loopback), None, "digest".to_string())
            .expect("should build");
        let addr = serve_test_router(gated.with_state(state)).await;
        let response = reqwest::Client::new()
            .get(format!("http://{addr}/control/probe"))
            .header("host", "localhost")
            .send()
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Loopback bind, forged Host: still rejected.
        let loopback = config_with_listen("127.0.0.1:0");
        let router: Router<AppState> = Router::new().route("/control/probe", get(probe));
        let gated = install_gate(router, &loopback);
        let state =
            AppState::new(Arc::new(loopback), None, "digest".to_string()).expect("should build");
        let addr = serve_test_router(gated.with_state(state)).await;
        let forged = reqwest::Client::new()
            .get(format!("http://{addr}/control/probe"))
            .header("host", "evil.example")
            .send()
            .await
            .expect("request failed");
        assert_eq!(forged.status(), StatusCode::NOT_FOUND);

        // Loopback bind, honest Host: reaches the handler — the gate isn't
        // blanket-denying a path, it applies the same real checks any
        // registered `/control/*` route gets.
        let honest = reqwest::Client::new()
            .get(format!("http://{addr}/control/probe"))
            .header("host", "localhost")
            .send()
            .await
            .expect("request failed");
        assert_eq!(honest.status(), StatusCode::OK);
    }

    /// `/status` must not be swept up by the `/control` path prefix — this
    /// pins that `install_gate` really does match on the literal prefix and
    /// nothing broader.
    #[tokio::test]
    async fn install_gate_does_not_touch_paths_outside_control() {
        let cfg = config_with_listen("0.0.0.0:8484"); // non-loopback: control disabled
        let router: Router<AppState> = Router::new().route("/status", get(|| async { "ok" }));
        let gated = install_gate(router, &cfg);
        let state = AppState::new(Arc::new(cfg), None, "digest".to_string()).expect("should build");
        let addr = serve_test_router(gated.with_state(state)).await;

        let response = reqwest::Client::new()
            .get(format!("http://{addr}/status"))
            .header("host", "evil.example")
            .send()
            .await
            .expect("request failed");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a non-/control path must reach its handler even on a non-loopback \
             bind and a forged Host — the gate only ever inspects /control paths"
        );
    }
}
