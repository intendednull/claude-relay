use anyhow::{Result, bail};
use indexmap::IndexMap;

use crate::config::ProfileConfig;
use crate::log_safety::safe_identifier;

/// Where a request's `model` field routes (spec §7d), decided before
/// Milestone 2's limit-state machine is ever consulted: a `claude-*` model
/// always resolves to `Anthropic` here regardless of route state. Whether
/// that request then fails over to a profile while Anthropic is `Limited` is
/// a later failover-policy decision layered on top of this one, not part of
/// it — this type only says where a request's *name* points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    Anthropic,
    Profile(String),
}

/// Resolves `model` to a route per spec §7d: `claude-*` always goes to
/// Anthropic; anything else goes to the first configured profile (in config
/// order) whose `serves` prefix-matches, falling through to
/// `active_profile` when nothing claims it, and failing cleanly when neither
/// exists. Pure and config-only — no route state, no HTTP.
///
/// `active_profile` is a plain `Option<&str>`, not read from `PolicyConfig`
/// directly: `policy.active_profile` is only the *startup* default (spec
/// §8b) — `POST /control/profile` switches it at runtime without ever
/// writing back to config, so the caller must be able to pass a
/// runtime-tracked value in its place without this function's signature
/// changing again once that control endpoint exists.
///
/// Assumes the caller already validated whatever it passes as
/// `active_profile` — via `PolicyConfig::validate` for the startup default,
/// or an equivalent check at the point a runtime switch is accepted — so a
/// name that doesn't exist in `profiles` is never expected here and this
/// never has to re-check it.
pub fn route(
    model: &str,
    profiles: &IndexMap<String, ProfileConfig>,
    active_profile: Option<&str>,
) -> Result<RouteDecision> {
    if model.starts_with("claude-") {
        return Ok(RouteDecision::Anthropic);
    }
    if let Some(name) = matching_profile(model, profiles) {
        return Ok(RouteDecision::Profile(name));
    }
    match active_profile {
        Some(name) => Ok(RouteDecision::Profile(name.to_string())),
        // `safe_identifier`, not `{model:?}`: this message's only destination is
        // `proxy::forward`'s log line, where the raw name would land beside the
        // clipped `model` field it already carries — escaped, so it cannot forge
        // a record, but unbounded, which is the other half of what clipping is
        // for (F2, `docs/decisions.md`).
        None => bail!(
            "no profile serves model {:?}, and no active profile is configured",
            safe_identifier(model)
        ),
    }
}

fn matching_profile(model: &str, profiles: &IndexMap<String, ProfileConfig>) -> Option<String> {
    profiles.iter().find_map(|(name, profile)| {
        profile
            .serves
            .iter()
            .any(|prefix| model.starts_with(prefix.as_str()))
            .then(|| name.clone())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(serves: &[&str]) -> ProfileConfig {
        ProfileConfig {
            base_url: "https://example.com".to_string(),
            api_key_env: "RELAY_KEY".to_string(),
            format: "openai".to_string(),
            serves: serves.iter().map(|s| s.to_string()).collect(),
            model_map: IndexMap::new(),
            params: IndexMap::new(),
        }
    }

    fn profiles(entries: &[(&str, &[&str])]) -> IndexMap<String, ProfileConfig> {
        let mut profiles = IndexMap::new();
        for (name, serves) in entries {
            profiles.insert((*name).to_string(), profile(serves));
        }
        profiles
    }

    #[test]
    fn a_claude_model_always_routes_to_anthropic() {
        let profiles = profiles(&[("deepseek", &["claude-"])]);
        assert_eq!(
            route("claude-opus-4", &profiles, None).unwrap(),
            RouteDecision::Anthropic,
            "a profile whose `serves` would also match must not shadow claude-* routing"
        );
    }

    #[test]
    fn a_name_matching_a_profiles_serves_routes_to_that_profile() {
        let profiles = profiles(&[
            ("kimi", &["moonshotai/"]),
            ("deepseek", &["deepseek-ai/", "Qwen/"]),
        ]);
        assert_eq!(
            route("deepseek-ai/DeepSeek-V4", &profiles, None).unwrap(),
            RouteDecision::Profile("deepseek".to_string())
        );
        assert_eq!(
            route("Qwen/Qwen3.6-27B", &profiles, None).unwrap(),
            RouteDecision::Profile("deepseek".to_string())
        );
        assert_eq!(
            route("moonshotai/Kimi-K3", &profiles, None).unwrap(),
            RouteDecision::Profile("kimi".to_string())
        );
    }

    #[test]
    fn the_first_matching_profile_in_config_order_wins() {
        // Both profiles' `serves` claim the same prefix; only declaration
        // order in the config distinguishes them (spec §7d: "first ... in
        // config order").
        let profiles = profiles(&[("first", &["open-model/"]), ("second", &["open-model/"])]);
        assert_eq!(
            route("open-model/v1", &profiles, None).unwrap(),
            RouteDecision::Profile("first".to_string())
        );
    }

    #[test]
    fn an_unclaimed_name_falls_through_to_the_active_profile() {
        let profiles = profiles(&[("deepseek", &["deepseek-ai/"])]);
        assert_eq!(
            route("some-other-provider/model", &profiles, Some("deepseek")).unwrap(),
            RouteDecision::Profile("deepseek".to_string()),
            "no `serves` entry claims this name, so it falls through to active_profile"
        );
    }

    #[test]
    fn an_unclaimed_name_with_no_active_profile_is_a_clean_error() {
        let profiles = profiles(&[("deepseek", &["deepseek-ai/"])]);
        let err = route("some-other-provider/model", &profiles, None)
            .expect_err("nothing claims this name and no active_profile is set");
        assert!(err.to_string().contains("no active profile"));
    }

    #[test]
    fn no_profiles_and_no_active_profile_is_a_clean_error_not_a_panic() {
        let err = route("some-open-model", &IndexMap::new(), None)
            .expect_err("zero profiles configured is valid, but routes nothing by name");
        assert!(err.to_string().contains("no profile serves"));
    }

    #[test]
    fn claude_prefixed_models_route_to_anthropic_even_with_zero_profiles_configured() {
        assert_eq!(
            route("claude-3-5-haiku", &IndexMap::new(), None).unwrap(),
            RouteDecision::Anthropic
        );
    }

    #[test]
    fn serves_matching_is_prefix_based_not_exact() {
        let profiles = profiles(&[("deepseek", &["deepseek-ai/"])]);
        assert_eq!(
            route("deepseek-ai/DeepSeek-V4-Flash", &profiles, None).unwrap(),
            RouteDecision::Profile("deepseek".to_string())
        );
        assert!(
            route("deepseek-ai", &profiles, None).is_err(),
            "the prefix itself without the trailing slash must not match"
        );
    }

    /// The router trusts `active_profile` without re-checking it against
    /// `profiles` — this documents that trust rather than testing an
    /// invariant `route` itself doesn't enforce. It also proves the
    /// decoupling this signature exists for: a caller can pass a
    /// runtime-tracked value that was never in `profiles` in the first
    /// place (e.g. read from the same config `profiles` came from, at a
    /// different point in time than a since-reloaded `profiles`), and
    /// `route` still resolves it without needing to know where it came from.
    #[test]
    fn active_profile_is_trusted_as_already_validated() {
        let profiles = IndexMap::new();
        assert_eq!(
            route("some-open-model", &profiles, Some("unvalidated-name")).unwrap(),
            RouteDecision::Profile("unvalidated-name".to_string())
        );
    }
}
