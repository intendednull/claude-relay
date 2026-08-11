use anyhow::{Result, bail};

use crate::config::Config;

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
/// `policy.active_profile` when nothing claims it, and failing cleanly when
/// neither exists. Pure and config-only — no route state, no HTTP.
///
/// Assumes `config` already passed `PolicyConfig::validate`: an
/// `active_profile` naming an unconfigured profile is rejected there, at
/// startup, so this never has to re-check it.
pub fn route(model: &str, config: &Config) -> Result<RouteDecision> {
    if model.starts_with("claude-") {
        return Ok(RouteDecision::Anthropic);
    }
    if let Some(name) = matching_profile(model, config) {
        return Ok(RouteDecision::Profile(name));
    }
    match &config.policy.active_profile {
        Some(name) => Ok(RouteDecision::Profile(name.clone())),
        None => {
            bail!("no profile serves model {model:?}, and no `policy.active_profile` is configured")
        }
    }
}

fn matching_profile(model: &str, config: &Config) -> Option<String> {
    config.profiles.iter().find_map(|(name, profile)| {
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
    use crate::config::ProfileConfig;
    use std::collections::HashMap;

    fn profile(serves: &[&str]) -> ProfileConfig {
        ProfileConfig {
            base_url: "https://example.com".to_string(),
            api_key_env: "RELAY_KEY".to_string(),
            format: "openai".to_string(),
            serves: serves.iter().map(|s| s.to_string()).collect(),
            model_map: HashMap::new(),
        }
    }

    fn config_with_profiles(profiles: &[(&str, &[&str])]) -> Config {
        let mut config = base_config();
        for (name, serves) in profiles {
            config.profiles.insert((*name).to_string(), profile(serves));
        }
        config
    }

    fn base_config() -> Config {
        use crate::config::{AnthropicConfig, NotifyConfig, PolicyConfig};
        use crate::detect::DetectConfig;
        use indexmap::IndexMap;

        Config {
            listen: "127.0.0.1:0".to_string(),
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
    fn a_claude_model_always_routes_to_anthropic() {
        let config = config_with_profiles(&[("deepseek", &["claude-"])]);
        assert_eq!(
            route("claude-opus-4", &config).unwrap(),
            RouteDecision::Anthropic,
            "a profile whose `serves` would also match must not shadow claude-* routing"
        );
    }

    #[test]
    fn a_name_matching_a_profiles_serves_routes_to_that_profile() {
        let config = config_with_profiles(&[
            ("kimi", &["moonshotai/"]),
            ("deepseek", &["deepseek-ai/", "Qwen/"]),
        ]);
        assert_eq!(
            route("deepseek-ai/DeepSeek-V4", &config).unwrap(),
            RouteDecision::Profile("deepseek".to_string())
        );
        assert_eq!(
            route("Qwen/Qwen3.6-27B", &config).unwrap(),
            RouteDecision::Profile("deepseek".to_string())
        );
        assert_eq!(
            route("moonshotai/Kimi-K3", &config).unwrap(),
            RouteDecision::Profile("kimi".to_string())
        );
    }

    #[test]
    fn the_first_matching_profile_in_config_order_wins() {
        // Both profiles' `serves` claim the same prefix; only declaration
        // order in the config distinguishes them (spec §7d: "first ... in
        // config order").
        let config =
            config_with_profiles(&[("first", &["open-model/"]), ("second", &["open-model/"])]);
        assert_eq!(
            route("open-model/v1", &config).unwrap(),
            RouteDecision::Profile("first".to_string())
        );
    }

    #[test]
    fn an_unclaimed_name_falls_through_to_the_active_profile() {
        let mut config = config_with_profiles(&[("deepseek", &["deepseek-ai/"])]);
        config.policy.active_profile = Some("deepseek".to_string());
        assert_eq!(
            route("some-other-provider/model", &config).unwrap(),
            RouteDecision::Profile("deepseek".to_string()),
            "no `serves` entry claims this name, so it falls through to active_profile"
        );
    }

    #[test]
    fn an_unclaimed_name_with_no_active_profile_is_a_clean_error() {
        let config = config_with_profiles(&[("deepseek", &["deepseek-ai/"])]);
        let err = route("some-other-provider/model", &config)
            .expect_err("nothing claims this name and no active_profile is set");
        assert!(err.to_string().contains("policy.active_profile"));
    }

    #[test]
    fn no_profiles_and_no_active_profile_is_a_clean_error_not_a_panic() {
        let config = base_config();
        let err = route("some-open-model", &config)
            .expect_err("zero profiles configured is valid, but routes nothing by name");
        assert!(err.to_string().contains("no profile serves"));
    }

    #[test]
    fn claude_prefixed_models_route_to_anthropic_even_with_zero_profiles_configured() {
        let config = base_config();
        assert_eq!(
            route("claude-3-5-haiku", &config).unwrap(),
            RouteDecision::Anthropic
        );
    }

    #[test]
    fn serves_matching_is_prefix_based_not_exact() {
        let config = config_with_profiles(&[("deepseek", &["deepseek-ai/"])]);
        assert_eq!(
            route("deepseek-ai/DeepSeek-V4-Flash", &config).unwrap(),
            RouteDecision::Profile("deepseek".to_string())
        );
        assert!(
            route("deepseek-ai", &config).is_err(),
            "the prefix itself without the trailing slash must not match"
        );
    }

    /// The router assumes a validated config, so it trusts `active_profile`
    /// without re-checking `config.profiles` — this documents that trust
    /// rather than testing an invariant `route` itself doesn't enforce.
    #[test]
    fn active_profile_is_trusted_as_already_validated() {
        let mut config = base_config();
        config.policy.active_profile = Some("unvalidated-name".to_string());
        assert_eq!(
            route("some-open-model", &config).unwrap(),
            RouteDecision::Profile("unvalidated-name".to_string())
        );
    }
}
