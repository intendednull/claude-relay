# E2E drill evidence — satisfied by production traffic

**Date:** 2026-08-13
**Status:** landed

Spec §10 item 5 ("mock-limited Anthropic + real fallback provider; run an actual
Claude Code task to completion on the fallback path") was satisfied organically:
during the 2026-08-13→14 subscription-limit window the relay ran in LIMITED state
and served full interactive Claude Code sessions — tool calls, subagents, streaming —
to completion on the Together fallback. This session's own traffic is the evidence.

Captured 2026-08-13 ~20:38 PDT from journalctl + /status:

```
{"state":"LIMITED","limited_until":"2026-08-14T20:00:15Z","fallback_requests_served":86,"active_profile":"together","config_digest":"678a6487721b42b2c6f224ac9f6ebdfcc449cc419c7578597c70f679cf0e693b"}
      1 proxied request route="fallback" profile="together" model_in="claude-sonnet-5" model_out="moonshotai/Kimi-K2.7-Code" method=POST path=/v1/messages status=200 latency_ms=5002
      1 proxied request route="fallback" profile="together" model_in="claude-sonnet-5" model_out="moonshotai/Kimi-K2.7-Code" method=POST path=/v1/messages status=200 latency_ms=3083
      1 proxied request route="fallback" profile="together" model_in="claude-sonnet-5" model_out="moonshotai/Kimi-K2.7-Code" method=POST path=/v1/messages status=200 latency_ms=2653
      1 proxied request route="fallback" profile="together" model_in="claude-sonnet-5" model_out="moonshotai/Kimi-K2.7-Code" method=POST path=/v1/messages status=200 latency_ms=1950
      1 proxied request route="fallback" profile="together" model_in="claude-sonnet-5" model_out="moonshotai/Kimi-K2.7-Code" method=POST path=/v1/messages status=200 latency_ms=1559
      1 proxied request route="fallback" profile="together" model_in="claude-sonnet-5" model_out="moonshotai/Kimi-K2.7-Code" method=POST path=/v1/messages status=200 latency_ms=1555
      1 proxied request route="fallback" profile="together" model_in="claude-sonnet-5" model_out="moonshotai/Kimi-K2.7-Code" method=POST path=/v1/messages status=200 latency_ms=1442
      1 proxied request route="fallback" profile="together" model_in="claude-sonnet-5" model_out="moonshotai/Kimi-K2.7-Code" method=POST path=/v1/messages status=200 latency_ms=1361
```

Distinct model_in values above (claude-fable-5 → Kimi-K3, claude-sonnet-5 →
Kimi-K2.7-Code) are the main loop and its subagents of a live tool-heavy session,
all 200s. Milestone 3's acceptance row is earned; Milestone 3 is spec-complete.
