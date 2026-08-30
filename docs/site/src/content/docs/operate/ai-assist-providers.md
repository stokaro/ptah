---
title: Configure a Ptah Assist provider
description: Define, secure, and test an OpenAI-compatible or Anthropic model profile for Ptah Assist.
type: how-to
audience:
  - "application-developer"
  - "platform-engineer"
readerQuestion: "How do I connect Ptah Assist to my model endpoint?"
goal: "Configure a provider profile without storing a credential in the profile, then verify tool calling."
sourceOfTruth:
  - "internal/assistconfig"
  - "internal/aiprovider"
generated: false
searchAliases:
  - Ollama Ptah
  - OpenAI-compatible Ptah
  - Anthropic Ptah
overlaps:
  - "/operate/ai-assist/"
disposition: keep
owns:
  - cli-ptah-assist-provider-list
  - cli-ptah-assist-provider-test
---

Ptah Assist supports the OpenAI-compatible Chat Completions protocol and the
Anthropic Messages API. Provider profiles belong to the operator, never to the
repository being analyzed.

## Start from environment variables

Ptah derives profiles from common variables:

| Variables | Derived profile |
| --- | --- |
| `OPENAI_API_KEY` and optional `OPENAI_BASE_URL` | `openai` |
| `ANTHROPIC_API_KEY` and optional `ANTHROPIC_BASE_URL` | `anthropic` |
| `OLLAMA_HOST` | `ollama` |

The variables do not provide a model. Name one explicitly:

```bash
export OPENAI_API_KEY=…
export PTAH_ASSIST_MODEL=gpt-4o-mini
ptah assist provider test
```

`PTAH_ASSIST_PROFILE` selects among several profiles;
`--provider-profile` overrides it. `provider list` reports where each profile
came from but does not resolve or print credentials.

An exported empty key is a configuration error rather than an absent profile.
An empty optional base URL or `OLLAMA_HOST` is treated as absent.

## Write a profile

Profiles live in `~/.ptah/assist.yaml`. Set `PTAH_ASSIST_CONFIG` to use another
file in a container or CI job.

```yaml
default: local

profiles:
  local:
    type: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: a-local-model

  work:
    type: anthropic
    model: a-hosted-model
    credential: env:ANTHROPIC_API_KEY

  gateway:
    type: openai-compatible
    base_url: https://ai-gateway.example.com/v1
    model: an-approved-model
    credential: file:/run/secrets/ai-gateway
    headers:
      X-Team: platform
```

For `openai-compatible`, Ptah appends `/chat/completions` and `/models` to
`base_url`, so the value normally ends at `/v1`. A scheme and host are required.
`OLLAMA_HOST` is the sole exception that accepts bare `host:port`, matching
Ollama's own convention.

Anthropic profiles may omit `base_url` to use the public API. Every profile also
accepts `timeout_seconds` and `max_retries`; `headers` and `query` support
organization gateways and Azure-style endpoints.

Named gateways are protocol examples, not a tested support matrix. The endpoint
must support tool calling, and the provider test measures that behavior.

## Keep credentials out of the profile

`credential` accepts a reference, never a literal secret:

```text
env:OPENAI_API_KEY
file:/run/secrets/openai
```

Ptah resolves the reference into process memory and never writes the value.
Credential commands are unsupported because they would be arbitrary code
execution driven by configuration.

On Unix, the configuration and `file:` credential must not be readable by group
or others; use mode `0600`. Windows permission bits do not represent ACL access,
so Ptah cannot enforce the same check there. A trailing newline in a credential
file is trimmed.

The `headers` map is sent verbatim and is not inspected for inline secrets. If a
gateway requires an authorization header, protect the configuration as a secret.

## Verify the endpoint

```bash
ptah assist provider test --provider-profile work
ptah assist provider test --provider-profile work --format json
```

The probe sends a fixed prompt with no project content and measures:

| Check | Meaning |
| --- | --- |
| reachable | The endpoint accepted a connection and did not time out. |
| credential | No authentication refusal was returned. |
| model listed | The endpoint's model list contains the configured identifier. |
| tool calling | The model returned the requested tool call. |

Anthropic exposes no model-list endpoint, so that row is reported as unavailable
and explained. Model listing is informative; tool calling is required.

Exit `0` means the profile is usable, `1` means the profile resolved but a check
failed, and `2` means configuration or credential resolution failed before a
report could be produced.

## Diagnose a provider failure

| Classification | Check |
| --- | --- |
| `auth` | Credential value or reference. |
| `unknown_model` | Model identifier and base URL. |
| `rate_limited` | Provider retry interval; Ptah honors it before retrying. |
| `too_large` | Request and tool-result size. |
| `unreachable` | Endpoint URL and server availability. |
| `provider_error` | Local request construction, often a base URL with no scheme. |
| `malformed_response` | Whether the endpoint actually serves the declared protocol. |

Ptah preserves the provider's message without including the credential. It
retries only failures that produced no answer; it never repeats a partial model
answer already shown to the user.
