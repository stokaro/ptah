---
title: Ptah Assist and your own model
description: Configure a model provider for ptah assist with your own key, endpoint, and model, and check that it works.
---

Ptah Assist talks to a model you choose, through a key you hold. There is no
Ptah account, no Ptah-hosted model, and no Ptah AI token. The model can be a
hosted API, a gateway your organization runs, or one running on this machine —
in the last case nothing about your schema leaves it.

This release carries the provider surface: naming a provider, resolving its
credential, and finding out whether the model can do what the workflow needs.
The conversational surface is separate work.

```bash
ptah assist provider list   # the profiles this machine can reach
ptah assist provider test   # whether one of them works, measured
```

## Start from what you already exported

A key in your environment is enough. No configuration file is needed to get a
working profile:

| variable | profile it produces |
| --- | --- |
| `OPENAI_API_KEY` | `openai`, against `https://api.openai.com/v1` |
| `OPENAI_API_KEY` and `OPENAI_BASE_URL` | `openai`, against the base URL you named |
| `ANTHROPIC_API_KEY` | `anthropic` |
| `OLLAMA_HOST` | `ollama`, against that host |

`ptah assist provider list` says where each profile came from, so an inferred
one is never mistaken for a file you wrote.

An exported but empty variable is a configuration error rather than an absent
one. `OPENAI_API_KEY=` produces a profile whose credential fails with the
variable named, which is what a typo in a CI environment file needs to hear.

## Write profiles for anything else

Profiles live in `assist.yaml` in your Ptah configuration directory:
`$XDG_CONFIG_HOME/ptah/assist.yaml` when that variable is set, and your
platform's configuration directory otherwise. `PTAH_ASSIST_CONFIG` overrides
both.

```yaml
default: local

profiles:
  # A model on this machine. No credential, and nothing leaves the machine.
  local:
    type: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: a-local-model

  # A hosted API.
  work:
    type: anthropic
    model: a-hosted-model
    credential: env:ANTHROPIC_API_KEY

  # A gateway your organization runs, with its own header.
  gateway:
    type: openai-compatible
    base_url: https://ai-gateway.example.com/v1
    model: an-approved-model
    credential: file:/run/secrets/ai-gateway
    headers:
      X-Team: platform

  # Azure OpenAI: the key goes in api-key, the version in the query.
  azure:
    type: openai-compatible
    base_url: https://example.openai.azure.com/openai/deployments/a-deployment
    model: a-deployment
    credential: env:AZURE_OPENAI_KEY
    headers:
      api-key: ""
    query:
      api-version: "2026-01-01"
```

Two provider types cover the field. `openai-compatible` speaks Chat Completions
and reaches OpenAI, Azure OpenAI, OpenRouter, LiteLLM and other gateways, vLLM,
LM Studio, Ollama, and MLX. `anthropic` speaks the Messages API.

Every profile also accepts `timeout_seconds` and `max_retries`.

### Profiles are yours, not your projects'

Profiles are read from your own configuration and from your environment, never
from a file inside the repository being worked on. A repository that could
define a profile could point Ptah at an endpoint of its author's choosing with
your key attached, and the first thing sent there would be the schema you asked
about.

## Credentials are references

`credential:` names where the key is, never what it is:

```text
env:OPENAI_API_KEY        an environment variable
file:/run/secrets/openai  a file, which must not be readable by other users
```

A key written directly into the file is refused. Ptah stores no credential
anywhere: the reference is resolved at the moment a request is made, the value
is held in memory for that request, and no Ptah command writes it to disk. A
credential file that group or others can read is refused the way `ssh` refuses a
private key, and a trailing newline is trimmed so the file an editor saved works.

A credential *command* is deliberately not supported. It is a real convention
elsewhere, and it is arbitrary code execution driven by a configuration file.

`ptah assist provider list` reads no credential at all, so a profile whose key
is missing still appears there and fails at `provider test`, where the failure
is the answer.

## Check that a provider works

```bash
ptah assist provider test
ptah assist provider test --provider-profile work
ptah assist provider test --format json
```

Four things are measured:

| check | what it means |
| --- | --- |
| reachable | the endpoint answered |
| credential | the endpoint accepted it |
| model listed | the endpoint's own model list contains this model |
| tool calling | the model returned a tool call when asked for one |

Tool calling is the capability Ptah Assist requires, and it is measured rather
than read off documentation: a deployment that documents it and one that
supports it are different things. A model without it is refused with a
capability error, because the alternative is a mode that generates SQL and
implies it was validated.

`model listed` is reported and not enforced — plenty of gateways serve models
they do not list.

The check sends nothing about your project. It is a fixed two-line prompt asking
the model to call one tool, so you can test a provider before deciding whether
to send it anything.

The command exits 0 when the profile is usable and 1 when it is not, so a script
can branch on it. In JSON mode the document is printed either way.

## When something fails

Provider failures are classified rather than flattened into one message,
because the remedies differ:

| classification | what to change |
| --- | --- |
| `auth` | the credential, or its reference |
| `unknown_model` | the model identifier, or the base URL |
| `rate_limited` | nothing; Ptah waits the time the provider asked for and retries |
| `too_large` | the size of what is being sent |
| `unreachable` | the base URL, or whether the endpoint is running |
| `malformed_response` | the endpoint, which is not serving the API it claims |

The provider's own message is preserved alongside the classification, because
the classification is for branching and the original text is what says what
happened. No message carries the credential.

Only failures that produced no answer are retried. A request the provider
answered is never repeated.

## Related

- [AI agents over MCP](../ai-agents/) — connect an AI client you already
  use to Ptah, which needs no provider configuration at all.
