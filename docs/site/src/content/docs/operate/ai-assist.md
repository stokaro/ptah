---
title: Ptah Assist and your own model
description: Configure a model provider for ptah assist with your own key, endpoint, and model, and check that it works.
---

Ptah Assist talks to a model you choose, through a key you hold. There is no
Ptah account, no Ptah-hosted model, and no Ptah AI token. The model can be a
hosted API, a gateway your organization runs, or one running on this machine —
in the last case nothing about your schema leaves it.

```bash
ptah assist                                        # hold a conversation
ptah assist explain "what do the migrations do?"   # ask once
ptah assist sessions list                          # the conversations saved for this project
ptah assist provider list                          # the profiles this machine can reach
ptah assist provider test                          # whether one of them works, measured
```

## Hold a conversation

`ptah assist` with no arguments opens one. Type a question; the model answers
using Ptah's tools. Lines beginning with `/` are commands rather than questions:

```text
  /tools     the Ptah tools this session can reach
  /session   where this conversation is being saved
  /trace     show or hide the tool trace
  /help      this list
  /exit      leave (Ctrl-D does the same)
```

They are slash-prefixed so a question that starts with one of those words is
still a question.

## Ask one question

```bash
ptah assist explain "what migrations does this project have?"

ptah assist explain --trace \
  --workspace . --migrations-dir ./migrations --dialect postgres \
  "what would adding a status column to users look like?"
```

The model answers using Ptah's own tools, and every one of those calls goes
through the same surface an external AI client reaches over the Model Context
Protocol: the same tools, the same capability broker, the same verification
gates, the same audit record. Ptah Assist gets nothing an external client does
not — it is a client of that surface, over an in-memory transport rather than
stdio.

Which tools the model reaches, and what it may do with them, is decided by the
same flags [AI agents over MCP](../ai-agents/) documents. Without `--workspace`
the model reaches the reading tools. With one it also reaches the artifact
tools, and writing stays refused until `--allow-write` names an artifact class.

When a patch needs approval, Ptah asks in the terminal and shows the artifact,
the paths and the exact digest the approval covers:

```text
Allow? [n]o / [o]nce / [s]ession:
```

`--non-interactive` removes the prompt rather than answering it: an operation
that needs approval is refused, which is what that flag has to mean.

## Conversations are saved

Each conversation is written to `.ptah/sessions/` in the project it was about,
one JSON object per line, as it happens. Append-only, because the failure mode
of a conversation is the process ending in the middle of one: a truncated JSON
document is unreadable and a truncated JSONL file is every record up to the last
complete one.

```bash
ptah assist sessions list              # what is saved, most recent first
ptah assist sessions show <id>         # one conversation, with what Ptah did
ptah assist sessions delete <id>       # remove one
ptah assist sessions prune             # remove ones untouched for 30 days
ptah assist --resume <id>              # continue one
```

An id or a unique prefix of one. There is no automatic retention: a conversation
goes away when somebody asks for it to, because a tool that quietly deleted the
record of what it changed would be the wrong kind of tidy.

### What is in a session file, and what is not

The conversation **and** what Ptah read on the model's behalf: migration text,
schema files, database object names. It is written so only you can read it, and
it belongs in `.gitignore`:

```text
.ptah/sessions/
```

`--ephemeral` keeps no record at all, which is the answer for a project whose
contents should not sit in a file afterwards.

No credential is ever stored. The provider profile's *name* is, because a
session that could not say which model answered cannot be read later.

Resuming replays the conversation and **not** the tool results. Those described
the project as it was; a resumed session that fed them back as current would
have the model reasoning about a directory that may have changed since. It
re-reads instead, which costs a tool call and is the answer that is still true.

The session file is not the audit record. `.ptah/agent-audit.jsonl` is what Ptah
decided — every capability request, granted or refused — and deleting a session
does not touch it.

### The answer and the evidence are different things

The answer is the model's words. What Ptah actually did is the tool trace, which
`--trace` prints and `--format json` always carries. A run where no tool
answered says so:

```text
-- No Ptah tool answered, so nothing above was checked against this project.
```

That line is the difference between an answer Ptah stands behind and a model
talking about databases in general, and the two look identical without it.

Every run is bounded — turns, total tool calls, repeats of one identical call,
and the size of a single tool result. A model that loops terminates with a
diagnostic naming which limit it hit, and the record is printed either way.
`--max-tool-calls` moves one of those bounds.

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

Profiles live in `~/.ptah/assist.yaml`. That is the same directory name Ptah
already uses beside a project — `./.ptah/` holds the approval keys and the agent
audit record — so there is one answer to "where does Ptah keep things" rather
than two. `PTAH_ASSIST_CONFIG` names a different file, which is what a container
or a CI job uses.

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
  use to Ptah, which needs no provider configuration at all. Everything there
  about permissions, artifact scopes and verification applies here unchanged:
  it is the same surface.
