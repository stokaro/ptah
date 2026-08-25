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

`ptah assist explain --non-interactive` removes the prompt rather than answering
it: an operation that needs approval is refused, which is what that flag has to
mean. It is on the one-shot command only. The conversation always has somebody
at a terminal, which is what makes it a conversation, so there is no fail-closed
mode of it to ask for.

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
schema files, database object names. On Unix it is written so only you can read it (the directory `0700`, the file
`0600`; on Windows those bits say nothing and access is an ACL question Ptah
does not read), and it, and
it belongs in `.gitignore`:

```text
.ptah/sessions/
```

`--ephemeral` keeps no record at all, which is the answer for a project whose
contents should not sit in a file afterwards.

No provider credential is ever stored, and a connection URL that reaches a tool
is recorded with its password removed. The provider profile's *name* is, because a
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

### What actually leaves this machine

```bash
ptah assist context "what changed in the last migration?" \
  --workspace . --migrations-dir ./migrations --dialect postgres
```

```text
Nothing below has been sent.

Asking this question would send the following to a-model via local.

  Ptah's instructions       1725 bytes  the same for every project
  Tool schemas              5206 bytes  8 tools: names and argument shapes
  Conversation                35 bytes  1 message(s)
  Total                     6966 bytes
```

The report is built by the **same code that builds the real request**, so it
cannot describe one thing while another leaves the machine. A test compares it
against what a provider was actually handed, and the command's own test uses an
endpoint that fails if anything reaches it.

On a first request nothing there describes your project. Ptah's instructions and
the tool schemas are the same whatever the project is, and the only other thing
is your question. Project content reaches the provider **when a tool answers** —
migration text, schema files, database object names — because that is what the
model asked to see.

So every text-mode run reports the size of it:

```text
-- 4182 bytes of project content reached local, from 2 tool answer(s).
```

`--format json` and `--format jsonl` do not carry that line. The tool records
they do carry hold the same content, so a consumer can size it itself.

`--resume` is the one case where a first request already carries something about
the project: the earlier conversation is part of what would be sent, and
`ptah assist context --resume <id>` prints that too rather than saying otherwise.

`ptah assist context` runs no tool and makes no decision, so it opens no audit
log and leaves nothing in the project — a command reporting what a question
would send, while dropping a file into the directory it is describing, would
contradict itself.

### The answer arrives as it is written

`ptah assist` and `ptah assist explain` print the model's words as they come,
rather than after the turn ends. On a local model answering a long question that
is the difference between a terminal that is working and one that looks stuck.

What streams is the model's prose, and only that. A tool call arrives in
fragments too — its arguments are not valid JSON until the last one lands — so
Ptah assembles it and the run acts on it whole. There is no state in which a
tool has been called with half its arguments.

`--format json` and `--format jsonl` do not stream: one renders a document and
the other a record stream, and neither has anywhere to put a fragment. The
session file is unaffected either way — it holds the finished answer, because a
file of half-sentences is not something anyone reads back.

A stream that fails partway is reported rather than retried. Ptah retries a
failed request up to twice, and stops doing so once any of the answer has been
shown: replaying it would print the first half a second time, which is worse
than the failure it came from.

### Reading a run with a program

`ptah assist explain --format jsonl` prints the conversation to stdout as JSON
lines, written as each thing happens rather than gathered at the end:

```bash
ptah assist explain "what changed in the last migration?" \
  --workspace . --migrations-dir ./migrations --dialect postgres \
  --format jsonl
```

```json
{"type":"session","at":"...","schema_version":1,"model":"...","provider":"local"}
{"type":"request","at":"...","text":"what changed in the last migration?"}
{"type":"tool","at":"...","tool":"read_artifact","result":"..."}
{"type":"answer","at":"...","text":"...","turns":2,"stop_reason":"answer","verified":true}
```

These are the session file's own records, in the same schema, carrying the same
`schema_version` — so what a program reads on stdout is what it will read back
out of `.ptah/sessions/`, byte for byte. There is one format to learn and one
thing to version, rather than a document for pipes and a file for later.

The `tool` line is printed when the tool answers, before the model is asked
again, so a long run reports what it is doing instead of going quiet. The same
records reach the session file at that moment: a run killed halfway leaves the
question and every tool that had already answered, rather than nothing at all.

A run that ends badly still ends with an `answer` record, carrying `stop_reason`
and an `error`. Without them an interrupted run is an empty answer, which reads
exactly like a model that had nothing to say — and stdout is the only channel
this format has. The summary line goes to **stderr**, so stdout stays one record
per line and nothing else.

Pair it with `--ephemeral` for a run that keeps no conversation. With
`--workspace` it still writes `.ptah/agent-audit.jsonl` — that is the record of
what Ptah *decided*, and `--ephemeral` is about the conversation, not about the
audit trail.

Every run is bounded — turns, total tool calls, repeats of one identical call,
and the size of a single tool result. A model that loops terminates with a
diagnostic naming which limit it hit, and the record is printed either way.
`--max-tool-calls` moves one of those bounds.

## Start from what you already exported

A key in your environment produces a profile without a configuration file:

| variable | profile it produces |
| --- | --- |
| `OPENAI_API_KEY` | `openai`, against `https://api.openai.com/v1` |
| `OPENAI_API_KEY` and `OPENAI_BASE_URL` | `openai`, against the base URL you named |
| `ANTHROPIC_API_KEY` | `anthropic` |
| `ANTHROPIC_API_KEY` and `ANTHROPIC_BASE_URL` | `anthropic`, against the base URL you named |
| `OLLAMA_HOST` | `ollama`, against that host |

**A profile is not yet a working one: none of these carries a model.** Name one
with `PTAH_ASSIST_MODEL`, or `--model` on the command:

```bash
export OPENAI_API_KEY=…
export PTAH_ASSIST_MODEL=gpt-4o-mini
ptah assist provider test
```

Without it every command stops with `profile "openai" states no model`, and
exits 2. `ptah assist provider list` shows the profile before that point, so a
listed profile is not by itself a usable one.

`PTAH_ASSIST_PROFILE` names which profile to use when several exist;
`--provider-profile` overrides it.

`ptah assist provider list` says where each profile came from, so an inferred
one is never mistaken for a file you wrote.

An exported but empty **key** variable is a configuration error rather than an
absent one: `OPENAI_API_KEY=` produces a profile whose credential fails with the
variable named, which is what a typo in a CI environment file needs to hear. The
other three treat empty as absent — `OLLAMA_HOST=` produces no profile, and an
empty base URL falls back to the default.

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

### What `base_url` has to be

Ptah appends to it. For `openai-compatible` it requests `<base_url>/chat/completions`
and, for the probe, `<base_url>/models`. So the value ends where those paths
begin — usually at `/v1`:

```text
http://127.0.0.1:11434/v1     ✓  Ollama
http://127.0.0.1:1234/v1      ✓  LM Studio
http://127.0.0.1:8000/v1      ✓  vLLM
localhost:11434/v1            ✗  no scheme; classified provider_error
http://127.0.0.1:11434        ✗  no /v1; the request 404s
```

A scheme and a host are required. `OLLAMA_HOST` is the one place a bare
`host:port` is accepted, because that project's own convention is to write it
that way, and Ptah turns it into a URL.

`base_url` is required for `openai-compatible`. For `anthropic` it is optional
and defaults to the public API; `ANTHROPIC_BASE_URL` sets it for a derived
profile.

Every profile also accepts `timeout_seconds` and `max_retries`. `headers` and
`query` are sent verbatim, which is how an Azure deployment or a gateway that
wants an extra header is reached.

**No provider is tested support.** Ptah implements the two protocols above; the
list of gateways is where they are known to be spoken, not a matrix anybody
maintains. What a model must be able to do is call tools, and
`ptah assist provider test` measures that against your endpoint rather than
reading it off documentation.

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

A key written into `credential:` is refused — that field takes a reference and
nothing else. Ptah stores no credential anywhere: the reference is resolved when
the provider is built, the value is held in memory for the life of that process,
and no Ptah command writes it to disk.

**`headers:` is not checked, and that is worth knowing.** A gateway that wants
`Authorization: Bearer …` takes it there, so Ptah cannot tell an inline key in
that map from a header a gateway needs. Nothing validates it and
`ptah assist provider list` does not print it. If your gateway needs a key in a
header, keep the file owner-only and treat it as a secret.

On Unix, the configuration file and any `file:` credential must not be readable
by group or others — `chmod 600` — and Ptah refuses to start otherwise, the way
`ssh` refuses a private key. **On Windows that check does not run**: `os.Stat`
there synthesizes permission bits from the read-only attribute, so they say
nothing about who can read the file, and access is an ACL question Ptah does not
read. A trailing newline in a credential file is trimmed so the file an editor
saved works.

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

Up to four things are measured:

| check | what it means |
| --- | --- |
| reachable | nothing refused the connection or timed out |
| credential | no 401 or 403 came back |
| model listed | the endpoint's own model list contains this model |
| tool calling | the model returned a tool call when asked for one |

Two of those are narrower than they look. `credential: accepted` means no
authentication error came back, so a profile with no credential at all against
an endpoint that wants none also reports accepted — there was nothing to accept.
And a `base_url` that will not parse is reported as reachable, because nothing
was contacted for anything to refuse; the cause is in the notes, and the
classification is `provider_error`.

For an `anthropic` profile only three are measured. That API serves no model
list, so `model listed` is always `no` and the report says why in a note.

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

Exit codes, which a script should branch on with all three in mind:

| code | meaning |
| --- | --- |
| 0 | the profile is usable |
| 1 | the profile resolved and failed a check; the document is printed |
| 2 | the profile could not be resolved at all — no model, an unreadable configuration file, an unresolvable credential reference, or no default among several — and no document is printed |

A script written for 0-or-1 reads a configuration mistake as a crash.

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
| `provider_error` | the request never left — most often a `base_url` with no scheme, such as `localhost:11434/v1` |
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
