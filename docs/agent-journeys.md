# AI-assisted journeys, and the state machines under them

Ptah's AI integration has two surfaces and one contract. `ptah mcp` serves an
external client; `ptah assist` drives a model itself over the same in-process
server. What a person is trying to do is a **journey**; what the code does while
they do it is a **state machine**.

Both machines are in the tree already. This document writes down the journeys
they were built for and the states they move through, so the code can be checked
against an intent rather than only against itself. Every state, limit and
lifetime below is read from the code and cited; nothing here is aspirational.

Related: [the agent surface](agent-surface.md) for what each verb does to a
database, [ADR 0006](adr/0006-one-authorized-agent-runtime.md) for the
authorization boundary, and [ADR 0004](adr/0004-constrained-artifact-mutation.md)
for why a change is previewed before it is applied.

## 1. The journeys

Five, in the order somebody meets them.

### J1 — Understand this project

*"What does this schema say, and what would it become?"*

No workspace needed and nothing is written. The person points a client at
`ptah mcp`, names a directory schemas may be read from, and asks. The model
calls `describe_session`, then one or more of `validate_schema`,
`render_schema`, `schema_lineage`.

**Ends when** the model answers. **Cannot** touch a database or a file.

### J2 — Ask about a live database

*"What does the database actually hold right now?"*

The operator configures the database — `--database-url`, `--database-class` —
and decides separately whether an agent may read it
(`--allow-database-inspect`). The model names the target by the name
`describe_session` reports and never sees a URL.

**Ends when** the catalogs are read, or when the policy refuses. An `ask`
verdict with nobody to ask is a refusal, not a promotion.

### J3 — Propose a change

*"Add a column, and let me see it before it lands."*

The journey the artifact tools exist for. `describe_session` reports the
artifact directories and their digests; `read_artifact` reads what is there;
`preview_patch` produces a diff and a token and writes nothing; the person
approves; `apply_patch` writes, verifies and keeps or undoes.

**Ends when** the patch is applied and the gates found nothing new, or when the
patch is undone and the diagnostics come back.

### J4 — Repair what the gates rejected

*"That patch broke the directory. Fix it."*

Not a separate mechanism: it is J3 again with the previous run's diagnostics as
input. The files are already back to what they were — an apply that introduces
an error undoes itself — so the agent re-reads, composes against the digest the
artifact holds **now**, and previews again.

**Ends** as J3 does. The distinguishing fact is that the old preview token is
spent and the old digest is stale, so neither can be reused.

### J5 — Review without changing

*"Show me what this would do, and stop."*

J3's first two steps and no third. `preview_patch` returns the diff, the
resulting digest and the capabilities an apply would need, having written
nothing. Letting the token expire is the whole of "no".

**Ends when** the token expires, fifteen minutes later.

## 2. The conversation machine

`internal/assistloop`. One request in, one answer out, with tool calls in
between. It exists only on the Assist surface: an MCP client runs its own loop
and Ptah is the tool server.

```text
                   ┌──────────────────────────────┐
                   ▼                              │
  request ──▶ ask the model ──▶ tool calls? ──yes─┘ run them, feed results back
                   │                 │
                   │                 no
                   │                 ▼
                   │            answer ────────▶ StoppedWithAnswer
                   │                             StoppedTruncated
                   ▼
          a limit is reached ─────────────────▶ StoppedAtTurnLimit
                                                StoppedAtToolCallLimit
                                                StoppedRepeating
```

**States.** Asking; running tool calls; finished. There is no waiting state:
approval on this surface is a terminal prompt inside a tool call, and on the MCP
surface it is an input request that ends the call.

**Terminal reasons**, and each is a distinct value a caller can branch on
(`internal/assistloop/assistloop.go`):

| stop reason | what happened |
| --- | --- |
| `answer` | the model replied with no tool call |
| `output limit` | the model's own reply was cut short by its token budget |
| `turn limit` | 12 round trips (`DefaultMaxTurns`) |
| `tool call limit` | 40 calls in one request (`DefaultMaxToolCalls`) |
| `repeated tool call` | the same call with the same arguments 3 times (`DefaultMaxRepeats`) |

A sixth outcome is not a stop reason: a provider failure returns an error with
no reason set, which is why the answer record carries `error` as well.

**Why the limits are values and not opinions.** Every one of them bounds a loop
a model drives. Without the repeat bound in particular, a model that misreads a
refusal will re-issue the same call forever, and the run that costs money is the
one nobody is watching.

**One tool result is bounded too**: 32 KiB (`DefaultMaxToolOutputBytes`), and
truncation is stated in the text rather than applied silently — a model that got
half a listing and was not told would report the half as the whole.

## 3. The patch machine

`internal/agentapi`, and it is the same machine on both surfaces because both
call the same session.

```text
  read_artifact ──▶ compose ──▶ preview_patch
                                     │
                                     ├── refused: gates, path, digest, policy
                                     │
                                     ▼
                              token minted ──── 15 minutes ────▶ expired
                                     │
                                approve (where policy asks)
                                     │
                                     ▼
                              apply_patch ──▶ write ──▶ verify
                                                          │
                                              ┌───────────┴───────────┐
                                              ▼                       ▼
                                        kept, digest              undone,
                                        recomputed                diagnostics
```

**The token is the state.** It is minted by a preview, belongs to exactly one
patch, is usable once, and expires 15 minutes later
(`previewLifetime`). At most 32 live at a time (`maxLivePreviews`), so a caller
that previews in a loop without applying cannot grow the process.

Three refusals are worth naming because they are the machine working:

- **`digest_mismatch`** — the artifact changed between preview and apply. The
  patch is refused rather than merged; the caller re-reads and composes again.
- **`unknown_preview`** — the token was never minted, is spent, has expired, or
  names another patch. All four are one code because the remedy is one thing.
- **`gate_failed`** — the write happened, verification found a new error, and
  the whole patch was undone. The response carries what the gates said and the
  digest the artifact holds now.

**A gate failure is not a rollback of the directory to some earlier idea of
correct.** It restores exactly the bytes that were there, recomputes the
integrity file, and hands back diagnostics. A directory that was already failing
does not become unpatchable: the applier decides on what the patch *introduced*,
not on the absolute count.

## 4. What the machines deliberately do not do

- **No queue and no retry across requests.** A failed request ends that request.
  Assist's conversation survives it because a person is there; a job does not,
  and its exit code says so.
- **No state shared between sessions.** A session grant lives in the process and
  dies with it. Nothing on disk grants anything.
- **No machine for applying to a database.** There is no such operation on this
  surface at any setting, which is why no journey above ends in one.

## 5. Where each state is observable

A reader checking the code against this document has three places to look, and
all three are the same events:

| | |
| --- | --- |
| `.ptah/agent-audit.jsonl` | one line per authorization decision, refusals included |
| `.ptah/sessions/*.jsonl` | the conversation and every tool call, as it happens |
| `--format jsonl` on stdout | the same session records, streamed live |

The audit log answers "what did Ptah decide". The session answers "what was
asked and what came back". They are different files on purpose: deleting a
conversation must not be able to edit the record of what it changed.
