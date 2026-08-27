# Agent journeys and the state machines behind them

Ptah's two AI surfaces — the MCP server an external client drives, and Ptah
Assist — run on one contract, so they run one set of workflows. This document
writes those workflows down: what a caller does in what order, which states the
machinery moves through, and what each state refuses.

It exists because the machines are in the code and the intent was not written
anywhere. A reader could see that a preview token expires and not know whether
fifteen minutes was a decision or an accident. Every number below is quoted from
the code that enforces it, with the file that owns it named, so the two can be
checked against each other.

## The three journeys

| journey | who drives it | what it needs | writes |
| --- | --- | --- | --- |
| Reading | any MCP client, or Assist | nothing but the server | nothing |
| Artifact change | any MCP client, or Assist | a workspace and a granted capability | files, under verification |
| Assisted request | a person, through `ptah assist` | a model provider and a key the person holds | whatever the two above write |

## Journey 1 — reading

```text
connect ──▶ describe_session ──▶ validate_schema
                              ├─▶ render_schema
                              ├─▶ schema_lineage
                              ├─▶ search_docs
                              └─▶ read_database
```

No state is kept between calls. `describe_session` comes first by convention
rather than by rule: it reports which artifact directories exist, which database
targets the operator configured, and what the policy permits, so a caller that
starts there does not have to discover the boundary by being refused at it.

Each of the six reading operations asks the capability broker before it runs.
The verdicts are the operator's, resolved once at startup — see
[ADR 0006](adr/0006-one-authorized-agent-runtime.md).

**Owner:** `internal/agentapi`.

## Journey 2 — an artifact change

This is the state machine. A patch is previewed, and the preview mints a token
the apply must present.

```text
                  ┌──────────── digest moved ◀───────────┐
                  │                                      │
read_artifact ──▶ preview_patch ──▶ [token live] ──▶ apply_patch ──▶ verified
                       │                 │                 │             │
                  invalid patch      expires (15m)     approval      gates found
                  unsafe path        superseded (32)   refused       something
                       │                 │                 │        introduced
                       ▼                 ▼                 ▼             ▼
                   refused           unknown_preview   approval_    rolled back
                                                       refused
```

### The preview token

| state | how it is reached | what an apply gets |
| --- | --- | --- |
| live | `preview_patch` minted it | the patch applies |
| expired | 15 minutes passed | `unknown_preview`, naming the expiry time |
| spent | an apply consumed it | `unknown_preview` |
| superseded | a 33rd preview evicted it | `unknown_preview` |
| wrong patch | the token is live but names another patch | `unknown_preview` |

`previewLifetime` is 15 minutes and `maxLivePreviews` is 32, both in
`internal/agentapi/session.go`. The lifetime is short because the token exists
to bind an apply to a preview a person saw, and a token that outlives the
conversation it was shown in has stopped doing that. The bound exists because a
caller that previews in a loop without applying should not grow the process.

The token is consumed **atomically**, so two applies racing on one token produce
one apply and one `unknown_preview` rather than two applies.

### Approval

Where the policy's verdict is `ask`, the apply cannot proceed until a person
answers. The protocol does not let a server interrupt a tool call to ask, so the
call returns what it needs and the client calls again with the same token:

```text
apply_patch ──▶ needs approval ──▶ client collects the answer ──▶ apply_patch
                                                                  (same token)
```

The token is deliberately **not** spent on the first attempt — spending it would
make the second call fail with `unknown_preview` and the approval pointless. A
client that cannot present a prompt gets `approval_unavailable`, never an
approval nobody gave.

### The apply, and its two endings

An apply that gets past the token and the policy runs in one order, and the
order is the only one that answers the question:

1. re-read the artifact and confirm the digest still matches the preview;
2. run the gates on what is there now, to know what was already broken;
3. write the files;
4. refresh the migration integrity file, where the class has one;
5. run the gates again.

A gate finding that was **not** in step 2 is something this patch introduced, and
the patch is undone — files, and the checksum file's prior state, including its
absence. The response then carries both halves: the error says the apply did not
stand, and the structured answer says what the gates found, which digest the
artifact holds now, and whether the undo completed.

**Owners:** `internal/agentapi` (the token machine), `internal/agentpatch` (plan,
write, undo), `internal/agentgate` (verification), `internal/agentpolicy` (the
verdicts). [ADR 0004](adr/0004-constrained-artifact-mutation.md) records why.

## Journey 3 — an assisted request

`ptah assist` gives the model no capability of its own. It speaks MCP to Ptah's
own server over an in-memory transport, so the model reaches exactly the tools an
external client reaches, through the same broker.

```text
request ──▶ model proposes ──▶ Ptah runs the tools ──▶ results back to the model
              ▲                                              │
              └──────────────── next turn ◀──────────────────┘
```

Tool calls in a turn run **serially**: two calls writing to one artifact
directory would race, and the loop cannot tell a reading call from a writing one
without knowing more about the tools than an adapter should.

A run ends for one of five reasons, and four of them are limits:

| stop reason | bound | why it exists |
| --- | --- | --- |
| answer | — | the model answered |
| turn limit | 12 turns | a model that will not stop |
| tool call limit | 40 calls | a model that calls in a loop |
| repeated tool call | 3 identical calls | a model stuck on one call |
| output limit | 32 KiB per tool result | a tool answer that would fill the context |

The defaults are `DefaultMaxTurns`, `DefaultMaxToolCalls`, `DefaultMaxRepeats`
and `DefaultMaxToolOutputBytes` in `internal/assistloop/assistloop.go`. A run
that hits one of them ends as a terminated run naming the reason, not as a
hang.

**Owner:** `internal/assistloop`.

## What every journey has in common

- **The policy decides before the work happens**, never after, and the decision
  is the operator's — resolved at startup from flags and configuration, not from
  anything the model sends.
- **A refusal names its code.** Every failure carries a member of the taxonomy in
  [`docs/agent-errors.md`](agent-errors.md), so a client can tell a denied
  capability from an expired token from a database it could not reach.
- **Nothing here applies anything to a database.** The artifact journey writes
  files, and a migration is applied by a person running the migration verbs.
