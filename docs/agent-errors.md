# The agent error taxonomy

Ptah's agent surfaces answer a failed call with a **code**: a short, stable
string a program can branch on, alongside the sentence a person reads. This
document is the list of codes, what each one means, and what a caller is
expected to do about it.

It exists because prose is not a contract. An agent handed only
`"denied by session policy"` has one recovery available to it — call again — and
a retry is the wrong move for almost every refusal this surface issues: a denied
capability, a spent preview token and a path that leaves the workspace all fail
again identically. A code lets a client tell those apart from the two or three
conditions where waiting genuinely helps.

The taxonomy lives in [`internal/agentdiag`](../internal/agentdiag). The table
below is checked against it: a code the package has and this file does not, or
the reverse, fails the test suite.

## What a failure carries

Every failed tool call carries one diagnostic:

| field | meaning |
| --- | --- |
| `code` | The taxonomy member. Stable; branch on this. |
| `actor` | Who can do something about it. |
| `retryable` | Whether the same call, unchanged, could succeed later. |
| `message` | What went wrong, in words. Names files, limits and identifiers. Not stable — never match on it. |
| `hint` | The action that would clear it on this surface, when there is one to name. Written by the surface, not by the taxonomy. |

## Where it appears on MCP

Two places, because two things read it.

A **model** reads the tool result's text content, which leads with the code:

```text
capability_denied: "write:migrations" denied by session policy. The operator
decides this when starting the server; describe_workspace reports what this
session may do.
```

A **client program** reads the result's `_meta`, under the key
`ptah.5x5.cz/diagnostic`:

```json
{
  "_meta": {
    "ptah.5x5.cz/diagnostic": {
      "code": "capability_denied",
      "actor": "operator",
      "retryable": false,
      "message": "\"write:migrations\" denied by session policy",
      "hint": "The operator decides this when starting the server; describe_workspace reports what this session may do."
    }
  },
  "isError": true
}
```

The failure is a tool result with `isError` set, not a JSON-RPC error. That is
the protocol's own split: a schema that will not load or a capability the policy
refuses is something the caller asked about, and the model is the one that has
to see it.

One tool result carries both a failure and an answer. `apply_patch` does it when
verification refused the patch: the patch was written, the gates found something
it introduced, and it was undone. The error says the apply did not stand and the
structured content says what the gates reported, which digest the artifact holds
now, and — the field that matters most and cannot be inferred — whether the undo
completed.

## The codes

| code | actor | retryable | meaning |
| --- | --- | --- | --- |
| `invalid_request` | caller | no | An argument is missing, malformed, or contradicts another one. |
| `schema_source_unreadable` | caller | no | The named schema source did not load. |
| `render_failed` | caller | no | The schema loaded but does not render for the requested dialect. |
| `database_unreachable` | environment | yes | The database connection could not be opened. |
| `database_read_failed` | environment | yes | The connection opened and the catalog read did not finish. |
| `no_workspace` | operator | no | The session was started without a workspace, so it has no artifact operations. |
| `no_database_target` | operator | no | The session was started with no live database configured. |
| `no_source_scope` | operator | no | The session was started with no directory a declared schema may be read from. |
| `artifact_class_not_configured` | operator | no | The workspace has no directory for the artifact class the call names. |
| `capability_denied` | operator | no | The policy refuses the capability the operation needs. |
| `approval_unavailable` | operator | no | The operation needs a human approval this client cannot be asked for. |
| `approval_refused` | person | no | A human was asked and refused. |
| `unsafe_path` | caller | no | The location is absolute, leaves its configured scope, is not in plain form, or would be fetched rather than read. |
| `artifact_too_large` | caller | no | A file, patch or directory is over one of the contract's stated limits. |
| `not_regular_file` | caller | no | The path names a directory, symlink or device rather than a file. |
| `unknown_preview` | caller | no | The preview token is unknown, expired, spent, or for another patch. |
| `digest_mismatch` | caller | no | The artifact changed between the preview and the apply. |
| `invalid_patch` | caller | no | The patch is not one this contract accepts. |
| `gate_failed` | caller | no | The patch was written, failed verification, and was undone. |
| `verification_unavailable` | operator | no | Verification could not run, so nothing was written. |
| `write_failed` | environment | yes | A filesystem write did not complete; what was written was undone. |
| `internal` | ptah | no | A failure with no better code, which is a defect rather than a decision. |

Two conditions that would lead a caller to do the same thing share a code rather
than splitting into a pair nobody can act on differently. A malformed database
URL is `database_unreachable` for that reason: the driver reports it the same
way it reports a server that is down, and a distinction Ptah cannot measure
would be a code that lies half the time.

## The actors

`actor` is the coarse branch, for a client that does not want a case per code.

| actor | who acts | what a well-behaved agent does |
| --- | --- | --- |
| `caller` | The agent itself | Change the request. The message names what to change. |
| `operator` | The person who started Ptah | Stop and report. No request will succeed until they change how the server was started. |
| `person` | A human who was asked | Stop. They answered. |
| `environment` | A database, a filesystem | Retry if `retryable`, otherwise report. |
| `ptah` | Ptah | Report it as a defect. |

## Stability

- A code is **never renamed and never repurposed.** Its meaning on the day it
  shipped is its meaning.
- New codes **are** added. A client must treat a code it does not recognize the
  way it treats `internal`: report it, do not retry, and read `actor` — which is
  one of a set that does not grow — to decide whether to stop.
- Adding a code changes the agent contract version reported by
  [`internal/agentapi`](../internal/agentapi) and by `describe_workspace`.
- `message` and `hint` are prose. They are written for people and models, they
  change without notice, and no client should match on them.

## For Ptah's own maintainers

A code is assigned at the sentinel:

```go
var ErrUnsafePath = agentdiag.Sentinel(agentdiag.CodeUnsafePath, "unsafe artifact path")
```

so every site that returns it, or wraps it with `%w`, reports the code without
naming it again. Errors arriving from outside the agent packages — a schema that
will not load, a database that will not dial — are coded where they cross into
the contract, which is `internal/agentapi`.

A sentinel with no code answers `internal`, and a test in `internal/agentdiag`
refuses that: it walks the source of every agent package, collects each
package-level `Err…` it declares, and requires each one to be classified or
named in an exemption list with its reason. An error that never reaches a client
— a configuration fault raised before the first tool call — is what the
exemption list is for.

[ADR 0006](adr/0006-agent-error-taxonomy.md) records why the taxonomy is shaped
this way.
