# ADR 0007: A closed agent error taxonomy, assigned at the sentinel and carried in `_meta`

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1484](https://github.com/stokaro/ptah/issues/1484), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Answers the Phase 0 checklist item *"Define structured diagnostics and stable agent error taxonomy."*

## 1. Context

The agent surface refuses a great deal. It refuses a capability the policy does
not grant, a preview token that was spent, a path that leaves the workspace, a
patch whose base digest moved, and a write its own gates would not stand behind.
Every one of those was prose.

### 1.1 What a client got, measured

Built from `master` at `944d4d8d` and driven over stdio with a JSON-RPC client.
Three failures, chosen because they are the three an agent meets first:

| failure | `isError` | `_meta` | `structuredContent` | text |
| --- | --- | --- | --- | --- |
| a required argument omitted | true | absent | absent | `validating "arguments": validating root: required: missing properties: ["source"]` |
| apply refused by policy | true | absent | absent | `"artifact.write:migrations" denied by invocation policy. The operator decides this…` |
| apply undone by a gate | true | absent | absent | `verification gate failed: migration-sql: expected table name…` |

Three observations, all of them from that table rather than from reading the
code.

**Nothing distinguishes them but English.** A client that wanted to branch — to
stop on the second and re-read the artifact on a third — had to match on
substrings of sentences written for people, which change whenever somebody
improves a message.

**The first row blames Ptah for the caller's mistake.** The SDK refuses
arguments that do not match the tool's input schema before Ptah's code runs. To
a client, that is indistinguishable from Ptah failing, and the only recovery a
model can attempt without understanding the difference is to call again.

**The third row loses the answer.** A patch was written, verification found what
it introduced, and it was undone. `agentpatch.Result` had the whole account —
which diagnostics were introduced, which digest the artifact holds now, whether
the undo completed — and `structuredContent` is absent, because the operation
returned an error and the SDK packs no structured content beside one. What
reached the client was `describe(introduced)`: the first few messages, as a
sentence. The field a caller must not miss, `rollback_failure`, could not reach
it at all.

### 1.2 Why this is worse for an agent than for a person

A person reading `denied by invocation policy` goes and looks at how the server
was started. A model has one recovery available to it that requires no
understanding, and it is the wrong one for almost everything in that table:
calling again. A denied capability, a spent token, and an unsafe path all fail
again identically, and an agent that retries them burns the conversation.

## 2. Decisions

### 2.1 A closed set of short string codes

**Chosen.** A finite list of lowercase identifiers — `capability_denied`,
`digest_mismatch`, `unsafe_path` — versioned with the agent contract, never
renamed, never repurposed. Twenty-two of them, listed in
[`docs/agent-errors.md`](../agent-errors.md).

**Alternative: numeric codes.** Rejected. A number has to be looked up to mean
anything, and the surface's primary reader is a model that sees the text before
any table. `capability_denied` is self-describing in the transcript;
`E1042` is not.

**Alternative: reuse the JSON-RPC error codes.** Rejected on the protocol's own
terms. Those describe the *call* — unknown method, bad params — and the
specification is explicit that a tool that failed to do its work reports a
result with `isError`, not a protocol error, precisely so the model sees it. A
refused capability is not a malformed request.

**Alternative: no taxonomy; improve the messages.** Rejected as the thing that
was already there. Better prose is still prose, and every improvement to it is a
breaking change to any client that learned to match on it.

The set is closed because a caller writing a branch per code needs the list to
be finite, and needs an unrecognized value to mean "a Ptah newer than the one I
was written against" rather than "another spelling of something I handle".
Two conditions that would lead a caller to do the same thing share a code
instead of splitting into a pair nobody can act on differently — which is why a
malformed database URL is `database_unreachable`: the driver reports it the same
way it reports a server that is down, and a distinction Ptah cannot measure
would be a code that lies half the time.

### 2.2 The code is assigned at the sentinel, not by a classifier

**Chosen.** A package declares its sentinel with its code:

```go
var ErrUnsafePath = agentdiag.Sentinel(agentdiag.CodeUnsafePath, "unsafe artifact path")
```

Every site that returns it, or wraps it with `%w`, carries the code without
naming it again. `errors.Is` still matches, because the sentinel is still the
same value.

**Alternative: one `Classify(err) Code` switch over every sentinel.** Rejected.
It has to live above all the packages it names, so it cannot be kept in step by
the compiler, and the failure mode is silent: a package gains a sentinel, the
switch does not, and the new error reaches clients as `internal` forever. The
same argument applies to a table keyed by error message.

The cost of the chosen shape is that `internal/agentdiag` must be a leaf — it
imports none of the agent packages, so each of them can import it. That is why
the remedy sentences are not in it (§2.5) and why classification is an interface
(`DiagnosticCode() Code`) rather than a type switch: `agentpolicy.DeniedError`
carries its own decision and joins the taxonomy by adding one method.

Errors arriving from outside the agent packages — a schema that will not load, a
database that will not dial — are coded where they cross into the contract,
which is `internal/agentapi`.

### 2.3 The code reaches the client twice

**Chosen.** In the result text, as a leading token, and in the result's `_meta`
under `ptah.run/diagnostic` as an object with `code`, `actor`, `retryable`,
`message` and `hint`.

Twice, because two different things read a tool result. Most clients hand the
model the content blocks and nothing else, so a code only in `_meta` would never
be seen by the consumer it was designed for. A code only in the text would have
to be parsed back out by a program, which is the prose-matching this ADR exists
to end.

**Alternative: put the diagnostic in `structuredContent`.** Rejected. That field
is where the tool's *answer* goes, described by the output schema the SDK
derives from the response type. A failure is not an instance of that schema, and
a client reading `structuredContent` before checking `isError` would find
something well-formed that is not the answer it asked for. `_meta` is the
protocol's designated place for a server to say something the schema does not
cover.

**Alternative: `_meta` only.** Rejected by the reader argument above.

### 2.4 `apply_patch` answers with both an error and a response

**Chosen.** When verification undoes a patch, the operation returns the response
*and* the error. The result carries `isError: true`, the coded text, and the
full `ApplyPatchResponse` in `structuredContent`.

Measured on this branch, the same call as §1.1 row three:

```json
{"code":"gate_failed","actor":"caller","retryable":false,
 "message":"verification gate failed: migration-sql: expected table name…",
 "hint":"The patch was undone. The response reports every diagnostic the patch
         introduced and the digest the artifact holds now."}
```

with `structuredContent` carrying `rolled_back`, `base_digest`, `result_digest`,
`introduced`, `resolved`, `baseline`, `verification` and `files` — thirteen
fields where there had been none.

**Alternative: summarize the diagnostics into the `_meta` object.** Rejected as
a second shape for something that already has one. The response type is what
`apply_patch` documents and what a successful call returns; a client should not
have to parse a different structure depending on whether the patch stood.

**Alternative: report a rolled-back apply as a success with a flag.** Rejected.
The patch did not apply. A surface that answered `isError: false` would leave
every client that checks only that field believing the write landed.

### 2.5 The remedy belongs to the surface

**Chosen.** `agentdiag` says what went wrong. The sentence naming what would
clear it — `ptah mcp --workspace . --allow-write=migrations` — is written in
`internal/mcpserver`, keyed by code.

An MCP operator and a Ptah Assist operator do not start the same process, so a
remedy in the shared taxonomy would be wrong for one of them. Keying it by code
rather than by sentinel is what keeps the remedies and the published table from
drifting apart.

### 2.6 An uncoded error at the wire is the caller's argument mistake

**Chosen.** Every error an operation produces is given a code — `internal` at
worst — on its way out of the tool handler. The middleware that annotates
results therefore knows that an error still carrying no code never reached an
operation, and the only thing that raises one there is the SDK refusing the
arguments against the tool's input schema. It is coded `invalid_request`.

**Alternative: match the SDK's message text.** Rejected: it is built with `%v`,
so there is no type to match and no wrapped error to unwrap, and matching
`validating "arguments"` would break on any SDK wording change.

**Alternative: leave it `internal`.** Rejected — that is §1.1's first row, which
tells an agent it found a defect in Ptah when it sent the wrong arguments.

## 3. Consequences

- **The agent contract version changes** to `2026-08-24`. A client that reads
  only the message text is unaffected; the text gained a prefix and lost
  nothing.
- **Adding a code is a contract change** and must be treated as one. Clients are
  documented to treat an unrecognized code the way they treat `internal`, and to
  branch on `actor` — a set that does not grow — when they want coarse handling.
- **A new sentinel that forgets its code is caught by a test, not by a review.**
  `internal/agentdiag` walks the source of every agent package, collects each
  package-level `Err…` it declares, and requires each one to be classified or
  named in an exemption list with its reason. The exemption list is empty today,
  and that is a claim rather than an omission.
- **The published table cannot drift from the code.** The same package's tests
  parse `docs/agent-errors.md` and compare every row — code, actor, retryable,
  meaning — against the taxonomy, in both directions.
- **`Retryable` is deliberately almost always false.** Three codes carry it:
  `database_unreachable`, `database_read_failed` and `write_failed`. A model
  told "retryable" calls again, so the flag is set only where waiting is
  genuinely the remedy.
- **A cost: two places render a failure.** The tool handler builds the result
  itself when the failing operation also has an answer, and middleware annotates
  everything else, because the SDK packs a returned error into a result that the
  handler never sees. Both call one function, and a test drives each path.
- **`agentdiag` must stay a leaf.** An import of an agent package from it would
  make every sentinel's declaration a cycle. This is the constraint that shapes
  §2.2 and §2.5, and it is worth stating because the pressure to break it will
  come from wanting the classifier to know more.
