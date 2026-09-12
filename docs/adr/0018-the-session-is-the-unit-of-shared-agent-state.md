# ADR 0018: The session is the unit of shared agent state, and Phase 9's deferred items are evaluated against it

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1492](https://github.com/stokaro/ptah/issues/1492), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Supersedes [ADR 0009](0009-remote-transport-authentication.md)

## 1. Why this record exists

ADR 0009 answered two of the seven items #1492 defers, and decided neither. It
also stated one claim about the code that measurement does not support, and that
claim is the whole argument under its open question 1. A record is superseded
rather than edited, so this one replaces it.

What changes: section 2 below replaces 0009's open question 1 with a
measurement, and section 4 evaluates the five items 0009 never reached. What
carries over unchanged: 0009's section 2 remains the description of the
transport seam, its section 2.4 remains the credential-precedent list, and its
section 3 remains the costed option set for authentication. This record
summarizes that option set in section 3 and does not restate its reasoning —
read 0009 for the costing.

## 2. What scopes the shared state

### 2.1 The measurement

`register` closes over `Config.Session`, so every tool handler on a server
answers through one `*agentapi.Session`. ADR 0009 read that as a property of a
server serving several connections. It is a property of the session, which is
wider, and `internal/mcpserver/shared_session_test.go` separates the three
candidate scopes by varying one at a time:

| what varies | what is held fixed | can the second caller spend the first one's preview token |
| --- | --- | --- |
| the connection | one server, one session | yes |
| the server | two servers, one `Config` and so one session | yes |
| the session | two sessions in one process | no |

The second row is the one that matters and the one ADR 0009 did not have. A
remote transport that built a server of its own would still share the token
namespace, the `maxLivePreviews` ceiling, the preview lifetime and
`Broker.granted` with every other server constructed from the same `Config`.
Building a second server is not a way to get a second scope.

### 2.2 What ADR 0009's open question 1 got wrong

That open question argued per-connection scoping is worth doing on its own
because "two concurrent `ptah assist` runs against one workspace share the same
four pieces of state today". They do not. Each run is a process that builds its
own session; `Broker.granted` is an in-memory map, `Session.previews` is a
`sync.Map`, and the live-preview count is a field. No path writes any of them
anywhere a second process could read.

So the sharing is real and unreachable. Nothing that ships makes a second
connection or a second server: `mcpserver.Run` hands one stdio transport to the
SDK's single-session convenience, and `ptah assist` connects one in-memory pair.
A remote transport would be the first caller to reach it.

### 2.3 What follows for the decision

The correction does not make the work optional, it moves when it is owed. The
honest form of the question is no longer "is this worth doing without a remote
transport" — measured, it buys nothing today — but "is this a precondition of
the first option that admits a second caller". It is, for every option in
section 3 except 3.1, and it is the larger piece of work in each.

Writing the scoping before a transport exists is therefore a choice about
sequencing and not about value. Doing it first costs a refactor nothing
currently exercises. Doing it with the transport risks shipping a surface where
one caller spends another's preview token, which is the failure the preview
token exists to prevent.

## 3. The authentication options, in one line each

ADR 0009 section 3 states what each buys and costs, and which precedent it
leans on. None is recommended, here or there.

- **3.1 Do not build it.** #1483's completion does not require a remote
  transport. The operator who wants Ptah on another machine runs it there.
- **3.2 A shared secret, one tenant.** Cheapest. Leaves open what an audit
  record means when every caller is the same principal.
- **3.3 The SDK's bearer-token middleware.** The pinned SDK already carries
  `auth.TokenInfo` and `RequireBearerToken`. The only Ptah-designed part is the
  verifier, and choosing a verifier is choosing an issuer.
- **3.4 Signed requests against `allowed_signers`.** Reuses the posture
  `planapproval` already ships. No off-the-shelf client speaks it.
- **3.5 Delegate to an identity provider.** Real multi-tenancy, at the cost
  `planapproval` was designed to avoid.

## 4. The five items ADR 0009 did not reach

Each is grounded in what the tree holds today, because an evaluation that
argues from how the code ought to look cannot be checked by the next reader.

### 4.1 Production apply approval — already shipped, natively

This item is not open. #1492 words it as "production apply approval bound to an
exact non-stale plan", and both halves exist on the native surface:

- `ptah schema plan` saves a plan carrying a source fingerprint;
- `ptah schema apply --plan` executes it after verifying that fingerprint, so a
  plan composed against a database that has since moved is refused rather than
  replayed;
- `ptah schema approve` records a detached SSH signature over the plan file and
  `ptah schema verify-approval` checks it against an allowed-signers list;
  `--require-approval` makes that a gate. `internal/cli/inference/approval.go`
  applies the same mechanism to inference state.

The signature covers the bytes, so a plan that changed at all is a plan nobody
approved, and the fingerprint covers the source state, so a plan that is stale
is refused before it runs. That is the item, and it needed no remote transport
and no identity provider to get there (stokaro/ptah#1857).

What remains is a naming question rather than a capability: the agent surface
applies patches to artifact files under a workspace and has no verb that
executes an approved plan against a production database. Whether that verb
should exist on the agent surface is section 4.2's question, not this one.

### 4.2 Hosted sessions — evaluated, and it contradicts a standing decision

A hosted session means Ptah keeps a session alive for a caller it does not
share a process with. Section 2 says what that costs: the session is the scope,
so hosting means either one session per tenant with a lifecycle Ptah now owns,
or a shared session with the token, ceiling and grant confusion measured above.

The alternative to hosting is the one the tree already takes: the operator runs
the process, and the state lives as long as it does. `planapproval` states the
reasoning in its package comment — the Cloud form of an approval needs an
identity and a service because approval is a claim about a person, Ptah has
neither and should not grow them.

Hosting a session is the same trade on a wider surface: it requires an identity
to attach the session to, a store to keep it in, and a revocation story. An
option that wants it should argue against that precedent explicitly rather than
arriving as a transport detail.

### 4.3 Organization policy — a layer question, and the hard part is precedence

`agentpolicy` assembles from four layers: `LayerBuiltin`, `LayerUser`,
`LayerInvocation` and `LayerProject`, and the rule that makes the stack
checkable is which layer may widen. `LayerProject` may only narrow, because the
repository under examination is the untrusted input.

An organization policy is a fifth layer, and the storage is the easy half.
The question that decides the design is where it sits against `LayerInvocation`:

- **Above it, narrowing only.** An organization can forbid a capability and no
  operator flag re-enables it. This matches `LayerProject`'s posture and is the
  only form in which the word policy means anything to an administrator.
- **Below it.** The operator's flags win, and the layer is a default rather
  than a policy. Cheaper, and honest about the fact that the operator controls
  the process and could edit any file the layer reads.

The second is what a local binary can actually enforce. An organization policy
that a local operator can overwrite is advisory, and the record that adopts one
should say which of the two it means, because the same feature name covers
both. `LayerUser` is declared and has no reader in production code, so the
stack has a slot for an operator-level layer already.

### 4.4 Remote workspaces — the digest contract is the whole question

`agentworkspace.Open` roots a workspace at a local directory, and its path
validation refuses traversal, drive letters and reserved device names. The
artifact contract above it is a content digest: a patch is composed against a
digest and refused when the directory has moved, which is what makes a lost
update impossible.

A remote workspace is therefore not a storage swap. It is a question about
whether the digest can still mean what it means:

- **A checkout, not a backend.** Ptah fetches to a local directory, works
  there, and pushes. Every existing guarantee holds unchanged, and the remote
  part is somebody else's tool. Cheapest by a wide margin.
- **A storage interface behind `Scope`.** Ptah reads and writes through an
  abstraction. Every digest, every rollback and every path rule has to be
  re-established against a store with different atomicity, and the verification
  that runs after each write has to run somewhere.

Nothing in #1492 requires the second, and the first is available without a
decision.

### 4.5 IDE-specific packaging — mostly not Ptah's to decide

Two programs already serve editors: `cmd/ptah-ls` speaks the language server
protocol, and `ptah mcp` serves the agent contract over stdio. An editor that
speaks either needs no package from Ptah.

So the item reduces to distribution, where the alternatives are a marketplace
extension per editor, maintained by whoever wants it, against a documented
configuration snippet per editor in this repository. The second is a page and
carries no release surface; the first multiplies the release matrix by the
number of editors and dates quickly. The repository already publishes binaries
and a container image, which is what an extension would wrap.

This is the one item where the evaluation has an obvious answer, and it is
"document the configuration, publish nothing new".

## 5. Decision

None on authentication: section 3 stands where ADR 0009 left it, and the open
question 2 that record states — whether a caller identity is wanted at all —
is unchanged and still the one that gates the rest.

Three findings are decided enough to act on:

- Production apply approval (4.1) is delivered. #1492 should stop counting it.
- Per-connection scoping (2.3) is a precondition of every option except 3.1,
  and buys nothing before one is chosen. It is sequencing, not value.
- IDE packaging (4.5) resolves to documentation.

`internal/mcpserver` keeps its stdio-only doc comment and the sentence that
names the reason. #1492 stays open on the remaining evaluation rather than on
anything being built.

## 6. Consequences

`internal/mcpserver/shared_session_test.go` pins section 2's measurement. It
passes today by recording that a second connection and a second server share a
session, so it is a characterization test: an implementation that scoped state
per connection would fail it, and that failure is the intended signal to come
back to this record rather than a regression.
