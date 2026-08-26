# ADR 0009: What a remote MCP transport would have to decide before it could exist

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1492](https://github.com/stokaro/ptah/issues/1492), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Records the authentication and tenancy decision [ADR 0002](0002-read-only-agent-mvp-scope-and-transport.md) deferred

## 1. Context

ADR 0002 pinned the agent surface to stdio and said why: a remote transport
brings authentication, and that is a security surface a first release does not
open. The package still says so at the top of `internal/mcpserver`.

Two of the three things #1492 was waiting on have since arrived. The conformance
foundation it defers to is [#1490](https://github.com/stokaro/ptah/issues/1490),
now closed. The names it refused to build against are pinned:
`internal/mcpserver/testdata/agent-contract.json` holds every served tool with
its description and full schemas, and a test fails when the published surface
changes.

What remains is this record's subject, and it is not one decision but two that
are usually confused: **what a remote transport does to the process**, and **who
the caller is**. The first is answerable from the code as it stands. The second
is a choice.

This ADR does not make that choice. It states what each option costs against the
patterns this repository already trusts, so that whoever makes it is choosing
between costed alternatives rather than inventing one.

## 2. What is already true

### 2.1 The transport seam exists

`mcpserver.Run` hardcodes `&mcp.StdioTransport{}` and has exactly one caller,
`cmd/mcp/run.go`. No test calls it. Everything else — including `ptah assist` —
builds the server with `mcpserver.New` and connects it over a transport of its
own choosing; assist already uses an in-memory pair. **A remote transport is a
third caller of `New`, not a change to `New`.**

The SDK agrees: its own doc says `Server.Run` is "a convenience for servers that
handle a single session" and "need not be called on servers that are used for
multiple concurrent connections".

### 2.2 One session is shared by every connection

This is the structural fact, and it is independent of authentication.

`register` captures one `*agentapi.Session` in every tool handler by closure, so
a single `mcp.Server` serving several connections hands all of them the same
session. Four things follow, each measurable today:

| shared state | consequence for a second client |
| --- | --- |
| `Session.previews` | a preview token is unguessable and single-use but has no owner, so client B can spend client A's token |
| `maxLivePreviews = 32` | the ceiling is per process, so one client previewing in a loop denies previews to the rest |
| `previewLifetime = 15m` | its stated purpose is to bind an apply to a preview *a person saw*, which assumes one interactive operator |
| `Broker.granted` | "allow for this session" answered by one client answers for every other client on that broker |

The package already solved this once, for elicitation only, and says so: the
protocol request is threaded into the context "because the alternative — a
package-level session — would make two concurrent connections share one person's
answer". The same reasoning applies to the four rows above and has not been
applied to them.

**So any remote transport, under any authentication model, has to build a
distinct `Session` (or a distinct `Server`) per connection.** That is a
prerequisite rather than an option, and it is worth separating from the choice
below because it is the larger piece of work.

### 2.3 There is no caller identity anywhere

`Policy.Decide` is a map lookup on `{Capability, Artifact, Database}`. No
identity, principal or path takes part. `Request.TargetID` is explicitly
excluded from resolution — it scopes a grant to one database, and it is a
resource identity rather than a caller's.

Nothing carries an identity through a call: there is no `context.WithValue` in
`agentapi`, `agentpolicy`, `agentworkspace`, `agentaudit`, `agenttarget`,
`agentgate` or `agentpatch`. The audit record's only identity fields are a
per-process session id — documented as a grouping key and explicitly not a
secret — and a surface name.

One slot exists and is unused: `LayerUser` is declared in the policy layers and
has no reader in production code. An option that wants per-operator policy has
somewhere to put it.

### 2.4 What this repository already does about credentials

Four precedents, and an option that contradicts one should say so:

- **References, never literals.** `assistconfig` resolves `env:NAME` and
  `file:PATH` at request time and refuses a bare literal with an explanation. A
  `file:` reference refuses a group- or world-readable file, ssh-style.
- **No credential commands.** `assistconfig` refuses a token-helper hook on the
  grounds that it is arbitrary code execution driven by a config file. An option
  proposing one has to argue against this.
- **No password on a command line, ever** — `internal/ociartifact` states the
  reason: shell history and the process list.
- **Delegate rather than implement.** OCI auth uses Docker's credential store;
  `cloudtoken` mints short-lived credentials from an ambient provider chain and
  stores none; `planapproval` — the one identity-bearing mechanism already
  shipped — uses an OpenSSH `allowed_signers` file and delegates principal
  matching, revocation and validity windows to `ssh-keygen` rather than
  reimplementing them. It was chosen explicitly to avoid an identity provider
  and a service.

## 3. The options

Each is stated with what it buys, what it costs, and which precedent it leans on
or breaks. None is recommended here.

### 3.1 Do not build it

The issue itself says #1483's completion "does not require remote MCP", and
every name in it is provisional. The cost is that an operator wanting Ptah
served to a client on another machine has no answer but to run it there.

### 3.2 A shared secret, one tenant

A bearer token from a credential reference, one operator, no principals. The
policy stays identity-free; §2.2's per-connection scoping is still required, but
"who" is answered by "whoever holds the secret".

Leans on the credential-reference precedent exactly. Buys the least. The
question it leaves open is what an audit record means when every caller is the
same principal.

### 3.3 The SDK's bearer-token middleware

The pinned SDK v1.7.0 already ships `auth.TokenInfo` carrying a `UserID`, and
`RequireBearerToken` with a caller-supplied `TokenVerifier` plus RFC 9728
protected-resource metadata. Its docstring names session hijacking as the reason
the primitive exists.

So the verification is not Ptah's to design — only the verifier is. That makes
this the cheapest option that produces a real caller identity, and it is the
only one that puts something in `LayerUser` and in the audit record. The cost is
that choosing a verifier is choosing an issuer, which is the decision §3.5
describes.

### 3.4 Signed requests against `allowed_signers`

Reuse `planapproval`'s posture: the operator commits an OpenSSH allowed-signers
file, and a caller signs. No issuer, no service, no new secret store, and
revocation is `ssh-keygen`'s.

Consistent with the strongest precedent in the tree. The cost is that MCP has no
request-signing notion, so this is Ptah-specific transport framing that no
off-the-shelf client speaks — which is the opposite trade from §3.3.

### 3.5 Delegate to an identity provider

OIDC or mTLS terminated ahead of Ptah. Buys real multi-tenancy. Costs the thing
`planapproval` was designed to avoid, and makes Ptah's answer depend on
infrastructure it does not ship.

## 4. Decision

None yet. This record exists so the next person starts from §2, which is
measured, and chooses among §3 rather than deriving it again.

Two things should be decided together rather than separately:

**Open question 1.** Whether per-connection session scoping (§2.2) is worth
doing on its own. It is required by every option except §3.1, it is the larger
piece of work, and it is worth something without a remote transport: two
concurrent `ptah assist` runs against one workspace share the same four pieces
of state today.

**Open question 2.** Whether a caller identity is wanted at all. §3.2 says no
and is cheapest; §3.3, §3.4 and §3.5 say yes and differ in who issues it. The
answer decides whether `LayerUser` gets a reader and whether the audit record
grows a principal — both of which are contracts, not implementation details.

## 5. Consequences

Until this is decided, `internal/mcpserver` keeps its stdio-only doc comment and
the sentence that names the reason. #1492 stays open on this record rather than
on anything being built.

If §3.1 is chosen, this record is superseded by one that says so, and #1492
closes citing it — not as declined for scheduling, but as decided.
