# ADR 0002: Read-only agent MVP scope, operation ownership, and transport

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1484](https://github.com/stokaro/ptah/issues/1484), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Supersedes: nothing
- Superseded in part: sections 1.1 and 1.2 by
  [ADR 0005](0005-agent-surface-inventory.md), which moves the inventory to a
  generated document and corrects the classification axis; section 2.3 by
  [ADR 0006](0006-one-authorized-agent-runtime.md), which puts the four read
  operations behind the capability broker instead of beside it. The frozen
  operation set, the naming rule and the transport decision stand.

## 1. Context

[#1483](https://github.com/stokaro/ptah/issues/1483) decides the architecture:
external AI clients reach a Ptah MCP server, the MCP server reaches a versioned
Ptah Agent API, and the Agent API reaches Ptah Core. Ptah Assist consumes the
same Agent API rather than a private path. That much is settled and this record
does not reopen it.

What is not settled is everything a first implementation needs to start:
which operations the read-only MVP exposes, which package owns each, and what
the server speaks over. [#1485](https://github.com/stokaro/ptah/issues/1485)
says so explicitly — it cannot begin until this is frozen, and it marks every
name in its own text as provisional until then.

Everything below was measured against the tree this record lands on.

### 1.1 The operation surface as it exists

Enumerated from the built binary rather than from the issue text:

| group | verbs |
| --- | --- |
| `schema` | annotations, apply, compare, diff, drift, export, fmt, inspect, lineage, plan, pull, push, render, serve, stats, test, validate |
| `db` | capabilities, drop-all, read |
| `migrations` | baseline, checkpoint, create, data, down, edit, generate, hash, import, lint, ls, plan, pull, push, rebase, repair, rm, set, show, status |
| `sql` | lint |
| `oci` | capabilities, copy, fetch, inspect, referrers, reindex, resolve, tag, tags, verify |
| other | introspect, seed, viz, version, license |

### 1.2 "Read-only" is two different things, and the difference matters here

The reading verbs do not form one class. Measured by which of them register
`--dev-url`:

| class | verbs | what it touches |
| --- | --- | --- |
| **pure reader** | `schema compare`, `drift`, `lineage`, `render`, `stats`, `validate`, `test`, `db read`, `db capabilities`, `migrations status`, `ls`, `show`, `sql lint` | reads the target; writes nothing anywhere |
| **needs a scratch database** | `schema inspect`, `schema diff`, `migrations lint` | reads the target, and requires a second database it **resets destructively** |

The second class is the finding that shapes this decision. `schema inspect
--help` states it plainly: the dev database "is reset destructively". A caller
that can name the dev URL can therefore destroy whatever it names, using an
operation whose name says "inspect".

For a human at a terminal that is a documented tool behavior. For a surface an
autonomous agent drives, it is a destructive capability wearing a read-only
label, and it is exactly the kind of thing a threat model exists to catch before
the first release rather than after.

### 1.3 There is no MCP dependency today

`go.mod` names none. Two candidate Go implementations exist, checked against the
module proxy on 2026-08-21:

| module | latest | published |
| --- | --- | --- |
| `github.com/modelcontextprotocol/go-sdk` | v1.7.0 | 2026-07-27 |
| `github.com/mark3labs/mcp-go` | v0.58.0 | 2026-08-11 |

## 2. Decision

### 2.1 The read-only MVP exposes the pure readers only

The first Agent API surface carries the pure-reader class of §1.2 and nothing
else. `schema inspect`, `schema diff` and `migrations lint` are **out of the
MVP**, despite reading rather than writing, because they cannot run without a
database they destroy.

They return in a later phase, under a rule stated now: **the dev database is
configured out of band and is never an agent-supplied parameter.** An operation
that takes a destination for destruction from its caller is not made safe by
documentation.

### 2.2 Operations are named for what they answer, not for the CLI verb

The Agent API contract is not a remote control for the command line. A verb name
carries flag history and CLI-shaped defaults that an API should not inherit, and
tying the two makes every future CLI rename a breaking API change.

The ownership map is one direction only: each agent operation names the package
that already implements it, and the operation adds no schema semantics of its
own. This is the #1483 invariant made checkable — if an operation cannot name an
existing owner, it is new behavior and does not belong in an adapter.

### 2.3 The transport is MCP over stdio, on the official SDK

`github.com/modelcontextprotocol/go-sdk`, for a reason that is not a preference:
it is at **v1**, which is a compatibility commitment, and it is published by the
protocol's authors, so protocol changes and SDK changes arrive together rather
than being tracked by a third party. `mark3labs/mcp-go` is at v0.58.0 — an
active project, and one whose major version says no compatibility is promised
yet. A schema tool that an agent drives against a production database is a bad
place to absorb breaking changes from a dependency.

Stdio only, in this phase. A remote transport is
[#1492](https://github.com/stokaro/ptah/issues/1492) and brings authentication,
which is a security surface this phase does not open.

## 3. Alternatives

**Expose every read verb, including the dev-database ones.** Rejected on §1.2:
it puts a destructive capability behind a read-only name on the surface most
likely to be driven without a human reading the flag documentation. It is also
the harder thing to withdraw later — an operation removed from an agent contract
breaks the clients that already call it.

**Expose the CLI directly and let the agent run commands.** Simplest to build,
and it is what a shell tool in an agent already does. Rejected because it makes
every flag an API, every text output a parse target, and the CLI unable to
change. The #1483 invariant also fails outright: Ptah Assist would then be a
second implementation path, not a consumer of one contract.

**Use `mark3labs/mcp-go`.** More mature in ecosystem terms today and widely
used. Rejected on the version alone: v0 promises nothing across releases, and
this dependency sits between an autonomous caller and a database.

**Write the protocol by hand.** No dependency, full control. Rejected: MCP is a
moving specification, and hand-rolling it means tracking it by hand forever, for
no gain over an SDK the protocol's authors maintain.

## 4. Consequences

**Accepted.** The MVP answers fewer questions than the CLI does. An agent cannot
ask for a machine-clean HCL inspection or a two-state diff in this phase, which
are two of the more useful things it might want, and the reason is not that they
are hard.

**Accepted.** Naming operations independently of CLI verbs means two vocabularies
to keep in step, and documentation that maps one to the other.

**Accepted.** A new dependency on the MCP SDK, measured rather than described.
Building a minimal stdio server against v1.7.0 pulls eight modules; four of them
-- `golang.org/x/oauth2`, `github.com/golang-jwt/jwt/v5`,
`github.com/segmentio/asm` and `github.com/google/go-cmp`, alongside `x/sync`
and `x/sys` -- this repository already carries. The net addition is **four**:

	github.com/modelcontextprotocol/go-sdk
	github.com/google/jsonschema-go
	github.com/segmentio/encoding
	github.com/yosida95/uritemplate/v3

Against 126 direct requirements and 392 modules in the current build, that is a
small addition rather than a large one. An earlier draft of this record called
the repository one that "vendors little", which the same measurement contradicts;
the claim is withdrawn rather than left standing, because a decision record that
argues from a flattering description of the tree cannot be checked.

Worth naming even so: OAuth2 and JWT enter the graph for a stdio-only server,
because the SDK carries its remote-transport authentication in the same module.
They are compiled and unused in this phase, and they become relevant when
stokaro/ptah#1492 opens that transport.

**Accepted, and worth stating.** Freezing a contract early means later phases
inherit these names. That is the point of freezing them, and it is also the cost:
a name that turns out wrong is a versioned change rather than an edit.

**Not decided here.** The permission model, artifact and provenance handling,
provider abstraction, session storage, and the evaluation corpus are the sibling
children of #1483. This record decides only what #1485 is blocked on, and says
so rather than pretending to a scope it did not research.
