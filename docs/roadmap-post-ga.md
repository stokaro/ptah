# Post-GA roadmap

This file records work that is deliberately **not** required for Ptah's general
availability release. It is the reader-facing half of the `post-ga` label on the
issue tracker: the label says an issue was deferred, and this page says on what
grounds.

Nothing here is closed, declined, or lower-quality work. Every entry keeps its
own issue and its own acceptance criteria.

## Evidence snapshot

This status was last verified on August 14, 2026, against repository commit
`37eda0642dbb35e410af6ad30c913ef551216619` and the open GitHub issue state on
that date. One entry has left the page since: ClickHouse roles and grants
([#1025](https://github.com/stokaro/ptah/issues/1025)) was implemented on
August 16, 2026, and a capability Ptah now has is not deferred work. To
reproduce the issue inventory, run:

```bash
gh issue list --repo stokaro/ptah --state open --label post-ga \
  --limit 1000 --json number,title,url,labels
```

Then follow each issue link below and compare its acceptance criteria and
evidence comments with the named repository commit. Refresh this page whenever
an entry's issue state or label changes, or when code or documentation changes a
support claim used by its rationale. The list is a dated classification, not a
live projection of the issue tracker.

## The test each entry had to fail

The README states what Ptah claims to be:

> Ptah is a schema and migration toolkit for Go projects. It can read annotated
> Go models, YAML schema files, supported HCL schema files, and live databases;
> render SQL; plan and run migrations; and validate migration hashes. A separate
> `ptah-compat` binary is a drop-in replacement for the Atlas CLI.

Every open issue was measured against one question: **does Ptah, as it
advertises itself today, do something wrong or misleading without this?**

- An unkept promise, a misleading diagnostic, a capability the documentation
  claims and the binary lacks, or a violated compatibility rule stays in the
  release scope.
- A capability nothing currently claims lands here. An operator who never had it
  is not surprised by its absence.

Two entries show where the line falls. The query builder's limit is published in
the same cell that describes the capability — "SQL Server and ClickHouse error"
on the feature matrix — so a reader meets the limitation where they meet the
feature, and closing it adds reach rather than correcting a claim. TimescaleDB
hypertables appear nowhere in the tree and nowhere in the documentation, so
nothing Ptah does today is wrong without them.

The reverse also held. Some issues filed as engine-feature requests turned out
to describe output that is already wrong, and those stayed in the release scope
rather than landing here.

## Engine capabilities nothing currently claims

Each entry adds coverage for an engine feature Ptah does not model. In every
case the current behavior is either absence or an explicit refusal, not a wrong
answer.

**[#1026 — TimescaleDB hypertables and continuous aggregates](https://github.com/stokaro/ptah/issues/1026).**
No file in the repository mentions TimescaleDB, and no documentation page offers
it as a target. Ptah connects to TimescaleDB through the PostgreSQL path and
manages the ordinary PostgreSQL objects it finds. This becomes required the day
any page lists TimescaleDB as a supported engine.

**[#1027 — CockroachDB row-level TTL](https://github.com/stokaro/ptah/issues/1027).**
CockroachDB is a first-class dialect with live coverage, but the TTL table
attributes are outside the modeled subset and no page claims otherwise. The
argument for promoting it is false convergence: a diff that reports zero changes
while a table's data-lifecycle policy differs. That argument becomes concrete
once TTL round-trip appears in any support claim.

**[#1029 — PostgreSQL transaction-pooling proxies](https://github.com/stokaro/ptah/issues/1029).**
PgBouncer appears in the tree only as an ordinary role identifier; it is not a
reserved role and does not constitute support for the proxy topology. No
documentation describes a supported pooling topology, so there is no contract
to break. It becomes required when a page states that Ptah is safe through a
transaction pooler, or when an operator reports silent lock loss behind one.

**[#1030 — SQL Server synonyms](https://github.com/stokaro/ptah/issues/1030).**
Synonyms are absent from the schema model. The dev-database cleanup guard
already recognizes the object kind, so the dangerous case — cleanup ordering
around a synonym — is guarded rather than mismanaged. Modeling them is new
coverage.

**[#1031 — SQL Server extended properties](https://github.com/stokaro/ptah/issues/1031).**
The SQL Server reader consumes `MS_Description` as a comment and the cleanup
guard sees database-scoped properties as artifacts, but extended properties are
not a schema object Ptah manages. No page says they are.

**[#1032 — Demand-driven roadmap](https://github.com/stokaro/ptah/issues/1032).**
The register that tracks the five entries above against their originating
reports. It follows its children; it cannot be required while none of them is.

**[#941 — Query builder dialect coverage](https://github.com/stokaro/ptah/issues/941).**
Measured today, `SELECT`, `INSERT`, `UPDATE` and `DELETE` all refuse for SQL
Server and ClickHouse, and the Spanner third of this issue is closed. The
feature-matrix row states exactly that, so the reader meets the boundary where
they meet the capability. Widening it is reach, not repair.

## Exported API contracts

**[#904 — Field-level projections for API schema export](https://github.com/stokaro/ptah/issues/904)
is implemented.** `api_expose` selects columns and their direction, and
`--api-field-policy=allowlist` makes an undeclared column reach no contract, so
an additive migration cannot widen a published one on its own. The controls
shape a generated document and are not access control; the export page says so
where a reader meets them.

## Atlas surface beyond the community binary

The drop-in promise is measured against the pinned Atlas community binary. Each
entry here concerns a surface that binary does not implement, so no drop-in
obligation is outstanding.

**[#1017 — `atlas script` command group](https://github.com/stokaro/ptah/issues/1017)** and
**[#1018 — `atlas cloud` command group](https://github.com/stokaro/ptah/issues/1018).**
Neither verb is registered in the community binary, and `ptah-compat` answers
exactly as that binary does. There is no parity gap to close. `atlas cloud` in
particular is a client for a hosted registry rather than a database capability,
so it would become required only if Ptah grew its own registry concept and chose
to match the spelling.

**[#1209 — Continuing an Atlas-written Flyway revision history](https://github.com/stokaro/ptah/issues/1209).**
Ptah refuses a revision history it cannot validate, which is the safe answer, and
the refusal is honest. Accepting one is an interoperability feature. It becomes
required if the documentation ever offers a hand-over path it does not implement.

**[#1210 — `atlas://` references over the OCI backend](https://github.com/stokaro/ptah/issues/1210).**
Measured on both spellings: `atlas://` fails closed with no network call, and
`schema inspect` names the scheme and points the operator at `oci://`. Adding
the resolver removes rewriting work from an adopting project; it does not correct
an answer. One wording follow-up is worth carrying into the work — the
`migrate --dir` refusal is generic where the `schema inspect` one is specific.

**[#1211 — `schema plan test`](https://github.com/stokaro/ptah/issues/1211).**
Registered as a boundary stub that refuses by name, with no flags silently
accepted and no test file parsed and ignored. The machinery it would reuse now
exists, which makes it tractable rather than required.

## Post-parity architecture and new products

Four of these issues carry their own timing conditions in their bodies, stating
that implementation must not begin until the compatibility campaign stabilizes.
Labeling them records a decision their authors already made.

**[#1214 — Unify native and compatibility capabilities over one core](https://github.com/stokaro/ptah/issues/1214)**
and **[#1215 — Atlas project HCL as a native Ptah subset](https://github.com/stokaro/ptah/issues/1215).**
Both open with an explicit instruction not to start until Atlas compatibility has
stabilized, on the reasoning that restructuring the architecture while the
capability surface is still moving repeats the work. They are consolidation
passes over a boundary that is still being discovered.

**[#1229 — Self-hosted control plane and web UI](https://github.com/stokaro/ptah/issues/1229).**
The epic states its own hard prerequisite: do not divert capacity from
compatibility, interoperability, or native capability work to start it. The CLI
and engine must stay fully usable without it, so nothing about Ptah today is
incomplete in its absence.

**[#1270 — Schema-aware SQL and procedural code analysis](https://github.com/stokaro/ptah/issues/1270).**
Titled post-GA and gated behind six preconditions, including a stable schema
model for database-resident programmable objects. Already carried the label.

**[#852 — Kubernetes-native schema reconciler](https://github.com/stokaro/ptah/issues/852).**
A research issue whose expected result is an architectural proposal and a limited
proof of concept. The maintainer's own note on it says the result most likely
belongs in a separate repository. Already carried the label.

**[#1365 — A SQL linter for Go source built on Ptah](https://github.com/stokaro/ptah/issues/1365).**
A proposal for a separate repository that consumes Ptah as a published
dependency. It proposes no code for this tree, so it cannot gate this tree's
release. Already carried the label.

**[#1143 — Complete the OCI artifact lifecycle](https://github.com/stokaro/ptah/issues/1143).**
The issue opens by scoping itself to what does not exist: `ptah oci` registers
one verb, and there is no `resolve`, `tag`, `copy`, or promotion path. Publishing
and consuming artifacts both work; what is missing is a namespace that owns an
artifact between the two.

**[#1035 — Schema security analysis](https://github.com/stokaro/ptah/issues/1035).**
A new rule family over inspected state rather than over a migration diff, plus a
new entry point. It also needs a severity-vocabulary decision that changes public
API, which is a reason to design it deliberately rather than quickly.

**[#1230 — Capability-driven version support](https://github.com/stokaro/ptah/issues/1230).**
The certification half is already built: a generated version matrix covering 26
declared release lines, a live capability probe, and tiered workflows. What
remains is the capability-model half — support-level vocabulary, a server
profile type, and non-boolean capability values — none of which any page
currently claims.

## Migration engine depth

**[#996 — Typed migration metadata and transaction preflight](https://github.com/stokaro/ptah/issues/996).**
Several acceptance criteria are already met. The remainder is a preflight that
would classify an invalid transactional mix before execution instead of letting
the engine reject it. Today those cases fail loudly with the database's own
error, which is unhelpful rather than misleading, and a directive written below
executable SQL is dropped with a warning naming the line and the remedy.

## Documentation and presentation

**[#493 — Stronger logo options](https://github.com/stokaro/ptah/issues/493).**
The current mark reads as an ambiguous letter. Nothing about Ptah's behavior
depends on it.

**[#946 — Feature matrix at phone widths](https://github.com/stokaro/ptah/issues/946).**
Five cells exceed the desktop height cap when rendered at 390px. The site's
responsive gate measures both widths for overflow and applies the cell-height cap
at desktop only, and says so in a comment. The issue asks for a layout decision
among three options, each with a different cost.

**[#1228 — Restructure the documentation around general schema management](https://github.com/stokaro/ptah/issues/1228).**
This changes the claim rather than keeping it. The README describes a toolkit for
Go projects, and the documentation presents Ptah consistently with that. Moving
to a language-neutral onboarding path is a positioning decision, and the issue's
own metadata note records that nothing misbehaves today.

**[#1454 — Inline glossary tooltips](https://github.com/stokaro/ptah/issues/1454).**
A new site pattern for dense matrices, where a cell is one or two characters and
its vocabulary is explained in prose elsewhere on the page. Worth doing, and
constrained by a responsive gate that a tooltip can fail.
