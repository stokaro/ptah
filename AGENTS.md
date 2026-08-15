# AGENTS.md

This file gives coding agents repository-local guidance for working in Ptah.

## What Ptah Is

Ptah generates SQL DDL from annotated Go structs, compares a desired schema
against a live database, and plans, generates and applies migrations. It ships
two command-line binaries: the native `ptah`, and `ptah-compat`, a drop-in
replacement for the Atlas CLI.

`--dialect` accepts nine spellings: `postgres`, `mysql`, `mariadb`, `sqlite`,
`clickhouse`, `cockroachdb`, `yugabytedb`, `sqlserver` and `spanner`. A dialect
being accepted is not a promise that every construct renders on it. `SERIAL`,
for one, is refused by name on ClickHouse, CockroachDB and Spanner rather than
downgraded behind the author's back, which is the compatibility policy below
applied to dialects. [`docs/capabilities.md`](docs/capabilities.md) explains how
capability sets decide what a concrete target accepts.

`dbschema.ConnectToDatabase` dispatches on the URL scheme to five drivers: `pgx`
for the PostgreSQL family (`postgres`, `cockroachdb`, `yugabytedb`, `spanner`),
`mysql` for MySQL and MariaDB, and one each for ClickHouse, SQLite and SQL
Server. A scheme outside that set is refused rather than guessed at.

## Repository Layout

Ptah's public Go surface is a small part of the tree. Most implementation
packages sit under `internal/` and cannot be imported from another module, so
check where a package actually lives before writing an import path for it.

Public packages:

- `core/ast` — dialect-agnostic AST for SQL DDL.
- `core/goschema` — Go source parsing and entity extraction; the annotation
  parser is `core/goschema/parser.go`.
- `core/renderer` — dialect-specific SQL generation from the AST. The entry
  point is `core/renderer/renderer.go`; per-dialect code sits under
  `core/renderer/internal/dialects/`.
- `core/platform` — dialect names, normalization, and family predicates such as
  `IsPostgresFamily`.
- `dbschema` — connection management plus schema reading and writing against a
  live database.
- `migration/generator` — migration file generation from schema diffs.
- `migration/migrator` — migration execution with rollback.
- `migration/planner` — migration planning and SQL generation.
- `migration/schemadiff` — schema comparison; the entry point is
  `migration/schemadiff/schemadiff.go`.

Internal packages worth knowing, none of them importable from another module:

- `internal/lexer`, `internal/parser` and `internal/dialectlexer` — SQL
  tokenizer and DDL parser.
- `internal/astbuilder` — fluent builders for AST nodes.
- `internal/convert/...` — conversions between schema representations.
- `internal/dbschema/...` — the per-dialect readers and writers `dbschema`
  selects between.
- `internal/envbool` — the one grammar for boolean `PTAH_*` variables.
- `internal/capabilityprobe` — the declared database release lines and the
  probe that measures a live server against the preset each line claims.
- `internal/capmatrix` — the tiered pipeline built on that declaration: the CI
  fan-out, one cell's result, and the aggregation that fails when a declared
  cell reports nothing.

Command tree:

- `cmd/ptah/main.go` — native binary entry point; `cmd/root/root.go` assembles
  the tree.
- `cmd/schema`, `cmd/db`, `cmd/migrations`, `cmd/oci`, `cmd/seed`, `cmd/sql`,
  `cmd/viz`, `cmd/introspect`, `cmd/version` and `cmd/license` — the namespaces
  the root command registers. Each leaf verb keeps its own package below them:
  `cmd/generate` backs `ptah schema render`, `cmd/readdb` backs `ptah db read`,
  `cmd/migrateup` backs `ptah migrations up`, and so on.
- `cmd/atlas` — the Atlas-compatible tree, shipped by the separate
  `cmd/ptah-compat` binary.
- `cmd/integration-test` — the integration-suite runner binary.
- `cmd/ptah-ls` — the language-server binary.

Both command trees are adapters.
[Native And Compatibility Capability Ownership](#native-and-compatibility-capability-ownership)
is authoritative for the boundary between them and for the dependency direction
it implies.

Entities to test against: `stubs/` and `examples/` hold annotated Go entities,
and `integration/fixtures/entities/` holds the numbered fixture series the
integration suite migrates through.

## Schema Annotations

Ptah reads structured comments from Go structs. The directive prefix is
`//ptah:`:

```go
//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	//ptah:schema:index name="idx_products_name" fields="name"
	Name string
}
```

An index annotation has to sit on a struct field, because the walker visits
comments attached to fields. A `//ptah:schema:index` written at file level,
after the closing brace and attached to no declaration, contributes no index and
says nothing while doing it. To declare an index away from its column, give it a
holder struct and name the table:

```go
type ProductIndexes struct {
	//ptah:schema:index name="idx_products_name" fields="name" table="products"
	_ int
}
```

`fields=` is the modern spelling of the column list; `columns=` is accepted as a
legacy synonym. Unknown attribute names are rejected at parse time, so a typo
surfaces as an error rather than as a missing index.

## The Native CLI Surface

`ptah` groups its verbs into namespaces. There is no `ptah generate`,
`ptah compare`, `ptah read-db`, `ptah drop-all` or `ptah migrate`: each answers
`error: unknown command` and exits 2. Atlas spellings live only in
`ptah-compat`.

```bash
# Render desired schema SQL from Go entities
ptah schema render --root-dir ./models --dialect postgres

# Compare a desired schema with a live database
ptah schema compare --root-dir ./models --db-url postgres://user:pass@localhost/db

# Read the schema of a live database
ptah db read --db-url postgres://user:pass@localhost/db

# Drop every schema object (DANGEROUS — try --dry-run first)
ptah db drop-all --db-url postgres://user:pass@localhost/db --dry-run

# Generate migration files from schema differences
ptah migrations generate --root-dir ./models \
  --db-url postgres://user:pass@localhost/db \
  --migrations-dir ./migrations --name create_products

# Apply, inspect, and roll back
ptah migrations up --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations
ptah migrations status --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations
ptah migrations down --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations \
  --target 0 --confirm

# Build metadata
ptah version
```

`--dry-run` belongs to the commands that write, not to the CLI as a whole:
`migrations up`, `migrations down`, `db drop-all` and `schema apply` carry it,
while `db read` and `version` do not. Check `--help` rather than assuming.

Most flags also read a `PTAH_`-prefixed environment variable, printed as
`[env: PTAH_...]` on the flag's `--help` line; a flag without that marker, such
as `db drop-all --auto-approve`, has no environment binding. The boolean
variables are strict; see
[Boolean `PTAH_*` environment variables are strict](#boolean-ptah_-environment-variables-are-strict).

`ptah migrations generate` writes a reversible, timestamped pair per migration:
`<unix-seconds>_<name>.up.sql` and `<unix-seconds>_<name>.down.sql`. Schema
rendering is deterministic and dependency-aware: the renderer prints the table
creation order it derived from foreign keys, and two runs over the same entities
produce byte-identical output.

## Building And Testing

```bash
# Build the native CLI
go build -o bin/ptah ./cmd/ptah

# Build the integration-suite runner
go build -o bin/ptah-integration-test ./cmd/integration-test

# Build every binary: ptah, ptah-ls, ptah-compat, ptah-integration-test
make build

# Unit tests
go test ./... -count=1
make test

# List integration scenarios without running any of them
bin/ptah-integration-test list
```

The integration suite runs under Docker Compose through `make integration-test`
and its per-database variants such as `make integration-test-postgres`. Those
targets bind fixed host ports and `make docker-clean` prunes system-wide Docker
state, so look at what is already running before invoking either.

### The version matrix

Which database release lines Ptah covers as matrix cells is declared in exactly
one place, `internal/capabilityprobe/cells.go`. Exact measured-line identifiers
shared with the version resolver live in `internal/capabilityline`; the cell
slice references those identifiers instead of duplicating their spelling.

```bash
# What the pipeline fans out over, and which declared lines it cannot run
go run ./internal/cmd/capmatrix matrix

# Fail when a declared line has no capability preset
go run ./internal/cmd/capmatrix presets

# Probe one cell against a server already listening for it
go run ./internal/cmd/capmatrix probe --cell postgres-17

# Keep the documented matrix tied to the declaration
scripts/check-version-matrix.sh
scripts/check-version-matrix.sh --write
```

`.github/workflows/capability-matrix.yml` runs the capability probe once per
cell on every pull request, and `capability-matrix-nightly.yml` runs the
integration suite over the same cells on a schedule. Both read the declaration
through `capmatrix matrix`, so adding a release line is a data change: one
literal in `cells.go`, then `scripts/check-version-matrix.sh --write`.

Prefer `go test ./... -count=1` over `test-ptah.sh` for a local unit run. The
script is committed without an executable bit, so `./test-ptah.sh` cannot be
invoked directly, and it exports `POSTGRES_TEST_DSN`, `MYSQL_TEST_DSN` and
`MARIADB_TEST_DSN` unconditionally, which makes its `unit` mode depend on
databases listening on those exact ports.

### The Go toolchain

`go.mod` carries two Go versions and they are different facts with different
lifecycles. Do not collapse them.

- `go 1.26.5` is the published compatibility floor. `go.5x5.cz/ptah` and
  `go.5x5.cz/ptah/testkit` are separately released import paths, so raising this
  forces every consumer of both onto the newer language version. It moves on a
  human decision.
- `toolchain go1.26.6` is what CI builds and scans with. It moves on every patch
  release and Renovate's built-in gomod manager proposes those bumps without
  configuration. A dependency's `toolchain` line does not propagate to its
  consumers, so this one is free to move.

Every `actions/setup-go` step reads `go-version-file: go.mod`, which honors the
`toolchain` directive in preference to the `go` directive. Never write a
`go-version:` literal into a workflow, never restate the version in
`.golangci.yml` (its `run.go` already defaults to the go directive), and never
raise the `go` directive to clear a standard-library advisory — that is a
consumer contract break for a reason that has nothing to do with the language.
Raise `toolchain` instead.

A `${{ }}` expression is not an escape from that rule. It shows only that a
value is derived, never from what, so `go-version: ${{ env.GO_VERSION }}` in a
workflow is a literal declared a few lines higher. The single exemption is
`.github/actions/ptah/action.yml`, which forwards its own `inputs.go-version`
and `inputs.go-version-file` because a composite action runs in the **caller's**
workspace and must not be pinned to this repository's `go.mod`. What that
forwarding resolves to is pinned by the `go-version-file` input's default, which
has to exist and to name `go.mod`: the forwarded value is opaque, so that
default is the only place left where the module is named.

If you raise the `go` directive, raise `testkit/go.mod`'s in the same change.
`testkit` both requires and replaces the root module, so under `testkit/` it is
the **main** module and the root module is its dependency; Go requires a main
module's `go` directive to be at least every dependency's, and a root floor
above testkit's fails the testkit build outright with `go: module .. requires go
>= <root floor>`. The reverse — testkit deliberately ahead — is legal and is
testkit's own decision.

```bash
# Fail when the toolchain grows a second declaration
scripts/check-go-toolchain-single-source.sh
```

## Compatibility Policy

Ptah aims to be a drop-in replacement for the Atlas CLI. That goal has two
halves, and only stating the first one is how a capability gets thrown away.

**Never be looser.** A configuration or invocation the community binary refuses
must not succeed here. Accepting something it rejects means a user's mistake
passes silently on Ptah and fails somewhere later, which is the worst outcome
available. Where Ptah cannot yet implement a construct the community binary
enforces, refuse loudly rather than accept and ignore.

**Matching is the floor, not the ceiling. We do not copy defects.** Where the
community binary's behavior is a defect -- it silently drops something the
author wrote, corrupts state, or fails for a reason unrelated to what the user
asked for -- reproducing it is a wrong answer. Be the same or better. A change
whose only justification is "this is what the other implementation does" is not
justified when what it does is broken.

When the two halves pull apart, say so in the commit and in the issue rather
than picking silently. "We are stricter here, deliberately, and here is the
measurement" is a complete answer; quietly matching is not.

**Compatibility never removes a capability. Constitute it, do not discard it.**
Where Ptah models something the community binary does not -- an extension, a
sequence, a policy, anything the Pro surface covers or that Ptah does better --
reaching CE compatibility must never mean deleting that capability from the
compatibility surface. `ptah-compat` is the migration path for Atlas
**Pro** users' scripts too, not only CE users'; a capability reachable only
through native `ptah` does not help someone porting a Pro pipeline.

The shape that satisfies both:

- the normal compatibility surface keeps every implemented Atlas Pro-like and
  best-effort capability reachable. This is the default, because
  `ptah-compat` is also the migration path for those pipelines;
- `PTAH_ATLAS_STRICT_COMPAT=1` selects a separate Atlas CE-only policy for
  oracle and conformance runs. It constructs the CE command and flag tree
  before Cobra dispatch, refuses extension environment values, and rejects
  authored or inspected content whose semantics CE cannot represent instead of
  silently dropping it. A strict inspect, apply, or clean run refuses a live
  Pro-only object before output or mutation. Strict schema workflows also
  refuse YAML sources and an authored `schema apply` lint policy that their CE
  execution path cannot enforce. Commands that execute, convert, or replay
  migration bodies refuse Atlas txtar, Ptah directives, and SQL templates;
  checksum-only migration reads preserve those bytes. The default profile
  retains every extension;
- strict mode rejects the known `PTAH_<FLAG>` twins and Ptah feature toggles
  that would otherwise be ignored or restore an extension. It must not reject
  an arbitrary `PTAH_*` name merely because of its prefix: `atlas.hcl` may read
  ordinary user inputs through `getenv`, and those values are not product
  feature switches;
- the strict selector is an environment variable, never a new flag, because
  the conformance `cli-surface` tier asserts flag parity with the pinned binary
  and an environment variable is invisible to the help surface;
- what the default leaves out is reported, not dropped in silence, so an
  operator is never told less than the truth about their database;
- the capability is written down -- feature matrix row, user documentation, and
  a test -- so it is a product decision rather than an accident of which branch
  of an `if` ran.

"CE refuses it, so we stopped emitting it" is an incomplete answer. The complete
one names where the capability still lives.

### Boolean `PTAH_*` environment variables are strict

Absence selects the documented default; a present value must parse as a boolean
or the owning command refuses before doing work. **Never convert a boolean
environment parse error into the default value.**

The four states are distinguishable and stay that way. `os.Getenv` answers the
empty string for an absent variable and for `PTAH_X=` alike, which is how a typo
in a CI environment file, a container manifest or a systemd unit became a silent
default; use `os.LookupEnv`, and treat an exported empty value as the
configuration error it is.

In practice that means: declare the variable once with
`envbool.New(name, defaultValue, class)` in the package that owns it, resolve it
through `Var.Resolve`, and never write `strconv.ParseBool(os.Getenv(...))` at a
feature call site. `internal/envbool` holds the one grammar (exactly
`strconv.ParseBool`'s spellings, nothing trimmed) and the one error shape
(`invalid boolean value %q for %s`); `cmd/internal/envboolguard` refuses a new
tree that reintroduces the pattern.

`class` is the strict Atlas Community Edition classification, and it is stated
at the declaration because that is the only place that cannot drift from the
name:

- `envbool.Gated` — the variable adds behavior the pinned community binary does
  not have. Strict mode refuses an enabled value.
- `envbool.Retained` — the variable restores or tightens something that binary
  already does, so it adds no Atlas capability. Strict mode keeps it reachable.
- `envbool.Selector` — reserved for `PTAH_ATLAS_STRICT_COMPAT` itself.

Say in a comment at the declaration which capability the pinned binary does or
does not have; the class alone is an answer without its reasoning.
`internal/atlascompatpolicy` derives its refusals from `envbool.Registered()`, so
a variable is validated by the act of declaring it and there is no second list to
edit. A declaration that states no class fails closed — strict mode refuses it —
and `cmd/internal/envboolguard` refuses the tree, so an unclassified variable
cannot ship. Retained variables are also named in the configuration reference,
and a test requires that prose to match the registry.

Resolve the variables a command owns **before** its early returns. A malformed
value must not stay dormant because this invocation did not reach the branch
where the value would have mattered -- that branch is the one the operator
already knows they changed, and the runs that never reach it are the whole of a
healthy pipeline. Validate on every invocation of the command or subsystem that
recognizes the variable, and on no others: an invalid PostgreSQL-inspection
variable must not break an unrelated SQLite command.

Boolean feature toggles opt in to the more permissive side, so a typo lands on
the strict default and fails closed. `PTAH_ATLAS_STRICT_COMPAT` is the one
policy selector that intentionally opts in to a narrower surface; it still
defaults to the complete compatibility surface and malformed values fail
before help, version, argument handling, configuration, filesystem, or database
work. Do not add another restrictive boolean without documenting why it cannot
be expressed as a capability gate.

### Compatibility with older Ptah is a different axis, and it is not owed

Everything above is about the community binary. Compatibility with **Ptah's own
previous behavior is a separate question, and until Ptah ships v1 the answer is
no.** There is no supported upgrade path to preserve, so:

- Do not keep a fallback, an alias, a tolerated old spelling, or a second reader
  for a retired format only because an earlier Ptah produced it.
- Do not soften a refusal because it would break something an earlier Ptah
  accepted.
- Do not carry a default only because changing it would alter existing output.
  Pick the default that is right for a reader meeting it for the first time.

Changing behavior is the normal, cheap thing to do right now, and the cost of
not changing it compounds. When a change alters behavior, say so plainly in the
issue and the commit -- "this changes behavior; pre-v1, so no compatibility is
owed" -- rather than quietly designing around it.

This does **not** license breaking parity with the community binary, which is a
contract with users of that CLI rather than with Ptah's own history, nor
silently discarding user data. It licenses changing Ptah's defaults, spellings,
internal formats, and error text without a migration path.

The rule expires when Ptah reaches v1.

### A worked example

`-- atlas:txmode none` marks a migration that must run outside a transaction --
`CREATE INDEX CONCURRENTLY`, for instance. Measured on PostgreSQL 18:

| file shape | community binary | Ptah |
| --- | --- | --- |
| directive, blank line, statement | applies | applies |
| directive, statement immediately below | **fails** | applies |

The community binary requires a blank line after the directive and silently
drops it otherwise, so the statement runs inside the transaction it asked to
stay out of and the migration fails partway through. Ptah honored both forms.

A change once "fixed" this by adopting the blank-line requirement, on the
grounds that it matched. That traded a place where Ptah was better for a place
where it was merely identical, and it was reverted. The directive is honored in
both forms, and the divergence is documented rather than hidden.

### A second worked example

`file()` in an `atlas.hcl` inlines a file's contents into a config value.
Measured on the pinned community v1.3.0 build:

| argument | community binary | Ptah |
| --- | --- | --- |
| `file("local.txt")` | reads it, exit 0 | reads it, exit 0 |
| `file("/etc/passwd")` | **reads it, exit 0** | refused, exit 1 |
| `file("../../../../etc/passwd")` | **reads it, exit 0** | refused, exit 1 |
| `file("link.txt")`, a link out of the directory | **reads it, exit 0** | refused, exit 1 |

An `atlas.hcl` is repository-controlled and evaluated before anything is
applied, and the value lands somewhere observable: put the read in `env.url` and
the file's contents come back in `Error: sql/sqlclient: unknown driver "..."`.
Matching would turn config authorship into an arbitrary-file read on the machine
running the migration, which is the second half of the policy, not the first.
Ptah keeps the confinement on both binaries and names the rule in the refusal.
See [`stokaro/ptah#1042`](https://github.com/stokaro/ptah/issues/1042).

### Deciding which you are doing

Before matching a measured behavior, ask what it costs the user. If the answer
is "nothing, it is a different spelling of the same outcome", match it -- wording, exit codes,
flag names and output shape are worth being identical on, because tooling reads
them. If the answer is "they lose something they asked for", do not match it.

## Native And Compatibility Capability Ownership

`ptah-compat` is an adapter over Ptah capabilities, not an independent product
implementation.

When implementing behavior for `ptah-compat`, distinguish between:

1. a general semantic capability; and
2. Atlas-specific interface or compatibility machinery.

A general semantic capability must live in a reusable Ptah package below the
CLI layer and, where meaningful to a native Ptah user, must be reachable
through the native Ptah surface as well.

Do not implement general product behavior exclusively inside `cmd/atlas` or
another compatibility-only package.

The native surface does not need to reproduce Atlas command names, flags,
configuration syntax, output shape, URI spelling, or other interface
conventions. This is functional parity, not interface parity.

Compatibility-only adapters, parsers, codecs, persistence bridges,
diagnostics, and behavioral shims may remain compat-only when they exist
solely because of an Atlas contract.

Conversely, when native Ptah already implements a capability that has an
Atlas-compatible spelling, prefer adapting the compatibility surface to the
existing capability rather than implementing the behavior again.

CLI and compatibility packages should translate inputs and outputs, resolve
compatibility policy, and delegate semantic work to reusable
application/core packages.

The intended architecture:

```text
                         shared Ptah capabilities
                        /                        \
                       /                          \
             native Ptah surface          compatibility surface
                  `ptah`                    `ptah-compat`
```

### Which side of the boundary a change is on

These are general capabilities. They mean something without Atlas, so their
execution semantics belong in shared Ptah code with a native entry point, even
when the work that produced them was Atlas compatibility work:

```text
schema plan testing
migration testing
drift detection
schema security analysis
migration checkpoints
pre-apply checks
schema planning
schema validation
artifact publishing/fetching
migration-directory import
```

These are compatibility machinery. They exist to interpret, reproduce, or
bridge an Atlas interface or persisted representation, and they imply no native
user-facing spelling:

```text
atlas:// -> OCI resolution
Atlas CLI flag spelling and precedence
atlas.hcl compatibility parsing/evaluation
Atlas .plan.hcl codec
Atlas .test.hcl adapter
Atlas revision-table representation compatibility
Atlas checksum encoding compatibility
Atlas-compatible stdout/stderr rendering
Atlas exit-code compatibility
Atlas-specific refusal diagnostics
```

A codec feeding a shared capability is the intended shape, not a violation of
the rule:

```text
Atlas .test.hcl
      |
      v
compatibility parser
      |
      v
shared test model / runner
      |
      +--> ptah
      |
      +--> ptah-compat
```

### How this reads against the compatibility policy

The [compatibility policy](#compatibility-policy) and this rule run in opposite
directions, and neither one relaxes the other.

- The compatibility policy forbids the compatibility surface from losing a
  capability native Ptah models. A capability reachable only through `ptah` is
  no migration path for someone porting an Atlas pipeline.
- This rule forbids the native surface from losing a capability the
  compatibility surface gained. A capability reachable only through
  `ptah-compat` turns the compat tree into a second product.

They are one invariant read from two ends: neither binary is where a generally
useful capability lives, because both are adapters over the package that
implements it.

Three consequences are worth naming, because getting them backwards satisfies
this rule while breaking the older one:

- Exposing a capability natively means a **native** verb or flag. The
  compatibility surface still takes no new flag: the conformance `cli-surface`
  tier asserts flag parity with the pinned community binary, so the fuller
  behavior there stays behind a `PTAH_*` environment variable. Precedent:
  `PTAH_ALLOW_EXTERNAL_SCHEMA`.
- Reusing an existing native capability means the compatibility surface adapts
  to it. It never means narrowing the native capability to whatever the Atlas
  contract can express.
- One implementation does not force one behavior. Where the two surfaces
  deliberately diverge -- Atlas revision bookkeeping on one, recoverable
  failure state on the other -- the divergence belongs in the shared package as
  a policy the caller selects, not as a second implementation.

### Dependency direction

```text
cmd/ptah  --------------------\
                               \
                                > shared capability/application/core packages
                               /
cmd/atlas / ptah-compat ------/
```

- Native Ptah code must not depend on the compatibility command layer.
- Shared semantic packages must not depend on `cmd/atlas`.
- Atlas-specific codecs and adapters may depend on shared domain models and
  capabilities.

Conceptually:

```text
Atlas input/output contract
          |
          v
compat adapter / codec
          |
          v
shared Ptah capability
          ^
          |
native Ptah adapter
```

Today `cmd/ptah-compat/main.go` is the only non-test file outside `cmd/atlas`
that imports `cmd/atlas`; native command packages reference it from tests only.
Keep it that way.

### Classify the change in the PR

Every PR that adds or substantially extends behavior under the compatibility
surface says which of the two it is:

```text
GENERAL CAPABILITY
```

or:

```text
COMPATIBILITY ADAPTER
```

A general capability answers three questions in the PR description:

1. Where does the semantic implementation live?
2. How can native Ptah consume it?
3. If no native surface is added in the same PR, why is that reasonable, and
   what issue records the exposure gap?

A compatibility adapter names the external Atlas contract that makes it
compatibility-specific.

No GitHub PR template is required for this; `AGENTS.md` and normal PR
self-review are enough. The requirement is that the decision is made
deliberately rather than made for you by where a file happened to be placed.

### Scope of this rule

The rule is prospective. It governs work written from now on. It does not
require auditing every capability already implemented on either surface, and it
does not require refactoring packages that predate it. Add no further
divergence from here; existing architectural debt may remain for now. The
repository-wide audit belongs in a separate post-parity issue, and no such
issue is open yet. See
[`stokaro/ptah#1213`](https://github.com/stokaro/ptah/issues/1213).

## Language And Spelling

Use American English spelling in code, comments, documentation, issue/PR text,
and user-facing CLI output unless preserving an exact external quote or protocol
token. Prefer spellings such as `behavior`, `color`, `canceled`, `initialize`,
`normalize`, and `analyze`.

## Documentation Obligations

All documentation work must follow the authoritative style guide at
[`docs/STYLE_GUIDE.md`](docs/STYLE_GUIDE.md): classify the page type before
writing, use the matching template, keep the canonical terminology, and run
the style guide's review checklist. When a reader-facing page is added, moved,
merged, split, or retired, update the content inventory at
[`docs/site/CONTENT_INVENTORY.md`](docs/site/CONTENT_INVENTORY.md) in the same
PR.

CI enforces the mechanical half of that guide. Section 13 of the guide lists
exactly which rules fail a build and which stay a reading responsibility. Run
`node docs/site/scripts/check-style.mjs` for any documentation change: it needs
no npm install, and it governs this file, the repository docs, the examples,
the integration docs, and every package README — not only the site.

Before finishing any change that affects external behavior, inspect and update
the relevant documentation. Do this as a required verification step, not as an
opportunistic cleanup. Purely code-internal refactors that do not alter public
behavior, user-facing output, generated artifacts, supported inputs, or
operational workflows may skip documentation edits, but the self-review should
still confirm that the change is internal-only.

External behavior includes at least:

- CLI command names, command grouping, flags, environment variables, help
  output, output formats, and exit codes.
- Config file formats, accepted keys, validation behavior, environment
  selection, and precedence rules.
- Generated SQL, parsed SQL, migration file formats, migration directives,
  revision table behavior, hash files, and validation/repair semantics.
- Public Go package APIs and any documented extension points.
- Atlas-compatible behavior in the `ptah-compat` drop-in binary.
- Conformance status, supported/unsupported feature claims, known gaps, and
  documented limitations.
- User-facing errors, warnings, diagnostics, logs, safety checks, and failure
  behavior.

When a change touches any of those areas, build a documentation impact map and
search the relevant `.md` files before considering the task complete. Check at
least:

- `README.md`.
- `docs/README.md`, `docs/*.md`, and the task-oriented docs under
  `docs/site/src/content/docs/`.
- `examples/**/README.md` and generated example artifacts when examples change.
- `integration/*.md` and test-runner docs when test, fixture, or database
  behavior changes.
- Package-level READMEs such as `internal/parser/README.md`,
  `migration/generator/README.md`, and `migration/migrator/README.md` when the
  corresponding package behavior changes.
- `AGENTS.md` itself when agent workflow or project rules change.

Search for both old and new terms: command names, aliases, flag names,
environment variables, config keys, issue numbers, dialect names, conformance
gap names, generated labels, and exact error strings. Documentation must stay
aligned with canonical Ptah command paths. Atlas OSS command parity lives only
in the separate `ptah-compat <command> ...` drop-in binary at process root;
the native `ptah` binary has no Atlas command paths. Do not document
root-level Atlas aliases inside the native `ptah` binary such as
`ptah migrate apply`. Do not claim full Atlas parity unless the current
conformance evidence proves it.

For deep documentation maintenance, use the repo-local skill at
`.agents/skills/ptah-documentation-maintenance/SKILL.md`. It is Ptah-specific:
it routes CLI, config, migration, parser/renderer, conformance, public API, and
example changes to the right documentation surfaces and uses Inventario's docs
site as the quality reference.

## Code Style And Linting

Ptah treats `.golangci.yml` as a strict contract. Fix code to satisfy the configured linters instead of relaxing thresholds, disabling checks, or broadening exclusions. In particular, keep `revive` `error-strings` enabled and preserve the current "stricter wins" lint posture unless a maintainer explicitly asks for a config change.

Ptah is pre-GA. Do not preserve old command aliases, compatibility wrappers,
fallback APIs, or backward-compatibility behavior only to keep an older internal
shape. Prefer the cleaner architecture and update callers/tests/docs unless a
maintainer explicitly asks for a compatibility layer. This paragraph is the
[compatibility-with-older-Ptah rule](#compatibility-with-older-ptah-is-a-different-axis-and-it-is-not-owed)
applied to code shape; the rule itself is broader and covers defaults, output,
formats and error text as well.

Atlas OSS command parity belongs in the separate `ptah-compat` binary, the
Atlas-style root command surface for drop-in script migration. The `ptah`
binary is purely native. Do not add Atlas command spellings or temporary
aliases such as `ptah migrate apply` or a `ptah atlas` namespace to the `ptah`
binary; remove or redesign old native paths instead of preserving them.

The `modernize` linter is enabled. Prefer current Go idioms when writing or editing code:

- Use standard library helpers such as `slices.Contains`, `maps.Copy`, `strings.CutPrefix`, and `strings.SplitSeq` when they fit the code.
- Use `any` instead of `interface{}`.
- Do not add pointer helper packages or local `stringPtr`/`strPtr` helpers for new code; follow the idioms accepted by `modernize`.
- Use `fmt.Fprintf(&builder, ...)` rather than `builder.WriteString(fmt.Sprintf(...))`.
- Prefer clear early returns and simple control flow that satisfies `revive`, `gocognit`, `gocyclo`, `nestif`, and `funlen`.
- Keep import aliases compliant with `importas`; for example, `github.com/frankban/quicktest` must be imported as `qt`.
- Add `//nolint` only when necessary, always with a specific linter name and an explanation.

When applying automatic lint fixes, run both passes:

```bash
golangci-lint run --fix ./...
golangci-lint run ./...
```

The fix pass can leave second-pass fallout such as unused imports, removed helper functions, or staticcheck suggestions. Clean those manually before considering the lint run complete.

### Package Documentation

Every Go package must carry a package-level doc comment (`// Package <name>
...`), either atop a central file of the package or in a dedicated `doc.go`.
This is CI-enforced through staticcheck's `ST1000` in `.golangci.yml`; a PR
that introduces a new package must ship the comment in the same PR. The rule
applies to every module in the repository, including `testkit/`. `main`
packages describe their binary; test-only packages (`package foo_test`) are
exempt.

The comment must say in one to three sentences what the package does and where
it sits in the system, grounded in the package's actual code. Generic filler
such as "Package x contains x utilities" is not acceptable — the anti-slop
rules of [`docs/STYLE_GUIDE.md`](docs/STYLE_GUIDE.md) apply in spirit.

## Testing Standards

### Where Tests Live

- Unit tests are ordinary `*_test.go` files that need no server.
  `go test ./... -count=1` runs the whole set.
- Every integration test lives in its module's dedicated `integration/`
  package or one of its subpackages. The root module uses `integration/`; the
  separate testkit module uses `testkit/integration/`. Do not place a
  live-database or external-process test beside production code. A test that
  does not cross a process, filesystem, network, or database boundary is a unit
  or pipeline test and belongs beside the production package instead.
- Every integration test file uses `//go:build integration`, without exception.
  Build constraints apply to whole files, so split mixed unit/integration files.
  Test-only helpers used by integration tests carry the same tag. There is no
  `//go:build !integration` escape hatch: a test that does not require the tag
  does not belong in an integration tree, and `internal/testcontour` refuses it.
  An integration tree holds nothing but tagged test files — library code that
  integration tests happen to use lives outside it, beside its own untagged unit
  tests. `internal/integrationharness` and `internal/integrationfixture` are the
  worked examples.
- An integration test is never white-box. Every test file under
  `integration/**` and `testkit/integration/**` uses `package <name>_test` and
  exercises only exported APIs. `*_internal_test.go` and same-package tests are
  forbidden anywhere in those trees. If a behavior can only be reached through an
  unexported symbol, move the deterministic logic behind a package boundary and
  cover it with a black-box unit test outside the integration trees, or
  introduce the real public/application boundary the integration test should
  exercise. Never use white-box access as an integration shortcut.
- A missing database environment variable causes a test skip, and a skip reads
  as a pass to ordinary `go test`. CI therefore runs the complete recursive
  package contour through
  `go run ./internal/cmd/testcontour --tags integration`.
  The canonical root workflow also passes `--race`. The runner serializes
  packages, derives expected top-level tests from Go source, and fails on an
  empty contour, a missing or incomplete result, or any skipped test or
  subtest. It also scans the repository and rejects an
  integration-tagged test outside a dedicated integration tree. Do not add
  domain-specific build tags, package loops, package selectors, test names, or
  regular expressions to select integration tests. The CLI intentionally has
  no package-selection flag. The separate testkit module uses the same runner
  with `--dir ./testkit`.
- The Docker suite in `cmd/integration-test` covers apply, rollback,
  idempotency, parallel-execution smoke, partial-failure recovery, and schema
  diff, and writes reports in stdout, text, JSON, or HTML form into the
  directory `--output` names.

### Declarative Tests Only

All tests MUST be purely declarative. The following are prohibited in test
functions:

- `if` statements.
- `switch` statements.
- `goto` statements.

`for` loops are allowed in test functions for table-driven tests that iterate
over a static list of cases, and are not considered conditional logic for this
guideline. Keep loop bodies simple and do not use loops to encode branching
logic.

Go 1.22 and newer makes range variables per-iteration, so the historical
`test := test` workaround is not needed when using `c.Run()` closures in
table-driven tests unless intentionally taking the address of a loop variable.

Run the test-style baseline before finishing test changes:

```bash
scripts/check-test-style.sh
```

The baseline records existing violations while issue #541 is being cleaned up.
New tests must not add entries. Cleanup PRs that intentionally remove entries
should refresh the baseline with:

```bash
scripts/check-test-style.sh --write-baseline
```

Regenerate through the script, not through `go tool teststyle -write-baseline`
directly: the bare tool walks the filesystem and cannot tell a linked git
worktree parked under the repository from the repository itself, so it records
tests that are not in the working tree.

Never use `testify` in Ptah code, tests, examples, or documentation snippets.
Use `quicktest` imported as `qt`, the Go standard library `testing` package, or
existing project-specific test helpers instead. Existing transitive dependency
metadata from third-party packages is not permission to add direct
`github.com/stretchr/testify` imports or `assert`/`require` examples.

The prohibition is enforced by a `depguard` deny entry in `.golangci.yml`, so it
fires on the import declaration and is reported by `golangci-lint run ./...`
along with every other finding. It is not a text scan: a comment that ends a
sentence with the word `assert` or `require` is not a violation, and `pkg`
matches by prefix so every testify subpackage is covered by the one entry.

Bad:

```go
func TestDialectFromURL(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		rawURL  string
		want    string
		wantErr string
	}{
		{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
		{name: "unsupported", rawURL: "spanner://localhost/dev", wantErr: `unsupported --dev-url dialect "spanner://localhost/dev"`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			if test.wantErr != "" {
				c.Assert(err, qt.ErrorMatches, test.wantErr)
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
```

Good:

```go
func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestDialectFromURL_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("unsupported", func(c *qt.C) {
		got, err := atlasurl.DialectFromURL("spanner://localhost/dev")
		c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "spanner://localhost/dev"`)
		c.Assert(got, qt.Equals, "")
	})
}
```

### Do Not Hide Conditionals In Helpers

Avoid helper functions that mask conditional logic, such as choosing between
`qt.ErrorIs`, `qt.ErrorMatches`, and `qt.IsNil` based on fields in a test case.
This makes tests harder to read and review.

Instead, write explicit assertions per case, even when it is a bit repetitive.

Bad:

```go
func checkError(c *qt.C, err error, wantIs error, wantLike string) {
	if wantIs != nil {
		c.Check(err, qt.ErrorIs, wantIs)
		return
	}
	if wantLike != "" {
		c.Check(err, qt.ErrorMatches, wantLike)
		return
	}
	c.Check(err, qt.IsNil)
}
```

Good:

```go
c.Run("unsupported dev url dialect", func(c *qt.C) {
	got, err := atlasurl.DialectFromURL("spanner://localhost/dev")
	c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "spanner://localhost/dev"`)
	c.Assert(got, qt.Equals, "")
})

c.Run("postgres dev url", func(c *qt.C) {
	got, err := atlasurl.DialectFromURL("postgres://localhost/dev")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "postgres")
})
```

### Separate Happy-Path And Failure-Path Tests

Do not mix success and error cases in the same table. Prefer either:

- `TestXxx_HappyPath` and `TestXxx_FailurePath`.
- Separate `c.Run("happy ...")` and `c.Run("failure ...")` groups with distinct
  tables.

Bad:

```go
tests := []struct {
	name    string
	rawURL  string
	want    string
	wantErr string
}{
	{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
	{name: "unsupported", rawURL: "spanner://localhost/dev", wantErr: `unsupported --dev-url dialect "spanner://localhost/dev"`},
}

for _, test := range tests {
	c.Run(test.name, func(c *qt.C) {
		got, err := atlasurl.DialectFromURL(test.rawURL)
		if test.wantErr != "" {
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			return
		}
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, test.want)
	})
}
```

Good:

Use table-driven tests with `c.Run()` for multiple test cases:

```go
func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestDialectFromURL_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{
			name:    "unsupported",
			rawURL:  "spanner://localhost/dev",
			wantErr: `unsupported --dev-url dialect "spanner://localhost/dev"`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}
```

Error checking patterns:

```go
// Success case.
c.Assert(err, qt.IsNil)

// Preferred for sentinel errors because it handles wrapped errors.
c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidConfig)

// Error type checks.
var pathErr *os.PathError
c.Assert(err, qt.ErrorAs, &pathErr)

// Regex match when no sentinel is available.
c.Assert(err, qt.ErrorMatches, "failed to load schema.*")

// Substring check when matching part of the message is clearer.
c.Assert(err, qt.IsNotNil)
c.Assert(err.Error(), qt.Contains, "connection refused")
```

### Black-Box Testing By Default

By default, all Go tests use black-box testing:

- Test file: `*_test.go`.
- Package name: `package atlasurl_test` with the `_test` suffix.
- Test only exported API.

Bad:

```go
package atlasurl

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)
	got, err := DialectFromURL("postgres://localhost/dev")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "postgres")
}
```

Good:

```go
package atlasurl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasurl"
)

func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)
	got, err := atlasurl.DialectFromURL("postgres://localhost/dev")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "postgres")
}
```

### White-Box Testing As An Exception

White-box testing, meaning same-package tests with access to unexported symbols,
is permitted only for unit tests. An integration test is never white-box.

For unit tests, white-box testing is permitted only when:

1. Testing unexported functions critical for correctness.
2. Testing internal state that cannot be observed through exported API.
3. There is a clear technical justification.

Requirements for white-box tests:

- File naming: `*_internal_test.go`.
- Package name: `package parser` without the `_test` suffix.
- Include a `// White-box testing required:` comment as the first non-empty line
  after the `package` line explaining the justification.

Bad:

```go
package parser

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func Test_cursor(t *testing.T) {
	c := qt.New(t)
	cursor := newCursor("CREATE TABLE users (id BIGINT);")
	c.Assert(cursor.peek(), qt.Equals, "CREATE")
}
```

Good:

```go
package parser

// White-box testing required: this file verifies parser cursor invariants that
// are not observable through the exported Parse API without making assertions
// dependent on renderer output.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)
```
