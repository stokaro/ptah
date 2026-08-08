---
title: Public Go API
description: Stable embedder packages and API compatibility guardrails.
---

Ptah is pre-GA, but embedders need a documented import surface. The packages on
this page are the stable embedder API. Packages not listed here are command
packages, examples, fixtures, tests, or implementation details.

## Stable packages

| Package | Purpose |
| --- | --- |
| `atlascompat` | Stable wrappers for Atlas-compatible schema, SQL, and migration-sum behavior. |
| `config` | Project-level config loading helpers. |
| `config/projectconfig` | Typed Ptah/Atlas project config IR, including validated online-DDL policy. |
| `core/ast` | Typed schema DDL AST nodes. |
| `core/goschema` | Go annotation parser and schema IR. |
| `core/platform` | Dialect and platform constants. |
| `core/platform/capability` | Capability flags for dialect/version behavior. |
| `core/platform/identifier` | Catalog identifier comparison and namespace semantics. |
| `core/ptaherr` | Typed public errors and sentinel errors. |
| `core/query` | Fluent builder for parameterized, dialect-aware SELECT statements. |
| `core/renderer` | Dialect-aware SQL rendering from AST/schema IR, including fail-closed two-phase foreign key ordering. |
| `core/sqlutil` | SQL utility helpers used by public paths. |
| `dbschema` | Live database schema introspection connection layer. |
| `dbschema/types` | Shared database schema types. |
| `migration/dbtest` | Declarative migration/schema test cases, runners, and reports. |
| `migration/generator` | Migration file generation. |
| `migration/lint` | Migration SQL linting rules, immutable analysis snapshots, and findings. |
| `migration/migrator` | Migration providers, revision metadata, dry-run plans, and execution. |
| `migration/planner` | Schema change planning. |
| `migration/risk` | Migration risk classification. |
| `migration/safety` | Destructive-change assessment and safety reports. |
| `migration/schemadiff` | Desired/live schema diffing. |
| `migration/schemadiff/types` | Shared schema-diff types. |
| `migration/seeder` | Seed discovery and execution. |

Import paths use the module prefix:

```go
import "go.5x5.cz/ptah/core/renderer"
```

The `migration/dbtest` package is the embeddable engine behind the native test
commands, including regular-expression case selection through `FilterCases`.
See [Test migrations and schemas](../../testing/migrations-and-schema/) for
its case model and [Database test commands](../../reference/test-cases/) for CLI behavior.

`migration/lint.ValidateOptions` checks rule definitions, configured selectors,
severity and path overrides, compatibility mode, and migration-directory format
without reading migration files. Host applications that can skip analysis on a
no-work or explicit-override path should validate first so policy errors cannot
bypass the gate.

`projectconfig.ParseAtlasFSWithOptions` evaluates `atlas.hcl` against a
caller-provided `fs.FS`. Use it when project config and its `file()` or
`fileset()` inputs must come from one anchored or immutable filesystem view.
`projectconfig.Config.IgnoredConstructs` carries every Atlas CE-compatible
no-op name with its kind and source location, and `projectconfig.Merge`
preserves the collection. Ptah's CLI reports each entry; embedders decide how
to expose the same metadata.

`renderer.ValidateSchema` and `renderer.ValidateSchemaWithCapabilities` check
a complete `goschema.Database` without rendering SQL. They use the same
foreign-key and capability validation as ordered schema rendering and migration
planning.

`goschema.Finalize` can be called again after mutating schema input. It rebuilds
materialized embedded fields and marks them with
`Field.GeneratedFromEmbedded`; source declarations should leave that derived
metadata false.

`Database.EmbeddedSources` preserves source-only field and embedding
declarations needed to rebuild nested embedded fields after `Finalize` or
`Merge`. Embedders normally should not modify this bookkeeping directly. Keep
it when copying a finalized `Database` that will be finalized or merged again;
discarding it can also discard the source declarations behind materialized
`GeneratedFromEmbedded` fields.

The separate [`testkit`](https://github.com/stokaro/ptah/tree/master/testkit)
module (`go.5x5.cz/ptah/testkit`) is an opt-in helper for tests that
need real databases. It keeps `testcontainers-go` out of Ptah's main module
graph and versions independently.

## Migration statement observation

`migration/migrator.WithStatementObserver` attaches a read-only callback to a
filesystem migration provider. The observer runs after every successfully
executed statement and receives its source path, one-based statement ordinal,
total statement count, SQL text, and an event-local copy of file directives.

Use `migrator.StatementObserverFunc` for a closure or implement
`migrator.StatementObserver` for a stateful collector. The callback receives
no database connection and cannot alter the migrator execution path. A
database-aware collector may capture a consumer-owned connection when that
consumer controls transaction visibility. Returning an error stops the
migration and returns a `migrator.StatementObservationError` with source and
statement context; dirty progress includes the statement that completed before
the callback failed.

For SQL-backed `no_transaction` migrations, Ptah writes a durable progress
checkpoint before invoking the observer. Before each statement, it first marks
that statement's outcome as unknown; after success, it advances the completed
count and clears the marker. Process exit, context cancellation, or deadline
while execution is in flight preserves the unknown-outcome marker. This
includes Atlas-format down execution. A custom `MigrationFunc` is opaque to the
migrator and does not receive statement-level checkpointing.

Dirty SQL-backed resumes verify the already committed source prefix before
skipping it. Native rows use the `partial:h1:` value in `Checksum`; Atlas rows
use cumulative `partial_hashes`. A failure after changing transaction mode
cannot reduce the recorded applied count below that verified prefix.

Negative `applied` or `total` values and `applied > total` are rejected whenever
a revision is read, including through `GetRevisions`, `GetAppliedRevisions`,
`GetAppliedMigrations`, `GetCurrentVersion`, and `GetMigrationStatus`. Native
rows accept only `applied`, `pending`, `failed`, `pending:down`, and
`failed:down`. An applied row cannot claim that state until `applied == total`;
other spellings, explicit `:up` suffixes, and direction-suffixed applied states
are invalid because a completed rollback deletes its revision row.

`RepairMigration` holds the session advisory lock across revision inspection,
resumed SQL, safety checks, and the final metadata write.

SQL-backed `MigrationTxModeNone` attempts pin their migration SQL to one
physical database session. Server-database revision metadata remains on the
original connection; SQLite uses the pinned session with a `main`-qualified
table to support its single-connection in-memory mode. Resume replays recognized
session controls from the verified committed prefix on a fresh session and
refuses prefixes whose session-local state cannot be reconstructed safely.
Top-level transaction-control statements are rejected before session pinning or
revision mutation because their commit boundary would conflict with Ptah's
durable per-statement checkpoints.

On PostgreSQL, an up migration may clean invalid index residue with a matching
`DROP INDEX` that executes before the create in the current attempt. The
migrator resolves unqualified drops and target tables through `search_path`,
rejects any other relation that owns the schema-level index name, and rechecks
transaction-local catalog state before writing a clean revision. It records the
resolved schema and target at each conditional create, rather than resolving a
deduplicated raw name under the final `search_path`. Repair without an explicit
replayable path checks every same-named target in PostgreSQL user schemas. A drop skipped
by resume does not satisfy the preflight. `RepairMigration` performs the same
positive index-state check, including when `Force` is set.

The observer composes with `StatementInterceptor`: a statement handled by an
external executor is observed once after that executor reports success.

Programmatic migrations set `Migration.UpTxMode` and `Migration.DownTxMode`
with `MigrationFileTxModeUnspecified`, `MigrationFileTxModeFile`, or
`MigrationFileTxModeNone`. Use `ParseMigrationUp` when a tool needs the
executable up-direction SQL, explicit mode, and source-line offset from plain
SQL or Atlas txtar content. Up and down values remain independent.

Atlas transaction-mode directive validation errors expose
`migrator.AtlasTxModeDirectiveError` through `errors.As`; the leaf error keeps
the source file and transaction-mode details in its message.

This pre-GA API replaces the former Boolean transaction fields. Use
`NewMigrationFromSQLFiles` or its interceptor variant to load an up/down pair:
they return a complete `Migration` with transaction modes, timeouts, source
paths, and functions attached, so execution policy cannot be discarded while
assembling a registered provider.

`MigrateUpOptions.PlanObserver` receives the plan recalculated under the
migration lock before transaction-mode validation, including an empty plan. It
captures metadata but cannot abort execution. Use the abort-capable `Preflight`
hook for work that must run after static validation and before any schema or
revision change.

## Pinned database sessions

`dbschema.DatabaseConnection.WithSession` pins one physical database session
for the duration of a callback and rebinds the dialect reader, writer, and SQL
runner to that session. Use it for cleanup, replay, and inspection workflows
that depend on session-local state or SQLite attached-database visibility.

Root MySQL capability metadata remains conservative. On MySQL 8.4+ the scoped
connection refines its referenced-key policy from
`restrict_fk_on_non_standard_key` on the pinned physical session before the
callback, so planning and execution use the same effective policy.

The scoped connection must not escape the callback. Ptah discards the physical
connection afterward so session-local state cannot leak to a later pool user.

`dbschema.DatabaseConnection.WithIsolatedQuerySession` exposes a query-only
`dbschema.IsolatedQueryer` on one physical session. Transaction-capable drivers
always roll the transaction back; ClickHouse runs directly on the disposable
session because its driver does not implement transactions. Ptah discards the
physical session afterward, except for in-memory SQLite, whose only connection
owns the database lifetime and is returned to the pool after rollback. The
callback cannot control transactions or reach Ptah schema writers. Callers
remain responsible for restricting SQL to read-only queries.

`migration/migrator.CheckFailedError` identifies one failed or invalid
pre-migration assertion. `CheckGroupFailedError` identifies an Atlas `oneof`
check file in which no assertion returned a truthy result, including an empty
group. Callers can distinguish a group-level precondition failure from an
assertion execution or result-shape failure with `errors.As`.

`migration/migrator.ParseChecks` requires the target dialect together with the
SQL source. This intentional pre-v1 signature change prevents fail-open parsing
when PostgreSQL escape strings or MySQL/MariaDB comment rules determine whether
a later check directive is SQL code or literal/comment content.

## Migration statement validation

`migration/migrator.WithStatementValidator` attaches a pre-execution SQL
safety gate to a filesystem provider. Ptah splits and validates every
statement in one migration before executing its first statement. Rejecting a
later statement cannot leave an earlier statement applied.

Implement `migrator.StatementValidator` when an embedder must confine replay to
a disposable database or reject unsupported statement forms. Validators
inspect SQL but do not replace execution. Combine a validator with
`StatementInterceptor` only when an external tool must execute accepted
statements.

## Schema diff and planning contracts

`migration/schemadiff/types.SchemaDiff` stores index additions and removals as
canonical `[]IndexRef` fields. Every index reference includes its owning
table. Live comparisons snapshot catalog identifier semantics into the diff so
comparison, policy, forward planning, and reverse planning share one source of
truth.

Use `migration/generator.GenerateCheckpointWithDatabase` for a SQL Server
schema whose live catalog semantics must survive checkpoint planning.
`GenerateCheckpointWithDatabaseInfo` accepts a caller-supplied complete
identifier snapshot; the dialect-only checkpoint helper uses conservative
offline rules.

`migration/generator.PlanMigration` returns an unpublished plan bound to the
migration-directory snapshot used during planning. `MigrationPlan.WriteFiles`
rejects changed history with `generator.ErrMigrationDirectoryChanged` under
the shared cross-process publication lock.

Embedders that need cancellation while waiting for that lock use
`WriteFilesContext`; concurrent use of one plan fails with
`generator.ErrMigrationPlanInUse`. `migration/planner.Planner` exposes only
checked planning; malformed references, unresolved additions, and target
index-namespace conflicts fail before SQL is returned. The returned
`generator.MigrationFiles.Files` slice is the authoritative list of generated
pairs and published paths, in apply order.

## Safety reports and shadow errors

`migration/safety.RenderJSON` writes a `safety.Report` with the highest risk,
the destructive verdict, and every rendered statement assessment. Use this
API when an embedder needs the same machine-readable contract as
`ptah migrations plan --report json`. Setting
`generator.GenerateMigrationOptions.ReportFormat` to `json` instead publishes
one `.safety.json` artifact beside each generated migration pair.

When candidate or baseline shadow verification fails,
`generator.PlanMigration`, `generator.GenerateMigration`, and
`generator.VerifyBaselineShadow` preserve a typed
`*generator.ShadowVerificationError`. Inspect it with `errors.As` instead of
parsing `Error()`:

```go
var shadowErr *generator.ShadowVerificationError
if errors.As(err, &shadowErr) {
	stage := shadowErr.Result.Stage
	mismatches := shadowErr.Result.Mismatches
	// Report stage and mismatches to the caller.
}
```

`Result.Stage` identifies the failed boundary, such as `connect`, `replay`, or
`schema-match`. Candidate and baseline verification use the same names at
shared boundaries. Baseline verification can additionally report
`target-introspect`, `reset-schemas`, and `drop-metadata`; candidate-only
`round-trip-down` and `round-trip-up` stages do not occur during baseline
verification.

Baseline display text keeps the
`baseline shadow check failed:` prefix expected by CLI users. A schema-match
result contains every mismatch in deterministic category and object order, not
only the first.
Each `ShadowMismatch` has a stable `Kind`, a human-readable `Message`, and the
available object, table, column, constraint, or changed-property fields.
Operational failures also preserve their underlying error through `Unwrap`;
a structural schema mismatch has no wrapped error. A successful verification
returns `nil` and continues planning; `ShadowVerificationResult` is only the
structured failure payload carried by `ShadowVerificationError`.

## Error contracts

Public failures should use `core/ptaherr` when callers can reasonably branch on
the error:

- annotation and parser failures should support `errors.As` with
  `*ptaherr.ParseError`;
- unsupported dialect failures should support `errors.Is` with
  `ptaherr.ErrUnsupportedDialect`;
- invalid schema diffs rejected during planning should support `errors.Is` with
  `ptaherr.ErrInvalidSchemaDiff`;
- shadow candidate and baseline verification should support `errors.As` with
  `*generator.ShadowVerificationError`;
- command wrappers should preserve typed errors instead of replacing them with
  string-only errors.

## API guardrails

CI protects the public API in three layers:

| Check | Purpose |
| --- | --- |
| `scripts/check-public-api.sh` | Fails if a new importable package appears outside the stable list. |
| `scripts/check-public-api-snapshot.sh` | Compares exported symbols with `docs/public_api.snapshot`. |
| `scripts/check-public-api-released.sh` | Compares stable packages against the latest `v0.x` release tag with `apidiff`. |

Any intentional public API change must update the docs and snapshot in the same
reviewed PR. Once release baselines exist, incompatible changes also require an
explicit approval entry.

## Embedding guidance

Use [Reusable components](../components/) for task-oriented examples.
Use this page to decide whether a package is supported for embedding. Do not
import `internal/...` packages from another module.
