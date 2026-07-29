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
| `core/renderer` | Dialect-aware SQL rendering from AST/schema IR. |
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
import "github.com/stokaro/ptah/core/renderer"
```

The `migration/dbtest` package is the embeddable engine behind the native test
commands, including regular-expression case selection through `FilterCases`.
See [Test migrations and schemas](../../testing/migrations-and-schema/) for
its case model and [Database test commands](../../reference/test-cases/) for CLI behavior.

The separate [`testkit`](https://github.com/stokaro/ptah/tree/master/testkit)
module (`github.com/stokaro/ptah/testkit`) is an opt-in helper for tests that
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

The observer composes with `StatementInterceptor`: a statement handled by an
external executor is observed once after that executor reports success.

## Pinned database sessions

`dbschema.DatabaseConnection.WithSession` pins one physical database session
for the duration of a callback and rebinds the dialect reader, writer, and SQL
runner to that session. Use it for cleanup, replay, and inspection workflows
that depend on session-local state or SQLite attached-database visibility.

The scoped connection must not escape the callback. Ptah discards the physical
connection afterward so session-local state cannot leak to a later pool user.

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
canonical `[]IndexRef` fields. Every index reference includes its owning table.
Live comparisons snapshot catalog identifier semantics into the diff so
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
the shared cross-process publication lock. Embedders that need cancellation
while waiting for that lock use `WriteFilesContext`; concurrent use of one plan
fails with `generator.ErrMigrationPlanInUse`.
`migration/planner.Planner` exposes only checked planning; malformed references,
unresolved additions, and target index-namespace conflicts fail before SQL is
returned.

## Error contracts

Public failures should use `core/ptaherr` when callers can reasonably branch on
the error:

- annotation and parser failures should support `errors.As` with
  `*ptaherr.ParseError`;
- unsupported dialect failures should support `errors.Is` with
  `ptaherr.ErrUnsupportedDialect`;
- invalid schema diffs rejected during planning should support `errors.Is` with
  `ptaherr.ErrInvalidSchemaDiff`;
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
