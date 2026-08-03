# Public Go API

Ptah is pre-GA, but embedders need a documented surface and a typed error
contract. Packages in this document are the only non-command, non-example,
non-fixture packages that may remain importable without an explicit review.
For task-oriented guidance and examples, see the
[Reusable components](site/src/content/docs/extend/components.md)
guide.

## Stable Embedder API

These packages are intended for application and tool embedders:

- `go.5x5.cz/ptah/atlascompat`
- `go.5x5.cz/ptah/config`
- `go.5x5.cz/ptah/config/projectconfig`
- `go.5x5.cz/ptah/core/ast`
- `go.5x5.cz/ptah/core/goschema`
- `go.5x5.cz/ptah/core/platform`
- `go.5x5.cz/ptah/core/platform/capability`
- `go.5x5.cz/ptah/core/platform/identifier`
- `go.5x5.cz/ptah/core/ptaherr`
- `go.5x5.cz/ptah/core/query`
- `go.5x5.cz/ptah/core/renderer`
- `go.5x5.cz/ptah/core/schemasource`
- `go.5x5.cz/ptah/core/sqlutil`
- `go.5x5.cz/ptah/dbschema`
- `go.5x5.cz/ptah/dbschema/types`
- `go.5x5.cz/ptah/migration/datadiff`
- `go.5x5.cz/ptah/migration/dbtest`
- `go.5x5.cz/ptah/migration/diffpolicy`
- `go.5x5.cz/ptah/migration/generator`
- `go.5x5.cz/ptah/migration/importer`
- `go.5x5.cz/ptah/migration/lint`
- `go.5x5.cz/ptah/migration/migrator`
- `go.5x5.cz/ptah/migration/planner`
- `go.5x5.cz/ptah/migration/risk`
- `go.5x5.cz/ptah/migration/safety`
- `go.5x5.cz/ptah/migration/schemadiff`
- `go.5x5.cz/ptah/migration/schemadiff/types`
- `go.5x5.cz/ptah/migration/seeder`

`atlascompat` is a narrow compatibility surface for external Atlas parity and
conformance tooling. It intentionally wraps parser, HCL schema,
conversion, and migration sum internals without making those implementation
packages importable directly.

`config/projectconfig` is the canonical typed project configuration IR. Its
online-DDL policy is parsed, merged, validated, and then passed to migration
execution without a second configuration-file read.
`ParseAtlasFSWithOptions` lets embedders evaluate `atlas.hcl` against an
already anchored or immutable `fs.FS`; `file()` and `fileset()` resolve only
through that filesystem.

`core/renderer.GetOrderedCreateStatements` and its capability-aware variant
render complete schema DDL fail-closed. Non-SQLite targets return all table
creation statements before phase-two foreign keys; SQLite keeps foreign keys
inline. Invalid or unsupported foreign keys return typed errors and no partial
statement list. Foreign-key-capable capability sets select exactly one
referenced-key policy: `ForeignKeysRequireUniqueReference`,
`ForeignKeysRequireIndexedReference`, or `ForeignKeysCreateBackingIndex`.
`ValidateSchema` and `ValidateSchemaWithCapabilities` run the same complete
schema validation without rendering SQL. Migration planning calls this path
before producing AST nodes.

`goschema.Finalize` rebuilds materialized inline, JSON, and relation fields on
every call. `Field.GeneratedFromEmbedded` identifies those derived fields so a
caller can mutate the source fields or embedded declarations and finalize the
database again without retaining stale or duplicate columns. Treat the flag as
derived metadata: source declarations should leave it false.

`Database.EmbeddedSources` retains source-only field and embedding declarations
that are needed to rebuild nested embedded fields after `Finalize` or `Merge`.
Callers normally should not modify this bookkeeping directly. A caller that
copies a finalized `Database` and expects to finalize or merge the copy again
must preserve `EmbeddedSources`; dropping it can discard the source declarations
behind materialized `GeneratedFromEmbedded` fields.

`core/platform/capability.MySQL80` was intentionally removed after `v0.1.2`.
The name implied one capability set for every MySQL 8.0 release even though
generic `DROP CONSTRAINT` support starts at MySQL 8.0.19, and MySQL 8.4 changes
the default foreign-key referenced-key policy. Pre-GA callers must select the
explicit `MySQL8016`, `MySQL8019`, or `MySQL84` preset that matches their server
instead of relying on an ambiguous compatibility alias.

`migration/lint` provides the compact `LintFS` findings API and the richer
`AnalyzeFS` API. `AnalyzeFS` captures each migration input once: SQL files,
integrity metadata, and `.ptah-lint.yaml`. It returns deep-copy views of
prepared files and findings together with a read-only source snapshot. Capture
does not apply the lint policy automatically: embedders call `LoadConfigFS`
and pass its `Dialect`, `DisabledRules`, and `Rules` through `lint.Options`.
Configuration decoding rejects unknown keys and noncanonical rule selectors,
including selectors with leading or trailing whitespace.

Finding contexts identify the exact statement and affected tables or columns;
column subjects can also carry the parent table and declared data type. Each
prepared up-migration file also carries the semantic schema changes it
expresses (`File.Changes`, typed `SchemaChange`), recovered from Ptah's
dialect-aware SQL parser so one statement can map to zero, one, or several
changes.

Atlas-ignored files are marked explicitly without changing version selection.
Compatibility-specific directive behavior must be selected explicitly; native
Ptah behavior is the zero-value default.

`migration/migrator` exposes `WithStatementObserver` for tools that need to
audit successful filesystem-migration execution without replacing the
interceptor, splitter, directive, or transaction path. Observers receive
structured source and statement metadata after execution but no connection
handle, so they cannot alter the migrator execution path. For SQL-backed
`no_transaction` migrations, Ptah durably checkpoints the statement before
calling the observer. Atlas-format down execution is excluded because it
preserves Atlas's unchanged-row bookkeeping. A custom `MigrationFunc` remains
opaque and has no statement-level checkpointing.

Programmatic migrations use `Migration.UpTxMode` and `Migration.DownTxMode`
with `MigrationFileTxModeUnspecified`, `MigrationFileTxModeFile`, or
`MigrationFileTxModeNone`. `ParseMigrationUp` gives tools the executable
up-direction SQL, explicit mode, and source-line offset from plain SQL or Atlas
txtar content. The two directions remain independent; the migrator resolves
the up value against its global transaction mode and treats an unspecified
down value as `file`.

This pre-GA API replaces the former `UpNoTransaction` and
`DownNoTransaction` Boolean fields. `NewMigrationFromSQLFiles` and its
interceptor variant return a complete `Migration`, preserving both directions'
transaction modes, timeouts, source paths, and execution functions as one
coherent value.

`MigrateUpOptions.PlanObserver` receives the plan recalculated under the
migration lock before transaction-mode validation, including empty plans. It
is a metadata-only observer and cannot abort execution. `Preflight` remains the
abort-capable hook after validation, so user-facing start output is not emitted
for a statically invalid migration.

`migration/migrator.WithStatementValidator` installs a pre-execution safety
gate on a filesystem provider. Ptah splits and validates every statement in one
migration before executing its first statement, so a rejected later statement
cannot leave an earlier statement applied. Validators inspect SQL but do not
replace the migrator's execution path; use `WithStatementInterceptor` only when
an external executor must take over accepted statements.

`dbschema.DatabaseConnection.WithSession` pins one physical database session
for a callback and rebinds the dialect reader, writer, and SQL runner to it.
Use it when session-local state must remain consistent across cleanup, replay,
introspection, and verification. The scoped connection must not escape the
callback. Root MySQL capability metadata remains conservative; on MySQL 8.4+
the scoped connection refines its referenced-key policy from
`restrict_fk_on_non_standard_key` on the pinned session before the callback,
so planning and execution can safely use that same effective policy.

`dbschema.DatabaseConnection.WithUntrustedSQLSession` pins a session that will
execute SQL the caller does not trust and applies every engine-level
restriction the dialect supports before the callback runs. Use it instead of
`WithSession` whenever the statements come from outside the operator's own
project, such as a plan file produced by another tool.

- On SQLite the engine refuses `ATTACH`, `DETACH`, and `VACUUM INTO`, so the
  callback's SQL cannot reach another database file or write a database copy
  to an arbitrary path, and extensions cannot be loaded. The restriction is
  verified to be in force before the callback runs.
- Storage-directory pragmas and `writable_schema` are not covered.
- Other dialects have no equivalent session-level control and run
  unrestricted.

Taking the session and the restrictions in one step is deliberate: the
restrictions are properties of the physical session, so applying them
separately would silently protect nothing.

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

`migration/generator.PlanMigration` performs loading, diff planning, safety
checks, and optional shadow verification without publishing files. Its
`MigrationPlan.WriteFiles` method publishes the validated artifacts once. The
plan records the migration-directory snapshot used during planning and refuses
publication with `generator.ErrMigrationDirectoryChanged` if that history
changed.

`WriteFilesContext` additionally lets an embedder cancel waiting for the
cross-process publication lock and rejects concurrent use of one plan with
`generator.ErrMigrationPlanInUse`. `GenerateMigration` remains the convenience
composition of planning and publication and propagates its context through
both phases.

`migration/safety.RenderJSON` writes a `safety.Report` containing the highest
risk, destructive verdict, and rendered statement assessments. The native
`ptah migrations plan --report json` command writes that document to standard
output. `GenerateMigrationOptions.ReportFormat: "json"` instead publishes one
`.safety.json` file beside each generated migration pair.

Candidate shadow failures preserve a typed
`*generator.ShadowVerificationError` through `PlanMigration`,
`GenerateMigration`, and command wrappers. Use `errors.As` to inspect
`Result.Stage` and the structured `Result.Mismatches`; operational failures
also expose their underlying error through `Unwrap`. A schema mismatch carries
the complete, deterministically ordered mismatch list without a wrapped
execution error.

`migration/generator.VerifyRollbackFromShadow` requires the caller's open
target `dbschema.DatabaseConnection`. It checks the target and shadow's live
dialects and selected database realms before resetting the shadow, rather than
trusting a caller-supplied dialect or URL-derived database name.

Atlas revision metadata is represented explicitly by `AtlasRevisionType` on
`MigrationRevision`. `SetAtlasRevision` implements Atlas's metadata-only
history transition: it preserves existing clean rows through the target, adds
missing manually-set rows, converts dirty rows to the combined applied and
manually-set type without discarding diagnostics, and removes rows above the
selected version. It returns an `AtlasRevisionSetResult` describing every
changed migration as a version-and-description `AtlasRevisionChange`.
`GetMigrationStatusSnapshot` returns status and the exact revision rows used to
derive it from one metadata query.

`migration/dbtest` exposes the declarative testing engine used by
`ptah migrations test` and `ptah schema test`. Embedders can construct
`Case`/`Step`/`Assertion` values in Go or load YAML, select cases with
`FilterCases`, run against an ephemeral or explicit throwaway database, and
render text, JSON, or HTML reports. See [Declarative database
testing](testing.md).

`core/schemasource` executes an explicitly configured program without a shell,
bounds its runtime and captured output, cleans up descendant processes, and
parses SQL, HCL, or YAML stdout into Ptah's schema IR. Empty output is rejected
to prevent an accidentally broken provider from becoming an empty desired
schema, and displayed stderr/parser diagnostics are bounded, secret-redacted,
and terminal-safe. Embedders can use the same external desired-schema contract
as the CLI without depending on Cobra or any `cmd/internal` package.

`migration/schemadiff/types.SchemaDiff` stores index additions and removals as
canonical `[]IndexRef` fields. Every index reference includes its owning
table. Live comparisons also snapshot catalog identifier semantics into the
diff so comparison, destructive-change policy, forward planning, and reverse
planning use one source of truth.

`core/platform/identifier` exposes the reusable value types and conservative
dialect defaults behind that contract.

`migration/generator.GenerateCheckpointWithDatabaseInfo` preserves the same
live semantics when an introspected schema is rendered as a checkpoint, but
SQL Server callers should use `GenerateCheckpointWithDatabase` so Ptah
resolves the complete candidate identifier set under the target catalog
collation. `GenerateCheckpoint` remains the conservative dialect-only entry
point.

`migration/generator.WriteAtlasCheckpointFile` writes the Atlas single-file
checkpoint convention (and refreshes `atlas.sum`), where
`WriteCheckpointFiles` writes the reversible Ptah pair (and refreshes
`ptah.sum`). `AtlasCheckpointArtifact` renders the same file name and contents
without touching the filesystem, so previews cannot drift from what is
written; the `AtlasCheckpointDirective` it emits is only honored on the file's
first line. `ResolveAtlasCheckpointVersion` supplies the timestamp version,
bumped past any newer migration already in the directory.

`migration/planner.Planner` exposes only checked planning; malformed
references, unresolved additions, and target index-namespace conflicts fail
before SQL is returned.

Public failures from these packages should use `core/ptaherr` where the caller
can reasonably branch on the error. In particular, annotation failures should
support `errors.As(err, *ptaherr.ParseError)`, and unsupported dialect failures
should support `errors.Is(err, ptaherr.ErrUnsupportedDialect)`. Invalid schema
diffs rejected during planning support
`errors.Is(err, ptaherr.ErrInvalidSchemaDiff)`.

## Provisional Surface

There is no provisional public surface. Packages that are not listed under
Stable Embedder API are either command/example/fixture/test packages or are
behind Go `internal/` boundaries. Promoting another package to public API must
be an explicit design decision that updates this document and the snapshot in
the same reviewed change.

## Compatibility Guard

CI runs three public API checks:

- `scripts/check-public-api.sh` fails when `go list ./...` finds a
  non-command, non-example, non-fixture package that is importable from outside
  this module but not listed here.
- `scripts/check-public-api-snapshot.sh` regenerates the `go doc -short`
  exported-symbol snapshot for every package listed here, then appends the full
  `go doc` output for every exported named type (struct, interface, alias, map,
  func type), and compares the result with `docs/public_api.snapshot`. Because
  the full per-type output is recorded, changes to exported struct fields and to
  methods on concrete named types are caught, not only interface method sets.
  Any exported surface change must update the snapshot in the same reviewed
  change. The guard is itself covered by a self-test in
  `internal/apiguard` that fails if this per-type coverage regresses.
- `scripts/check-public-api-released.sh` compares each stable package against
  the latest `v0.x` release tag with `apidiff -incompatible`. Until the first
  `v0.x` tag exists, the script reports that no released baseline is available
  and exits successfully. Once a `v0.x` tag exists, CI checks out repository
  tags and uses that real release tag as the baseline.

## Intentional API Changes Before v1

Ptah is still pre-v1, so maintainers may intentionally approve breaking changes
to the stable embedder API. Intentional approval must be explicit in the same
reviewed change:

- update this document if packages move between stable and non-public surfaces;
- update `docs/public_api.snapshot` when any exported surface changes —
  symbols, struct fields, interface method sets, or methods on concrete named
  types;
- add one package-level approval line to `docs/public_api_approvals.txt` when
  `scripts/check-public-api-released.sh` reports an incompatibility against the
  latest `v0.x` baseline;
- include the compatibility rationale in the PR description.

Do not weaken the CI checks, broaden exclusions, or silently remove packages
from the stable list to hide an API change.
