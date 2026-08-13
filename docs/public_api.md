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
- `go.5x5.cz/ptah/core/coverage`
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
through that filesystem. Use `ParseAtlasFSCollectionWithOptions`,
`ParseAtlasCollectionWithOptions`, or `LoadCollection` when an env `for_each`
can select several independent configs. The singular functions require exactly
one selected instance and return an error rather than discarding the others.

`AtlasLoadOptions.RejectListMapForEach` lets a compatibility adapter retain
tuple, object, and set expansion while refusing Ptah's list/map extension. Its
zero value keeps the complete dynamic-environment capability.
`Config.IgnoredConstructs` identifies names that
Atlas CE accepts without acting on, with kind and source location. `Merge`
preserves this diagnostic metadata from both inputs. Ptah's command layer warns
for each entry; embedders can choose their own reporting policy.

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

`goschema.Extension.Schema` records a PostgreSQL extension's installation
schema. `ast.ExtensionNode.Schema` and `SetSchema` carry the same intent into
SQL rendering, which emits `CREATE EXTENSION ... WITH SCHEMA ...` after any
`IF NOT EXISTS` clause and before `VERSION`. An empty schema means the target's
default schema. Embedders should preserve the field when converting or copying
schema IR; dropping it can move an extension into the wrong namespace.

`core/coverage` carries what a schema description does **not** claim to
describe. `goschema.Database.NotDescribed` and `dbschema/types.DBSchema.NotDescribed`
hold one, and schema comparison consults both: the desired state's record gates
removals and the introspected state's record gates additions. Its zero value
claims everything, so an embedder that never sets one gets exactly the
comparison it got before the field existed. Set it when a reader was asked about
less than the whole database, or a projection left something out on purpose;
leaving it zero there is how an object nobody looked at becomes a `DROP`.

`schemadiff.CompareReportingUndecidedAdditions` exposes desired additions that
an offline comparison could not plan safely, and
`schemadiff.CompareWithDatabaseReportingUndecidedAdditions` provides the same
report while resolving the connected catalog's identifier semantics and
default comparison options. Command adapters use that report for warnings;
embedders can choose their own diagnostic policy.

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

`CockroachDB25` and `CockroachDB26` are the measured CockroachDB resolver arms.
CockroachDB 25.4 refuses generic and guarded `DROP CONSTRAINT` plus
`CREATE OR REPLACE TRIGGER`; CockroachDB 26.2 accepts those statements. Use
`ResolveServerVersion` when a live banner is available so the correct arm is
selected instead of choosing a preset by its name.

`migration/lint` provides the compact `LintFS` findings API and the richer
`AnalyzeFS` API. `AnalyzeFS` captures each migration input once: SQL files,
integrity metadata, and `.ptah-lint.yaml`. It returns deep-copy views of
prepared files and findings together with a read-only source snapshot. Capture
does not apply the lint policy automatically: embedders call `LoadConfigFS`
and pass its `Dialect`, `DisabledRules`, and `Rules` through `lint.Options`.

Configuration decoding rejects unknown keys and noncanonical rule selectors,
including selectors with leading or trailing whitespace. It also rejects
unsupported dialects and empty, malformed, or non-normalized exclusion globs
instead of silently weakening the effective policy. `lint.ValidateOptions`
checks selectors against the active rule registry without reading migration
files; call it before a no-work return or an execution override that can skip
`LintFS` or `AnalyzeFS`.

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
calling the observer, including Atlas-format down execution. Process exit,
context cancellation, or deadline while execution is in flight preserves the
unknown-outcome marker. A custom `MigrationFunc` remains opaque and has no
statement-level checkpointing.

Dirty SQL-backed resumes verify the already committed source prefix before
skipping it. Native rows use the `partial:h1:` value in `Checksum`; Atlas rows
use cumulative `partial_hashes`. A failure after changing transaction mode
cannot reduce the recorded applied count below that verified prefix.
`RepairMigration` holds the session advisory lock across revision inspection,
resumed SQL, safety checks, and the final metadata write.

Programmatic migrations use `Migration.UpTxMode` and `Migration.DownTxMode`
with `MigrationFileTxModeUnspecified`, `MigrationFileTxModeFile`, or
`MigrationFileTxModeNone`. `ParseMigrationUp` gives tools the executable
up-direction SQL, explicit mode, and source-line offset from plain SQL or Atlas
txtar content. The two directions remain independent; the migrator resolves
the up value against its global transaction mode and treats an unspecified
down value as `file`.

Atlas transaction-mode directive validation errors expose
`migrator.AtlasTxModeDirectiveError` through `errors.As`. The leaf error keeps
the source file and transaction-mode details in its message.

`migration/migrator.ParseFileDirectives` reads the file's directive header —
the run of blank lines and line comments before the first executable statement
— rather than the whole file. Both directive families answer to that one rule,
so a `-- +ptah` line written below the statements it claims to govern is no
longer honored, matching what `-- atlas:txmode` already did and what Atlas CE
does. A directive outside its region is reported at `WARN` by the migrator
rather than dropped in silence. `PTAH_DIRECTIVES_ANYWHERE=1` restores the
earlier file-wide scope for `-- +ptah` directives; `ParseFileDirectives`
reports no error, so a malformed value for that variable fails the migration
run instead and this function keeps the header rule. Ordered
`-- +ptah check` directives are unaffected: they are position-insensitive by
design and `ParseChecks` still reads the whole file.

Position and value stay separate verdicts. A `-- +ptah` directive whose key is
recognized but whose value cannot be read fails `ParseMigrationUp`, `Migration.Up`
and `Migration.Down` wherever the line sits, and the error names the line, so a
typo is never demoted to a position warning and the verdict does not depend on
`PTAH_DIRECTIVES_ANYWHERE`. An unrecognized bare token and an unknown
`key=value` pair are not directives and produce neither. The `-- atlas:txmode`
spelling is reported but not refused outside its block, because Atlas CE applies
such a directory.

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

`MigrateUpOptions.AllowDirty` authorizes a verified retry only when the current
provider still owns the dirty migration's exact identity and body. A dirty
exact-history row whose source file was removed remains blocking: without that
body, the migrator cannot verify or resume the committed statement prefix.

`MigrateUpOptions.DiscardRolledBackFailure` models the Atlas revision-table
compatibility surface, which treats a confirmed transactional rollback as no
recorded attempt. It has no effect with the native Ptah revision-table format.
With the Atlas format, it removes only the failed revision written by the
current invocation and only after `Rollback` succeeds. Existing dirty rows,
partial progress, rollback failure, commit failure, and unknown statement
outcomes remain dirty and block automatic retry.

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
Use `dbschema.DatabaseConnection.WithSessionOrCurrent` for reusable components
that may run inside an existing pinned callback or from a pool-backed
connection; it pins only when needed and otherwise leaves the current session
lifecycle with the caller.

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

When its URL or connection selects SQLite, `PlanMigration` validates
`PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP` before resolving `OutputDir`. A malformed
value therefore fails before filesystem work; non-SQLite plans do not consult
the variable.

`GenerateCheckpointFromShadow` and `VerifyBaselineShadow` apply the same
SQLite-only validation before connecting to or mutating a shadow database.
The checkpoint path therefore cannot drop and replay a shadow database before
reporting a malformed value.

`migration/generator.PlanBidirectionalSchemaDiff` is the lower-level planning
boundary for callers that already hold a schema diff. Its input binds the diff,
desired and current schemas, normalized dialect capabilities, and concurrent
index policy into one result with forward and reverse diffs, AST nodes, exact
table-qualified concurrent-index references, and an independent
`RequiresNoTransaction` classification for each direction. The reverse restores
the introspected current schema rather than only exchanging structural lists.
On MySQL and MariaDB, that includes removing a foreign-key backing index created
by the forward migration while preserving any prior or same-run index whose
leading key columns cover the foreign key. Planning refuses an ambiguous
incomplete-index shape or a plan that later removes every covering index.

`SchemaDiff.ForeignKeysRemovedWithTables` carries the local and referenced
columns needed for that ordering as supplemental metadata keyed by table and
constraint name. It does not independently represent a removal and is ignored
without a matching `ConstraintsRemovedWithTables` entry, leaving the existing
comparable `ConstraintRemovalInfo` value unchanged.

`ConcurrentIndexAutomatic` uses the native populated-table heuristic,
`ConcurrentIndexDisabled` selects ordinary index statements, and
`ConcurrentIndexAll` requires the requested forward target capability. The
reverse selects its concurrent modifier independently: it uses the matching
concurrent capability when available and an ordinary blocking statement when
the reverse-only capability is absent. An explicitly requested unsupported
forward operation still fails. This bidirectional API replaces the pre-v1
structural-only `ReverseSchemaDiff` function; no compatibility wrapper is
retained.

For generated pairs, `MigrationFilePair.NoTransaction` is true when either the
forward or reverse file requires non-transactional execution; inspect the two
directional `RequiresNoTransaction` values when a caller needs to distinguish
which side carries the requirement.

`WriteFilesContext` additionally lets an embedder cancel waiting for the
cross-process publication lock and rejects concurrent use of one plan with
`generator.ErrMigrationPlanInUse`. `GenerateMigration` remains the convenience
composition of planning and publication and propagates its context through
both phases. The returned `generator.MigrationFiles.Files` slice is the
authoritative list of generated pairs and published paths, in apply order.

`migration/safety.RenderJSON` writes a `safety.Report` containing the highest
risk, destructive verdict, and rendered statement assessments. The native
`ptah migrations plan --report json` command writes that document to standard
output. `GenerateMigrationOptions.ReportFormat: "json"` instead publishes one
`.safety.json` file beside each generated migration pair.

Candidate and baseline shadow failures preserve a typed
`*generator.ShadowVerificationError` through `PlanMigration`,
`GenerateMigration`, `VerifyBaselineShadow`, and command wrappers. Use
`errors.As` to inspect `Result.Stage` and the structured `Result.Mismatches`;
operational failures also expose their underlying error through `Unwrap`. A
schema mismatch carries the complete, deterministically ordered mismatch list
without a wrapped execution error. Baseline failures retain the
`baseline shadow check failed:` display prefix while preserving this typed
contract. Candidate and baseline verification share stage names at common
boundaries. Baseline verification can additionally report `target-introspect`,
`reset-schemas`, and `drop-metadata`; candidate-only `round-trip-down` and
`round-trip-up` stages do not occur during baseline verification.

`migration/generator.VerifyRollbackFromShadow` requires the caller's open
target `dbschema.DatabaseConnection`. It checks the target and shadow's live
dialects and selected database realms before resetting the shadow, rather than
trusting a caller-supplied dialect or URL-derived database name.

Atlas revision metadata is represented explicitly by `AtlasRevisionType` on
`MigrationRevision`. `SetAtlasRevision` implements Atlas's metadata-only
history transition: it preserves existing clean rows through the target, adds
missing manually-set rows, converts dirty rows to the combined applied and
manually-set type without discarding diagnostics, and removes rows above the
selected version.

In exact-identity mode, it also removes source-retired rows
that the compatibility adapter's source comparator places above the selected
target, matching Atlas CE even when their stored numeric ordering key is no
longer reconstructable. If the source format cannot order a retired identity
without missing role or walk-position context, the operation refuses before
changing metadata instead of comparing opaque identity bytes.

It returns an
`AtlasRevisionSetResult` describing every changed migration as a numeric order
key, exact revision identity, and description in `AtlasRevisionChange`.
`GetMigrationStatusSnapshot` returns status and the exact revision rows used to
derive it from one metadata query.

`migration/migrator.WithAtlasRevisionVersions` separates an Atlas revision's
opaque string identity from the numeric `Migration.Version` that governs
execution order. The map key is that numeric order key and the value is the
exact revision-table identity; a present empty string is an owned empty
identity, not a request to fall back to the file name. Compatibility adapters
may include mappings for migrations a baseline squashed out of the loaded
filesystem so existing history and the high-water mark remain interpretable.

`Migrator.WithAtlasRevisionVersionComparator` supplies the matching source
format's relationship between a retired identity and the selected target for
metadata-only set operations. The callback receives
`AtlasRevisionOrderIdentity` values carrying the exact key, Atlas row type,
operator marker, and a provider-owned repeatable bit. This lets an adapter keep
baseline, versioned, and repeatable roles distinct instead of reconstructing a
retired identity from its token. Its false result is a fail-closed ambiguity
signal, not an instruction to fall back to lexical order.

Passing a non-nil map also keeps a persisted exact identity readable after its
source file is removed: the retired row receives a history-only runtime, while
its exact key still contributes to source ordering. Only migrations that the
provider actually loads own identities and pending work. `MigrationRevision`
JSON emits `atlas_version` for a present Atlas
identity, including the exact empty identity, and omits it for an ordinary
numeric revision; unmarshaling preserves the same distinction.

`MigrationStatus` JSON likewise emits `current_version_key` for every present
exact current identity, including an empty one, and omits it for ordinary
numeric status without an exact key. Unmarshaling restores
`CurrentVersionKeySet` from the member's presence without exposing the presence
bit as a second JSON field. The presence bit is authoritative during marshaling,
so a stale key value with the bit unset stays absent.

`migration/migrator.WithAtlasRevisionTypes` carries source-format metadata that
filename conversion cannot recover. A compatibility adapter marks a surviving
Flyway baseline with the combined baseline-and-applied bits: Atlas CE writes the
ordinary applied type for both `V2` and `B2`, but the combined marker lets Ptah
distinguish an already-settled `B2` from an unsafe `V2` to `B2` replacement when
both own exact revision identity `2`. It still renders as `applied` and does not
create the implicit lower-history boundary of a pure baseline row. Missing map
entries retain the ordinary applied type. `SetAtlasRevision` adds the manually
set bit to the source marker, so a settled Flyway baseline becomes the combined
value `7`; it renders as `manually set` while retaining the baseline
discriminator for later apply.

`BaselineWithOptions` preserves the pure baseline type when it records one of
those source baselines and writes `Ptah/source-baseline` to `operator_version`.
That durable marker proves which source the boundary selected. An ordinary
mapped source migration instead keeps `Ptah/source-identity`; because it lacks
the source-baseline marker, a same-token baseline introduced later remains
ambiguous and fails closed. The source-identity marker also distinguishes an
exact numeric source token from an ordering key recorded by an older Ptah
build.

`migration/migrator.WithAtlasRepeatableVersions` preserves the repeatable role
when a compatibility adapter converts source files to numeric Atlas filenames.
It takes numeric execution-order keys, deduplicates them, and does not infer the
role from an empty revision identity: an ordinary source migration can also own
an exact empty token. Missing keys retain the repeatability parsed from the
Atlas filename.

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

`SchemaDiff.ExtensionsModified` contains `ExtensionDiff` entries with the
extension name and its `FromSchema`/`ToSchema` placement. Empty and explicit
default-schema spellings compare under the diff's identifier semantics. The
PostgreSQL planner currently rejects a placement change with
`ptaherr.ErrInvalidSchemaDiff` before emitting any AST; additions and removals
remain independently plannable.

Row-level security policies are carried the same way. `RLSPoliciesAdded` and
`RLSPoliciesRemoved` are `[]RLSPolicyRef`, and `RLSPoliciesModified` is
`[]RLSPolicyDiff`; all three name the owning table alongside the policy name.
The pair is the identity, not decoration: a PostgreSQL policy name is scoped to
its table, so two tables in one schema may each carry a policy called
`tenant_isolation` and neither the comparator nor the planner can tell them
apart from the name. The table half is matched under the diff's identifier
semantics rather than as a raw string, which is what makes the desired
spelling `public.orders` and the introspected spelling `orders` one table
across the forward and reverse directions. Embedders that build these lists by
hand must fill both fields; a reference the target schema cannot resolve is
rejected with `ptaherr.ErrInvalidSchemaDiff` rather than silently omitted from
the plan.

`migration/schemadiff/types.ViewDiff` also records the view body that is in
force before the diff is applied, and whether the entry is being planned as a
rollback. Planners read the body to decide whether the target engine accepts an
in-place view replacement; PostgreSQL accepts `CREATE OR REPLACE VIEW` only when
the new query appends to the old column list over the same relations. Where that
can be neither proved nor ruled out — an unknown prior body, a `WITH` prefix, a
`SELECT *` projection, a top-level set operation — the rollback flag settles it:
a forward plan keeps the replacement, which preserves dependent objects and the
privileges on the view and fails loudly if the engine refuses it, while a
rollback drops and recreates, which always applies. Embedders that build a
`ViewDiff` by hand and leave both fields empty get the forward answer.

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
