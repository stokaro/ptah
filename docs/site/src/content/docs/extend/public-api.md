---
title: Public Go API
description: Stable embedder packages and API compatibility guardrails.
type: reference
audience:
  - "go-developer"
readerQuestion: "Which Go packages and exported symbols are stable for embedders?"
goal: "Identify the Go packages and exported symbols stable for embedders."
sourceOfTruth:
  - "docs/public_api.md"
  - "scripts/check-public-api-released.sh"
generated: false
overlaps: []
disposition: keep
---

Ptah is pre-GA, but embedders need a documented import surface. The packages on
this page are the stable embedder API, and the table below is enforced: a
ledger in the repository is the source of truth, and
`scripts/check-public-api-docs-sync.sh` keeps this page's table equal to it.

The ledger classifies every importable library package into one of two
categories, and only the first is on this page. Anything it does not classify is
a program, a directory holding only tests, or behind a Go `internal/` boundary.

## Stable packages

| Package | Purpose |
| --- | --- |
| `atlascompat` | Stable wrappers for Atlas-compatible schema, SQL, and migration-sum behavior. |
| `config` | Project-level config loading helpers. |
| `config/projectconfig` | Typed Ptah/Atlas project config IR, including validated online-DDL policy. |
| `core/ast` | Typed schema DDL AST nodes. |
| `core/astbuilder` | Fluent builders that construct `core/ast` DDL nodes without hand-written struct literals. |
| `core/coverage` | Schema description scope facts, so an absent object is not read as a removed one. |
| `core/objectidentity` | Structured object identities, target-aware name comparison, and reference resolution. |
| `core/plangraph` | Owner-contributed steps, object effects, and validated dependency ordering. |
| `core/featureplan` | Contextual feature-planning batches, captured parents, and typed graph contributions. |
| `core/goschema` | Go annotation parser. Produces a `schemamodel.Database`. |
| `core/manageddata` | YAML row-data loading and resolution, separate from captured declarations. |
| `core/schemamodel` | The desired-schema model every authoring source produces: Go annotations, HCL, YAML, SQL, DBML and live-catalog conversion. |
| `core/platform` | Dialect and platform constants. |
| `core/platform/capability` | Capability flags for dialect/version behavior. |
| `core/platform/identifier` | Catalog identifier comparison and namespace semantics. |
| `core/ptaherr` | Typed public errors and sentinel errors. |
| `core/query` | Fluent builder for parameterized, dialect-aware SELECT, INSERT, UPDATE, DELETE and YDB UPSERT statements. |
| `core/renderer` | Selected AST and whole-schema rendering services, omission records, and local visitor contracts. |
| `core/schemacapture` | Independent desired and observed table captures for contextual services. |
| `core/schemapreparation` | Selected column-key and facet resolution with independent source and prepared captures. |
| `core/schemaext` | Typed immutable feature values, positive source coverage, versioned codecs, and conservative operation effects. |
| `core/schemaprojection` | Target-owned CREATE defaults, constraint effects, and table-state predictions. |
| `core/schemavalidation` | Whole-schema validation services with structured diagnostics and explicit completion. |
| `engine` | Explicit provider registration, model codecs, and batched service dispatch. |
| `engine/builtin` | Dialect-aware SQL rendering from AST/schema IR, including fail-closed two-phase foreign key ordering. |
| `core/schemasource` | Runs an external desired-schema program and parses its output into schema IR. |
| `core/sqlutil` | SQL utility helpers used by public paths. |
| `core/yamlschema` | Reads Ptah's YAML authoring format into the schema IR, strictly. |
| `dbschema` | Live database schema introspection connection layer. |
| `dialect/postgres/pgproject` | PostgreSQL constraint backing-index and column effects. |
| `dialect/clickhouse/chprepare` | ClickHouse key membership, retained settings, and CREATE defaults. |
| `dialect/clickhouse/chresolve` | Table-setting resolution with retained intent and property origins. |
| `dialect/clickhouse/chschema` | Desired and observed table settings with versioned model codecs. |
| `dialect/clickhouse/chsource` | Table property encoding and decoding that preserves setting intent. |
| `dialect/clickhouse/chreport` | Captured storage-setting counts and export omission labels. |
| `core/schemaproperties` | Selected table property decoding and export without engine-specific field access. |
| `dialect/clickhouse/chcompare` | Comparison of resolved table settings with explicit knowledge limits. |
| `dialect/clickhouse/chconvert` | Lossless projection between complete table declarations and observations. |
| `dialect/clickhouse/chdiff` | Captured prior and desired table settings for directional changes. |
| `dialect/ydb/ydbast` | Typed YDB changefeed operations carried by AST extension envelopes. |
| `dialect/ydb/ydbcompare` | Coverage-aware comparison of individual YDB feature objects. |
| `dialect/ydb/ydbconvert` | YDB feature representation conversion. |
| `dialect/ydb/ydbdiff` | Directional YDB feature changes. |
| `dialect/ydb/ydbreport` | Inventory and omission reports for captured YDB feature values. |
| `dialect/ydb/ydbreverse` | Changefeed reversal and recovery limits. |
| `dialect/ydb/ydbplan` | Captured-state validation and graph contributions for changefeed changes. |
| `dialect/ydb/ydbschema` | YDB feature values and model codecs. |
| `catalog` | Shared database schema types. |
| `docs` | Ptah's own documentation embedded in the binary as an `embed.FS`. |
| `migration/datadiff` | Row-level diffing between declared managed data and live table rows. |
| `migration/dbtest` | Declarative migration/schema test cases, runners, and reports. |
| `migration/diffpolicy` | Declarative policy for which destructive changes a planner may emit. |
| `migration/generator` | Migration file generation. |
| `migration/importer` | Converts migration directories with caller context and explicit rendering services for typed changes. |
| `migration/lint` | Migration SQL linting rules, immutable analysis snapshots, and findings. |
| `migration/migrationfile` | Migration file names, directory formats, directives, and per-file transaction modes. |
| `migration/migrator` | Migration providers, revision metadata, dry-run plans, and execution. |
| `migration/planner` | Schema change planning. |
| `migration/risk` | Migration risk classification. |
| `migration/safety` | Destructive-change assessment and safety reports. |
| `migration/schemadiff` | Desired/live schema diffing. |
| `migration/schemadiff/difftypes` | Shared schema-diff types. |
| `migration/seeder` | Seed discovery and execution. |
| `migration/shadow` | Migration verification against a live disposable database. |

Import paths use the module prefix:

```go
import "ptah.run/engine/builtin"
```

`engine.New` assembles only the providers the caller supplies. Registration
rejects duplicate target ownership and aliases. `Runtime.Render` sends each
batch to one owner, propagates errors and cancellation, and returns no partial
SQL on failure. Each result has one `Fragments` entry per input node. Empty
entries account for nodes that emit nothing; other entries may contain several
statements. `Result.SQL()` joins them without adding separators.
`renderer.Render` validates this contract when calling a service directly.
`builtin.New` explicitly selects the bundled rendering providers.

Completed validation diagnostics become `schemavalidation.RefusalError` through
`Result.Err`. The error retains a snapshot of the report and exposes its schema
and capability causes through `errors.Is` and `errors.As`. A service error or
invalid completion receipt supplies no completed refusal. Common declaration
checks before comparison use `schemadiff.RefusalError` with the original cause.
The schema census discards its measurement when any selected service fails,
including after earlier cells completed.

`Runtime.PlanFeatures` returns `featureplan.Diagnostic` data with original change
or parent indexes. Refusals discard all operations and receipts; service failures
and cancellation also discard diagnostics. Call `Result.Err(request)` before
lowering operations. Its `featureplan.RefusalError` preserves the report and error
identities. Process adapters transfer diagnostic data instead of Go errors.

`Request.CommonSteps` supplies isolated accepted operations; `Result.Rewrites`
claims their replacements. `plangraph.ScheduleRewritten` validates claims,
preserves logical writes, redirects dependencies, and rejects conflicts and cycles.
Owners supply replacement semantics and execution requirements; grouping implies
no transaction. `AlterTable` requests assessment of unchanged attached state.
Process adapters define explicit common-operand wire models instead of
serializing Go AST structs.

`Provider.Conversions` registers a batched conversion service for explicit target
and feature-kind pairs. `Runtime.ConvertFeatures` validates each ordered batch,
propagates cancellation and service errors, and returns no partial result.
Both desired and observed codecs must belong to the registering provider.
`Registry.ConvertCoverage` retains unknown source state during conversion;
installing another codec does not make the source authoritative for that kind.

`Provider.Comparisons` assigns named object and change kinds to a comparison
service. `Runtime.CompareObjects` requires an explicit completion receipt and returns typed changes, effective desired
objects for captures, and structured diagnostics for undecided state. It
rejects dropped declarations and duplicate child changes during parent creation
or removal. Service errors and cancellation return no partial result. The YDB
service preserves inspected changefeeds omitted by an incomplete desired source.

`Provider.FacetComparisons` registers comparison of settings attached to common
objects. `Runtime.CompareFacets` keeps their common owner identity and source
knowledge. Explicit knowledge limits prevent a partial value from authorizing
a change. Parent creation or removal captures attached settings without a
separate facet operation. A provider checks `FacetComparisonRequest.Includes`
for each model and owner. Source scope may exclude one setting while retaining
another on the same object. The runtime refuses changes and diagnostics for
excluded pairs, even when source coverage is complete.

`Runtime.CompareFeatures` combines named objects and attached facets. It validates
both sources before dispatch and discards all output if either comparison fails.
Replies must set `Complete`. The migration comparator captures table facets and
applies effective desired settings before capturing common table changes. Other
attachment points currently refuse because they lack comparison identity capture.
Adding a provider does not add coverage claims to an existing source.
Comparison binds table coverage claims to the connection's identifier semantics
before selecting parent state. The default database applies to unqualified table
claims; explicit schemas and knowledge limits remain intact. Conflicting claims
for the resulting identity are refused.

Table selection preserves the selected tables' feature children and their source
coverage. `Coverage.SelectSubjects` keeps kind-wide knowledge while filtering
explicit records. Use it with the same object projection; removing an override
alone changes which knowledge a lookup returns.
Unrepresentable enrolled state and explicit subject limits require a selected
comparison handler even without concrete values. An uninspected namespace alone
makes no claim about target applicability; the runtime preserves that knowledge.
A known empty namespace alone does not require target support for its model.

`Provider.Reporting` registers omission labels and count metrics for a model
representation. `Runtime.ReportFeatures` validates the complete batch, calls
each selected service once, and preserves value order. Metadata is frozen when
the runtime is built. Missing handlers, malformed replies, service failures,
and cancellation return no partial report.

Reporting is independent of target support. It can describe feature values
from Go declarations without choosing a database. The optional target supplies
source context only. Metrics count captured values, so zero does not establish
inspected absence. Statistics and DBML or JSON omission reports use these model
services. Counts from YDB's owner include disabled changefeeds and their consumers.

Every `schemadiff` entry point takes a context and a selected runtime. Catalog
comparison uses `schemapreparation.Runtime`; `CompareSchemas` uses
`schemadiff.DocumentRuntime` to include offline CREATE prediction. Non-reporting calls return no diff when evidence
is incomplete; `ErrIncompleteComparison` identifies that refusal, and
`IncompleteComparisonError` retains the structured diagnostics. Reporting calls
return established changes, `Diagnostics`, and an error. Reports must retain
both `Diagnostics.Common` and `Diagnostics.Features`; an empty diff with a
knowledge limit does not establish agreement.

`Target.Preparation` selects table preparation before feature and column comparison.
Providers that preserve input flags register `schemapreparation.Identity`;
a missing service is an error. Services may resolve column key membership and
return `ResolvedFacets` for declared table models they own. Source facets, target
bindings, observations, and knowledge stay unchanged. Incomplete replies return
no diff. `SchemaDiff.TablePreparation` retains independent source and prepared
captures as comparison provenance, including through reversal.

`Target.Creations` selects `schemaprojection.TableCreationService` for source
files used as current state. The service predicts CREATE defaults and column key
membership from decoded declarations. Computed facets stay separate from source
intent; they may describe defaults the author did not declare. The host validates
ordered completeness, identities, column names, model ownership, and codecs.
It retains source bindings, exclusions, and explicit knowledge limits. New kinds
are bound to the selected target. Predictions prove no inspection or execution.
Providers with no additional effects register `IdentityCreations` explicitly;
a missing service is unavailable.

Feature providers register their local model codecs through `Provider.Codecs`.
`Runtime.Codecs` exposes context-aware batch encoding, decoding, and canonical
fingerprints. A document records its provider, kind, representation, version,
and model-definition hash. Unknown or incompatible definitions are errors;
registering a codec alone does not grant a target support for that feature.

`chschema.DesiredTable` distinguishes an omitted setting, a request for its
default, and an explicit value. Explicit empty settings remain distinct from
default requests. `ObservedTable` requires every setting, including empty
optional values, and keeps sorting and primary keys separate. Register
`chschema.Codecs()` with a provider to preserve these distinctions in envelopes.
The bundled renderer and new-table migration planner consume programmatically
supplied desired table facets, including reverse DROP plans. Mixing a typed facet
with storage overrides is refused, including empty overrides. The reader captures
typed settings; source properties reach their selected owner before rendering or
comparison.

Register `chcompare.Service` in a selected provider's `FacetComparisons` to
compare resolved table settings. `chdiff.Table` and its codec retain complete
before and after operands. Missing evidence needed for declared settings produces
an undecided result. Unmentioned tables stay unmanaged, and explicit inspection
limits remain visible. The bundled runtime registers these services; migration
planning refuses changes to existing table settings.

`chresolve.Table` keeps the original declaration beside fully explicit settings
and records each property's source. On creation, omitted settings use creation
rules. A default primary key inherits the sorting key; an explicit empty key
stays empty. On an existing table, omitted settings require a usable observation.
Missing evidence returns an error without a partial result.

`chconvert.Service` projects complete observations into explicit declarations and
fully resolved declarations into predicted observations. Empty settings and
separate key roles survive the conversion. The migration generator uses this
projection when capturing what a reverse DROP removes. A prediction does not
establish that a database was inspected.

`Provider.Properties` declares source property ownership by target, format, and
feature kind. The runtime validates complete batches before dispatch and rejects
changed kinds, missing results, and properties outside the owner's declared keys.
Empty values remain distinct from missing properties. Conversion failures and
cancellation return no partial result or new catalog knowledge.

`chsource.Service` preserves ClickHouse intent through the table platform property
format. Bare keys carry explicit values; a `.state` suffix with value `default`
requests the creation rule. Register its definitions and service with a selected
provider. The bundled runtime uses it for source lowering and Go annotation export.

`schemaproperties.DecodeTables` attaches decoded properties as facets bound to
the selected target. Unclaimed keys and other target groups stay untouched.
`EncodeTables` exports those facets through the same owner. Both refuse mixed
typed/property declarations and duplicate alias keys. Export also refuses facets
whose scope or presence the property format cannot preserve. Neither operation
adds inspection coverage. Native Go export, whole-schema rendering, validation,
and comparison decode source properties through the selected runtime. File-to-file
comparison resolves the current document's creation rules before projecting it
into catalog form. This prediction preserves explicit knowledge limits and proves
no inspection or execution.

The ClickHouse reader carries storage settings in `chschema.ObservedTable` facets.
It preserves sorting and primary keys independently, including empty values.
Coverage describes only tables retained in the read. `chreport.Service` supplies
counts and omission labels for captured settings. Table-setting ALTER planning
remains part of [#4140](https://github.com/stokaro/ptah/issues/4140).

`schemaext.Facets` captures one typed value per kind. `schemaext.Objects` captures
individually named objects with structured references, including parentage.
Both clone inputs and returned values and refuse duplicates. Use the registry
to serialize them: ordinary JSON encoding refuses interface payloads.

Use `Facets.WithTargetScope` for a source binding and `ForTarget` with the
runtime's selected target. An excluded value keeps its target binding without
its payload. `DeclaredKinds` includes these exclusions; `Kinds` and `Len` count
concrete values. Repeated projection, codec round trips, snapshots, and schema
conversion preserve the binding. `EncodedFacet` carries it separately from the
owner's payload. An excluded payload needs no codec. Select from the original
source when another target needs a value already excluded from a capture.

`schemaext.Coverage` records only definitions the source explicitly enrolled.
Its empty value is uninspected, so a newly installed provider cannot turn an
older document's omission into a removal request. Subject claims distinguish
absence, defaults, complete inspection, and state the source cannot represent.
Local `Value.Equal` and canonical fingerprints have separate purposes from a
provider's target-aware comparison of desired and observed state.

The `migration/dbtest` package is the embeddable engine behind the native test
commands, including regular-expression case selection through `FilterCases`.
Its `Options.Runtime` and `SchemaOptions.Runtime` require a selected
`engine.SchemaRuntime`, reused for every case's schema convergence.
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
Use the collection-valued parse or load functions for an env `for_each` that
selects several configs; singular functions reject that cardinality.

Set `projectconfig.AtlasLoadOptions.Context` or
`projectconfig.LoadOptions.Context` to govern project data-source connections,
runtime-variable reads, and subprocesses. A nil context uses a background
context. `projectconfig.Config.MigrationDirectoryFS` returns the immutable
filesystem behind a resolved `data.template_dir` URL; a host that consumes the
configured migration directory should check it before opening the URL as an
ordinary directory. `projectconfig.Config.MigrationDirectorySource` also
returns the sandbox-relative source path for hosts that synchronize newly
written migration files.

`projectconfig.Config.IgnoredConstructs` carries every Atlas CE-compatible
no-op name with its kind and source location, and `projectconfig.Merge`
preserves the collection. Ptah's CLI reports each entry; embedders decide how
to expose the same metadata.

`builtin.ValidateSchema` and `builtin.ValidateSchemaWithCapabilities` check
a complete `schemamodel.Database` without rendering SQL. They use the same
foreign-key and capability validation as ordered schema rendering and migration
planning.

Ordered rendering resolves constraint owners against the complete table set
before checking or emitting them. An explicit table identity takes precedence
over an unqualified match in another schema. Equal foreign-key names on
different PostgreSQL tables remain separate constraints.

`core/astbuilder` writes `core/ast` DDL nodes as method chains: `NewTable` and
`NewIndex` build one statement, `NewSchema` builds an `*ast.StatementList` in
declaration order. The builders return AST types and nothing of their own, so a
chain and a hand-written literal mix freely. They validate nothing — an unknown
type or an unresolved foreign key reaches the AST and is reported by
`engine/builtin` or by the database.

`core/yamlschema` reads Ptah's YAML authoring format: `Parse` from bytes,
`ParseFile` from a path, both returning the `*schemamodel.Database` that Go
annotations, HCL, SQL, and DBML also produce. Parsing refuses an unknown key
and refuses a second YAML document in the same stream, so a misspelled
attribute cannot pass as an intentional setting. Use `core/schemasource` when
the YAML is written by an external program rather than held in a file.

`schemamodel.Extension.Schema` is the PostgreSQL installation namespace.
`ast.ExtensionNode.Schema` and `SetSchema` preserve it through SQL rendering;
the renderer emits `CREATE EXTENSION ... WITH SCHEMA ...` in PostgreSQL's
required clause order. Empty means the target's default schema. Preserve the
field in custom schema codecs so an extension is not relocated silently.

`schemamodel.Finalize` can be called again after mutating schema input. It
rebuilds materialized embedded fields and marks them with
`Field.GeneratedFromEmbedded`; source declarations should leave that derived
metadata false.

`Database.EmbeddedSources` preserves source-only field and embedding
declarations needed to rebuild nested embedded fields after `Finalize` or
`Merge`. Embedders normally should not modify this bookkeeping directly. Keep
it when copying a finalized `Database` that will be finalized or merged again;
discarding it can also discard the source declarations behind materialized
`GeneratedFromEmbedded` fields.

MySQL-family readers populate the JSON-hidden
`catalog.Function.Definer` and `CurrentAccount` execution facts.
Database-aware `schemadiff.CompareWithDatabase` entry points use them to refuse
a modified `SQL SECURITY DEFINER` routine when recreating it would change the
executing account. Custom readers that supply a modified definer routine must
preserve both fields; missing facts fail closed with
`ptaherr.ErrInvalidSchemaDiff`. Offline comparison has no live ownership facts
and is not the safety boundary for applying such a replacement.

The separate [`testkit`](https://github.com/stokaro/ptah-testkit) module
(`ptah.run/testkit`) is an opt-in helper for tests that need real
databases. It keeps `testcontainers-go` out of Ptah's main module graph, lives
in its own repository, and versions independently. It depends on Ptah one way
and consumes a published release, so nothing here builds against it.

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
a revision is read, including through `GetRevisions`,
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

An `ALTER TABLE ... ADD CONSTRAINT ... USING INDEX` attachment with an explicit
constraint name updates the observed index name on the same target table.
The post-check, retry, and `RepairMigration` follow PostgreSQL's rename while
preserving the requirement for a usable index on that target.

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
`NewFSMigrationProvider` (with `WithStatementInterceptor` when an external
executor takes over statements) to load up/down pairs: loaded migrations carry
transaction modes, timeouts, source paths, and functions attached, so
execution policy cannot be discarded while assembling a provider.

`MigrateUpOptions.PlanObserver` receives the plan recalculated under the
migration lock before transaction-mode validation, including an empty plan. It
captures metadata but cannot abort execution. Use the abort-capable `Preflight`
hook for work that must run after static validation and before any schema or
revision change.

`MigrateUpOptions.PlanGuard` receives the same plan right after the observer
and can refuse it. It runs for every selection, an empty one included, and a
non-nil error stops the run before transaction-mode validation, `Preflight`, or
any schema or revision change. Use it when the caller approved a plan earlier
and has to know that the run executes that plan: the selection under the lock is
the only one the run acts on, and a history that moved in between shows up
there. `Preflight` does not fit that job, because it is skipped when the
selection is empty.

`MigrateUpOptions.DiscardRolledBackFailure` applies only to the Atlas
revision-table format; it has no effect with native Ptah metadata. It removes
only the failed revision written by the current invocation, and only after
transaction rollback succeeds. Existing dirty revisions and commit, rollback,
partial-progress, and unknown-outcome failures remain recorded and block
automatic retry.

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
Use `dbschema.DatabaseConnection.WithSessionOrCurrent` when the same operation
can be called either from a pool-backed connection or from an existing pinned
session; it pins only when needed and otherwise reuses the caller's current
session lifecycle.

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

`migration/schemadiff/difftypes.SchemaDiff` stores index additions and removals as
canonical `[]IndexRef` fields. Every index reference includes its owning
table. Live comparisons snapshot catalog identifier semantics into the diff so
comparison, policy, forward planning, and reverse planning share one source of
truth.

Bidirectional planning applies the selected feature owner's predictions to the
captured table. Named children and attached table settings keep separate
identities. Unchanged values and source knowledge limits survive; replaced
settings keep their target bindings. Unknown prior state is refused. A prediction
describes the accepted plan, not a new database inspection.

Bidirectional planning projects accepted index additions, removals, renames,
visibility, and comments into the reverse table capture. Skipped removals retain
their captured indexes and comments. Captured index definitions own their mutable
fields, so changing a source document cannot change an accepted index addition.
Index partitioning still needs an owner projection before it can accompany a
feature reversal.

Named CHECK creation and removal, constraint comments, and constraint validation
also update the reverse table capture. Simultaneous column changes appear in the
same capture. Validation remains in effect during rollback. Key, foreign-key, and
exclusion-constraint creation and removal use the selected `Target.Constraints`
service for backing-index and column effects. The service returns complete state
or an explicit unavailable reason. A feature reversal that needs an unavailable
prediction is refused.

The PostgreSQL service projects primary and unique backing indexes, column key
flags, and foreign-key defaults. Partition descendants, exclusion-index
expressions, and uncaptured server-generated NOT NULL names remain unavailable.

Table captures own their nested mutable definitions and catalog metadata. The
`Clone` methods on catalog tables, columns, constraints, and indexes, and on schema
model tables, fields, constraints, indexes, enums, and triggers provide the same
isolation to callers.

`SchemaDiff.ExtensionsModified` contains `ExtensionDiff` values with the name
and `FromSchema`/`ToSchema` placement. Default-schema normalization follows the
diff's identifier semantics. PostgreSQL extension moves currently return
`ptaherr.ErrInvalidSchemaDiff` before any AST is emitted; creates and drops are
still planned.

Row-level security policies use the same shape. `RLSPoliciesAdded` and
`RLSPoliciesRemoved` are `[]RLSPolicyRef`, `RLSPoliciesModified` is
`[]RLSPolicyDiff`, and every entry names the owning table next to the policy
name. That pair is the policy's identity: a PostgreSQL policy name is scoped to
its table, so two tables in one schema may each carry `tenant_isolation`. The
table half is compared under the diff's identifier semantics, not as a raw
string, so the desired spelling `public.orders` and the introspected spelling
`orders` resolve to one table in both the forward and the reverse direction.
A reference the target schema cannot resolve is rejected with
`ptaherr.ErrInvalidSchemaDiff`; it is never dropped from the plan.

Use `migration/generator.GenerateCheckpointFromShadow` for a SQL Server
schema whose live catalog semantics must survive checkpoint planning: the
shadow replay reads identifier semantics from the live catalog, where the
dialect-only offline rules stay conservative.

`migration/generator.PlanMigration` returns an unpublished plan bound to the
migration-directory snapshot used during planning. `MigrationPlan.WriteFiles`
rejects changed history with `generator.ErrMigrationDirectoryChanged` under
the shared cross-process publication lock.

A plan holds the migration directory open until it is published, so an
embedder that may not publish should `defer plan.Close()`. `Close` is a no-op
on a published plan and a no-op called twice; skipping it leaves the directory
held until the plan is garbage collected, which on Windows blocks removing or
renaming it in the meantime.

When its URL or connection selects SQLite, `PlanMigration` validates
`PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP` before resolving `OutputDir`. A malformed
value therefore fails before filesystem work; non-SQLite plans do not consult
the variable.

`generator.GenerateCheckpointFromShadow` and `shadow.VerifyBaseline` apply the
same SQLite-only validation before connecting to or mutating a shadow database.
The checkpoint path therefore cannot drop and replay a shadow database before
reporting a malformed value.

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

`migration/shadow` owns verification against a live disposable database:
`VerifyMigration` measures a candidate migration, `VerifyBaseline` measures a
replayed history against the target, `VerifyRollback` rehearses a rollback plan,
and `PlanDynamicRollback` derives rollback statements from the schema a version
defines rather than from a down body. `migration/generator` calls the first of
these when `ShadowDatabaseURL` is set.

When candidate or baseline shadow verification fails,
`generator.PlanMigration`, `generator.GenerateMigration`, and
`shadow.VerifyBaseline` preserve a typed
`*shadow.VerificationError`. Inspect it with `errors.As` instead of
parsing `Error()`:

```go
var shadowErr *shadow.VerificationError
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
Each `shadow.Mismatch` has a stable `Kind`, a human-readable `Message`, and the
available object, table, column, constraint, or changed-property fields.
Operational failures also preserve their underlying error through `Unwrap`;
a structural schema mismatch has no wrapped error. A successful verification
returns `nil` and continues planning; `shadow.VerificationResult` is only the
structured failure payload carried by `shadow.VerificationError`.

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
  `*shadow.VerificationError`;
- command wrappers should preserve typed errors instead of replacing them with
  string-only errors.

## API guardrails

CI protects the public API without duplicating its declarations in a committed
snapshot:

| Check | Purpose |
| --- | --- |
| `scripts/check-public-api.sh` | Fails if an importable library package is classified by neither ledger category. |
| `scripts/check-public-api-released.sh` | Compares stable packages against the latest `v0.x` release tag with `apidiff`. |
| `scripts/check-exported-docs.sh` | Requires documentation on exported functions and types. |
| `scripts/check-public-api-docs-sync.sh` | Keeps this page's package table aligned with the canonical ledger. |

Additive API changes receive normal code review. An intentional incompatible
change must update the relevant documentation and include an explicit approval
entry for the current release baseline in the same PR.

## Documentation-only packages

The ledger's second category is a short list of sample packages that stay
importable because published documentation reaches them. The migrator's godoc
examples import the sample migration directory they run against, and an example
nobody outside this module can import is not documentation.

They carry no compatibility guarantee of any kind. Their contents change with
the documentation they serve, `apidiff` does not compare them against a release
baseline, and they are absent from the table above for that reason. Read them;
do not build against them.

## Embedding guidance

Use [Reusable components](../components/) for task-oriented examples.
Use this page to decide whether a package is supported for embedding. Do not
import `internal/...` packages from another module, and do not import a
documentation-only package from production code.
