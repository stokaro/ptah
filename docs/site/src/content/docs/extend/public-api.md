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

Embedders use the packages below as Ptah's stable import surface. The repository
ledger defines it; `scripts/check-public-api-docs-sync.sh` keeps the table aligned.

The ledger classifies every importable library package into one of two
categories, and only the first is on this page. Anything it does not classify is
a program, a directory holding only tests, or behind a Go `internal/` boundary.

## Stable packages

| Package | Purpose |
| --- | --- |
| `atlascompat` | Stable wrappers for Atlas-compatible schema, SQL, and migration-sum behavior. |
| `config` | Project-level config loading helpers. |
| `config/projectconfig` | Typed Ptah/Atlas project config IR, including validated online-DDL policy. |
| `core/annotation` | The Go annotation contract: directive metadata, owner decoders, and the owner set a parse selects. |
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
| `core/schemaext` | Typed feature values, source coverage, explicit codecs, dependency captures, and conservative effects. |
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
| `dialect/clickhouse/chprobe` | Server-normalized row policy filters. |
| `dialect/clickhouse/chresolve` | Storage-setting resolution with retained intent and property origins. |
| `dialect/clickhouse/chschema` | Desired and observed storage settings and row policies with versioned model codecs. |
| `dialect/clickhouse/chsource` | Table and index property encoding and decoding that preserves setting intent. |
| `dialect/clickhouse/chreport` | Captured storage-setting counts and export omission labels. |
| `core/schemaproperties` | Selected table and index property decoding and export without engine-specific field access. |
| `dialect/clickhouse/chcompare` | Comparison of resolved table and index settings with explicit knowledge limits. |
| `dialect/clickhouse/chast` | Typed TTL and skipping-index operations with explicit codecs. |
| `dialect/clickhouse/chrender` | Owner-selected TTL and skipping-index rendering. |
| `dialect/clickhouse/chplan` | TTL and skipping-index planning with common-column dependencies. |
| `dialect/clickhouse/chreverse` | Reverse TTL and index definitions with recovery limits. |
| `dialect/clickhouse/chconvert` | Conversion between complete table/index declarations and observations. |
| `dialect/clickhouse/chdiff` | Captured prior and desired storage settings for directional changes. |
| `dialect/cockroachdb/crdbschema` | Desired and observed row-level TTL with versioned model codecs. |
| `dialect/cockroachdb/crdbsource` | Row-level TTL as `platform.cockroachdb` table properties, and source coverage. |
| `dialect/cockroachdb/crdbcompare` | Row-level TTL comparison that reads rewritten intervals as values. |
| `dialect/cockroachdb/crdbdiff` | Captured prior and desired row-level TTL for directional changes. |
| `dialect/cockroachdb/crdbast` | Typed row-level TTL operation with an explicit codec. |
| `dialect/cockroachdb/crdbrender` | Owner-selected row-level TTL rendering for CREATE and ALTER. |
| `dialect/cockroachdb/crdbplan` | Row-level TTL planning and dropped-table accounting. |
| `dialect/cockroachdb/crdbreverse` | Reverse row-level TTL changes with recovery limits. |
| `dialect/cockroachdb/crdbconvert` | Conversion between row-level TTL declarations and observations. |
| `dialect/cockroachdb/crdbreport` | Captured row-level TTL counts and export omission labels. |
| `dialect/spanner/spannerschema` | Desired and observed row deletion policies with versioned model codecs. |
| `dialect/spanner/spannersource` | The row deletion policy as `platform.spanner` table properties, and source coverage. |
| `dialect/spanner/spannercompare` | Row deletion policy comparison that reads rewritten intervals as values. |
| `dialect/spanner/spannerdiff` | Captured prior and desired row deletion policies for directional changes. |
| `dialect/spanner/spannerast` | Typed row deletion policy operation with an explicit codec. |
| `dialect/spanner/spannerrender` | Owner-selected row deletion policy rendering for CREATE and ALTER. |
| `dialect/spanner/spannerplan` | Row deletion policy planning in place. |
| `dialect/spanner/spannerreverse` | Reverse row deletion policy changes with recovery limits. |
| `dialect/spanner/spannerconvert` | Conversion between row deletion policy declarations and observations. |
| `dialect/spanner/spannerreport` | Captured row deletion policy counts and export omission labels. |
| `dialect/mssql/mssqlschema` | SQL Server security policy model, with every predicate binding, and codecs. |
| `dialect/mssql/mssqlcompare` | Security policy comparison with access assessment and the one-enabled-policy-per-table refusal. |
| `dialect/mssql/mssqldiff` | Directional security policy changes, their access effect, and the statements each needs. |
| `dialect/mssql/mssqlast` | Typed security policy operation with an explicit codec. |
| `dialect/mssql/mssqlrender` | Owner-selected security policy rendering for SQL Server. |
| `dialect/mssql/mssqlplan` | Security policy planning, table hand-offs and declarations. |
| `dialect/mssql/mssqlprobe` | A connected server's spelling of declared security policy predicates. |
| `dialect/mssql/mssqlrelation` | Tables and predicate functions a security policy binds. |
| `dialect/mssql/mssqlreverse` | Reverse security policy changes with recovery limits. |
| `dialect/mssql/mssqlconvert` | Conversion between security policy declarations and observations. |
| `dialect/mysql/mysqlschema` | Desired and observed MySQL-family table options and column settings with versioned codecs. |
| `dialect/mysql/mysqlsource` | Table and column platform property decoding and encoding. |
| `dialect/mysql/mysqlcompare` | Table option and column setting comparison that keeps declared values and plans no change of them. |
| `dialect/mysql/mysqldiff` | Table option and column setting change models that a planner and a reversal read. |
| `dialect/mysql/mysqlplan` | Table option and column setting accounting through table creation, rebuild and removal. |
| `dialect/mysql/mysqlrender` | Owner-selected CREATE TABLE options and column clauses. |
| `dialect/mysql/mysqlconvert` | Conversion between table option and column setting declarations and observations. |
| `dialect/mysql/mysqlreport` | Captured table option and column setting counts. |
| `dialect/mssql/mssqlreport` | Captured security policy and predicate counts. |
| `dialect/timescaledb/tsast` | TimescaleDB operations and codecs. |
| `dialect/timescaledb/tscompare` | TimescaleDB comparison. |
| `dialect/timescaledb/tsconvert` | TimescaleDB conversion. |
| `dialect/timescaledb/tsdiff` | TimescaleDB changes. |
| `dialect/timescaledb/tsplan` | TimescaleDB planning and refusals. |
| `dialect/timescaledb/tsprobe` | Server-normalized aggregate bodies. |
| `dialect/timescaledb/tsrelation` | Aggregate-to-hypertable dependencies. |
| `dialect/timescaledb/tsrender` | TimescaleDB rendering. |
| `dialect/timescaledb/tsreport` | TimescaleDB reports. |
| `dialect/timescaledb/tsreverse` | TimescaleDB reversal. |
| `dialect/timescaledb/tsschema` | TimescaleDB models and codecs. |
| `dialect/timescaledb/tssource` | TimescaleDB Go annotation directives. |
| `dialect/ydb/ydbast` | Typed YDB feature operations and their codecs. |
| `dialect/ydb/ydbcompare` | Coverage-aware comparison of individual YDB feature objects. |
| `dialect/ydb/ydbconvert` | YDB feature representation conversion. |
| `dialect/ydb/ydbcoordination` | Coordination-node models and codecs. |
| `dialect/ydb/ydbdiff` | Directional YDB feature changes. |
| `dialect/ydb/ydbexternal` | External data source and external table declarations, observations, checks, statements, and codecs. |
| `dialect/ydb/ydbrender` | Rendering and validation of YDB feature operations. |
| `dialect/ydb/ydbreplication` | Async replication and transfer declarations, observations, checks, statements, and codecs. |
| `dialect/ydb/ydbreport` | Inventory and omission reports for captured YDB feature values. |
| `dialect/ydb/ydbreverse` | Feature reversal and recovery limits. |
| `dialect/ydb/ydbplan` | Feature declaration and migration planning. |
| `dialect/ydb/ydbschema` | YDB feature values and model codecs. |
| `dialect/ydb/ydbscheme` | Shared physical paths for object dependency planning. |
| `dialect/ydb/ydbsecret` | Secret declarations, observations, rotation requests, statements, and codecs, without values. |
| `dialect/ydb/ydbstreaming` | Streaming-query declarations, observations, settings, and codecs. |
| `dialect/ydb/ydbsyntax` | YQL quoting helpers. |
| `dialect/ydb/ydbtopic` | Topic and consumer declarations, observations, checks, statements, and codecs. |
| `dialect/ydb/ydbworkload` | Pool and classifier models and codecs. |
| `catalog` | Shared database schema types. |
| `docs` | Ptah's own documentation embedded in the binary as an `embed.FS`. |
| `feature/pgpolicy` | PostgreSQL row-security policy and table-state models and codecs. |
| `feature/pgpolicy/policycompare` | Comparison of PostgreSQL row-security policies and table switches. |
| `feature/pgpolicy/policyconvert` | Projection of PostgreSQL row-security values between representations. |
| `feature/pgpolicy/policyplan` | Planning of PostgreSQL row-security changes into ordered operations. |
| `feature/pgpolicy/policyprobe` | A connected server's spelling of declared PostgreSQL row-security policies. |
| `feature/pgpolicy/policyreport` | Inventory counts of PostgreSQL row-security policies and switches. |
| `feature/pgpolicy/policyrender` | Rendering of the PostgreSQL row-security operations. |
| `feature/pgpolicy/policyreverse` | Reversal of PostgreSQL row-security changes. |
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

`engine.New` registers caller-supplied providers; duplicate targets and aliases
are refused. `builtin.New` selects the bundled providers. Runtime services
propagate errors and cancellation without partial output. Rendering returns one
`Fragments` entry per input node: empty for no output, or holding one or more
statements. `Result.SQL()` joins fragments without adding separators.
`renderer.Render` validates this contract for direct service calls.

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

`Provider.Declarations` selects owners for `Runtime.PlanDeclarations`, which
plans standalone creation from desired and operation codecs. Each object and
operation is accounted for; refusals retain diagnostics indexed to input objects.
Contributions join the common creation graph. `atlascompat.SchemaToAST` requires
context, a lowering runtime, schema, target, and capabilities. It decodes source
properties and shares these checks with whole-schema rendering, and returns no
partial AST list.

`Request.CommonSteps` supplies isolated accepted operations; `Result.Rewrites`
claims their replacements. `plangraph.ScheduleRewritten` validates claims,
preserves logical writes, redirects dependencies, and rejects conflicts and cycles.
Owners supply replacement semantics and execution requirements; grouping implies
no transaction. `AlterTable` requests assessment of unchanged attached state.
Process adapters define explicit common-operand wire models instead of
serializing Go AST structs.

`Provider.Conversions` assigns target/kind pairs to `Runtime.ConvertFeatures`.
The provider must own both representation codecs. Conversion preserves batch
order. `Registry.ConvertCoverage` retains unknown state; installing a codec
does not establish source knowledge.

`Provider.Comparisons` assigns named object and change kinds to a comparison
service. `Runtime.CompareObjects` requires an explicit completion receipt and returns typed changes, effective desired
objects for captures, and structured diagnostics for undecided state. It
rejects dropped declarations and duplicate child changes during parent creation
or removal. Service errors and cancellation return no partial result. The YDB
service preserves inspected changefeeds omitted by an incomplete desired source.

`Provider.FacetComparisons` registers attached settings and required `OwnerKinds`.
Each service receives only matching owners, including those without values.
Misplaced values or subject coverage fail before dispatch.

`Runtime.CompareFacets` preserves common identity and source knowledge. Explicit
knowledge limits override partial values. Parent creation or removal includes
attached settings, so separate facet operations are refused. Providers check
`FacetComparisonRequest.Includes` for each model/owner pair: exclusions permit
neither changes nor diagnostics, even with complete coverage.

`Runtime.CompareFeatures` validates named objects and facets before dispatch,
discarding output if either comparison fails. Replies set `Complete`. The
migration comparator applies effective table, index and materialized view
facets before capturing common changes; index identity follows the target's
table or schema namespace. Other attachment points refuse until identity
capture exists. Registering providers never enrolls source coverage.

A change value implementing `schemaext.OwnerReplacement` says it needs its
owner replaced. The comparator replaces a materialized view for it and refuses
it on a table or an index. `MaterializedViewDiff.FeatureChanges` carries view
changes, and `Replaces` reports whether the entry is a drop and a create.

Comparison binds table and index coverage to connection identifiers and the default
database; reverse CREATE projection binds table coverage. Binding preserves explicit
schemas, knowledge limits, and source snapshots and rejects colliding claims.

Table selection preserves the selected tables' feature children and their source
coverage. `Coverage.SelectSubjects` keeps kind-wide knowledge while filtering
explicit records. Use it with the same object projection; removing an override
alone changes which knowledge a lookup returns.
Unrepresentable enrolled state and explicit subject limits require a selected
comparison handler even without concrete values. An uninspected namespace alone
makes no claim about target applicability; the runtime preserves that knowledge.
A known empty namespace alone does not require target support for its model.

`Provider.Reporting` registers model omission labels and metrics, frozen at
runtime construction. `Runtime.ReportFeatures` validates each batch, calls each
service once, and preserves order. Missing handlers and malformed replies fail.
Reporting needs no target; an optional target supplies source context only.
Metrics count captured values, including disabled YDB changefeeds and consumers.
Zero does not establish inspected absence. Statistics and DBML/JSON omission
reports use these services.

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
return `ResolvedFacets` records keyed by declared table or index identities.
Duplicate owners, undeclared kinds, and new target scopes are refused. Source
facets, bindings, observations, and knowledge stay unchanged. Incomplete replies return
no diff. `SchemaDiff.TablePreparation` retains independent source and prepared
captures as comparison provenance, including through reversal.

`Target.Creations` selects `schemaprojection.TableCreationService` for source
files used as current state. It predicts CREATE defaults and column keys.
`TableCreation.Facets` contains records keyed by captured table or index identity,
separate from source intent. Duplicate or invented owners, changed spelling, and
new bindings are refused. The host validates completeness, column names, ownership,
and codecs. Source bindings, exclusions, and knowledge limits stay intact. New
kinds are target-bound with coverage only for predicted owners, not their siblings.
Predictions prove no inspection or execution.
Providers with no additional effects register `IdentityCreations` explicitly;
a missing service is unavailable.

Feature providers register their local model codecs through `Provider.Codecs`.
`Runtime.Codecs` exposes context-aware batch encoding, decoding, and canonical
fingerprints. A document records its provider, kind, representation, version,
and model-definition hash. Unknown or incompatible definitions are errors;
registering a codec alone does not grant a target support for that feature.

`chschema.DesiredTable` and `DesiredIndex` distinguish omitted settings,
defaults, and explicit values. `ObservedTable` and `ObservedIndex` require
complete settings; table observations keep sorting and primary keys separate.
The versioned codecs preserve these distinctions, including unsigned 64-bit
index granularity. Mixing table facets with storage overrides is refused.

`chcompare.Service` and `IndexService` compare resolved table and surviving-index
settings. `chdiff.Table` and `chdiff.Index` retain complete before and after
operands. Missing evidence for declared settings is undecided; unmentioned
objects stay unmanaged, and explicit inspection limits remain visible.

`chplan.Service` orders MergeTree TTL changes after required column additions
and before dependent removals, and refuses changes to columns used by retained
storage. `chplan.IndexService` replaces an index whose type or granularity
changes, unless the host already replaces it. `chast.AlterTTL`,
`AddSkippingIndex`, and `DropSkippingIndex` use explicit codecs and `chrender`
handlers outside transactions. Wrap them in `ast.ExtensionAlterOperation` under
`ast.AlterTableNode`; non-owning targets refuse them without partial SQL.

`chreverse.Service` and `IndexService` restore captured definitions and report
what they cannot recover: expired TTL data and materialized index data. Their
state projections feed reverse planning without claiming new inspection.

`chschema.DesiredRefresh` and `ObservedRefresh` hold a materialized view's
refresh schedule. `chsource.RefreshFacets` reads a Go annotation's clause in
the spelling the server stores. `chcompare.RefreshService` adopts a schedule an
undescribing source leaves unmanaged; `chdiff.Refresh` replaces the view when a
schedule or its `APPEND` is gained or lost. `chplan.RefreshService` plans any
other change as `chast.ModifyRefresh` in the ALTER envelope naming the view.

`chschema.DesiredRowPolicy` and `ObservedRowPolicy` hold a ClickHouse row
policy as a feature object named by database, table and policy through
`RowPolicyRef`: the SELECT filter, permissive or restrictive composition, and
a `RoleSelection` of named users and roles or `TO ALL` with exceptions. A
database-wide policy has no representation and its reference is refused.

`chresolve.Table` and `chresolve.Index` retain declarations, resolved settings,
and property origins. Existing objects require observations for omitted settings.
On creation, a default primary key inherits the sorting key; skipping indexes
use Ptah's `minmax` type and granularity `1`. Missing evidence returns no partial result.

`chconvert.Service` converts complete table/index observations to explicit
declarations and resolved declarations to predictions. Invalid values or
unresolved settings discard the whole batch.

`Provider.Properties` declares source property ownership by target, format, and
feature kind. The runtime validates complete batches before dispatch and rejects
changed kinds, missing results, and properties outside the owner's declared keys.
Empty values remain distinct from missing properties. Conversion failures and
cancellation return no partial result or new catalog knowledge.

`chsource.Service` and `IndexService` preserve ClickHouse intent through the
table and index platform property formats. Bare keys carry explicit values; a
`.state` suffix with value `default` requests the creation rule.

`schemaproperties.DecodeTables` and `DecodeIndexes` attach decoded properties as
facets bound to the selected target, leaving other target groups untouched.
Unclaimed table keys stay for their common readers; an unclaimed index key of
the selected target is refused. An index owner claiming `type` consumes the
common `Type`. The `Encode` functions export facets through the same owner.
They refuse mixed typed/property declarations and duplicate alias keys.

Export also refuses facets whose scope or presence the property format cannot
preserve. Neither operation adds inspection coverage. Native Go export,
whole-schema rendering, validation, and comparison decode source properties
through the selected runtime. File-to-file comparison resolves the current
document's creation rules before projecting it into catalog form. This
prediction preserves explicit knowledge limits and proves no inspection or
execution.

The ClickHouse reader supplies observed table, index and refresh facets; table
coverage is limited to retained tables. `chreport.Service`, `IndexService` and
`RefreshService` supply counts and omission labels. The bundled runtime
registers every service above.

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

An object carries its source binding in `Object.Targets`, and the binding
travels with the object through collections and codecs. `Objects.ForTarget`
leaves out an object bound to other targets and keeps no record of it, so a
caller that must not read its absence as deletion intent asks
`schemamodel.OmissionsForTarget` first, as the comparison does.

`Registry.EncodeChanges` writes change records as `EncodedChange`: the subject
beside an envelope naming the owner, the change kind and the codec version.
`DecodeChanges` reads them back with the same codecs and refuses a kind the
registry does not hold.

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

`dbschema.ReadRehearsalSchemaContext` selects a reader's optional
`catalog.RehearsalSchemaReader` to observe baseline environment, otherwise using
its ordinary read. Unknown state stays unknown. Observations grant no write
authority: validate the entire generated baseline against its execution scope
before applying any statement.

`dbschema.DatabaseConnection.WithSession` pins one physical database session
for the duration of a callback and rebinds the dialect reader, writer, and SQL
runner to that session. Use it for cleanup, replay, and inspection workflows
that depend on session-local state or SQLite attached-database visibility.

Root MySQL capability metadata remains conservative. On MySQL 8.4+ the scoped
connection refines its referenced-key policy from
`restrict_fk_on_non_standard_key` on the pinned physical session before the
callback, so planning and execution use the same effective policy.

The scoped connection must stay inside the callback. Ptah discards the physical
connection afterward to prevent state leaks. `WithSessionOrCurrent` reuses an
existing pinned session and its lifecycle, or pins a pool-backed connection.

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

A statement rendered from an operation that implements
`schemaext.AccessEffectSource` also carries `access` (`widens`, `narrows`,
`unchanged` or `unknown`) and `access_reason`. Its severity is the higher of
the lifecycle effect and the access effect: a widening or unknown access effect
is destructive and a narrowing is a warning. An assessment the owner did not
establish reads as `unknown`, never as `unchanged`. Drift and diff findings
count these changes under `feature_access_widened:<kind>`,
`feature_access_narrowed:<kind>`, `feature_access_unchanged:<kind>` and
`feature_access_unknown:<kind>`.

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
- a plan a planner built in breach of the planning contract, such as an owner
  operation sharing a node with other work, supports `errors.Is` with
  `migration/planner.ErrInvalidPlan`;
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
