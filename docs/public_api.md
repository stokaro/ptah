# Public Go API

Ptah is pre-GA, but embedders need a documented surface and a typed error
contract. Packages in this document are the only non-command, non-example,
non-fixture packages that may remain importable without an explicit review.
For task-oriented guidance and examples, see the
[Reusable components](site/src/content/docs/extend/components.md)
guide.

## Stable Embedder API

These packages are intended for application and tool embedders:

- `ptah.run/atlascompat`
- `ptah.run/config`
- `ptah.run/config/projectconfig`
- `ptah.run/core/annotation`
- `ptah.run/core/ast`
- `ptah.run/core/astbuilder`
- `ptah.run/core/coverage`
- `ptah.run/core/featureplan`
- `ptah.run/core/goschema`
- `ptah.run/core/manageddata`
- `ptah.run/core/objectidentity`
- `ptah.run/core/plangraph`
- `ptah.run/core/schemamodel`
- `ptah.run/core/platform`
- `ptah.run/core/platform/capability`
- `ptah.run/core/platform/identifier`
- `ptah.run/core/ptaherr`
- `ptah.run/core/query`
- `ptah.run/core/renderer`
- `ptah.run/core/schemacapture`
- `ptah.run/core/schemapreparation`
- `ptah.run/core/schemaproperties`
- `ptah.run/core/schemaext`
- `ptah.run/core/schemaprojection`
- `ptah.run/core/schemavalidation`
- `ptah.run/engine`
- `ptah.run/engine/builtin`
- `ptah.run/core/schemasource`
- `ptah.run/core/sqlutil`
- `ptah.run/core/yamlext`
- `ptah.run/core/yamlschema`
- `ptah.run/dbschema`
- `ptah.run/dialect/clickhouse/chast`
- `ptah.run/dialect/clickhouse/chcompare`
- `ptah.run/dialect/clickhouse/chconvert`
- `ptah.run/dialect/clickhouse/chdiff`
- `ptah.run/dialect/clickhouse/chplan`
- `ptah.run/dialect/clickhouse/chprepare`
- `ptah.run/dialect/clickhouse/chprobe`
- `ptah.run/dialect/clickhouse/chresolve`
- `ptah.run/dialect/clickhouse/chschema`
- `ptah.run/dialect/clickhouse/chsource`
- `ptah.run/dialect/clickhouse/chreport`
- `ptah.run/dialect/clickhouse/chrender`
- `ptah.run/dialect/clickhouse/chreverse`
- `ptah.run/dialect/cockroachdb/crdbast`
- `ptah.run/dialect/cockroachdb/crdbcompare`
- `ptah.run/dialect/cockroachdb/crdbconvert`
- `ptah.run/dialect/cockroachdb/crdbdiff`
- `ptah.run/dialect/cockroachdb/crdbplan`
- `ptah.run/dialect/cockroachdb/crdbrender`
- `ptah.run/dialect/cockroachdb/crdbreport`
- `ptah.run/dialect/cockroachdb/crdbreverse`
- `ptah.run/dialect/cockroachdb/crdbschema`
- `ptah.run/dialect/cockroachdb/crdbsource`
- `ptah.run/dialect/mssql/mssqlast`
- `ptah.run/dialect/mssql/mssqlcompare`
- `ptah.run/dialect/mssql/mssqlconvert`
- `ptah.run/dialect/mssql/mssqldiff`
- `ptah.run/dialect/mssql/mssqlplan`
- `ptah.run/dialect/mssql/mssqlprobe`
- `ptah.run/dialect/mssql/mssqlproperty`
- `ptah.run/dialect/mssql/mssqlrelation`
- `ptah.run/dialect/mssql/mssqlrender`
- `ptah.run/dialect/mssql/mssqlreport`
- `ptah.run/dialect/mssql/mssqlreverse`
- `ptah.run/dialect/mssql/mssqlschema`
- `ptah.run/dialect/mysql/mysqlcompare`
- `ptah.run/dialect/mysql/mysqlconvert`
- `ptah.run/dialect/mysql/mysqldiff`
- `ptah.run/dialect/mysql/mysqlplan`
- `ptah.run/dialect/mysql/mysqlrender`
- `ptah.run/dialect/mysql/mysqlreport`
- `ptah.run/dialect/mysql/mysqlschema`
- `ptah.run/dialect/mysql/mysqlsource`
- `ptah.run/dialect/postgres/pgproject`
- `ptah.run/dialect/spanner/spannerast`
- `ptah.run/dialect/spanner/spannercompare`
- `ptah.run/dialect/spanner/spannerconvert`
- `ptah.run/dialect/spanner/spannerdiff`
- `ptah.run/dialect/spanner/spannerplan`
- `ptah.run/dialect/spanner/spannerrender`
- `ptah.run/dialect/spanner/spannerreport`
- `ptah.run/dialect/spanner/spannerreverse`
- `ptah.run/dialect/spanner/spannerschema`
- `ptah.run/dialect/spanner/spannersource`
- `ptah.run/dialect/timescaledb/tsast`
- `ptah.run/dialect/timescaledb/tscompare`
- `ptah.run/dialect/timescaledb/tsconvert`
- `ptah.run/dialect/timescaledb/tsdiff`
- `ptah.run/dialect/timescaledb/tsplan`
- `ptah.run/dialect/timescaledb/tsprobe`
- `ptah.run/dialect/timescaledb/tsrelation`
- `ptah.run/dialect/timescaledb/tsrender`
- `ptah.run/dialect/timescaledb/tsreport`
- `ptah.run/dialect/timescaledb/tsreverse`
- `ptah.run/dialect/timescaledb/tsschema`
- `ptah.run/dialect/timescaledb/tssource`
- `ptah.run/dialect/ydb/ydbast`
- `ptah.run/dialect/ydb/ydbcompare`
- `ptah.run/dialect/ydb/ydbconvert`
- `ptah.run/dialect/ydb/ydbcoordination`
- `ptah.run/dialect/ydb/ydbdiff`
- `ptah.run/dialect/ydb/ydbexternal`
- `ptah.run/dialect/ydb/ydbplan`
- `ptah.run/dialect/ydb/ydbrender`
- `ptah.run/dialect/ydb/ydbreplication`
- `ptah.run/dialect/ydb/ydbreport`
- `ptah.run/dialect/ydb/ydbreverse`
- `ptah.run/dialect/ydb/ydbschema`
- `ptah.run/dialect/ydb/ydbscheme`
- `ptah.run/dialect/ydb/ydbsecret`
- `ptah.run/dialect/ydb/ydbstreaming`
- `ptah.run/dialect/ydb/ydbsyntax`
- `ptah.run/dialect/ydb/ydbtopic`
- `ptah.run/dialect/ydb/ydbworkload`
- `ptah.run/catalog`
- `ptah.run/docs`
- `ptah.run/feature/pgpolicy`
- `ptah.run/feature/pgpolicy/policycompare`
- `ptah.run/feature/pgpolicy/policyconvert`
- `ptah.run/feature/pgpolicy/policyplan`
- `ptah.run/feature/pgpolicy/policyprobe`
- `ptah.run/feature/pgpolicy/policyreport`
- `ptah.run/feature/pgpolicy/policyrender`
- `ptah.run/feature/pgpolicy/policyreverse`
- `ptah.run/feature/synonym`
- `ptah.run/migration/datadiff`
- `ptah.run/migration/dbtest`
- `ptah.run/migration/diffpolicy`
- `ptah.run/migration/generator`
- `ptah.run/migration/importer`
- `ptah.run/migration/lint`
- `ptah.run/migration/migrationfile`
- `ptah.run/migration/migrator`
- `ptah.run/migration/planner`
- `ptah.run/migration/risk`
- `ptah.run/migration/safety`
- `ptah.run/migration/schemadiff`
- `ptah.run/migration/schemadiff/difftypes`
- `ptah.run/migration/seeder`
- `ptah.run/migration/shadow`

`atlascompat` is a narrow compatibility surface for external Atlas parity and
conformance tooling. It intentionally wraps parser, HCL schema,
conversion, and migration sum internals without making those implementation
packages importable directly.

`atlascompat.SchemaToAST` takes caller context, a selected `LoweringRuntime`,
the desired schema, target, and capabilities. The runtime supplies declaration
planning and the target's source-property owners: the schema's table and index
properties are decoded into owner facets before lowering, as rendering and
comparison decode them. It returns a statement list and an error.

Invalid identities and feature state without an AST lowering path return a nil
list. Table facets and named feature children remain attached to their table,
and index facets to their index node. Facet values and target bindings reach
the selected renderer unchanged; lowering does not establish that the renderer
supports them. Other facet placements are refused. Standalone objects use the
selected declaration owners and join the common creation graph. Missing owners,
incomplete receipts, graph conflicts, and cancellation return no statement
list.
Coverage records describe source knowledge and never authorize destructive SQL.
A concrete table facet with an explicit absent claim is refused before visiting
any statement.

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
`AtlasLoadOptions.RejectCompositeSchema` refuses a `data "composite_schema"`
block with the community binary's own message; its zero value evaluates the
block. `Config.CompositeSchema` returns the parts behind a
`CompositeSchemaMarkerScheme` value in `Config.SchemaSources`, as a copy, and
`Config.HasCompositeSchemaSource` reports whether a source is one.
`AtlasLoadOptions.Context` and `LoadOptions.Context` govern project data-source
database calls, runtime-variable reads, and subprocesses; nil uses a background
context.

`Config.MigrationDirectoryFS` returns the immutable filesystem behind a
resolved `data.template_dir` URL. Embedders that consume migration directory
URLs from project config must check this method before treating the URL as a
local or remote directory. `Config.MigrationDirectorySource` additionally
returns the sandbox-relative backing path that a host can use when a writing
operation must synchronize new migration files.

`Config.IgnoredConstructs` identifies names that
Atlas CE accepts without acting on, with kind and source location. `Merge`
preserves this diagnostic metadata from both inputs. Ptah's command layer warns
for each entry; embedders can choose their own reporting policy.

`core/objectidentity` provides the structured identity and reference rules used
by comparison and planning. Source spelling stays separate from normalized
identity. Custom feature kinds use the same contract as common objects. A
component the target's default filled in is marked `Defaulted`, and its
`Source` holds that default; `Part.Authored` returns what the source wrote,
the empty string for a defaulted component, for a consumer that resolves an
unqualified name under a default of its own.

`core/manageddata.LoadRowValues` reads a managed-data YAML file while retaining
scalar spelling and tags in `schemamodel.ManagedRow`. `LoadRows` reads resolved Go
values for row comparison. `ResolveRows` resolves carried declarations without
opening a file. The schema model owns the data types; this parser package owns
YAML interpretation and source-file access. Captured schema contracts therefore
do not import a parser to carry a declaration.

`core/annotation` is the contract between the Go annotation frontend,
`core/goschema`, and the feature owners. An `Extension` holds:

- an owner's directives and their decoder;
- the attributes it adds to the frontend's own directives, the directive's
  own attributes it reads beside them, such as an index's type, and the
  decoders that turn them into facets and into options the common model
  carries, such as an index's WITH options;
- the models it produces;
- the frontend directives whose target-scoped declarations it reads;
- the `ptah:schema:notdescribed` kinds it reads;
- the coverage a Go annotation source holds about those models, given the
  not-described declarations of those kinds the source wrote.

A decoder reads one declaration at a time. An owner whose declarations refer
to each other or to the file's tables, such as a consumer of a changefeed
declared later in the file, sets `File` instead, and its `FileDecoder` reads
every declaration of one file before `Finish` contributes what they declare
together. A `FileDecoder` that also implements `FileCoverage` narrows the
owner's claim for its file once it has finished.

An owner may also read declarations of the frontend's own directives that
their target scope makes its own. A `TargetScope` names a directive, the
dialects whose declarations the owner reads, and whether it also reads the
declarations that name none: the row-security owner reads `rls:policy` and
`rls:enable` without a scope or scoped to PostgreSQL-family targets.
`Set.TargetOwner` is the one place that routes a declaration by its scope, and
it refuses a scope that names one owner's targets beside others. The routed
declaration carries its `Targets`, and a facet it contributes keeps them. `Tables.Owning` places a part on its table by the rule the frontend
applies to owner facets. A `DeclarationError` names the attribute a refusal is
about and, from `Finish`, the declaration it refuses; the frontend reports the
refusal there as a `ptaherr.ParseError` that wraps the owner's error.

`NewSet` freezes the extensions one parse selects and refuses a directive, an
attribute of one directive, a not-described kind, or a model that two owners
claim. `Set.Reader` reads one file's owner declarations for the frontend.
`engine.Provider.Annotations` registers extensions, and
`engine.Runtime.Annotations` returns the frozen set.

Every goschema entry point takes the set first. The zero set is refused with
`ErrUnselected`, and `None` reads the frontend's own directives only. A parse
that does not select an owner ignores the owner's directives, as it ignores a
directive it does not know. It refuses the owner's attributes and
not-described kinds as unknown, and it does not enroll the owner's models in
coverage, so they stay unknown rather than absent.

`core/schemacapture` holds complete table declarations and observations for
contextual services. Both include common children, named feature objects, and
feature coverage. `Clone` isolates mutable common definitions; feature containers
retain immutable ownership. The zero value means no table was captured. Neither
an empty child list nor a projected observation proves absence or execution.
These data contracts import no migration pipeline or concrete provider package.
`schemacapture.DeclareTable` assembles declarations for comparison and document
projection. `ConstraintsFor` and `EnumsFor` select independent child definitions.
`difftypes.TableObservationFor` assembles comparison-time observations.

`core/featureplan.Service` plans a batch of typed changes against captured
parents and explicit target facts. `Provider.Planning` assigns change kinds and
attached `ParentKinds` to their owner and declares the operation kinds it may
return. Parent models require both desired and observed codecs. The runtime validates
the entire request before dispatch. Local codecs isolate requests and replies
without encoding or semantic service calls. Missing services, malformed replies,
errors, and cancellation return no result. A successful result explicitly sets
`Complete`, including a no-op.

A completed refusal returns `Diagnostics` and no contributions or receipts.
Diagnostics contain data and optional change or parent indexes. The runtime
validates their assigned kinds and maps change indexes back to the caller's
batch. It collects refusals from every selected service and discards all
operations if any service refuses. Provider failures and cancellation discard
diagnostics as well. `Result.Err(request)` converts a validated outcome into
`featureplan.RefusalError` for local callers, preserving schema and capability
error identities. The common planning host performs this conversion before
lowering operations. Transport adapters carry the diagnostic data.

A captured table's `Action` requests `DropTable` or `RebuildTable` assessment.
`CreateTable` asks owners about a table the plan creates: its CREATE TABLE
carries the settings attached to it, and each owner creates the table's named
children in steps of its own. The PostgreSQL-family planner sends every
created table that has a named child, and a child no owner accounts for is
refused rather than dropped.

The zero action supplies context only. The runtime calls every assigned parent
model for the target even without child changes, concrete feature values, or
source coverage. Callers cannot narrow the registered `ParentKinds`. Concrete
attached state without a parent planning owner is refused before dispatch.
An unavailable parent planning service is an error even for an empty capture.
`Table.CapturedKinds` names the kinds whose state the capture carries, on the
table, its columns, indexes, constraints and triggers, in owned objects, and in
coverage records under it; the runtime refuses an action on a table any of them
has no owner for, and a host that sends only such tables asks the same
question.

Each `ParentPlan` accounts for one model and table action, preserving the exact
subject, kind, and action. Its strategy describes how the parent operation treats
that state. Unknown observations do not become absent because the child diff is
empty. Missing receipts and incomplete responses are errors. Parent strategies
may contribute steps, which must join the same graph as other changes.

Each `ChangePlan` preserves its input subject and kind and records a strategy,
including changes that need no operation. Its step references, together with parent
receipts, account for every emitted operation. Operations retain typed payloads, grammatical placement,
single-line notes, and parent identities. A service reply must join the complete
host graph before its operations can be used; dependencies may refer to other
owners' steps. An operation's `Phase` selects the host window its step joins.
Every host accepts `PhaseDefault`. The migration planners of the PostgreSQL
family and of SQL Server accept `PhaseDependent`: each places the operation
after its creations and changes and before its removals, in one window whose
creations and drops the owner orders. Every other host, and a whole-schema
render, refuses it. The runtime refuses a phase the contract does not define.

`core/featureplan.DeclarationService` plans authored standalone objects, and
the named children of a declared table, for schema creation.
`Provider.Declarations` assigns desired model kinds and allowed operation kinds
to an owner. This service requires no observed or change codec:
an authored CREATE request does not claim that a live object was inspected and
found absent. Declared tables and common-step metadata provide dependency context.
Each service receives one isolated batch, and each `DeclarationPlan` preserves
the input identity and accounts for the operations that create it. The runtime
refuses duplicate objects, a child of a table the request does not declare,
unregistered kinds, missing receipts, and unaccounted operations before
returning a successful reply.

`DeclarationRuntime.DeclaresKind` says which
kinds a target declares: a whole-schema render plans a table's child of such a
kind through its owner, scheduled against the common creation steps, and
creates any other child with its table.

`DeclarationResult.Diagnostics` records completed refusals with optional object
indexes. Any refusal discards every operation and receipt; provider failures
discard the whole result. `DeclarationResult.Err(request)` preserves local schema
and capability error identities. Successful contributions still need scheduling
with the complete host graph. These local request types are not a wire protocol;
a process adapter maps common definitions to explicit protocol records.

`dialect/ydb/ydbplan.Service` validates changefeed operands against desired and
observed table captures. It plans stream replacement and backing-topic changes
with explicit dependencies. A host-selected table rebuild still owns attached
streams; the service validates and accounts for those changes without emitting
them again. Drop and rebuild assessment requires complete captured changefeed
coverage, including subject-specific limits. A known stream is removed with its
parent; an uninspected or unrepresentable stream blocks the operation. Stream
state remains irrecoverable. A receipt describes planning ownership, not
successful execution. Semantic refusals return a completed diagnostic result;
invalid service setup and execution failures remain errors.

`core/plangraph.Schedule` combines owner contributions into a deterministic
dependency order before rendering. Steps carry typed payloads, structured object
effects, transaction requirements, and conservative safety assessments. It refuses
missing steps, cycles, competing writers, unordered reads and writes, and
inconsistent declared lifecycles. Cancellation returns no partial plan.
Ordering preserves unknown effects and transaction requirements as unknown.
The scheduler copies metadata slices; payload ownership stays with the caller.
A step's `Placement` is a preference, never a dependency: among the steps ready
to run, `PlacementEarly` steps come first, so an owner keeps a step ahead of the
host's statements without depending on one of them.

`LifecycleDependencies` orders effects of different contributions on one
subject the only way a lifecycle allows: a drop before another contribution's
creation, a creation before a read, a read before a drop, and an alteration
before a read unless the reader is early. A host passes every contribution
before rewrites merge them; a pair the contributions already order is left to
`Schedule`.

`ScheduleRewritten` lets a target owner replace explicitly named host steps with
one contributed ordering unit. It transfers incoming and outgoing dependencies
to that unit before calling the same scheduler. Claims cannot overlap or name
steps outside the supplied host contribution. Each source needs a known object
footprint; its logical write actions must remain in the replacement. The owner
supplies the replacement's complete semantics, risk, and transaction assessment.
Combining steps does not establish transactional execution.

`featureplan.Request.CommonSteps` exposes accepted common operations with their
identities, effects, and available column-addition operands. Each selected service
receives an independent snapshot. `Result.Rewrites` claims source steps by identity
and names a contributed replacement; the runtime validates and copies those claims,
and the host applies them to its complete graph. A refusal discards the claims.
`AlterTable` requests parent assessment even when no attached feature changed.
Common AST operands are local data; process adapters must map them to an explicit
protocol schema instead of serializing Go AST structs.

YDB changefeed changes contribute graph steps with table references and explicit
drop-before-add dependencies. Surrounding target-planner phases participate in
that order as batches; their object-level effects remain unspecified. Parent
rebuilds own their attached streams. A target planner orders the statements
inside its own phases; the graph orders owner steps against those phases as
whole batches.

`core/renderer.Service` renders a complete batch with a context and explicit
capabilities. `engine.New` freezes the caller's provider selection and rejects
conflicting target names or aliases. An empty runtime has no built-ins.
`Runtime.Render` propagates service failures and cancellation without partial
SQL. An accepted result sets `Complete` and contains one `Fragments` entry per
input node, including
empty entries for nodes that emit nothing. A fragment can contain several
statements. `Result.SQL()` joins them without adding separators. Use
`renderer.Render` to invoke a selected service and reject incomplete replies;
`Runtime.Render` applies the same check. Providers may work in process without
serialization. `engine/builtin.New` selects the bundled implementations.

A completed AST refusal sets `Complete` and returns `Diagnostics`, with no
fragments or omissions. Each diagnostic may identify its request node with a
zero-based `Input` index. The guard returns `BatchRefusalError`, preserving the
diagnostic data and exposing typed schema/capability errors with the caller's
input-node reference. Service errors mean rendering could not be performed.
An empty batch still requires completion; an empty reply is not success.

`Omissions` records declared properties the provider did not emit. These records
use the same validation and snapshot contract as whole-schema rendering.
Reporting is not necessarily exhaustive; an empty list does not prove lossless
rendering.

`migration/importer.WithRendering` supplies a renderer, canonical target name,
and capability snapshot for Liquibase typed changes. It accepts custom targets
and does not select a built-in provider. `Parser.Parse` and `Import` require a
caller context. Rendering failures, incomplete replies, and recorded omissions
refuse conversion before migration files are written. Cancellation is checked
before parsing, during rendering, and before directory emission. The native
import command selects the bundled renderer with its resolved release profile.
This replaces the implicit `WithDialect` and `WithDialectCapabilities` factories.

`core/renderer.SchemaService` lowers and renders a whole captured schema.
`Target.SchemaRendering` selects it independently of AST rendering. Requests
carry target facts and read-only schema data. Results set `Complete` and carry
ordered SQL fragments in `Statements`, with recorded `renderer.Omission` values.
The grouping preserves the provider's output and does not define transactions.

A schema refusal is a completed reply with validation diagnostics and no SQL
or omissions. `renderer.RenderSchema` checks the reply and converts a refusal
into `SchemaRefusalError`, whose diagnostic data and typed schema/capability
errors remain available. Missing completion, malformed output, provider failure,
and cancellation return no output. In-process services use typed schema values;
transport adapters encode whole requests and diagnostic data, not Go errors.
The runtime applies declaration target scope, then checks model codecs before
invoking the selected schema renderer. Excluded declarations require no codec
for this target. Validation uses the same ordering.

`Runtime.ResolveTarget` returns a `schemaext.TargetSelection` containing the
canonical name and every alias registered for it. Unregistered targets are
errors. The snapshot owns its names and contains no services or capabilities.
`schemamodel.ScopeToTarget` and `OmissionsForTarget` require this resolved value
and reject its zero value. An empty declaration scope includes every resolved
target; a nonempty scope matches only registered names, ignoring ASCII case
and surrounding whitespace. Built-in registration derives all accepted names,
including transport aliases, from `platform.DialectSpellings`.

Typed declarations can scope objects to custom registered targets. Go annotation
and YAML scope parsing still accept only built-in names. Those source frontends
do not yet expose custom-target registration.

Native rendering, inspection reports, dev-schema materialization, agent schema
gates, and the schema census use this selection. An agent gate reports completed
schema refusals as findings and service failures as errors. Both census surfaces
accept an explicit runtime and discard the measurement on operational failure,
including a failure after earlier cells completed. A registry's
`schemaext.UnknownCodecError` counts as a declaration refusal because the registry
cannot dispatch that model. A callback error wrapping `ErrUnknownCodec` alone
does not establish this receipt and stops the measurement.

`core/schemavalidation.Service` validates a whole captured schema in one
contextual call. `Target.Validation` selects the service explicitly. Requests
carry the target, capabilities, identifier semantics, and `NoSkipped` policy.
Schema values are read-only; validation neither loads source documents nor
inspects a server. `schemavalidation.Validate` copies the outer schema and fact
maps, validates the reply, and discards diagnostics on errors or cancellation.
Nested schema values remain read-only under the service contract.

A completed reply sets `Complete` even when there are no findings. Diagnostics
classify invalid schemas, unsupported features, and omitted declarations.
`Result.Err(target)` returns `schemavalidation.RefusalError` for a nonempty
completed report. Its accessors retain an independent diagnostic snapshot;
`errors.Is` and `errors.As` still expose the schema and capability causes.
An absent completion receipt or malformed diagnostic returns `ErrInvalidResult`
without a refusal receipt. A service failure remains an error, even when it wraps
a schema or capability sentinel. The runtime checks selected model codecs before dispatch.
A missing validator never selects built-in validation implicitly.

`migration/safety.AssessRendered` and `AssessRenderedWithCapabilities` require
the caller's context and selected `renderer.Service`. Assessment units render
in one batch. Fragments retain the source operation for each SQL statement,
including null-fill and narrowing-type checks. An extension's risk applies to
all statements in its fragment; unknown effects continue to require manual
review. Generation and checkpoints use the selected renderer for safety,
forward SQL, and rollback SQL. This changes the Go API; pre-v1, so no
compatibility is owed.

All `migration/planner.GenerateSchemaDiff*` functions and `Planner.GenerateMigrationAST`
require the caller's context and selected runtime. AST planning consumes
`featureplan.Runtime`. SQL planning consumes `planner.Runtime`, which adds
rendering to feature planning and local codecs. `engine.SchemaRuntime` adds schema
comparison, validation, and whole-schema rendering for workflows that use all these stages. Nil runtimes are refused even
for empty changes. The same context reaches selected planning and rendering;
errors or cancellation expose no AST or SQL prefix. This changes the Go API;
pre-v1, so no compatibility is owed.

`Provider.Conversions` assigns each target and feature kind to one conversion
service. The provider must own the desired and observed codecs for that kind.
`Runtime.ConvertFeatures` batches values by registration and preserves their
original order. A missing handler, malformed reply, service failure, or canceled
context returns no converted values. A codec alone cannot supply a conversion.

`Provider.Comparisons` assigns named object kinds and change codecs to a batched
comparison service. `Runtime.CompareObjects` validates both source states and
the complete reply. It rejects lost declarations, unrelated changes, and child
operations already owned by a parent transition. A reply includes effective
desired objects for table captures and structured diagnostics for undecided
changes. A completed reply sets `Complete`, including when it has no changes.
Provider failures and cancellation return no partial result. An enrolled kind
with unrepresentable source-wide state requires a selected comparer even without
values. A source-wide uninspected namespace makes no claim about applicability
to the selected target; its knowledge remains unchanged without a handler.
Concrete values, explicit defaults, and subject limitations require a handler.

`Provider.FacetComparisons` assigns attached model kinds and their change codecs
to a contextual comparison service. Each registration must declare its common
object kinds in `OwnerKinds`. The runtime captures that list and sends only those
owners, including owners without concrete facet values. Values and subject
coverage attached to the wrong kind fail before any comparison service runs.
`Runtime.CompareFacets` uses the common owner's identity and lifecycle. Several models may attach to one owner, but a
model cannot also be registered as a named object on the same target. Replies
must set `Complete`, retain explicit declarations, and preserve source knowledge.
A change requires declared intent and observed state; an explicit subject limit
takes priority over a partial concrete value. Parent creation and removal own
their attached state, so they cannot also produce separate facet changes.

Facet providers check `FacetComparisonRequest.Includes` for each model and
common owner. A source binding can exclude one model while retaining another
on the same owner. Excluded values are removed before codec checks; their
bindings survive repeated projection. Whole-schema validation and rendering
check the remaining model identities without changing source coverage; later
comparison still uses that coverage for owners without an exclusion. The runtime suppresses the corresponding
observed values during comparison and refuses changes, adopted values, or
undecided diagnostics for excluded pairs. A captured parent observation still
retains its actual settings for rebuild and reversal.

`Runtime.CompareFeatures` joins named-object and facet comparison. It validates
both input surfaces before dispatch and returns no result if either fails.
Installing a provider never enrolls its models in a captured source. The
migration comparator consumes this combined service through
`schemapreparation.Runtime`. It captures table and index facets on both sides and
applies effective desired settings before common table captures. Index identities
follow the selected table or schema namespace. Table-owned changes attach to the
table diff; schema-scoped index changes use the schema diff. Other attachment
points currently refuse because their comparison identity capture is not
implemented. A successful runtime reply sets `Complete`;
undecided diagnostics remain distinct from operational failures.

`ComparisonRequest.Requests` carries `schemaext.ChangeRequest` values: changes
the caller asks for that no comparison can find, such as a new value for an
object whose value the server never returns. A request belongs to one
comparison and to neither schema state. An `ObjectComparison` lists the
`Actions` it accepts; the runtime sends each request to the owner of its
subject's named-object kind, in subject and action order, and refuses a
request no owner accepts, a duplicate, and one naming a facet model before any
service runs. `config.CompareOptions.FeatureRequests` is how a migration
comparison receives them.

Before table preparation, comparison binds table and index coverage claims to the
same identifier semantics and default database as their common owners. Explicit schemas
remain explicit. Binding preserves source knowledge, refuses identity collisions,
and leaves the source snapshots unchanged.

`Provider.Reversals` assigns each target and change kind to its codec owner.
`Runtime.ReverseChanges` validates the complete input before dispatching one
batch per registered service. Each reply preserves its subject and change kind,
reconstructs its directional operands, and describes its strategy and recovery
limits. Missing handlers, invalid replies, errors, and cancellation return no
partial result. Callers must retain and report the limits with the plan. A
reply with no change value says the reverse direction runs no statement, as
for a value nothing can read back; it keeps its subject, must state at least one
limitation, and a host publishes no reverse change for it.

`Reversal.ForwardState` carries complete typed values for the state left by each
accepted forward change. It distinguishes named objects from attached facets;
a nil value establishes absence. The runtime checks model ownership and rejects
competing projections. A host applies these values to its prior capture and
preserves unlisted siblings. It must not replace the capture with the requested
schema, because a diff policy may have excluded some requested changes.
`Registry.ProjectObjects` and `Registry.ProjectFacets` apply these predictions
to captured objects and attached settings. They update affected subjects while
retaining unrelated values and source knowledge limits. Facet replacement keeps
its target binding. Removal drops that binding; excluded facets cannot be
projected. Both operations refuse unreadable prior state and never enroll a kind
merely because the runtime gained its codec.

`schemaext.ReversalRuntime` is the narrow contract for this stage. A predicted
post-forward operand is not inspection evidence. `ErrIrreversible` means the
owner cannot restore the prior schema definition; recoverable definitions with
unrecoverable data carry explicit limitations instead. `dialect/ydb/ydbreverse`
reconstructs stream definitions and reports lost messages and consumer positions.
It refuses recreation of a disabled stream while allowing an in-place topic
change that leaves the disabled stream intact.

`Target.Constraints` selects a `schemaprojection.ConstraintService` for common
constraint side effects. `Runtime.ProjectConstraints` gives that owner independent
captures before and after accepted intrinsic changes. It validates the transition
operands, preserves the target's identifier semantics, and rejects malformed or
misowned replies. A result contains complete table state or an explicit unavailable
reason. Missing support never means that constraints leave indexes unchanged.
`ConstraintResult.Validate` checks this outcome contract for custom runtime
implementations too; the generator refuses missing or conflicting outcomes.

`dialect/postgres/pgproject.Constraints` projects PostgreSQL primary and unique
backing indexes, column key flags, and foreign-key defaults. It retains unrelated
indexes. Partition descendants, exclusion-index expressions, and an uncaptured
server-generated NOT NULL name require further projection evidence. Other targets
need their own selected service; PostgreSQL behavior is not inherited by aliases
for different engines.

`Provider.Reporting` assigns inventory labels and count metrics to the owner of
each model representation. `Runtime.ReportFeatures` validates and snapshots the
whole request before calling one service per registration. It preserves value
order and rejects undeclared metrics, negative counts, and partial replies.
Metadata is frozen at registration. Errors or cancellation discard the report.

Reporting needs model ownership, not a target registration. Its optional target
is source context, so an export from Go declarations can report omitted values
without choosing a database. `schemaext.ReportingRuntime` supplies this stage;
`schemaext.ModelRuntime` supplies codecs without requiring unrelated services.
Counts describe captured values. A zero count does not establish inspected
absence. `dialect/ydb/ydbreport` counts changefeeds and their consumers, including
disabled streams, in both desired and observed representations.

`dialect/ydb/ydbcompare.Service` compares individual changefeeds using YDB
defaults. It preserves inspected streams omitted by an incomplete desired
source. A table namespace claim can establish absence; an empty object list
cannot. An explicit subject limitation prevents comparison of that stream even
when its surrounding namespace was inspected. Unknown namespaces remain visible
in comparison diagnostics. Disabled streams retain their observed state.

Schema conversion passes named objects and every attached facet through this
service. `Registry.ConvertCoverage` preserves source knowledge while selecting
the destination model definitions. It does not enroll additional kinds or turn
unknown state into absence. An unresolved default request cannot become an
observation. If common-object conversion removes the envelope carrying a facet,
the conversion refuses rather than omitting the facet.

`core/schemaext` defines namespaced feature identities, immutable data
collections, positive source coverage, explicit codecs, and operation effects.
`Facets` holds one typed value per kind; `Objects` holds individually named
values with structured references. Insertions and lookups clone values.
Duplicate kinds or object identities and nil payloads are errors. `Value.Equal`
compares local representations; target-aware comparison resolves defaults and
inspection limits separately. `OwnedCoverage` enrolls one model in a source's
coverage claim from its owner's codecs alone, so a runtime that registers more
models later cannot widen a claim already captured.

`DecodeObject` decodes one strict JSON object for a codec: it refuses a key
outside the `ObjectShape`, spelled exactly, so a key in another letter case
that `encoding/json` would match to a struct field is refused too. It also
refuses a null where the shape allows none, an empty value under a `NonEmpty`
key, which an `omitempty` encoder never writes, a missing required key, a
duplicate key, and a value that is not an object. A refusal wraps
`ErrInvalidValue` and names the first offending key in sorted order.
`ValidText` refuses invalid UTF-8 and a NUL byte, which a statement or a
catalog cannot carry faithfully.

`ModelCodec` builds the codec of a payload whose wire form is its Go value as
`encoding/json` writes it, from the owner's shape check, validator and
canonical form: a model value, a change or an operation. A model value clones
itself; another payload names its clone function. Cloning, encoding and
decoding validate first, and every refusal is an `InvalidModelError` of the
codec's kind and representation; one the validator already typed is returned
unchanged.

`engine.Provider.Relations` assigns dependency discovery by target, model kind,
and source representation. `runtime.CaptureRelations` validates a complete
`schemaext.RelationRequest` before dispatching one batch per owner. Each concrete
value has an ordered receipt. Missing handlers, malformed receipts, errors, and
cancellation return no snapshot. A receipt can explicitly report unresolved
references; source coverage remains unchanged. `runtime.RelationKinds` names the
kinds that have relation discovery for a target and representation, so a caller
can ask about those values only.

`RelationSnapshot.CaptureRelated` captures the connected feature context around
structured object references. It retains each multi-table object once, with its
complete definition, and includes other values sharing its dependencies.
Exclusive table ownership comes from the object identity; facet ownership comes
from its location. Unknown namespaces or unresolved references return
`ErrIncompleteRelations`, even when some definitions are known. Runtime growth
does not enroll an older source in a new model. Returned references establish
neither the existence of their targets nor permission to expand user scope.

Built-in scope selection uses these contracts. Before `--include`,
`--exclude` or `--schema` narrows a comparison or an inspection, it captures
the tables each standalone feature object binds on either side. An object is
kept whole when the scope holds every table it binds and left out when it holds
none. A scope that would split one is refused, naming the tables on each side,
since comparing the object would change bindings outside the scope. A kind the
scope selects by its own name keeps that selection; for it the tables decide
only the refusal of a split.

Built-in comparison uses them too. Once the plan is known, it refuses one that
drops a table, a column or a function a standalone object of the effective
desired feature state still binds, and one that drops or replaces any function
while an object's owner could not list every reference. The effective state
counts unchanged objects and observed ones the description could not express,
so dropping a table never removes another object's binding silently: the
declaration removes the binding, which the owner plans, or the comparison
refuses. Functions count only on a target whose plans write routines.

The external-provider fixture checks the public path without built-in
providers. It builds as a module of its own, imports only packages this ledger
lists under Stable Embedder API, and links no package under `engine/builtin`,
`dialect` or `feature`. Process adapters must map model values and references
into explicit records; implicit JSON encoding of a relation value or snapshot is
refused.

`ChangeValue` implementations that also implement `OwnerReplacement` report
whether applying them requires replacing the common owner; `ReplacesOwner`
answers false for a value that does not. The migration comparator honors it
for materialized views, which it can replace, and refuses it for tables and
indexes with `ptaherr.ErrUnsupportedFeature`.

A `ChangeValue` that also implements `StateRemoval` says whether applying it
takes state away from the database: it drops an object, or turns off a
setting the database had on. The owner whose change can remove state must
implement it, because only the owner knows; a change that never removes
state may leave it out. Silence reads as "removes nothing": `RemovesState`
answers false for a value that does not implement it, so an additive apply
such as `migration/dbtest` keeps that change. It leaves out a change that
takes state away, and applies one whose owner says it does not even beside no
structural change.

`Facets.WithTargetScope` binds a value to target names from its source.
`ForTarget` uses an explicit `TargetSelection`, including its registered aliases.
An excluded value retains its binding without its payload. `Kinds` and `Len`
describe concrete values; `DeclaredKinds` includes exclusions, and `IsZero`
remains false when an exclusion is present. Reproject the source declaration
when selecting a target that needs a previously excluded value.

`Object.Targets` binds an object to target names from its source in the same
way. A collection normalizes and sorts the names, and the binding travels with
the object through every collection it is copied into. `Objects.ForTarget`
keeps an unrestricted object and one whose binding names the target, and
leaves the others out. It keeps no record of them, because an object is named:
`schemamodel.OmissionsForTarget` reports each one with its identity in
`ScopedObject.Ref`, and the comparison suppresses the observed object of that
identity, bound to the connection's identifiers, so an exclusion is never read
as a drop. `HasTargetScopes` reports whether any object is bound, and
`Objects.Equal` ignores bindings, as `Facets.Equal` does.

Built-in schema rendering and direct AST rendering resolve facet scopes before
checking support. An excluded table facet contributes no SQL; its source
binding remains available in the captured model. Included unknown facets are
refused, as is rendering a captured exclusion on a target that needs its value.
Go annotation export refuses facet bindings it cannot preserve, including
bindings whose payload was excluded.

`EncodeObjects` and `DecodeObjects` carry an object's binding in
`EncodedObject.Targets`. `EncodeFacets` and `DecodeFacets` carry `EncodedFacet`
records with separate host-owned target bindings and owner-defined payload
envelopes. An excluded
record has no payload and needs no model codec. `SnapshotFacets` and common
schema conversion preserve bindings; ordinary value replacement does too.
Changing a value's target scope does not change its local semantic equality.

`EncodeChanges` and `DecodeChanges` carry `ChangeRecord` values as
`EncodedChange` records: the structured subject beside an envelope that names
the owner, the namespaced change kind, the change representation and the codec
version. A kind without a change codec in the registry returns an
`UnknownCodecError` naming it; an envelope recorded under another
representation wraps `ErrInvalidValue`, and a different owner, version or
definition wraps `ErrIncompatibleCodec`. Either direction returns no partial
batch. Ptah's own diff and drift JSON documents write owner changes this way.

`Coverage` records the model definitions a source actually describes. Its zero
value is uninspected. Registering another provider cannot make an older source
authoritative over that provider's objects. Subject claims distinguish explicit
absence, requested defaults, complete inspection, and unrepresentable state.

`Coverage.SelectSubjects` projects explicit subject and parent records while
retaining the source's kind-wide knowledge. Apply the same projection to schema
objects: deleting an override alone makes lookups fall back to kind-wide knowledge.
Table selection carries retained feature children and their coverage together.

Providers supply versioned `Codec` descriptors through `Provider.Codecs`.
`Runtime.Codecs` returns the frozen registry. Its context-aware batch methods
refuse unknown kinds, changed definitions, and incompatible versions without
partial results. Wire identity uses the provider, semantic kind, representation,
version, and definition hash; Go package names are absent. A codec registration
does not establish target support. Owner callbacks are pure local operations.
`Fingerprint` uses owner-defined canonical ordering, retaining ordered lists;
it does not decide semantic equality. Default JSON serialization of feature
collections is refused so callers cannot lose concrete payload types.

`dialect/clickhouse/chschema` owns the ClickHouse table-settings model and its
versioned codecs. A desired setting distinguishes an unmanaged omission, a
request for the target default, and an explicit value. An explicit empty value
remains distinct from a default request. An observation requires every property,
including empty optional values. Sorting and primary keys stay separate even
when their expressions agree. Codecs preserve expression text and order.

Register `chschema.Codecs()` with the selected provider to encode table facets.
`ObservedTable.Desired` makes every property explicit; `DesiredTable.Observed`
refuses unresolved settings. This projection records a prediction, not a new
database observation. Invalid model values return `schemaext.InvalidModelError`,
which identifies the kind and representation and wraps `ErrInvalidValue`.

`chschema.IndexCodecs()` handles data-skipping facets under `IndexKind`.
`DesiredIndex` retains type and granularity intent; key expressions remain in
the common index parts.
`ObservedIndex` requires a nonempty type and positive granularity. Integer
values retain their full precision through the codecs. `ObservedIndex.Desired`
makes settings explicit, while `DesiredIndex.Observed` refuses unresolved
settings. Null, duplicate or unknown fields, and invalid intent are refused.
`ValidateDesiredIndex` also refuses an explicit type that names a PostgreSQL or
MySQL access method, such as `GIN` or `BTREE`, as an invalid model value,
because no ClickHouse server accepts one. Registering these codecs establishes model understanding without granting
target support or inspection completeness.

The bundled runtime registers the table and skipping-index models with their
source properties, preparation, conversion, comparison, planning, reversal, and
reporting.
Programmatically supplied desired facets render through the schema API and new
table migration plans with their reverse DROP plans. A typed facet and storage
overrides on the same table are refused together, including empty overrides.
Other targets and non-table
attachment points refuse active ClickHouse table facets.

Existing-table comparison needs complete captured typed settings. The bundled
reader supplies them with subject-level coverage. `chplan.Service` plans TTL
changes on MergeTree tables and assesses storage dependencies before common
column changes. Required column additions precede the TTL change; removal of a
column used by the prior rule follows it. Removing or modifying a column used
by retained storage rules is refused. Missing observations and coverage remain
unknown and prevent planning.

`chast.AlterTTL` carries both complete storage operands. Its codec and
`chrender` handlers refuse changes to other settings inside a TTL operation.
Use it inside `ast.ExtensionAlterOperation` with an `ast.AlterTableNode` parent.
An empty desired TTL removes the rule; whitespace-only rules are invalid.
Non-owning targets refuse the payload without returning partial SQL.
The planner contributes explicit object effects and requires execution outside
a transaction. Its result must join the host's complete graph before execution.
Engine, key, partitioning, sampling, and table-setting changes remain refused.

`chast.AddSkippingIndex` carries the index name, SQL expression, type, and
unsigned 64-bit index granularity. Its explicit codec and `chrender` handler
work without the bundled runtime. Use the same ALTER envelope and parent as TTL
operations. An empty type selects `minmax`; zero granularity selects `1`.
`Validate` holds a type to `chschema.ValidateDesiredIndex`.
`DeclaredFacets` returns the operation's settings as a `chschema.DesiredIndex`
facet bound to the clickhouse target, stating what the operation renders: an
empty type and zero granularity request the defaults, minmax and one granule,
so a SQL statement that left `GRANULARITY` out means one granule, as ClickHouse
defines it. `SkippingIndexExpression` joins key parts the way ADD INDEX takes
them: several parts, or one part that is a top-level comma list as the catalog
reports a tuple key, become one tuple.

`chast.DropSkippingIndex` removes an index by name, and its effect reports the
loss of index data built for existing parts. Other targets refuse both
operations. `ast.ExtensionChangeReporter` supplies the logical addition or
removal for schema-change reports independently of its workload risk.
Payloads without a valid single-action report retain a parent-level modification.
The former `ast.AddSkippingIndexOperation` is removed without an alias. This
changes behavior; pre-v1, so no compatibility is owed.

`chreverse.Service` reconstructs the prior TTL definition and projects the
forward state for reverse planning. This projection establishes no inspection
evidence. Its recovery limitations report that restoring TTL rules cannot
recover expired or aggregated rows and values, or undo TTL data movement.

`chcompare.Service` compares resolved table settings through a selected
provider's `FacetComparisons` registration. Each `chdiff.Table` captures the
complete prior observation and fully explicit desired settings. Its codec keeps
both operands, including explicit empty settings. Changes concern surviving
tables; creation and removal belong to the common table lifecycle.

Comparison ignores whitespace between SQL tokens and redundant outer key
parentheses while preserving quoted text, identifier case, and key order.
Missing evidence needed for declared settings produces an undecided diagnostic.
An unmentioned table without inspected settings stays unmanaged; registering a
model does not create intent on every table. Explicit inspection limits remain
undecided. These services perform no database I/O or ALTER planning.

`chresolve.Table` retains the declaration beside fully explicit settings and
records each property's origin. Creation uses `MergeTree` and common ordered
primary-key columns when the engine and sorting key are omitted. A common engine
declaration is a fallback; target-specific engine intent takes precedence. A default
primary key inherits the resolved sorting key. An explicit empty sorting or
primary key renders as `tuple()` and stays empty in the table's catalog state.
Missing sorting-key input for a MergeTree table returns `ErrMissingSortingKey`.

For existing tables, omitted settings retain a usable observation; a default
request selects the creation rule instead. Callers must check source coverage
before supplying that observation. Missing evidence required by an omitted
setting returns `ErrUnknownCurrent` without a partial result. Resolution does
not establish server support or a new observation.

`chresolve.Index` applies the same separation to skipping-index settings.
`IndexRequest` accepts captured settings or established creation intent;
`IndexResult` retains the declaration, prepared values, and `IndexOrigins`.
Omitted creation settings and explicit default requests select Ptah's `minmax`
type and granularity `1`. These rules do not describe server configuration.
Existing indexes retain usable observations for omitted settings. Key expressions
remain in the common index, and unknown required settings return `ErrUnknownCurrent`.

`chconvert.Service` projects complete table and index observations into explicit
declarations and fully resolved declarations into predicted observations. It
preserves empty table properties, separate sorting and primary keys, and unsigned
64-bit index granularity. Unresolved settings, invalid inputs, and cancellation
return no partial batch, including mixed table and index batches. The migration generator uses
this conversion to capture the table that a reverse DROP removes; the prediction
does not replace a catalog read.

`Provider.Properties` assigns source property keys to feature owners for a
selected target and format. A `PropertySource` must own the desired model codec
for each kind, and two definitions cannot claim the same key in that format.
`PropertyDefinition.Prefixes` also claims every key that begins with a
lower-case prefix, compared without regard to case, so the owner decodes or
refuses a misspelled key in its namespace instead of leaving it unread.
`PropertyDefinition.Claims` answers both forms. A prefix may cover its own
definition's keys and no key or prefix of another definition. A prefix widens
only what a decoder is handed; an encoder's output must use the exact `Keys`.

`PropertyDefinitions` returns independent copies for a frontend to group input
without knowing the feature's Go type. A definition's `Absorbs` names the
common attributes it takes over, each into one of its keys; registration refuses
an attribute of another format, a key the definition does not own, and a second
owner of one attribute on a target and format. `schemaext.IndexTypeAttribute`,
the common index type, is the one attribute defined. `PropertyFormats`
distinguishes a selected target without property services from an unknown
target.

`DecodeProperties` and `EncodeProperties` preserve ordered batches and explicit
empty values. Every input is validated before dispatch. Replies must preserve
the count, kind order, and property ownership; codecs validate desired models.
Provider errors and cancellation discard the entire result. Unsupported formats
remain errors on empty batches. These operations establish no catalog coverage.

`chsource.Service` implements the table platform property grammar. A bare key
carries an explicit setting, including empty. A `.state` suffix with value
`default` requests its creation rule; a setting cannot have both spellings.
An omitted setting writes neither key. Register `chsource.Definitions()` and the
service in an application-selected provider. The bundled runtime registers this
service for source lowering and Go annotation export.

`chsource.IndexService` and `IndexDefinitions()` supply the index property
format, `schemaext.IndexPlatformProperties`. They own `type` and `granularity`
with the same intent grammar. Explicit granularity must be a positive decimal
integer within the unsigned 64-bit range. Registration selects decoding, not
server support or a migration strategy.

`chcompare.IndexService` compares resolved skipping-index settings of indexes
both sides hold and adopts unmanaged observations without changing the source.
Missing observations and explicit knowledge limits remain undecided. Common
index lifecycle owns creation and removal. `chdiff.Index` and `IndexCodecs()`
preserve complete directional operands, including unsigned 64-bit granularity;
its effect is behavioral because applying it replaces the index.

`chplan.IndexService` plans a settings change as `chast.DropSkippingIndex`
followed by `chast.AddSkippingIndex` with the declared key expression and the
desired settings, outside a transaction. Both operands must agree with the
captured index on both table sides. The removal is ordered before common
changes to columns the captured expression reads, and the addition after
changes to columns the declared expression reads. When the host's common steps
drop and create the same index, the service contributes no steps and accounts
for the change through that replacement.

Because the removal comes first, the service refuses a declared key that names a
column the desired table does not declare, checking each bare key part and bare
call argument, and a plan that removes a column the declared expression reads.
Parent receipts cover every table action: a surviving table keeps its settings
unless a change or a common replacement covers the difference, a dropped table
loses them, and a rebuild is refused.

`chreverse.IndexService` restores the captured definition and reports that
replacement cannot restore materialized index data. Reverse planning projects
the forward settings onto the captured index. `chreport.IndexService` and
`IndexDefinitions()` supply counts and omission labels.

`chschema.RefreshCodecs()` handles a materialized view's refresh schedule under
`RefreshKind`. `DesiredRefresh` and `ObservedRefresh` each hold a `Schedule`:
`EVERY` or `AFTER` with its interval, and the optional `OFFSET`, `RANDOMIZE
FOR`, `DEPENDS ON` and `APPEND` clauses. A view without a schedule has no
value; whether that absence is known is coverage, which `RefreshCoverage`
builds. `Schedule.Clause` renders the clause the CREATE carries, and
`Schedule.Clone` copies a schedule without sharing its dependency list.

`chsource.RefreshFacets` reads a declared clause into a facet bound to the
clickhouse target, in the spelling the server stores, and refuses a clause the
server would refuse. `chsource.Annotations` adds the `refresh` attribute to
`//ptah:schema:matview` and makes the claim `chsource.RefreshCoverage` builds,
so a Go source that selects the owner and declares a view without a schedule
asks for a plain view. A Go parse without the owner refuses `refresh` as an
unknown attribute; YAML, HCL and SQL sources enroll none and leave a server's
schedule unmanaged.

`chcompare.RefreshService` compares schedules of views both sides hold, reading
both in the spelling the server stores, with dependencies qualified by the
view's schema. An unmanaged observed schedule is adopted into the effective
declaration, so a view replaced for another reason keeps it. A stored schedule
the reader could not read is undecided. `chdiff.Refresh` carries an observed
`Before` and a desired `After`, where nil is a plain view; its codec writes
null for that side. `ReplacesOwner` is true when a schedule is gained or lost,
or `APPEND` changes, because `MODIFY REFRESH` refuses those; the effect of such
a change is destructive. `String` writes the transition as two clauses.

`chplan.RefreshService` plans an in-place change as `chast.ModifyRefresh`
inside the ALTER envelope that names the view, outside a transaction, and
contributes nothing when the host replaces the view. `chreverse.RefreshService`
restores the prior schedule in place, or reports that replacing the view does
not restore its rows. `chconvert.Service` projects refresh observations and
declarations; `chreport.RefreshService` and `RefreshDefinitions()` supply the
count and omission label. The former `ast.MatViewRefreshSpec`,
`ast.AlterMaterializedViewRefreshNode`, the `Refresh` fields of
`ast.CreateMaterializedViewNode`, `schemamodel.MaterializedView` and
`catalog.MaterializedView` are removed without aliases. This changes behavior;
pre-v1, so no compatibility is owed.

`chschema.RowPolicyCodecs()` handles ClickHouse row policies under
`RowPolicyKind`, the ClickHouse model of ADR 0020. A policy is a feature
object identified by its database, table and name through `RowPolicyRef`, so
two tables may each hold a policy of one name; an empty database is the
connection's. `ValidateRowPolicyRef` refuses a reference without a table,
because a database-wide policy (`ON db.*`) is a variant this model does not
hold.

`DesiredRowPolicy` declares the SELECT filter, permissive or restrictive
composition and a `RoleSelection`; an omitted filter is a policy without USING,
an omitted composition is permissive, and a zero selection applies the policy
to nobody, as a policy without TO does.

`DesiredRowPolicy.NormalizedFilter` is the connected server's spelling of the
filter. A live comparison attaches it, because the server reformats a filter
and 24.10 and 26.9 format it differently; a source never writes it.
`ObservedRowPolicy` holds what `system.row_policies` reports, with composition
and selection always stated.

`RoleSelection` holds named users and roles, or `TO ALL` with optional
exceptions, each list a set, so a role named `ALL` stays a name. Its
`MarshalJSON` writes both lists in byte order, so a policy encodes the same
alone and inside a change or an operation. The codecs refuse unknown, null and
empty-valued keys and a selection ClickHouse cannot express, as do the
`chdiff` and `chast` codecs that carry a policy, each refusal a
`schemaext.InvalidModelError`. `RowPolicyCoverage` builds the model's coverage.

The ClickHouse provider registers the row policy services, and row policies
are the owner's alone. `chsource.Annotations` adds the Go directive
`chsource.RowPolicyDirective` (`//ptah:schema:rowpolicy`), whose policies are
bound to the clickhouse target, and `chsource.RowPolicyCoverage` is the claim a
desired source that declares them makes. A YAML `rls_policies` entry scoped to
ClickHouse is one too, and `chsource.YAML` makes the same claim on a YAML
document. The ClickHouse reader reports each policy on a table of
the database as an `ObservedRowPolicy`, and a read the account may not make as
uninspected. A shared row-level security declaration or diff entry is refused
on ClickHouse.

`chcompare.RowPolicyService` pairs policies by
resolved identity, so a declaration that leaves the database to the
connection matches the policy the server reports with the database named. A
policy whose table the plan creates or removes is left to that table's
transition. A held policy that a source cannot describe is adopted into the
effective declaration rather than dropped, and a policy whose presence or
content was not established is undecided.

`chprobe.Service` attaches `NormalizedFilter` to each declared policy the
server holds by asking the server to format the declared filter inside a
rolled-back transaction of the probe session; where no probe ran, the
comparison compares the filters token by token.

`chdiff.RowPolicy` carries an observed `Before`, a desired `After`, where nil
is an absent policy, and an `Access` assessment. A change of composition alone
narrows access (permissive to restrictive) or widens it; every other change
is unknown. The effect of a creation or an alteration is behavioral, and of a
drop destructive. `chast.RowPolicy` is the operation `chrender` writes as
`CREATE ROW POLICY`, `ALTER ROW POLICY` or `DROP ROW POLICY`.

`chplan.RowPolicyService` plans one statement per change outside a
transaction. ClickHouse keeps a policy when its table is dropped, so the
service drops the captured policies of a dropped table itself, and a created
table renders its declared policies after `CREATE TABLE`.
`chreverse.RowPolicyService` restores the prior policy and reports that doing
so cannot undo rows read while the forward plan applied.
`chreport.RowPolicyService` and `RowPolicyDefinitions()` supply the count and
omission label.

`schemaproperties.DecodeTables` attaches decoded property groups as desired
facets bound to the selected target. It consumes only claimed keys; other keys
and target groups remain in `Overrides`. `EncodeTables` writes table facets as
properties for that target. `DecodeIndexes` and `EncodeIndexes` apply these
rules to index owners. Index decoding consumes the common `Type` declaration
only when a selected definition absorbs `schemaext.IndexTypeAttribute`, and it
refuses an index property of the selected target that no owner claims, because
nothing else reads index properties. `DecodeColumns` and `EncodeColumns`
apply the table rules to the fields in `schemaext.ColumnPlatformProperties`;
an unclaimed key stays, since the same group carries the common per-target
overrides. `Decode` applies the table, the index and then the column rules.
Export writes owned settings to scoped properties.

These operations refuse duplicate alias keys and mixed typed and property
declarations, even when a property's value is empty. They copy the selected
owners, also when there is nothing to decode or export, and leave other schema
data shared and read-only. Errors and cancellation return no schema. They
establish no inspection coverage and do not resolve omitted settings.

`dialect/mysql/mysqlschema` owns the options a MySQL or MariaDB table is
created with as a table facet under `TableKind`. `DesiredTable` holds the
engine, the first auto-increment value and the default character set a
declaration states; `ObservedTable` holds the character set a table created
from one reports, since the engine and the next auto-increment value are not
read.
`ValidateDesiredTable` refuses an engine or a character set that is not a name
and an auto-increment value that is not a whole number. `TableCodecs` and
`TableCoverage` complete the model under `Owner`.

`mysqlsource.Service` reads
the options from the `engine`, `auto_increment` and `charset` platform
properties of the mysql and mariadb targets; an engine stated there is written
over the table's common engine. `mysqlcompare.TableService` plans no change of an existing
table's options, `mysqlplan.TableService` accounts for them through table
creation and removal, and `mysqlrender.CreateTableOptions` writes them into
CREATE TABLE; `mysqldiff`, `mysqlconvert` and `mysqlreport` complete the
provider.

`mysqlschema` owns an index's options under `IndexKind` the same way:
`DesiredIndex` and `ObservedIndex` hold the parser of a FULLTEXT index, read
from the index's `parser` platform property by `mysqlsource.IndexService`,
attached by the SQL parser bound to `Targets` with `WithIndexOptions`, and
written by `mysqlrender.IndexOptions` into `WITH PARSER`. The table and index
services of `mysqlcompare`, `mysqlconvert`, `mysqlplan` and `mysqlreport`
share one shape. The index comparison registers no change kinds: it reports
no change, as the column settings comparison does.

The former `ast.IndexNode.Parser`, `ast.IndexNode.ForeignKeyIndex` and
`schemamodel.Index.Parser` fields are removed without aliases. A SQL schema
file's parse answers which index a `FOREIGN KEY name (columns)` clause names,
which does not travel on a common node; the parser travels as the owner's
facet.

The former `AutoIncrement` and `Charset` fields of `schemamodel.Table` are
removed without aliases; `schemamodel.Table.Engine` stays a common declaration
attribute. This changes behavior; pre-v1, so no compatibility is owed.

`mysqlschema` also owns the MySQL-family settings of a column that no other
target has a clause for, its character set and its `ON UPDATE` expression, as
a column facet under `ColumnSettingsKind`.
`DesiredColumnSettings` and `ObservedColumnSettings` hold the two settings, an
empty one stating nothing; a value stating neither is refused. The server
reports a character set for every text column, so an observed one does not mean
a declaration wrote it.

`Settings` reads either representation from a facet collection.
`WithColumnSettings` and `WithObservedColumnSettings` add a value bound to
`Targets`, mysql and mariadb, which is how the SQL parser and the reader
declare them. Atlas HCL, Go annotations and YAML state them as platform
properties.

The column owner is split over these services:

- `mysqlsource.ColumnService` decodes and encodes the `charset` and
  `on_update` column platform properties.
- `mysqlcompare.ColumnService` is the comparison owner on column subjects. It
  keeps the declared settings, adopts nothing and reports no change, so a
  column whose only difference is one of these settings is not planned, and a
  column modified for another reason is rewritten with the declared settings.
  It registers no change kinds; `engine.FacetComparison` accepts that, and the
  runtime refuses any change such an owner reports.
- `mysqlplan.ColumnService` accounts for the settings through table creation,
  rebuild and removal.
- `mysqlrender.ValidateColumnFacets` and `ColumnClauses` serve the
  MySQL-family column definition.
- `mysqlconvert.ColumnService` and `mysqlreport.ColumnService` complete the
  provider.

The former `UpdateExpression` and `Charset` fields of `ast.ColumnNode`,
`schemamodel.Field` and `catalog.Column`, and their setters and builder
methods, are removed without aliases.

`dialect/cockroachdb/crdbschema` owns CockroachDB row-level TTL as a table
facet under `RowTTLKind`. `DesiredRowTTL` and `ObservedRowTTL` each hold a
`Policy` whose fields are the storage parameters, which `Parameters` returns in
statement order under the names CockroachDB uses. A table without a TTL has no
value; whether that absence is known is coverage, which `RowTTLCoverage` builds
under the bundled provider identity `Owner`. `ValidateDesired` refuses what the
server refuses or does not keep as written: a parameter without
`ttl_expiration_expression` or `ttl_expire_after`, a count below one, and an
interval or poll duration the owner cannot read. `DecodeDeclared` reads declared
parameters strictly and `DecodeStored` reads catalog parameters leniently. The
codecs write each parameter under its own name and a flag only when it is true.

`crdbsource.Service` decodes and encodes `platform.cockroachdb` table
properties, one per storage parameter. Its definition also claims the derived
`ttl` marker, so declaring it is refused with the server's reason, and every key
that begins with `ttl` in any case, so a misspelled or upper-case parameter is
refused by name.
`crdbsource.Coverage` is the knowledge a source format with platform properties
holds. Go annotations enroll it through `crdbsource.Annotations` and YAML
through `crdbsource.YAML` when the parse selects the owner, so a table without
the properties requests no TTL. HCL, SQL, hand-built schemas and a parse
without the owner do not, and leave a live policy unmanaged.

`crdbcompare.Service` compares the facet on tables both sides hold and reads
`ttl_expire_after` and `ttl_row_stats_poll_interval` through the value each
spelling denotes. `crdbdiff.RowTTL` carries an observed `Before` and a desired
`After`; a nil side is a known absence. A source that manages the policy
against uninspected live state is undecided, declaring one or declaring none,
unless the capability set establishes that the target has no row-level TTL. So
is a declaration the source could not describe, on a table the plan creates
too, and a live policy the read could not describe. An unmanaged live policy is
adopted into the effective declaration so a rebuild keeps it.

`crdbast.AlterRowTTL` carries one change in `ast.ExtensionAlterOperation`.
`crdbrender` lowers it to `RESET (ttl)` for a removal, and otherwise to a
`SET` of every parameter the new policy names followed by a `RESET` of the
parameters it stops naming, so an enabler stays set at every step. `crdbrender.CreateTableClause` renders the `WITH`
clause of a CREATE TABLE. Both refuse a target without
`capability.RowLevelTTL`, and other targets refuse the payload and the facet.
`crdbplan.Service` plans changes in place, accounts for the policy a dropped
table takes with it, and refuses a rebuild. `crdbreverse.Service` restores the
prior policy and reports that rows deleted under the forward policy cannot be
recovered. `crdbconvert.Service` and `crdbreport.Service` complete the provider.

The former `ast.RowTTLSpec`, `ast.SetRowTTLOperation`,
`ast.ResetRowTTLOperation`, the `RowTTL` fields of `ast.CreateTableNode`,
`schemamodel.Table` and `catalog.Table`, and `difftypes.RowTTLChange` are
removed without aliases. This changes behavior; pre-v1, so no compatibility is
owed.

`dialect/spanner/spannerschema` owns Spanner's row deletion policy as a table
facet under `RowDeletionKind`. `DesiredRowDeletion` and `ObservedRowDeletion`
each hold a `Policy` of a column and an interval: a declaration keeps the
interval as written, an observation as Spanner stored it. `ValidateDesired`
refuses a policy without its column, a quote in either field, and an interval
that is negative, is not a whole number of days, or is in a spelling the owner
does not read. `ValidateObserved` accepts any stored spelling. `Equivalent`
compares the column under the caller's identifier rule and the interval by the
hours it denotes, at the server's arithmetic, and compares as text when either
side cannot be read. `RowDeletionCoverage` builds coverage under the bundled
provider identity `Owner`.

`spannersource.Service` decodes and encodes the `platform.spanner` table
properties `row_deletion_column` and `row_deletion_interval`, and claims every
key that begins with `row_deletion`, so an unknown one is refused by name.
`spannersource.Coverage` is the knowledge Spanner SQL enrolls, and Go
annotations and YAML through `spannersource.Annotations` and
`spannersource.YAML` when the parse selects the owner. HCL, hand-built schemas
and a parse without the owner do not, and leave a live policy unmanaged.

`spannercompare.Service` compares the facet with the same undecided cases as
the CockroachDB owner, and `spannerdiff.RowDeletion` carries the change.
`spannerast.AlterRowDeletion` carries it in `ast.ExtensionAlterOperation`, and
`spannerrender` lowers it to `ADD TTL`, `ALTER TTL` or `DROP TTL` by which sides
the change holds, since Spanner refuses each of the first two in the other's
place. `spannerrender.CreateTableClause` renders the clause of a CREATE TABLE.
`spannerplan.Service` plans changes in place, `spannerreverse.Service` restores
the prior policy and reports that rows deleted under the forward policy cannot
be recovered, and `spannerconvert.Service` and `spannerreport.Service` complete
the provider.

`dialect/ydb/ydbschema` owns a YDB table's TTL as a table facet under
`TTLKind`. `DesiredTTL` and `ObservedTTL` hold a `TTL` of a column, an ISO 8601
interval and, for an integer column, the unit it counts; `ObservedTTL` also
records the run interval the SDK or CLI set, which YQL cannot write.
`ValidateDesiredTTL` refuses a missing column, an interval YDB would not keep as
written, and a unit outside the four YDB names. `EquivalentTTL` compares the
interval by the seconds it denotes. `TTLCoverage` builds coverage under `Owner`.
The TTL services in `ydbcompare`, `ydbconvert`, `ydbplan`, `ydbreverse` and
`ydbreport` handle the facet, `ydbdiff.TTL` carries a change, and
`ydbast.AlterTTL` lowers through `ydbrender.TTLHandler` to
`SET (TTL = ...)` or `RESET (TTL)`. `ydbplan.TTLService` refuses a change that
would reset a run interval and allows its removal. A column table's tiered TTL
belongs to its column storage, below.

`ydbschema` owns a YDB table's column storage as a table facet under
`ColumnStoreKind`; a table without the value is a row table.
`DesiredColumnStore` and `ObservedColumnStore` hold a `ColumnStore` of hash
columns, a shard count and a `TieredTTL`, whose `TTLTier` values move rows to an
external data source named by its absolute path or delete them.
`CheckColumnStore` and the validators refuse repeated hash columns, intervals
that do not grow, deletion before the last tier, and a policy that moves no row.
`LayoutSatisfied` and `TieredTTLEqual` are the comparison's equality, and
`ColumnStoreCoverage` builds coverage under `Owner`.

`ydbdiff.ColumnStore` carries a column storage change, and `ydbast.AlterColumnStoreTTL` lowers through
`ydbrender.ColumnStoreTTLHandler` to `SET (TTL = ...)` or `RESET (TTL)`.
`ydbplan.ColumnStoreService` refuses a change of storage kind, hash key or shard
count, and plans a TTL change as a RESET that reads the old policy's sources
early and a SET that reads the new one's, both acting on the TTL setting the
row TTL's owner writes too.

`ydbschema` owns a YDB row table's column families as a table facet under
`ColumnFamiliesKind`. `DesiredColumnFamilies` and `ObservedColumnFamilies` hold
`ColumnFamily` values, each with a name, the settings DATA, COMPRESSION and
CACHE_MODE, the columns it holds, and `KeepInMemory`, which only a read and a
value adopted from one carry. A setting left empty is not stated, and a table
keeps what it holds of it. `ValidateDesiredColumnFamilies` and
`ValidateObservedColumnFamilies` refuse two families of one name, a column in
two families, a default family listing columns, and a compression or a cache
mode outside the values a row table takes. `ColumnFamiliesCodecs` write the
families sorted, through the types' `MarshalJSON`, and `ColumnFamiliesCoverage`
builds coverage under `Owner`.

`ydbcompare.ColumnFamiliesService` compares what a table holds once the
declaration is applied: every family it holds stays, and each setting the
declaration leaves out keeps the held value. A source that cannot describe
families, HCL or DBML, leaves them unmanaged, and the comparison adopts the
families the table holds.

`ydbdiff.ColumnFamilies` carries a change and
`ydbast.AlterColumnFamilies` lowers through `ydbrender.ColumnFamiliesHandler` to
one `ALTER TABLE` of `ADD FAMILY`, `ALTER FAMILY ... SET` and
`ALTER COLUMN ... SET FAMILY` actions. `ydbrender.CreateTableFamilies` writes a
new table's families. `ydbplan.ColumnFamiliesService` plans a change after the
table's added columns and leaves a dropped column out of it, and
`ydbplan.RebuiltFamilies` gives a rebuilt table the families the old one holds.
`ydbreverse.ColumnFamiliesService` reports the families and settings a rollback
cannot remove, and `ydbconvert` and `ydbreport` complete the provider.

`ydbschema` owns a YDB row table's settings -- how it splits into
partitions, its read replicas, its key bloom filter and the partitions it
starts with -- as a table facet under `TablePartitioningKind`.
`DesiredTablePartitioning` and `ObservedTablePartitioning` hold a
`TablePartitioning`, each field a setting it states; a read never states a
starting layout and names only the settings that differ from YDB's
documented defaults. `TablePartitioningCodecs` and
`TablePartitioningCoverage` complete the model.

`ydbcompare`, `ydbconvert`,
`ydbplan`, `ydbreverse` and `ydbreport` carry `TablePartitioningService`, and
`ydbdiff.TablePartitioning` lowers through `ydbast.AlterTablePartitioning`
and `ydbrender.TablePartitioningHandler` to one `ALTER TABLE ... SET (...)`.
`ydbrender.CreateTablePartitioning` writes a new table's settings,
`ydbplan.RebuiltTablePartitioning` gives a rebuilt table every setting the
old one holds, and `ydbplan.TablePartitioningRebuildReason` says which change
only a rebuild makes.

A YDB global index's partitioning and read replicas are an index facet under
`ydbschema.IndexPartitioningKind`: `DesiredIndexPartitioning` and
`ObservedIndexPartitioning` hold an `IndexPartitioning`, and a read names only
the settings that differ from YDB's documented defaults.
`IndexPartitioningCodecs` and `IndexPartitioningCoverage` complete the model.
`ydbcompare`, `ydbconvert`, `ydbplan`, `ydbreverse` and `ydbreport` carry
`IndexPartitioningService`. `ydbdiff.IndexPartitioning` lowers through
`ydbast.AlterIndexPartitioning` and `ydbrender.IndexPartitioningHandler` to
one `ALTER TABLE ... ALTER INDEX ... SET (...)`, and
`ydbplan.RebuiltIndexPartitioning` gives an index of a rebuilt table every
setting the old index holds. A renamed index is compared under its new name.

The former `ast.YDBColumnFamilySpec`, `ast.CloneYDBColumnFamilies`,
`ast.SetYDBColumnFamiliesOperation`, the `YDBColumnFamilies` fields of
`ast.CreateTableNode`, `schemamodel.Table` and `catalog.Table`,
`difftypes.YDBColumnFamiliesChange` and `coverage.ColumnFamily` are removed
without aliases, and so are `ast.YDBTablePartitioningSpec`,
`ast.SetYDBTablePartitioningOperation`, the `YDBPartitioning` fields of
`ast.CreateTableNode`, `schemamodel.Table` and `catalog.Table`,
`difftypes.YDBTablePartitioningChange`, `TableDiff.YDBPartitioningChange`
and `YDBHeldSettings.Partitioning`, and so are `ast.IndexPartitioningSpec`,
`ast.SetIndexPartitioningOperation`, the `Partitioning` fields of
`ast.IndexNode`, `schemamodel.Index` and `catalog.Index`,
`SchemaDiff.IndexPartitioningChanged`, `difftypes.IndexPartitioningChange`,
`SchemaDiff.CurrentYDBSettings` and `difftypes.YDBHeldSettings`. This changes
behavior; pre-v1, so no compatibility is owed.

`ydbschema` owns a YDB vector index's settings as an index facet under
`VectorIndexKind`. `DesiredVectorIndex` and `ObservedVectorIndex` hold the
`VectorSettings` of a `vector_kmeans_tree` index: the metric, the element type,
the dimension, and the levels and clusters of its tree. A declaration may leave
any setting out, and the stage that builds the index refuses what YDB would
refuse; an observation names one metric, an element type and a dimension.
`VectorIndexCodecs` and `VectorIndexCoverage` complete the model, and
`HasVectorIndex` reports the facet. A source attaches a declaration to every
index it declares as a vector index, and folds a pgvector operator class into
the metric it names.

`ydbcompare.VectorIndexService` compares the settings of an index both sides
hold under one name, and `ydbplan.VectorIndexService` plans a change as
`ydbast.DropVectorIndex` and `ydbast.AddVectorIndex`, which
`ydbrender.DropVectorIndexHandler` and `ydbrender.AddVectorIndexHandler` lower,
because YDB changes no setting of a built vector index. A common replacement or
a table rebuild carries the settings itself. `ydbrender.ValidateIndexFacets`
and `ydbrender.VectorIndexDeclaration` serve the YDB renderer, and
`ydbconvert`, `ydbreverse` and `ydbreport` complete the provider. An index
carrying an owner's facet a rename cannot carry, such as a vector index's
settings, is never paired as a rename. An index's partitioning is carried, and
the owner compares it under the new name.

The former `ast.VectorIndexSpec` and the `Vector` fields of `ast.IndexNode`,
`schemamodel.Index` and `catalog.Index` are removed without aliases. This
changes behavior; pre-v1, so no compatibility is owed.

The former `ast.RowDeletionPolicySpec`, `ast.SetRowDeletionPolicyOperation`,
`ast.DropRowDeletionPolicyOperation`, the `RowDeletionPolicy` fields of
`ast.CreateTableNode`, `schemamodel.Table` and `catalog.Table`, and
`difftypes.RowDeletionPolicyChange` are removed without aliases, and so are the
bare `row_deletion_*` table attributes and YAML keys: each engine's policy is a
platform property of that engine. This changes behavior; pre-v1, so no
compatibility is owed.

An export refuses excluded facets, bindings outside the selected target, missing
source codecs, and empty fragments that cannot preserve a facet's presence.
Native Go export uses both operations before writing annotations. Whole-schema
rendering, validation, and comparison decode source properties after target
selection. Document-to-catalog projection resolves creation rules through the
selected creation projector before converting representations. Its coverage
describes predicted values, preserves explicit source limits, and proves no
inspection or execution.

The ClickHouse reader attaches `chschema.ObservedTable` to each returned table.
It preserves both key expressions, including equal or empty keys. Coverage is
complete only for tables retained in that read. Each skipping index carries a
`chschema.ObservedIndex` with the full type and unsigned granularity from
`system.data_skipping_indices`; the key expression stays in the common
columns. Index coverage is complete for the database when that catalog table
exists and unknown otherwise. A server whose catalog table has no `type_full`
column fails the read with an error that names the column.
`chreport.Service` supplies the storage-settings count and omission label for
formats that cannot retain facets.
Planning changes to storage settings other than TTL is not supported; the
owner refuses them. [stokaro/ptah#4355](https://github.com/stokaro/ptah/issues/4355)
tracks that capability.

A refreshable materialized view carries a `chschema.ObservedRefresh` read from
its CREATE statement, for the views `system.view_refreshes` lists. Refresh
coverage is complete for each view read, except a view whose stored clause
cannot be read, such as one with refresh `SETTINGS`, which is unrepresentable.
`system.view_refreshes` is read only when the database holds a materialized
view. When the server refuses it for privilege or answers that it is unknown,
every view's refresh coverage is uninspected and the read goes on; any other
failure fails the read.

Rendering a desired schema whose coverage marks a table, index or materialized
view setting unrepresentable or uninspected lowers the object without it and
emits a comment naming the setting and the reason. A comparison refuses to
replace a materialized view while the current side marks one of its settings
that way and the desired side neither states the setting nor asks for its
absence. `objectidentity.Builder.SchemaScoped` builds a schema-scoped identity
from a declared name that may carry its schema.

`Target.Preparation` selects `schemapreparation.Service` for captured tables.
A missing service is unavailable; providers that need no normalization register
`schemapreparation.Identity` explicitly. The service may resolve desired column
primary-key flags and mark them prepared. `ResolvedFacets` contains
`schemaext.FacetRecord` values keyed by declared table or index identities.
The runtime rejects duplicate or invented owners, undeclared kinds, and new
target scopes. Source facets, target bindings, observations, and knowledge stay
unchanged. The runtime validates ownership and snapshots values through codecs
before comparison. Incomplete or invalid replies and cancellation return no result.

`Target.Creations` selects `schemaprojection.TableCreationService` for offline
source projection. It receives decoded table captures and returns a complete
ordered batch of computed facets and column key membership. `TableCreation.Facets`
contains `schemaext.FacetRecord` values keyed by the captured table or index.
Computed facets use the desired representation for subsequent owner conversion;
they may describe creation defaults absent from the source. Duplicate or invented
owners, changed identity spelling, excluded kinds, and new bindings are refused.
The runtime validates keys, model ownership, and codecs. Missing services, partial replies, errors, and cancellation
return no prediction. `IdentityCreations` explicitly selects no additional effects.

Document projection preserves explicit source knowledge limits. New computed
kinds are bound to the selected target and gain coverage only for their captured
owners; sibling objects remain uninspected. The source declaration stays unchanged. `CompareSchemas` requires
`schemadiff.DocumentRuntime`; it predicts the current document before comparison.
Column-only changes use representation conversion without applying table
creation defaults.

`chprepare.Service` derives column membership from ClickHouse key expressions.
Table settings use `chresolve.Table` and require complete feature coverage before
retaining observed values. Typed index settings use `chresolve.Index`. Captured
index settings remain usable when sibling enumeration is incomplete; an explicit
limit on that index takes precedence. Native index settings must be decoded into
facets before preparation, without a competing common type.
CREATE prediction resolves typed index facets independently of table-facet
exclusions. Key expressions stay in common index fields and parts.

The shared comparator consumes prepared column flags
without reading ClickHouse clauses. Tables sharing a Go struct retain separate
captures. `SchemaDiff.TablePreparation` stores source and prepared captures;
filtering, cloning, and reversal preserve independent copies of this provenance.
It is not reverse intent or evidence that a migration ran. Report JSON omits it;
durable plan serialization requires an explicit capture codec.

`core/ast.ExtensionStatement` and `ExtensionAlterOperation` carry typed,
cloneable owner payloads. The ALTER interface stays sealed. An
`ExtensionAlterOperation` may sit in an `ast.AlterTableNode` that names a
materialized view, which is how an owner changes a setting of the view in
place. `ast.CreateMaterializedViewNode.Facets` carries owner settings the
CREATE states inline; rendering prepares them for the target like table facets
and refuses an active value the target's owner does not accept. YDB changefeed
operations live in `dialect/ydb/ydbast`: `AddChangefeed`, `DropChangefeed`, and
`AlterChangefeedTopic` each travel inside an `ExtensionAlterOperation`.

`ydbast.StreamingQuery` travels inside an `ExtensionStatement`, with separate
directory and leaf names, creation guards, both ALTER operands, and explicit
reset permission. `StreamingCodec` provides the versioned operation record;
`ydbrender.StreamingHandler` validates captured capabilities and renders it.
Clones preserve independent optional run settings. Safety uses the owner's
checkpoint-loss effect, including guarded replacement and body changes.

`ydbast.ResourcePool` and `ResourcePoolClassifier` travel inside an
`ExtensionStatement`. Create requires desired settings, alter requires both
operands, and drop carries neither. Their versioned codecs distinguish missing
settings from explicit zero limits and require an explicit classifier rank.
`ydbrender.ResourcePoolHandler` and `ResourcePoolClassifierHandler` require the
selected YDB target and its resource-pool capability. A registered codec grants
neither. Invalid operations fail before any SQL is returned.

Pool and classifier payloads expose structured subjects and workload effects.
Safety reports retain their names and classify routing or limit changes as
warnings. Removing the server-owned `default` pool is refused.
`dialect/ydb/ydbworkload` owns `PoolSpec`, `ClassifierSpec`, and distinct desired
and observed values. Go, YAML, and YQL sources carry these values in
`Database.FeatureObjects`, with exact database-scoped names. Dots in a pool or
classifier name are literal. They never introduce a scheme directory.
The common desired and observed models hold no separate pool or classifier
collections. Merging declarations with the same feature identity returns
`schemaext.ErrDuplicate`, including identical settings from different holders.

Include selectors retain captured destination pools of selected classifiers
and streaming queries. Explicit exclusions still remove those pools. Filtering
preserves namespace knowledge and limits subject records to the retained scope.
HCL cannot declare workload objects: export reports omitted objects, and reading
the document leaves these namespaces uninspected.

The workload services in `ydbcompare`, `ydbconvert`, `ydbplan`, `ydbreverse`, and
`ydbreport` handle these objects through the selected runtime. Changes retain
complete before and after settings in `ydbdiff.ResourcePool` and
`ydbdiff.ResourcePoolClassifier`, carried by `SchemaDiff.FeatureChanges`.
Safety reports classify these records through their owner-provided effects.
Reversal restores captured settings and reports that past query execution
cannot be undone.

`ydbplan.WorkloadStreamingService` plans
pools, classifiers, and streaming queries together: a classifier rank swap
releases occupied ranks before assigning them, and running queries stop before
workload mutations and resume afterward. These operations cannot run inside a
SQL transaction.

`ydbast.DefaultPoolSettings` is a set-only declaration of the existing default
pool. It preserves omitted settings without inventing an observation. An empty
declaration emits no SQL. A migration that changes default-pool settings still
requires an inspected before state.

Pool and classifier inspection records namespace and subject coverage
separately. A refused read, a directory outside the database-wide scope, or an
empty system view without enabled resource-pool support cannot establish
absence. Valid returned objects remain captured independently of target
capabilities. Unsupported settings leave the affected subject unrepresentable.

Workload comparison requires `resource_pools` capability for changes, not for
retaining observed objects. Incomplete namespace enumeration does not block an
unrelated table migration because source omission never removes workload objects.
A declared workload object still requires evidence of its current settings or
absence. Explicit subject limitations remain undecided; comparison preserves
source coverage without claiming new inspection.

Go export writes explicit unmanaged-namespace annotations when workload
coverage is missing and preserves source-authored namespace and object limits
through repeated export. It refuses recorded inspection limits it cannot preserve.
The generated Go keeps unmanaged scopes unmanaged when parsed.
Explicit absence and default coverage assertions also require a lossless
spelling; Go export refuses them instead of replacing them with omission.
Coverage must name the exact desired model definition the source supports.
Matching a kind's name alone does not establish that the model is understood.

`dialect/ydb/ydbsyntax` provides YQL identifier and string-literal quoting to
owner packages without importing host implementation helpers.

`dialect/ydb/ydbstreaming` owns the query `Spec` and its desired and observed
representations in `Database.FeatureObjects`. `Desired.AllowStateReset` carries
authored permission; conversion from an observation never grants it. The common
schema, catalog, AST, and diff types contain no streaming-query fields.

The streaming services in `ydbcompare`, `ydbconvert`, `ydbplan`, `ydbreverse`, and
`ydbreport` consume this model. `ydbdiff.StreamingQuery` captures both change
operands. Explicit namespace and subject coverage distinguish absence from
uninspected or unrepresentable queries. Planning stops running queries before
common operations and restarts them afterward. Reverse planning preserves
permission from an accepted body change and reports checkpoint recovery limits.

`dialect/ydb/ydbsecret` owns YDB secrets: the desired and observed models in
`Database.FeatureObjects`, the declaration grammar and the statements. No
model carries a secret's value. `Desired.ValueEnv` names the environment
variable the value comes from, and an empty one selects `DefaultValueEnv` for
the secret's path. `Observed` is empty, because the server returns a secret's
path and nothing else. A desired and an observed secret compare by path alone;
a rotation request from `RotationRequests` is the only way a plan writes
`ALTER SECRET`, and no declaration carries one; `WithRotations` adds them to
compare options without repeating one.

`Declare` is how every source format adds a secret declaration, and
`ParsePath` reads every other spelling of a secret as its path below the
database root, with a slash as the only separator, and refuses a leading or
trailing slash. `ResolvePath` reads the path a statement names, which YDB
stores absolute, against the database root, and refuses one outside it. The
common schema, catalog, AST, and diff types contain no secret fields.

The secret services in `ydbcompare`, `ydbconvert`, `ydbplan`, `ydbreverse`, and
`ydbreport` consume this model, `ydbdiff.Secret` captures both change operands,
and `ydbast.Secret` is the statement payload `ydbrender.SecretHandler` writes;
a change with both operands is a rotation. Planning creates or drops a secret
before the common statements, except a creation at a path the plan frees or
beneath one, and before every external data source, async replication, or
transfer that names the secret by path. Every standalone YDB object orders
itself against the paths above it the same way, and the planner orders objects
of different owners against each other's paths. `ydbscheme.CommonEffects` reads such a
path relative to the database root it is given. Reverse planning reports the
values a rollback cannot restore, and the reversal of a rotation carries no
change.

`dialect/ydb/ydbtopic` owns standalone YDB topics: the `Spec` of settings and
consumers, the desired and observed models in `Database.FeatureObjects`, the
declaration grammar, the checks a declaration and a change are held to, and the
statements. A setting a declaration leaves out stands for the value YDB gives a
new topic, and `Equal` compares two specs with every setting resolved that way.
`ConsumerSpec` is the consumer of a topic and of a changefeed's topic alike.
`Declare` is how every source format adds a topic declaration and refuses a
second one with a `DuplicateError`, and `ParsePath` and `ResolvePath` read a
topic's path by the rules a secret's path follows. The common schema, catalog,
AST, and diff types contain no topic fields.

The topic services in `ydbcompare`, `ydbconvert`, `ydbplan`, `ydbreverse`, and
`ydbreport` consume this model, `ydbdiff.Topic` captures both change operands,
and `ydbast.Topic` and `ydbast.TopicConsumer` are the statement payloads
`ydbrender.TopicHandler` and `ydbrender.TopicConsumerHandler` write. Planning
places a topic statement as a secret's, creates or changes a topic before a
transfer reads it, and drops it after the transfer that reads it. A reversal
returns a changed topic to the nearest state YDB reaches in place and reports
what it cannot restore.

`dialect/ydb/ydbexternal` owns YDB external data sources and external tables:
the `DataSource` and `Table` specs, the desired and observed models of both
kinds in `Database.FeatureObjects`, the declaration grammar, the checks a
declaration is held to, and the statements. A spec keeps every setting as
written, and `SameDataSource` and `SameTable` compare two specs with a path
an option or a table holds read against the database root. `DeclareSource` and
`DeclareTable` are how every source format adds a declaration and refuse a
second one with a `DuplicateError`; `ParsePath` reads a limit by the rules a
secret's path follows, and `ResolveSource` reads the data source an external
table names against the database root. The common schema, catalog, AST,
coverage and diff types contain no external object fields.

The external services in `ydbcompare`, `ydbconvert`, `ydbplan`, `ydbreverse`,
and `ydbreport` consume this model for both kinds in one batch.
`ydbdiff.ExternalDataSource` and `ydbdiff.ExternalTable` capture both change
operands, and `ExternalDataSource.Displaces` is the one predicate the
comparison and the planner ask whether the tables over a changed source must
be dropped and created again. `ydbast.ExternalDataSource` and
`ydbast.ExternalTable` are the statement payloads
`ydbrender.ExternalDataSourceHandler` and `ydbrender.ExternalTableHandler`
write; `ExternalRecreate` is a data source's drop and creation as one
operation. Planning places each statement early, reads the secrets a data
source names and the source an external table names, and orders the host's
column-table eviction policies and removed tables against the sources they
read. `ydbscheme.TieredTTLReads` names the sources a policy reads.

`dialect/ydb/ydbreplication` owns YDB async replications and transfers: the
`Connection`, `ReplicationSpec` and `TransferSpec` specs, the desired and
observed models of both kinds, whose observations carry the state the object
reported, the declaration grammar, the checks a declaration and a change are
held to, and the statements. `ReplicationsEqual` and `TransfersEqual` compare
two specs as YDB keeps them: a directory item read back as its tables and a
consumer YDB created for a transfer are no change.

`DeclareReplication` and `DeclareTransfer` are how a source declares one, and
refuse a second declaration at the same path with `schemaext.ErrDuplicate`.
The Go, YAML and YQL sources declare these models, and the YDB reader reports
them, with a replication or a transfer it could not describe recorded as
uninspected under `UnsupportedReplicationReason`, `UnsupportedTransferReason`
or `ServiceUnavailableReason`. The common schema, catalog, AST, coverage and
diff types contain no replication or transfer fields. `difftypes.FeatureContext`
carries the owned objects of both sides into planning, where the YDB planner
reads them for the rules that span a replication and the tables it owns, or a
transfer and the table and topic it uses.

The replication services in `ydbcompare`, `ydbconvert`, `ydbplan`,
`ydbreverse` and `ydbreport` consume this model, one batch per kind.
`ydbdiff.AsyncReplication` and `ydbdiff.Transfer` capture both change operands,
and `AsyncReplication.Cascade` says whether a drop takes the replica tables
along. `ydbast.AsyncReplication` and `ydbast.Transfer` are the statement
payloads `ydbrender.AsyncReplicationHandler` and `ydbrender.TransferHandler`
write, and `RefuseReplication` and `RefuseTransfer` are the one rule the
planner and the renderer hold a statement to.

Planning places a drop early and any other statement after the common
statements, with these effects, which the host orders against the other
owners' statements on the same subjects:

- A creation writes a replica at each target path, and a drop with CASCADE
  drops them.
- A creation or a change reads the secrets its connection names by path.
- A transfer reads its table and the topic or changefeed it reads, and its drop
  reads what the transfer used.

`ydbscheme.SecretPathReads` and `ydbscheme.TopicPathReads` name those reads for
the common statements and the owner alike. A reversal returns a change in place
in the state the forward change ran in, keeps a credential YDB cannot take away,
and reports what it cannot restore.

`dialect/ydb/ydbschema` owns changefeed data. Desired and observed changefeeds
are distinct values in `Database.FeatureObjects`, with their table recorded as
a structured parent. `Table` has no changefeed slice. A `CreateTableNode` carries
its children in `OwnedObjects`. `dialect/ydb/ydbconvert.Service` converts between
these representations; `dialect/ydb/ydbdiff.Changefeed` carries the before and
after values of one named stream.

`ObservedChangefeed.Replication` records a server-reported destination binding.
Conversion preserves it as `DesiredChangefeed.RetainedReplication`, a requirement
to keep an existing stream. Comparison can retain that state across repeated
plans, but a renderer cannot treat it as a creation instruction. Codecs, cloning,
and equality include the binding. The spec-only `DesiredChangefeeds` and
`ObservedChangefeeds` accessors omit it; use the typed objects when replacing or
capturing state. A binding can refer to a remote destination and does not name a
local replication controller.

`dialect/timescaledb/tsschema` owns the TimescaleDB models. Hypertable
settings are a table facet under `HypertableKind`: `DesiredHypertable` declares
the partitioning column, an optional chunk interval, `IfNotExists` and a
comment; `ObservedHypertable` records the first dimension, its type, the
interval the catalog reports and the dimension count. A continuous aggregate is
a named object under `ContinuousAggregateKind` in `Database.FeatureObjects`,
identified by `ContinuousAggregateRef`. `DesiredContinuousAggregate` keeps the
body as written and a nil `MaterializedOnly` for the server's default;
`Normalized` holds a connected server's spelling of the body and is set only by
normalization.

`dialect/timescaledb/tssource` declares both Go annotation directives,
`//ptah:schema:hypertable` and `//ptah:schema:continuousaggregate`, through
`Annotations`, an `annotation.Extension` the bundled runtime registers. A parse
that does not select it reads neither directive and claims no knowledge of
either model.

The common schema, catalog, AST, coverage and diff types carry no TimescaleDB
field. `CompleteCoverage` is the claim a source makes when it
describes every hypertable and aggregate; `RequireNoLimits` refuses a subject
limit a document format cannot write.

`tscompare.HypertableService` compares the settings of surviving tables. An
omitted chunk interval matches any reported interval, and the column compares
without letter case. `tscompare.AggregateService` compares aggregates by
schema-scoped identity under the request's identifier rules; a body is compared
only after normalization, and an omitted option matches. Both keep an
undescribed observation when the desired source cannot describe the kind, and
report an undecided change when the read could not describe the subject.
`tsdiff.Hypertable` and `tsdiff.ContinuousAggregate` carry both operands and
their safety effects.

`tsplan.Service` plans `tsast.CreateHypertable` for an existing table and
`tsast.ContinuousAggregate` for each aggregate transition, as statement
extensions. It refuses a hypertable the desired schema leaves ordinary, a
changed partitioning column and a changed chunk interval, because TimescaleDB
has no statement for the first two and Ptah plans none for the third. An
aggregate whose name a common step also creates as a relation is refused.

A new table carries its hypertable facet on `CreateTableNode`, and the
PostgreSQL-family renderer writes `create_hypertable` after the table through
`tsrender.LowerTableFacets`. `tsrender.Registry` renders both payloads and
refuses targets outside the PostgreSQL family; a target without
`capability.Hypertables` or `capability.ContinuousAggregates` writes a skip
line and records an omission. `tsrelation.Service` reports the hypertable an
observed aggregate reads. `tsreverse.Service` reverses both changes and reports
that a recreated aggregate starts empty. `tsprobe.Service` normalizes declared
aggregates the database holds inside rolled-back transactions of the probe
session it is given.

`engine.Provider.Normalizations` registers a `schemaext.NormalizationService`
for a target and kinds whose desired codecs the provider owns. One target and
kind has one normalizer; an empty kind list, a nil service, an unknown target
or a kind without the provider's desired codec is refused with
`engine.ErrInvalidRegistration`.

`engine.Runtime.NormalizeObjects` resolves the target and refuses an unknown
one with `ptaherr.ErrUnsupportedDialect` before any owner is asked. It sends
each owner one batch: the declared objects of its kinds, the observations of
the same kinds, the coverage of those kinds, and the request's
`schemaext.ProbeSession`. Objects of a kind no owner normalizes pass through
unchanged and are not snapshotted, so a target without a normalizer returns
the declaration it was given.

A reply must be `Complete` and return every
object under its identity and kind with the source coverage unchanged;
otherwise the call fails with `schemaext.ErrInvalidValue`. An owner error, a
refused reply and cancellation return no result. Objects keep their identity
order. `engine.SchemaRuntime`, `migration/schemadiff.DatabaseRuntime` and
`migration/generator.Runtime` include the method, so a live comparison
normalizes declarations before it compares them.

A `schemaext.ProbeSession` runs one body inside a transaction it rolls back
whatever the body does. It answers `ran == false` with a nil error when no
isolated transaction is available; the body did not run, and an owner treats
every probe as unanswered. A nil session is a request error the owner refuses.
A `dbschema.DatabaseConnection` is a probe session.
`schemadiff.Connection` is what a database-aware comparison takes: a probe
session that also reports the server's `Info` and resolves identifier
semantics. A `dbschema.DatabaseConnection` implements it, and the comparison
does not link the schema readers through it. A nil `Connection` is refused.

`schemaext.ComparisonRequest.DeclaredRelations` and
`schemaext.ObjectComparisonRequest.DeclaredRelations` name the views and
materialized views the desired schema declares, for an owner whose objects
hold their names as relations; tables are in `Parents`. The runtime refuses an
entry that is not a named view or materialized view with
`schemaext.ErrInvalidValue`, sends the owner a copy in identity order, and
leaves the caller's slice unchanged. A nil list declares none.

`schemaext.ComparisonRequest.DatabasePath`,
`schemaext.ObjectComparisonRequest.DatabasePath` and
`featureplan.Request.DatabasePath` carry the absolute path of the database the
state describes, such as `/local`, so an owner can read a path an object or a
statement writes absolute against it. The comparison host takes it from the
read (`catalog.Database.DatabasePath`) and the planning hosts from the diff
(`difftypes.SchemaDiff.CurrentDatabasePath`); the runtime sends every batch the
caller's value. It is empty when the target has none or the caller does not
know it, as for a declaration rendered without a database, and an owner then
cannot tell an absolute path of this database from one of another.

`atlascompat.DBSchemaToGoSchema` requires a context, target name, and selected
feature runtime. It returns a schema and an error. `FacetSlots` on the desired
and observed database models enumerates the mutable slots holding immutable
facet collections, so representation converters account for every common scope.

`core/renderer.Extensions` freezes local handlers by kind, payload type, and
statement role. It refuses unknown kinds, incorrect types, and unsupported
roles before rendering. A supported standalone ALTER payload requires its real
parent; validation never invents a table. Non-owning targets refuse the
extension even when the caller supplies another target's capabilities.
Handlers run inside the provider's batched rendering service.

Payloads may supply local `schemaext.EffectSource` metadata for safety reports.
Missing, invalid, or unexplained effects require manual review at the
`Destructive` severity. Schema reversal cannot restore records or consumer
positions lost when a changefeed is dropped.

A change or operation payload that affects what roles may read or write also
implements `schemaext.AccessEffectSource`. Its `schemaext.AccessEffect` is
separate from `Effect`. `Access` is one of these values, and `Reason` is a
required one-line explanation:

- `AccessWidens`: the change can grant access, even if it also restricts some.
- `AccessNarrows`: the change can remove access and cannot grant any.
- `AccessUnchanged`: the owner established that no role gains or loses access.
  Unchanged predicate text alone does not establish it.
- `AccessUnknown`: the owner cannot establish the effect.

The owner computes the assessment while it holds the captured model,
enforcement state, and sibling objects, and stores it in the payload as data.
`Access.Valid` and `AccessEffect.Validate` reject the zero value, other
spellings, and a reason that is empty, not valid UTF-8, padded with white
space, or that holds a control character or a line or paragraph separator,
with `ErrInvalidValue`. The JSON form is the record
`{"access": ..., "reason": ...}`. Encoding or decoding an invalid record fails.
Decoding refuses unknown, duplicate, or missing fields, a key spelled in
another letter case, and a value that is not a string. An owner embeds
`schemaext.AccessEffectSchema()` in its codec `Definition`, so the definition
hash follows the record shape; its reason pattern states the same constraint
`Validate` enforces.

Codec snapshots, encoding, decoding, and `ChangeRecord.Clone` refuse a payload
that implements the interface without a valid assessment, and refuse a clone
that lost it. A codec that drops the record therefore fails when it decodes,
and a planning reply whose operation lacks one is refused with no partial
result. `ValidatePayload` checks identity only, so a codec prototype needs no
assessment. A payload without the interface makes no claim about access.

`migration/safety` reads the assessment beside the lifecycle effect and takes
the higher severity. `AccessWidens` and `AccessUnknown` are `Destructive`,
`AccessNarrows` is `Warning`, and `AccessUnchanged` adds nothing. An assessment
it cannot validate is reported as `AccessUnknown`, never as unchanged.

`core/ast.PlacementOf` classifies how a node carries owner operations:
`NoExtension`, `IsolatedExtension` for a node that is exactly one owner
operation (an `ExtensionStatement`, an `ExtensionAlterOperation`, an ALTER
TABLE whose only operation is one, or a statement list holding only such a
node), and `MixedExtension` for a node holding an owner operation beside other
work or more than one. `OwnerOperations` returns the owner payloads a node
carries, read from the same node shapes.
`migration/planner.GenerateSchemaDiffASTWithOptions` refuses a plan containing
a `MixedExtension` node with an error wrapping
`migration/planner.ErrInvalidPlan`, whichever planner built it. That error
names a planner that broke the planning contract, not a defect in the diff, so
it is not `ptaherr.ErrInvalidSchemaDiff`. The bundled planners build every
owner operation as a node of its own.

`StatementAssessment.Access` and `AccessReason` report the assessment for each
statement an isolated owner operation rendered, and stay empty for every
statement no owner operation rendered. Every statement of an isolated node
takes that node's verdict, and only its own: a policy beside a `DROP TABLE` in
the plan keeps its verdict, and two policies keep two. A statement rendered by
a `MixedExtension` node takes the node's verdict raised to `Destructive`, with
an unknown access effect when the owner operation makes an access claim,
because which of its statements the operation wrote is not known. Raising
keeps a widening the node established. Nothing is rendered twice. A node
carrying several assessed operations reports the strongest, in the order
unchanged, narrows, unknown, widens. The text and HTML reports print it under
the statement. `AccessSeverity` is the severity an assessment requires.

`ClassifySchemaDiff` counts assessed changes under
`feature_access_widened:<kind>`, `feature_access_narrowed:<kind>`,
`feature_access_unchanged:<kind>`, and `feature_access_unknown:<kind>`, apart
from their `feature_changes:<kind>` lifecycle finding. A change that cannot be
snapshotted, an invalid assessment included, is counted as `Destructive` under
`feature_changes:<kind>` and, when it declares an assessment,
`feature_access_unknown:<kind>`; the kind is left out only when the change
names no valid one. Encoding the same diff through the owners' codecs refuses
that change instead. Comparison snapshots every change through its owner's
codec, so only a diff assembled by hand carries one.

`migration/planner.GenerateSchemaDiffRenderedPlan` plans and renders a diff
once and returns a `RenderedPlan`: the rendering request, which holds the
planned nodes, and one fragment per node. `PlannedStatements` splits it one
fragment at a time and gives each statement the index of the node that
rendered it, or -1 for a comment no statement follows. A comment-only piece of
a fragment joins the next statement and takes its node, and no fragment's
statements run into the next fragment's. `Statements` is the SQL of
`PlannedStatements`, and `SQL` the joined script; they are what
`GenerateSchemaDiffSQLStatementsWithOptions` and
`GenerateSchemaDiffSQLWithOptions` return for the same input.

`safety.OwnerVerdicts` takes the planned nodes and that provenance and returns,
position for position, the verdict of the owner operation that rendered each
statement, or a zero assessment. A statement an owner operation rendered always
carries a severity. Nothing is split, counted or compared there, and an index
outside the nodes is refused with `renderer.ErrInvalidResult`. `Fold` attaches
a verdict to the same statement classified from its text: the severity only
rises and the strongest access assessment is kept.

A saved schema plan uses `OwnerVerdicts` and `Fold`. The JSON plan marks each
statement an owner operation rendered with `owned` and records `access`,
`access_reason`, and the owner's higher verdict. Reading a plan refuses an
unrecognized access value, an access value without its reason, a reason
without a value, an access value on a statement that is not `owned`, and a
severity below the one the access value requires.

An `--edit` that leaves the statement sequence as it was, comments and
whitespace aside, keeps every recorded verdict by position. After any other
edit, a statement whose text the plan recorded takes the strongest verdict
recorded for that text. When the plan carried an `owned` statement, every
statement that carries an owner verdict and every statement the edit
introduced is then raised to `Destructive` with an unknown access effect; the
raise keeps the reason the statement had and never lowers an access value. The
edit does not try to decide which changes leave an owner's verdict valid;
planning again gives fresh verdicts. The Atlas `.plan.hcl` format stores SQL
alone, so a plan read back from it has only the text verdict.

`feature/pgpolicy` owns the PostgreSQL row-security models of ADR 0020. A
policy is a feature object of `PolicyKind`, identified by its schema, table and
name through `PolicyRef`, so two tables may each hold a policy of one name.
`DesiredPolicy` declares the command, the role selectors, optional USING and
WITH CHECK expressions, permissive or restrictive composition, and a comment.
An omitted value requests PostgreSQL's default (ALL, PUBLIC, permissive), and a
nil expression declares no clause. `ObservedPolicy` holds what `pg_policy`
reports, every value definite.

A `RoleSelector` is a keyword or a role name and never both, and a name keeps
its exact bytes. PostgreSQL reserves the role names `public` and `none` and
stores `TO "public"` as PUBLIC, so neither is accepted as a name; PUBLIC is the
keyword, and it stands alone, because the server keeps only PUBLIC from a list
that names other roles beside it. An observation carries only the PUBLIC
keyword, because the catalog resolves the others to a role when the policy is
created. The role list is a set: equality and the canonical encoding ignore its
order, and a reader records a role `polroles` lists twice once. `PolicyRef`
takes its parts as the catalog stores them, untrimmed, because PostgreSQL keeps
a policy named ` p` beside one named `p`.

`DesiredTableState` and `ObservedTableState` are a table facet
of `TableStateKind` that holds ENABLE and FORCE ROW LEVEL SECURITY as
independent flags. The codecs refuse unknown, null, empty and case-variant
keys, a clause the command does not take, and an empty expression, and a
refused value is a `schemaext.InvalidModelError`.

Changes and operations carry the owner's access assessment as data.
`PolicyChange` holds the observed and declared policy, the comparison's
comment-only finding, which the operands cannot show because the catalog
respells expressions, and an `AccessEffect`. `TableStateChange` holds both
switch states. `PolicyAccess` and `TableStateAccess` assess what a change can
do:

- a permissive policy that reaches more commands or roles can widen access,
  and a restrictive one can only narrow it;
- a changed expression, and a role keyword the server resolves when the
  policy is created, are unknown;
- ENABLE and FORCE narrow, DISABLE and NO FORCE widen, and FORCE matters only
  while row security is enabled.

`UnenforcedAccess` is the assessment for a table that enforces row security
neither before nor after the change, and `CreatedTableAccess` the assessment
for a statement on a table the plan creates, which no role could read before.
`PolicyOperation`,
`PolicyCommentOperation` and `TableStateOperation` are the statement payloads.
A comment is its own operation, gated by `capability.PolicyComments`, because
CockroachDB holds policies and not their comments.

The change and operation codecs check each operand, and each change inside an
operation, with its own codec. The model's wire rules hold there too, and a
value encodes to the same bytes nested as alone. A refused value is a
`schemaext.InvalidModelError`.

`feature/pgpolicy/policyrender` renders the payloads for the PostgreSQL
family. CREATE POLICY writes only what the declaration names, so PostgreSQL
applies its own defaults; a change drops the policy and creates it again; and
ENABLE precedes FORCE. The TO list is written in the canonical role order, and
`CreateStatement` is the one place the statement is written. `LowerTableFacets`
turns a new table's declared switches into the statements that follow its
CREATE TABLE. The builtin runtime registers the change and operation codecs and
composes the handlers on every PostgreSQL-family target. No source or planner
produces these payloads.

A declaration can carry a connected server's spelling of itself,
`DesiredPolicy.Normalized`: the role list and clauses as `pg_policy` would
report them. PostgreSQL stores a clause as a parse tree whose casts depend on
the column types, and it records the role CURRENT_USER and its siblings
resolved to, so the declared text alone differs from the catalog for a policy
nobody changed. `ComparedRoles` is the role list a comparison and the access
assessment hold against an observation. `DesiredPolicy.Observed` and
`ObservedPolicy.Desired` project between the representations; a policy TO a
role keyword has no projection until a server resolves the keyword.

`feature/pgpolicy/policycompare` compares policies by schema, table and name.
`PolicyService` resolves PostgreSQL's defaults (an omitted FOR is ALL, an
omitted TO is PUBLIC, an omitted AS is permissive), compares the role list as
a set, and compares the clauses through the server's spelling where a probe
attached one and through a textual fold otherwise. `TableStateService` compares
ENABLE and FORCE on surviving tables; where a source describes the switches, a
table without the facet has both off. Neither changes what a source could not
describe: an observed policy or switch it could not describe is adopted into
the effective declaration, and unread current state is undecided. A policy or
switch on a table the plan creates or drops belongs to the table's lifecycle.

`feature/pgpolicy/policyconvert` projects both models between representations.
`feature/pgpolicy/policyprobe` attaches the spelling: inside a rolled-back
transaction it copies the policy's table into `pg_temp` with `LIKE`, creates
the declared policy on the copy and reads `pg_policy` back. A probe the server
refuses, in a read-only transaction, for a role without SELECT on the table or
for a role that does not exist yet, leaves the declaration unanswered and is
not an error.

`feature/pgpolicy/policyplan` plans the changes of surviving tables and the
policies of a table the plan creates. Every operation asks for the dependent
phase. A statement names its table as the source spelled it, so a table the
source left in the default schema is named without one. A policy that changes
is dropped and created again in one step that requires a transaction, a
declared comment is set after the policy is created, and a table's switches are
written under `TableStateSubject`, apart from the table the host alters. The
steps of one table run in access order: what can only narrow access first, what
can widen it last, and a change of unknown effect between, so a plan without a
transaction never admits more than its start or its end. A table the host drops
takes its policies and switches with it, and one it alters keeps them; a
rebuild is refused.

A table the host creates gets its policies from the owner, with unchanged
access, and its switches from the statements after its CREATE TABLE. A
whole-schema render has no phases, so there each declared policy is created
after every common step: its expressions may name any table, view or routine
the render creates.

`feature/pgpolicy/policyreverse` swaps a change's
operands, projects the forward declaration as the server reports it, and
assesses the access of each inverse again. Every inverse states that it does
not undo access the forward plan granted or withheld, and a policy TO a role
keyword nobody resolved is irreversible.

`feature/pgpolicy/policyreport` counts policies, and the tables whose
switches are on, for inventory and omission reports. A dropped policy and a
switch turned off are the changes that take state away (`RemovesState`).

The bundled runtime selects these services on PostgreSQL, CockroachDB and
YugabyteDB. No source or reader produces the models yet, so they act only on
values a caller builds. Spanner speaks the PostgreSQL dialect without row
security, so a declared policy or set of switches is refused there.

`dialect/mssql/mssqlproperty` owns SQL Server extended properties. A property
is one feature object of `Kind`, addressed to the database, a schema, a table or
a column; `Property.Ref` folds every part, since the default collation compares
names without case, and keeps the table out of the parent slot, so the owner
plans it apart from the table's own transition. `DesiredProperty` and
`ObservedProperty` hold the value; a read records a value held under a type Ptah
cannot write back as unrepresentable coverage (`UnrepresentableValue`) instead
of a value. `DeclaredObject` binds a declaration to SQL Server. The package
holds every stage's service: `CompareService`, `PlanService`, which joins the
host's dependent window and reads the table or column a property is on,
`ReverseService`, `ConvertService`, `ReportService` and the render `Handlers`.

`feature/synonym` owns synonyms, the aliases SQL Server and Oracle resolve to
another object. A synonym is one feature object of `Kind`, identified by its
schema and name through `Synonym.Ref`, which folds case; the target is not part
of the identity, so a changed target is one change. `TargetParts` reads a
target of one to four parts from the right and `DeclaredTarget` writes it back,
an empty middle part included, and `SameTarget` compares two targets under a
connection's identifier rules, without their quoting and case and with an
absent schema read as the default schema. `DeclaredObject` binds a declaration to `Targets`, SQL
Server and Oracle. The package holds every stage's service: `CompareService`,
`PlanService`, which joins the host's dependent window, `ReverseService`,
`ConvertService`, `ReportService` and the render `Handlers` for both targets.

`dialect/mssql/mssqlschema` owns the SQL Server security policy model of ADR
0020. A policy is one feature object of `SecurityPolicyKind`, identified by its
schema and name through `SecurityPolicyRef`, with no table parent, because one
policy may bind several tables. `DesiredSecurityPolicy` holds its predicates,
its state, its schema binding, whether replication agents skip it, and the Go
struct it was read from. A nil `Enabled` or `SchemaBinding` requests SQL
Server's default, ON. Its `Normalized` predicates are the connected server's
spelling of the declared ones, attached by a probe before a live comparison
and never rendered. `ObservedSecurityPolicy` holds what
`sys.security_policies` and `sys.security_predicates` report, every value
definite. Either may hold no predicate, as SQL Server allows.

Each `Predicate` keeps its table and its function as schema-qualified
`ObjectName`s, the function's arguments in parameter order, FILTER or BLOCK,
and for a block predicate the exact operation: AFTER INSERT, AFTER UPDATE,
BEFORE UPDATE or BEFORE DELETE, or none for every write. The predicates are a
set: equality and the canonical encoding ignore their order. Validation
refuses a filter predicate with an operation and two predicates for one
operation on one table, where a block predicate for every operation conflicts
with any other block predicate on that table. Two spellings of a table that
differ in letter case or trailing spaces count as one table, since a
case-insensitive database refuses them as one; a case-sensitive database would
accept them. A blank schema, name or argument is refused.

The decoders accept only what the encoders write, so an omitted value spelled
out, such as an empty struct name or argument list, is refused. Every refusal
of a value, from a validator, a codec or an object constructor, is a
`schemaext.InvalidModelError`; `ValidateSecurityPolicyRef` refuses an identity
with `schemaext.ErrInvalidValue`.

The package also holds the offline comparison the services share: `CompareArgument`
reads a declared predicate argument and the catalog's spelling as T-SQL
tokens, since SQL Server stores `tenant_id` as `[tenant_id]` and `CAST(t AS
int)` as `CONVERT([int],[t])`. Two single identifiers or literals agree or
differ, equal token sequences agree, and anything else is `Undecided`.
`ComparePolicy` applies that to whole policies under identifier rules,
comparing a declaration by its `Normalized` spelling where it has one, and
`EnabledTableConflicts` finds a table two enabled policies bind, which SQL
Server refuses with Msg 33264. `ParseInvocation` reads a predicate written as
a call of a two-part function, in a declaration's spelling or the catalog's,
and `CatalogPredicate` reads one row of `sys.security_predicates`.

The owner's services are registered for SQL Server in the bundled runtime. The
Go source declares a row-level security annotation scoped to SQL Server as a
security policy, and the SQL Server reader reports the policies a database
holds, recording one whose predicate it cannot read as uninspected. The shared
row-level security model holds no SQL Server policy: a shared policy or switch
node that reaches the SQL Server renderer is named and skipped.

- `mssqlcompare` pairs policies by schema and name. It plans a creation, a drop
  or a change carrying `mssqldiff.Assess`'s access effect, and reports a pair
  that differs only in arguments the server may have rewritten as undecided.
  It keeps an observed policy a description could not express, and refuses a
  desired schema in which two enabled policies bind one table.
- `mssqlrelation` names the tables and predicate functions a policy binds,
  complete only when every argument is a column or a literal.
- `mssqlplan` places a change in the dependent phase. Its step reads every
  table, function and argument column the policy binds before and after. When
  a table moves from one enabled policy to another, the step that releases it
  runs before the step that takes it, and both need the plan's transaction. A
  whole-schema render creates each policy after the common statements.
- `mssqldiff.SecurityPolicy.Edit` is the statement plan the renderer writes
  and the planner sizes its transaction by. Schema binding and replication are
  changed by drop and create, a slot is altered where both sides spell it the
  same, and the state is set around the predicate statements.
- `mssqlconvert`, `mssqlreverse` and `mssqlreport` convert, reverse and count
  policies.
- `mssqlprobe` attaches the server's spelling of a declared policy whose
  comparison with the one the database holds is undecided. It creates the
  declaration turned off, under a probe name, with
  `mssqlrender.CreateStatement`, reads `sys.security_predicates` back, and
  rolls the creation back. A probe the server refuses leaves the declaration
  unanswered.

`engine/builtin.GetOrderedCreateStatements` and its capability-aware variant
render complete schema DDL fail-closed. Non-SQLite targets return all table
creation statements before phase-two foreign keys; SQLite keeps foreign keys
inline. Invalid or unsupported foreign keys return typed errors and no partial
statement list. Foreign-key-capable capability sets select exactly one
referenced-key policy: `ForeignKeysRequireUniqueReference`,
`ForeignKeysRequireIndexedReference`, or `ForeignKeysCreateBackingIndex`.
`ValidateSchema` and `ValidateSchemaWithCapabilities` run the same complete
schema validation without rendering SQL. The selected built-in validation service
uses those checks before comparison produces a diff for planning.

`GetOrderedCreateStatementsReportingOmissions` renders the same statements and
also returns the declarations the target did not carry, as `renderer.Omission`
values. The record belongs to the neutral rendering contract.
It is the same render rather than a second one, so the statements it returns
equal what the capability-aware variant returns for the same arguments.

A `renderer.Omission` names the target, a stable `Reason`, the owning object, the lost
property where a property rather than the object was lost, the declared value,
and a remedy only where one works on that target. The order is deterministic
and does not follow the walk.

A refusal is an error, not an omission: a non-nil error carries no statements
and no omissions, because nothing was rendered for a declaration to be missing
from. Reporting is not exhaustive over every property every dialect drops. It
covers what a renderer names as skipped, the table options a target cannot
carry, the comments a target does not store, an index's partial condition and
operator class, and a column's identity clauses. stokaro/ptah#2983 records what
remains.

An object whose key payload is empty is refused with
`ptaherr.ErrInvalidSchemaDiff` before any statement is emitted:

- an index naming no column and no expression;
- a UNIQUE or PRIMARY KEY constraint naming no column;
- a CHECK constraint carrying no expression.

These are not dialect refusals. No engine accepts the shape, so the guard sits
on the AST that both entry points converge on and `RenderSQL` refuses the same
nodes. A blank column name counts as no column, because a structured key part
carrying only a direction or a prefix length converts to one.

`core/ast` and `engine/builtin` carry the DDL language: the visitor node tree
and the dialect engines that turn it into schema SQL. `core/query` carries the
whole DML language: the SELECT / INSERT / UPDATE / DELETE statement and
expression tree, the fluent builders that produce it, and `RenderSelect`,
`RenderInsert`, `RenderUpdate`, and `RenderDelete`, which return parameterized
SQL plus its arguments for a named dialect. Each has a `WithCapabilities`
variant that renders for the release line whose capabilities the caller
passes; on YDB the arguments are `sql.NamedArg` values named after the
placeholders.

`core/astbuilder` builds `core/ast` nodes by method chaining instead of by
nested struct literals. `NewTable` and `NewIndex` return one statement node;
`NewSchema` returns an `*ast.StatementList`. The builders return AST types and
nothing of their own, so a chain and a hand-written literal mix freely, and a
node the builders do not model stays reachable through `core/ast` directly. The
schema-scoped types — `SchemaTableBuilder` and its siblings — carry the same
configuration methods as the standalone ones and differ in where `End` returns.
Nothing here validates: an unknown type, an unresolved foreign key, or an
unparsable default reaches the AST and is reported by `engine/builtin` or by the
database.

`core/yamlschema` reads a desired schema written in Ptah's YAML format. `Parse`
takes the document as bytes and `ParseFile` reads it from a path; both return
the `*schemamodel.Database` that Go annotations, HCL, SQL, and DBML also
produce, and nothing downstream can tell which reader filled it. Parsing is
strict in two ways: an unknown key is an error rather than a silent drop, and a
second YAML document in the same stream is refused rather than ignored.
`core/schemasource` covers the other direction — an external program that
writes YAML to its standard output — and parses that output through this
package.

Both entry points take a `yamlext.Set` first: the feature owners whose models
the document can declare. `core/yamlext` is that contract. An `Extension`
names its owner, the models it claims and the knowledge a YAML document holds
about them; `engine.Provider.YAML` registers it, and `engine.Runtime.YAML`
returns the frozen set. A document claims knowledge of an owner's models only
when the parse selects the owner, so a parse without it leaves them unknown
rather than absent. The zero set is refused with `yamlext.ErrUnselected`, and
`yamlext.None` selects no owner on purpose. `schemasource.Run` takes the owners
the same way, through `schemasource.Owners`, which `*engine.Runtime`
implements.

An owner may also read top-level keys of its own. Each `yamlext.Section` names
a key and decodes its value through a callback. The callback refuses a key the
owner's type does not declare and names the line, as the frontend does for its
own keys. The decoder receives the document's tables, and `Tables.Find`
applies the document's rule for a table reference. It returns objects and
facets of those tables. A key that no selected owner reads is unknown and
refused, so a parse without the owner refuses the owner's section instead of
dropping it.

`schemamodel.Extension.Schema` records a PostgreSQL extension's installation
schema. `ast.ExtensionNode.Schema` and `SetSchema` carry the same intent into
SQL rendering, which emits `CREATE EXTENSION ... WITH SCHEMA ...` after any
`IF NOT EXISTS` clause and before `VERSION`. An empty schema means the target's
default schema. Embedders should preserve the field when converting or copying
schema IR; dropping it can move an extension into the wrong namespace.

`core/coverage` carries what a schema description does **not** claim to
describe. `schemamodel.Database.NotDescribed` and
`catalog.Database.NotDescribed` hold one, and schema comparison consults both:
the desired state's record gates removals and the introspected state's record
gates additions. Its zero value applies no limits to common kinds. Set it
when a reader was asked about less than the whole database, or a projection
left something out on purpose; leaving it zero there is how an object nobody
looked at becomes a `DROP`.

Feature models use `schemaext.Coverage`, whose zero value is unknown.
Resource pools and classifiers have no common coverage kind.
`coverage.DecodeHeader` takes an explicit `HeaderExtension` callback, or nil
for common kinds only. The callback receives validated directives outside the
common vocabulary and records them in the owning feature's coverage. It cannot
override common kinds. Unclaimed kinds and callback errors refuse the document.
`Object.Directive` encodes an owner-validated record without adding it to the
common set. Split exports carry recognized owner records, including their
reason, provenance, and exact name, in every output file.

`Registry.EncodeCoverageHeader` and `DecodeCoverageHeader` transport a versioned
feature account in a leading `ptah:feature-coverage` comment. They validate exact
model identities without adding registered models to the account. No header
means no claims. `coverage.HeaderComments` supplies the shared boundary before
the first content line. HCL uses this account for coordination nodes; ordinary
Atlas HCL does not establish their absence. Split HCL preserves the account in
every member.

Every schema comparison takes a context and an explicitly selected runtime.
Catalog comparisons accept `schemapreparation.Runtime`. Document comparisons
accept `schemadiff.DocumentRuntime`, which also selects CREATE prediction. Offline target-aware
entry points accept `schemadiff.TargetRuntime`, which also validates the desired
schema against target facts.

Comparisons resolve the target before selecting default capabilities or checking
identifiers. They collect scoped omissions before filtering the desired schema
and suppress the same observed identities, so an excluded declaration cannot
request a drop. `ValidateRolePasswordComparison` also requires the resolved
`TargetSelection` and refuses an unresolved value.

Live comparison requires `schemadiff.DatabaseRuntime`,
adding the selected AST renderer for normalization probes. Each probe renders
its cleanup or fallback statements in the same batch before execution. Completed
rendering refusals, omissions, and empty fragments leave normalization unresolved.
Service failures and malformed replies abort comparison.

Oracle generated-expression probes render all probe tables through the selected
service in one batch. They require nonempty SQL for every table and reject
reported omissions before opening the dev database. Inference-store schema
creation uses the same batch contract for its tables and indexes. The native
inference command supplies an explicit PostgreSQL baseline profile; store reads
do not require rendering.

`ValidateDesiredSchema` requires context and a selected
`schemavalidation.Runtime`, which combines validation with target resolution;
nil declarations are errors. Provider
failures return an error and no diff.
Nil schema pointers are missing inputs and return `ErrInvalidSchemaDiff`.
Pass an explicit empty schema value to compare against an empty declaration or
catalog; its feature coverage still determines what the source established.
A comparison involving feature values, or a limit a source recorded about one
subject, also requires an explicit target. A kind-level knowledge claim with no
value on either side does not: a PostgreSQL-family read records what it knows
about the TimescaleDB models even where it found none. A supplied
identifier-semantics snapshot must cover every compared identity; an incomplete
snapshot is refused rather than replaced by fallback name rules.

`schemadiff.CompareReportingUndecidedAdditions` returns established changes,
`schemadiff.Diagnostics`, and an error. The diagnostics retain common-object
coverage limits and feature limits with structured subject identities and
reasons. `schemadiff.CompareWithDatabaseReportingUndecidedAdditions` provides
the same report while resolving the connected catalog's identifier semantics.
Callers presenting partial results must also report those limits. Non-reporting
entry points refuse an incomplete comparison with `ErrIncompleteComparison`
and return no diff; `errors.As` exposes the diagnostics through
`IncompleteComparisonError`.

`schemadiff.RefusalError` identifies a completed common declaration check and
preserves its original error. Selected validation diagnostics use
`schemavalidation.RefusalError`. Service failures and malformed provider replies
remain errors without either receipt; an error's message or sentinel alone
does not establish that comparison completed its checks.

`generator.GenerateMigrationOptions.Runtime` requires a `generator.Runtime`,
which combines comparison, conversion, validation, planning, rendering, and
reversal services. Rollback validation uses the same selection on the captured
prior schema before publishing files. The
`BidirectionalSchemaPlanOptions` and `CheckpointFromShadowOptions` require the
same explicit selection. `shadow.MigrationVerifyOptions`, `shadow.BaselineVerifyOptions`, and
`shadow.DynamicRollbackOptions` carry the services their comparisons use.
Migration and baseline verification include selected target validation. Dynamic
rollback keeps that selection through replay and comparison. A generator caller that accepts partial
comparison evidence supplies `OnUndecided` and reports its structured limits;
without that callback, incomplete evidence returns `ErrIncompleteComparison`.

Checkpoint generation compares the captured schema with an explicitly empty
baseline. That baseline records known absence for the selected runtime's observed
feature models. This does not change the source's coverage: unreadable source
definitions still cause refusal before any checkpoint SQL is returned.

Modified table operands capture their effective desired declaration and current
observation, including owned feature objects and coverage. Preserved observed
objects reach the desired operand before it is captured. A feature-only change
keeps its table diff non-empty. Constraint-only changes retain both
`DeclaredConstraintHosts` and `ObservedConstraintHosts`, so a table rebuild
never infers current feature state from its desired declaration. Rebuilds require
complete relevant coverage, including when the captured namespace is empty.

MySQL-family readers populate the JSON-hidden execution facts
`catalog.Function.Definer` and `catalog.Database.CurrentAccount`, the second a
fact about the reading connection rather than the database. Database-aware
`schemadiff.CompareWithDatabase` entry points use them, on a target whose
capability set has `capability.RoutineReplacementResetsDefiner`, to refuse a
modified `SQL SECURITY DEFINER` routine when recreating it would change the
executing account. The capability states how Ptah's plan replaces a routine,
so the rule is not tied to a dialect. Custom readers that supply a modified
definer routine must preserve both facts; missing facts fail closed with
`ptaherr.ErrInvalidSchemaDiff`. Offline comparison has no live ownership facts
and is not the safety boundary for applying such a replacement.

`schemamodel.Finalize` rebuilds materialized inline, JSON, and relation fields
on every call. `Field.GeneratedFromEmbedded` identifies those derived fields so
a caller can mutate the source fields or embedded declarations and finalize the
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

`BannerPlatform` answers a narrower question than `ResolveServerVersion`: which
product does a version string name, if any. Callers holding a version a person
typed need it, because two typed values naming two different servers are a
contradiction no preset resolves. It is deliberately not the same answer as
`VersionResolution.ResolvedDialect`, which reports the ladder the capabilities
came from — a banner naming only PostgreSQL leaves an explicitly declared
CockroachDB, YugabyteDB or Spanner target on its own preset, because all three
speak the PostgreSQL wire protocol and may report exactly that banner.

`migration/lint` provides the compact `LintFS` findings API and the richer
`AnalyzeFS` API. `AnalyzeFS` captures each migration input once: SQL files,
integrity metadata, and `.ptah-lint.yaml`. It returns deep-copy views of
prepared files and findings together with a read-only source snapshot. Capture
does not apply the lint policy automatically: embedders call `LoadConfigFS`
and pass its `Dialect`, `DisabledRules`, and `Rules` through `lint.Options`.

`lint.Target` is the server an analysis plans against, and every prepared
`File` and `Statement` carries it, so a rule whose verdict depends on the
server reads `Target.Capabilities` rather than comparing version strings of its
own. `ResolveTarget` maps a dialect and an operator-supplied version onto one,
with the same refusals `--server-version` carries elsewhere: a version naming
no server, a version naming another product, and a version with no dialect are
each an error rather than a silent fallback to the default. `TargetFromServer`
is the live counterpart and refuses nothing, because a banner a server wrote is
what is actually there. A run that passes no target analyzes against the
dialect default, `Analysis.Target` reports which, and `Target.Named` separates
a server that was named from a default that was assumed.

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

`migration/migrationfile` is the migration file-layout toolkit: what a
migration directory and its files mean, with no database and no execution.
`Discover` walks a filesystem in a `DirFormat` (`ParseDirFormat` normalizes the
CLI spelling) and returns `File` values; `ParseFileName` and
`ParseAtlasFileName` read one name; `FileName`, `CheckpointFileName`, and
`NextVersion` produce names for writers. `ParseDirectives` and `ParseTimeouts`
read the `-- +ptah` directive header, `ParseFileTxMode` and `ParseUp` resolve a
file's transaction mode across both directive families, `MisplacedDirectives`
diagnoses directive lines outside the region where they are significant,
`ParseAtlasTxtar` unpacks Atlas txtar archives, and `RenderAtlasTemplateSQL`
renders Atlas SQL templates. The migrator engine builds on this package;
linters, importers, and compatibility tooling use it without importing the
engine.

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
with `migrationfile.FileTxModeUnspecified`, `migrationfile.FileTxModeFile`, or
`migrationfile.FileTxModeNone`. `migrationfile.ParseUp` gives tools the
executable up-direction SQL, explicit mode, and source-line offset from plain
SQL or Atlas txtar content. The two directions remain independent; the
migrator resolves the up value against its global transaction mode and treats
an unspecified down value as `file`.

Atlas transaction-mode directive validation errors expose
`migrationfile.AtlasTxModeDirectiveError` through `errors.As`. The leaf error
keeps the source file and transaction-mode details in its message.

`migration/migrationfile.ParseDirectives` reads the file's directive header —
the run of blank lines and line comments before the first executable statement
— rather than the whole file. A `-- +ptah` line written below the statements it
claims to govern is not honored. Atlas transaction mode keeps the stricter
initial column-1 comment-block rule measured from Atlas CE. A directive outside
its region is reported at `WARN` by the migrator
rather than dropped in silence. Ordered `-- +ptah check` directives are
unaffected: they are position-insensitive by design and `ParseChecks` still
reads the whole file. Atlas directives retain their stricter individual header
rules; for example, `atlas:txmode` must be in the unbroken column-1 comment
block at the start of the file.

Position and value stay separate verdicts. A `-- +ptah` directive whose key is
recognized but whose value cannot be read fails `migrationfile.ParseUp`,
`Migration.Up` and `Migration.Down` wherever the line sits, and the error names the line, so a
typo is never demoted to a position warning. An unrecognized bare token and an
unknown `key=value` pair are not directives and produce neither. The `-- atlas:txmode`
spelling is reported but not refused outside its block, because Atlas CE applies
such a directory.

This pre-GA API replaces the former `UpNoTransaction` and
`DownNoTransaction` Boolean fields. `NewFSMigrationProvider` (with
`WithStatementInterceptor` when an external executor takes over statements)
loads complete `Migration` values, preserving both directions' transaction
modes, timeouts, source paths, and execution functions as one coherent value.

`MigrateUpOptions.PlanObserver` receives the plan recalculated under the
migration lock before transaction-mode validation, including empty plans. It
is a metadata-only observer and cannot abort execution. `Preflight` remains the
abort-capable hook after validation, so user-facing start output is not emitted
for a statically invalid migration.

`MigrateUpOptions.PlanGuard` receives the same plan right after the observer
and can refuse it. It runs for every selection, including an empty one, and a
non-nil error stops the run before transaction-mode validation, `Preflight`, or
any schema or revision change. It exists for a caller that approved a plan
earlier and has to know that the run executes that plan and nothing more;
`Preflight` cannot carry that check, because it is skipped for an empty plan.

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

`dbschema.ReadRehearsalSchemaContext` uses a private reader and selects its
optional `catalog.RehearsalSchemaReader` service. This service can include
observed environment objects outside the reader's writable scope for baseline
comparison. Readers without it perform an ordinary schema read. Unknown state
stays unknown, and a failed read returns no partial schema. The caller must
validate the entire generated baseline against the allowed execution scope
before applying any statement. An environment observation grants no permission
to change it. YDB uses this to compare database-wide workload settings while
rehearsing directory-local table changes.

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

`dbschema.DatabaseConnection.WithRolledBackTransaction` runs a callback inside
one transaction on one throwaway physical session, rolls the transaction back
whatever the callback does, and then discards the session. It exists for
callers that must create something on the server only to read back how the
server stored it -- Ptah's own expression probes are the canonical consumer --
and it is the supported way to do that without leaving state behind. A
connection already pinned to a session reports `ran` false with a nil error
and never runs the callback: the rollback would discard the session owner's
work. Callers that need the transaction must check `ran` as well as the error.

`dbschema.DatabaseConnection.WithIsolatedQuerySession` exposes a query-only
`dbschema.IsolatedQueryer` on one physical session. Transaction-capable drivers
always roll the transaction back; ClickHouse runs directly on the disposable
session because its driver does not implement transactions. Ptah discards the
physical session afterward, except for in-memory SQLite, whose only connection
owns the database lifetime and is returned to the pool after rollback. The
callback cannot control transactions or reach Ptah schema writers. Callers
remain responsible for restricting SQL to read-only queries.

`migration/migrator.CheckFailedError` identifies one failed or invalid
assertion, in either phase: its `Phase` field carries `CheckPhaseBefore` or
`CheckPhaseAfter`, and the zero value is the precondition phase a directive
that names none selects. `CheckGroupFailedError` identifies an Atlas `oneof`
check file in which no assertion returned a truthy result, including an empty
group. Callers can distinguish a group-level precondition failure from an
assertion execution or result-shape failure with `errors.As`.

`migration/migrator.PostMigrationCheckFailedError` wraps the check failure of a
`phase=after` assertion, and is a type of its own because the outcome it names
has no equivalent on the precondition path: the body is committed. On the up
direction the migration is recorded as applied, nothing is rolled back and
nothing is re-run; on the down direction the rollback completed and its
revision is gone. `Direction` says which, and embedders must classify this
error BEFORE `CheckFailedError`, which it unwraps to — advice to retry with
checks skipped is wrong for a migration that already applied.

`migration/migrator.ParseChecks` requires the target dialect together with the
SQL source. This intentional pre-v1 signature change prevents fail-open parsing
when PostgreSQL escape strings or MySQL/MariaDB comment rules determine whether
a later check directive is SQL code or literal/comment content.

`migration/migrator.VerifyChecks` evaluates those same checks against a database
that is already running and writes nothing to it, returning a `VerifyReport`
with one `VerifyResult` per check in the order given. Each assertion is proved
to be a single read-only `SELECT` before any query is sent and is evaluated in
its own read-only session, so one the server refuses cannot decide the outcome
of the next. The guarantee covers what Ptah sends and the session it sends it
in: a routine the assertion calls that opens a transaction of its own is not
undone with that session. A returned error means no session, no answers, or a
session that could not be undone; an assertion
that ran and did not hold, or could not run, is a status in the report rather
than an error. `VerifyReport.Verdict` reduces a run to one word and never
reports an empty run as verified.

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

`migration/shadow` verifies migrations against a live disposable database:
`VerifyMigration` measures a candidate before its files are written,
`VerifyBaseline` measures a replayed history against the target,
`VerifyRollback` rehearses a rollback plan, and `PlanDynamicRollback` derives
rollback statements from the schema a version defines rather than from a down
body. Every entry point refuses a shadow database that holds anything the reset
would drop or resolves to the target's live realm, drops it clean before the replay, and
empties it again before returning.

`migration/generator.GenerateCheckpointFromShadow` and
`migration/shadow.VerifyBaseline` apply the same SQLite-only validation before
connecting to or mutating a shadow database. The checkpoint path therefore
cannot drop and replay a shadow database before reporting a malformed value.

`migration/generator.PlanBidirectionalSchemaDiff` is the lower-level planning
boundary for callers that already hold a schema diff. Its input binds the diff,
desired and current schemas, normalized dialect capabilities, and concurrent
index policy into one result with forward and reverse diffs, AST nodes, exact
table-qualified concurrent-index references, and an independent
`RequiresNoTransaction` classification for each direction. The reverse restores
captured table removals and the surrounding current schema. Restored constraints
retain their validation status, exclusion elements, and exclusion predicates.
The entry point takes the caller's context and propagates service failures and
cancellation. `PriorSchema` retains the converted rollback target, so rendering
does not select another runtime or reconstruct the target again.

`SchemaDirectionPlan.Recovery` retains owner-provided reversal assessments.
Reverse AST nodes include their strategy and every recovery limit as comments.
Named-object and table-facet projections, with accepted column, index, and CHECK changes, produce the
reverse input capture. Index additions, removals, renames, visibility, and comments
are projected using captured identifier semantics. Named CHECK creation and removal,
constraint comments, and constraint validation update the captured host, including
simultaneous column changes. Validation remains in effect during rollback.
Skipped removals preserve captured columns and indexes, including their comments.

Reverse table removals project accepted creations and their accepted child
additions. They retain feature coverage limits and do not use an edited desired
document as evidence of what the forward plan creates. A projected capture is
planning input; it does not establish that the migration executed.

A programmatically built YDB declaration must enroll its changefeed model even
when it declares no streams if rollback will remove the created table. Use
`ydbschema.ChangefeedCoverage(schemaext.Desired, nil)` for a complete empty
namespace. Zero coverage means the source did not describe that namespace;
reverse planning retains this limit and refuses the drop. A later edit to the
desired document cannot strengthen the already accepted creation capture.

Rollback restores a removed table and its captured children from the observation.
Later edits to the caller's catalog cannot replace that state. Missing captures,
mismatched identities, and children owned by another table are refused before
planning. `CurrentSchema` in the result includes these restored observations.
Their coverage applies only to the captured parents; it cannot establish
knowledge about unrelated tables.

Table captures own their nested mutable fields. The `Clone` methods on catalog
tables, columns, constraints, and indexes, and on schema model tables, fields,
constraints, indexes, enums, and triggers provide the same isolation to callers.

A table transition that lacks a complete projection is refused when reversal
needs its feature state. Key, foreign-key, and exclusion-constraint creation and
removal use the selected target's constraint projector for backing indexes and
column effects. An unavailable prediction leaves no reverse capture and refuses
feature reversal that needs it.
Index partitioning, table properties, triggers, and security transitions also
need their corresponding state projectors before such combinations can be planned.

On MySQL and MariaDB, rollback also removes a foreign-key backing index created
by the forward migration while preserving any prior or same-run index whose
leading key columns cover the foreign key. Planning refuses an ambiguous
incomplete-index shape or a plan that later removes every covering index.

`SchemaDiff.ForeignKeysRemovedWithTables` carries the local and referenced
columns needed for that ordering as supplemental metadata keyed by table and
constraint name. It does not independently represent a removal and is ignored
without a matching `ConstraintsRemoved` entry, leaving the existing comparable
`ConstraintRemovalInfo` value unchanged.

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

`MigrationPlan.Close` releases the migration directory the plan holds without
publishing anything, for an embedder that builds a plan and then decides
against it. It is a no-op on a published plan and a no-op called twice, so it
composes with `defer`. An embedder that never calls it leaves the directory
held until the plan is garbage collected, which on Windows blocks removing or
renaming that directory for as long as it takes.

`GenerateMigrationOptions.PriorMigrationsFS` carries an immutable,
already-authorized migration history into shadow verification. The same
snapshot becomes a publication precondition: `WriteFiles` returns
`generator.ErrMigrationDirectoryChanged` if the bound output directory no
longer matches it, so a refreshed integrity file cannot legitimize different
history.

`migration/safety.RenderJSON` writes a `safety.Report` containing the highest
risk, destructive verdict, and rendered statement assessments. The native
`ptah migrations plan --report json` command writes that document to standard
output. `GenerateMigrationOptions.ReportFormat: "json"` instead publishes one
`.safety.json` file beside each generated migration pair.

Candidate and baseline shadow failures preserve a typed
`*shadow.VerificationError` through `PlanMigration`,
`GenerateMigration`, `shadow.VerifyBaseline`, and command wrappers. Use
`errors.As` to inspect `Result.Stage` and the structured `Result.Mismatches`;
operational failures also expose their underlying error through `Unwrap`. A
schema mismatch carries the complete, deterministically ordered mismatch list
without a wrapped execution error. Baseline failures retain the
`baseline shadow check failed:` display prefix while preserving this typed
contract. Candidate and baseline verification share stage names at common
boundaries. Baseline verification can additionally report `target-introspect`,
`reset-schemas`, and `drop-metadata`; candidate-only `round-trip-down` and
`round-trip-up` stages do not occur during baseline verification.

`migration/shadow.VerifyRollback` requires the caller's open
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
Both `Options.Runtime` and `SchemaOptions.Runtime` require an explicitly
selected `engine.SchemaRuntime`. A missing runtime or canceled context
is refused before any database is opened. The same runtime converts and
compares the desired schema and plans convergence for every case.
`dbtest.Options.MigrationsFS` supplies one immutable history to every
`migrate_to` step; nil retains the pathname-based fallback for embedders that
have not captured a snapshot.

`core/schemasource` executes an explicitly configured program without a shell,
bounds its runtime and captured output, cleans up descendant processes, and
parses SQL, HCL, or YAML stdout into Ptah's schema IR. Empty output is rejected
to prevent an accidentally broken provider from becoming an empty desired
schema, and displayed stderr/parser diagnostics are bounded, secret-redacted,
and terminal-safe. Embedders can use the same external desired-schema contract
as the CLI without depending on Cobra or any command-tree package.

`migration/schemadiff/difftypes.SchemaDiff` stores index additions and removals as
canonical `[]IndexRef` fields. Every index reference includes its owning
table. Live comparisons also snapshot catalog identifier semantics into the
diff so comparison, destructive-change policy, forward planning, and reverse
planning use one source of truth.

The index changes a plan makes in place each have a `SchemaDiff` list:

- `IndexesRenamed`, as `IndexRename` entries naming the table and both names;
- `IndexCommentsChanged`, an index comment a plan writes apart from the
  index, as `IndexCommentChange` entries naming the table, the index, the
  name a renamed index had, and both comments. An entry is a comment that
  differs, a renamed index's comment, which moves to the new name, or a
  dropped index's comment, which its table keeps until a plan removes it.

An index renamed is in neither `IndexesAdded` nor `IndexesRemoved`. The
comparison fills these lists only on a target whose capability set holds
`index_rename` or `comment_attributes`, which only the YDB presets do. A
change of a YDB global index's partitioning is the YDB owner's feature
change, carried by its table's `TableDiff.FeatureChanges`. Every planner but YDB's
refuses a diff that carries one with `ptaherr.ErrUnsupportedFeature`, so a
diff built by hand cannot reach a planner that would plan nothing for it.

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

`migration/schemadiff/difftypes.ViewDiff` also records the view body that is in
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

`migration/schemadiff/difftypes.RLSPolicyRef` and `RLSPolicyDiff` carry a
`Desired` policy and a `TableSchema`, both off the wire. The policy is what
CREATE POLICY renders from; the schema is the one the owning table is declared
under, which SQL Server addresses a policy by and which cannot be read off the
policy itself. An added or modified entry carrying no policy is refused with
`ptaherr.ErrInvalidSchemaDiff` rather than skipped, because a plan that
silently drops an access-control operation reports success while leaving the
database unprotected. A removal carries neither: `DROP POLICY name ON table` is
written from the two names.

`SchemaDiff.TablesAdded` is `TableChanges` rather than `[]string`. Each entry
carries the table's declaration, that table's columns with embedded fields
already folded in, the enums those columns name, and the table-level
constraints it owns — everything CREATE TABLE
renders from, which otherwise lives in three flat lists keyed by the Go struct
rather than owned by the table. The constraints are there for a target that
cannot ALTER one into place: SQLite has no `ADD CONSTRAINT`, so a constraint
missing from the `CREATE` has no second chance, while every other target plans
each one as its own addition and never reads them. `Names()` gives the table
names in the spelling
the comparison produced. The `tables_added` JSON report is an array of names.

`SchemaDiff.TablesRemoved` is `TableRemovals`. Each removal carries its report
name and a `schemacapture.TableObservation` with columns, indexes, constraints,
triggers, facets, named feature children, and coverage limits. `Clone()` owns
the nested common state. `Names()` and the `tables_removed` JSON report expose
only names; that report cannot replay a plan. A missing capture does not prove
that a table has no children.

`SchemaDiff.DeclaredTables` carries every table the declaration holds, also once
and off the wire. A foreign key names the table it references, and that table is
usually one the diff does not touch — an existing parent a new child points at —
so resolving `parents` to `app.parents` needs the declared list rather than
anything a per-entry operand could carry. A `TableCreation` carries the columns
whose references become constraints and the self-references the declaration
recorded for it; this is the other half.

`TableCreationFor`, `TableObservationFor`, and
`TableCreationsFor` require the
target's identifier semantics. `TableObservationFor` also requires the target
dialect so SQLite names retain their exact whitespace. Each capture selects named feature children by
structured parent identity and retains their coverage limits. A literal dot in
a table name cannot select children from a similarly spelled qualified table.
An empty capture retains unknown namespace claims; it does not prove absence.
Captured tables, fields, constraints, indexes, enums, and triggers do not share
mutable definitions with the source document. Observed captures own their nested
catalog columns, constraints, indexes, and table settings as well.

`SchemaDiff.DeclaredUserTypes` carries the declaration's type vocabulary — the
domains, composite types, ranges and enums a column may name — once for the
whole diff rather than on each entry. A column carries a type NAME and the
declaration carries the schema that type lives in, so `positive_int` renders as
`app.positive_int` only once the two are put together; and a column may name a
type nothing in the diff changes, so no per-entry operand reproduces it. A
planner resolves a created table's column types through it. `TableChanges`
keeps its columns as written until `Qualified` runs, so a caller that wants the
declaration rather than the rendering has it. An embedder building a diff by
hand fills the field with `difftypes.UserTypeVocabularyOf(desired)`; one that
omits it renders user-typed columns as the bare names the author wrote.

`SchemaDiff.DeclaredForeignKeys` carries every foreign key the schema the plan
runs against holds, once and off the wire. The MySQL family cannot `MODIFY` a
column a foreign key references, so a column type change drops that column's
keys and puts them back; the keys themselves are unchanged, which is why the
diff's own change lists never name them. The field is direction-dependent in a
way the other three are not read for: a rollback drops and restores what the
PRE-CHANGE database held, so the reversal fills it from the introspected schema
rather than carrying the forward value across. An embedder building a diff by
hand fills it with `difftypes.ForeignKeyDeclarationsOf(desired)`; one that omits
it gets a bare `MODIFY COLUMN`, which MySQL refuses with errno 3780 and MariaDB
with errno 1832.

`SchemaDiff.DeclaredConstraintHosts` carries the whole declaration of every
table a constraint change names — columns, enums, constraints, indexes and
triggers. A target with no `ALTER` for a constraint change rebuilds the table
around it, and a rebuild renders the table entire; such a table has no entry in
`TablesModified` at all when the constraint is its only change, so no per-table
operand carries it. It is direction-dependent for the same reason
`DeclaredForeignKeys` is: a rollback rebuilds the table the pre-change database
had. An embedder building a diff by hand fills it with
`difftypes.ConstraintHostDeclarationsOf`; one that omits it gets a refusal
naming the table rather than a rebuild from nothing.

`SchemaDiff.DeclaredTableDependencies` carries the table dependency graph of
the schema the plan runs against, keyed by qualified table name. A child must
be dropped before the parent it references. Captured removals retain their
prior constraints; this graph also describes tables that remain in the schema.
A reversal carries the pre-change database's graph, and a table that graph
does not name orders as it arrived. Reverse removals of accepted creations use
`TableCreation.DependsOn` to order those tables before this graph is applied.
An embedder building a diff by hand and omitting the graph gets its removals
in the order it wrote them.

`SchemaDiff.DeclaredFunctions` carries what putting a set of functions in
creation order needs beyond the functions themselves: the order the
author declared them in, and which function's body calls which. A body may call
another function, so a `CREATE` has to come after what it calls; the bodies
travel with the additions, but the additions are sorted by name, and neither the
edges nor the author's order is a property of any one entry. It is
direction-dependent like the carries above. Omitting it costs the ORDER and not
the statements: a routine the ordering does not name is still created, in the
order the diff stated it.

`SchemaDiff.IndexesAdded` is `IndexChanges` rather than `[]IndexRef`. An index
addition used to be a **reference** — a name and a table — with the definition
left in the declaration for a planner to look up, which is what made rendering
one `CREATE INDEX` need the whole document. Each entry carries the index and the
relation it belongs to; the owner is not written on the index, so it is resolved
once where the declaration is (a declaration may name the table, may name none
and belong to the struct it was written on, and a materialized view is an owner
too). `IndexAdditions()` still answers the references, which is what an identity
check, an ordering or a pairing is written from, and the JSON is unchanged:
`indexes_added` has always been an array of references.

An addition that describes no index is refused rather than rendered, because a
`CREATE INDEX` with neither columns nor an expression is not SQL.
`IndexesRemoved` stays `[]IndexRef`, because `DROP INDEX` is written from the
name.

`SchemaDiff.ConstraintsAdded` and `ConstraintsRemoved` are `ConstraintAdditions`
and `ConstraintRemovals` rather than `[]string`. Each direction carried two
lists — a list of names beside a list of records under
`ConstraintsAddedWithTables` — explicitly not index-aligned, so every consumer
correlated them by name, and a name with no record was a shape a planner had to
resolve against the declaration it was handed. One list per direction carries
the records; `Names()` answers the question the name list answered, including
its multiplicity: a name repeats once per host, which is what an embedded
inline-relation mixin produces and what `migration/safety` counts.

The JSON keys `constraints_added` and `constraints_removed` carry the records
now, and the `_with_tables` keys are gone. An embedder building a diff by hand
fills the additions with `difftypes.ConstraintAdditionsFor(desired, names...)`,
the constraint counterpart of `TableCreationsFor` and `IndexAdditionsFor`, and
runs `constraintscope.Normalize` if the diff reaches anything but a planner —
a planner normalizes at its own door.

`SchemaDiff.RLSEnabledTablesAdded` and `RLSEnabledTablesRemoved` are
`RLSEnabledTableChanges` rather than `[]string`. An ADDED entry is the
declaration, which is what a target rendering a declared comment needs; a
REMOVED entry carries the table name and nothing else, because the enablement
being removed is one the database reports and no declaration describes.
`Names()` gives the table names, and the JSON is unchanged: both keys have
always been arrays of names.

`SchemaDiff.RLSPolicyIdentityConflicts` is the companion, also off the wire. It
records declared policies that resolve to one identity — something the three
lists cannot show, because a colliding pair is already one entry by the time
they exist. A planner refuses a diff that carries any. An embedder building a
diff by hand will not produce one and need not populate it; an embedder reading
a diff the comparison produced must not plan it while it is non-empty.

`migration/schemadiff/difftypes.MaterializedViewDiff` carries one too. No engine
has an in-place replacement that keeps a materialized view's rows, so a change
to the common view is a drop and a create, and the create renders from this
field. `FeatureChanges` carries owner changes to settings attached to the view,
as `TableDiff.FeatureChanges` does for a table. `Replaces` reports whether the
entry needs the drop and the create: a common change does, and so does an owner
change value that implements `schemaext.OwnerReplacement` and reports true; an
entry holding only in-place owner changes keeps the view. The former
`RefreshChange` field and `MatViewRefreshChange` type are removed without
aliases. This changes behavior; pre-v1, so no compatibility is owed.

`migration/schemadiff/difftypes.TriggerRef` and `TriggerDiff` carry a `Desired`
field too, and the reference type is the one place where it means something on
one list and nothing on another: a `TriggersAdded` entry carries the declaration
CREATE TRIGGER renders from, while a `TriggersRemoved` entry carries none,
because a DROP is written from the trigger's name and its table. A rollback
therefore does more than exchange the two lists: it resolves each reversed
addition against the pre-change database, and strips the operand from each
reversed removal.

`migration/schemadiff/difftypes.FunctionDiff` and `SequenceDiff` carry a
`Desired` field for the same reason: a function modification renders as CREATE
OR REPLACE and needs the whole body and attribute set, and an ALTER SEQUENCE
reads the option values off the declaration while the change map only names
which options moved. An empty one plans nothing for that entry. `FunctionDiff.Desired` is the declaration as
written, not the copy the comparison folds: the comparison canonicalizes case
and normalizes MySQL type spellings on both sides so that two spellings of one
function converge, and rendering from that copy would write Ptah's
normalization into the user's DDL.

`migration/schemadiff/difftypes.DomainDiff`, `CompositeTypeDiff` and
`RangeDiff` each carry a `Desired` field, off the wire, holding the definition
the change is reconciled to. PostgreSQL has no in-place ALTER for a domain's
base type, a composite's field list or a range's subtype, so those changes are
planned as DROP TYPE followed by CREATE TYPE, and the create renders from this
field. An empty one withholds BOTH halves and emits a warning comment naming
the object: a type Ptah cannot rebuild is a type Ptah must not drop. An
embedder that builds one of these entries by hand and leaves `Desired` empty
therefore gets the warning rather than a plan. A reversal resolves the field
against the pre-change database, so a rolled-back modification rebuilds the
definition that database held.

`migration/schemadiff/difftypes.RoleDiff` carries the object it renders rather
than a reference to it: a `Desired` field, off the wire, holding the declaration
the planner writes the change from. A role's `password_update_required` entry
records only that a password has to be set and never the value. An embedder
that builds the entry by hand and leaves `Desired` empty gets no statement for
it. A reversal resolves the field against the pre-change database, so a
rolled-back password change sets nothing, the database holding no password to
restore.

`core/platform/identifier` exposes the reusable value types and conservative
dialect defaults behind that contract.

`migration/generator.GenerateCheckpointFromShadow` preserves the same live
semantics when a replayed history is rendered as a checkpoint: the render goes
through the shadow connection, so on SQL Server Ptah resolves the complete
candidate identifier set under the shadow catalog's collation rather than
under conservative offline rules.

`migration/generator.WriteAtlasCheckpointFileWithOptions` writes the Atlas
single-file checkpoint convention (and refreshes `atlas.sum`), where
`WriteCheckpointFilesWithOptions` writes the reversible Ptah pair (and
refreshes `ptah.sum`). `AtlasCheckpointArtifact` renders the same file name
and contents without touching the filesystem, so previews cannot drift from
what is written; the `-- atlas:checkpoint` directive it emits is only honored
on the file's first line. `ResolveAtlasCheckpointVersion` supplies the
timestamp version, bumped past any newer migration already in the directory.

`CheckpointWriteOptions.AuthorizedMigrationsFS` binds publication to the
history that produced the checkpoint body. The writer returns
`generator.ErrMigrationDirectoryChanged` before creating a checkpoint, or
withdraws the checkpoint before publishing the sum, when the rooted
destination does not match the authorized expected state. The sum is computed
from that state rather than from a newly reopened path.

`migration/planner.Planner` exposes only checked planning; malformed
references, unresolved additions, and target index-namespace conflicts fail
before SQL is returned.

Public failures from these packages should use `core/ptaherr` where the caller
can reasonably branch on the error. In particular, annotation failures should
support `errors.As(err, *ptaherr.ParseError)`, and unsupported dialect failures
should support `errors.Is(err, ptaherr.ErrUnsupportedDialect)`. Invalid schema
diffs rejected during planning support
`errors.Is(err, ptaherr.ErrInvalidSchemaDiff)`.

### `ptah.run/docs`

This package holds Ptah's own documentation as an `embed.FS` and nothing else.
It is public because `go:embed` patterns cannot leave their package's
directory, so the only package that can carry `docs/` is one that lives in it —
the alternative is committing a generated copy of the documentation under
`internal/`, which every documentation edit would have to regenerate and which
merges as an opaque blob.

Its surface is one variable and it is listed here so the snapshot gate covers
it. Embedders may read from it; the paths inside it are the repository's own
layout and move when the documentation is reorganized.

## Documentation-Only Packages

These packages are importable and carry no compatibility guarantee at all:

- `ptah.run/examples/annotation_parser/models`
- `ptah.run/examples/migrator`
- `ptah.run/examples/viz/models`

They are sample entities and sample migration directories that documentation
reaches. `migration/migrator/example_test.go` imports
`ptah.run/examples/migrator` for the migration directory its published godoc
examples run against, and an embedder who copies that example has to be able to
import the same fixture. That is the whole reason the category exists: an
example nobody outside this module can run is not documentation.

Nothing here is designed, reviewed or versioned as an API. The contents change
whenever the documentation they serve changes, `check-public-api-released.sh`
does not compare them against a release baseline, `check-exported-docs.sh` does
not require doc comments on them, and the site's stable-packages table does not
list them. Depend on one and a documentation edit is free to break the build.

The category is enumerated rather than pattern-matched. A new package under
`examples/` fails `check-public-api.sh` until it is either listed here or moved
behind an `internal/` boundary, because "it looks like an example" is the kind
of recognition that quietly widens.

## Provisional Surface

There is no provisional public surface. A package this document does not
classify is a `main` package, a directory with no production source, or behind a
Go `internal/` boundary. Promoting another package to public API must be an
explicit design decision that updates this document in the same reviewed change.

That invariant is being closed subtree by subtree under stokaro/ptah#2974, and
until it is, `check-public-api.sh` still carries a named exemption for each
subtree whose move has not landed. An exemption is not a boundary: it keeps a
package out of this document while Go continues to publish it. Each one is
deleted by the change that internalizes the subtree it names, and none may be
added.

## Compatibility Guard

CI runs four public API checks:

- `scripts/check-public-api.sh` fails when a library package is importable from
  outside this module and neither category above lists it. A library package is
  one `go list` reports with production source of its own, so a `main` package
  and a directory holding only `_test.go` files are outside the surface for a
  measured reason rather than by name. It reads both categories; every other
  check below reads the stable one alone.
- `scripts/check-public-api-released.sh` compares each stable package against
  the latest `v0.x` release tag with `apidiff -incompatible`. Until the first
  `v0.x` tag exists, the script reports that no released baseline is available
  and exits successfully. Once a `v0.x` tag exists, CI checks out repository
  tags and uses that real release tag as the baseline.
- `scripts/check-exported-docs.sh` fails when a package listed here carries an
  exported function or type with no doc comment. Methods are exempt because an
  implementation of a documented interface repeats what the interface already
  says.
- `scripts/check-public-api-docs-sync.sh` fails when the package table in the
  reader-facing documentation differs from this ledger.

Ptah does not commit a generated declaration snapshot. Such a file makes every
additive change update a second copy of the source but does not decide whether
the addition is well designed. Additive changes receive normal code review;
the released-baseline check enforces the compatibility guarantee that clients
actually depend on.

## Intentional API Changes Before v1

Ptah is still pre-v1, so maintainers may intentionally approve breaking changes
to the stable embedder API. Intentional approval must be explicit in the same
reviewed change:

- update this document if packages move between stable and non-public surfaces;
- add one package-level approval line to `docs/public_api_approvals.txt` when
  `scripts/check-public-api-released.sh` reports an incompatibility against the
  latest `v0.x` baseline;
- include the compatibility rationale in the PR description.

Do not weaken the CI checks, broaden exclusions, or silently remove packages
from the stable list to hide an API change.
