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
- `ptah.run/core/yamlschema`
- `ptah.run/dbschema`
- `ptah.run/dialect/clickhouse/chast`
- `ptah.run/dialect/clickhouse/chcompare`
- `ptah.run/dialect/clickhouse/chconvert`
- `ptah.run/dialect/clickhouse/chdiff`
- `ptah.run/dialect/clickhouse/chplan`
- `ptah.run/dialect/clickhouse/chprepare`
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
- `ptah.run/dialect/postgres/pgproject`
- `ptah.run/dialect/ydb/ydbast`
- `ptah.run/dialect/ydb/ydbcompare`
- `ptah.run/dialect/ydb/ydbconvert`
- `ptah.run/dialect/ydb/ydbcoordination`
- `ptah.run/dialect/ydb/ydbdiff`
- `ptah.run/dialect/ydb/ydbplan`
- `ptah.run/dialect/ydb/ydbrender`
- `ptah.run/dialect/ydb/ydbreport`
- `ptah.run/dialect/ydb/ydbreverse`
- `ptah.run/dialect/ydb/ydbschema`
- `ptah.run/dialect/ydb/ydbscheme`
- `ptah.run/dialect/ydb/ydbsecret`
- `ptah.run/dialect/ydb/ydbstreaming`
- `ptah.run/dialect/ydb/ydbsyntax`
- `ptah.run/dialect/ydb/ydbworkload`
- `ptah.run/catalog`
- `ptah.run/docs`
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
identity. Custom feature kinds use the same contract as common objects.

`core/manageddata.LoadRowValues` reads a managed-data YAML file while retaining
scalar spelling and tags in `schemamodel.ManagedRow`. `LoadRows` reads resolved Go
values for row comparison. `ResolveRows` resolves carried declarations without
opening a file. The schema model owns the data types; this parser package owns
YAML interpretation and source-file access. Captured schema contracts therefore
do not import a parser to carry a declaration.

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
owners' steps.

`core/featureplan.DeclarationService` plans authored standalone objects for
schema creation. `Provider.Declarations` assigns desired model kinds and allowed
operation kinds to an owner. This service requires no observed or change codec:
an authored CREATE request does not claim that a live object was inspected and
found absent. Declared tables and common-step metadata provide dependency context.
Each service receives one isolated batch, and each `DeclarationPlan` preserves
the input identity and accounts for the operations that create it. The runtime
refuses duplicate objects, table-bound input objects, unregistered kinds, missing
receipts, and unaccounted operations before returning a successful reply.

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
rebuilds still own their attached streams. This graph does not yet replace every
target planner's internal ordering.

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

Before table preparation, comparison binds table and index coverage claims to the
same identifier semantics and default database as their common owners. Explicit schemas
remain explicit. Binding preserves source knowledge, refuses identity collisions,
and leaves the source snapshots unchanged.

`Provider.Reversals` assigns each target and change kind to its codec owner.
`Runtime.ReverseChanges` validates the complete input before dispatching one
batch per registered service. Each reply preserves its subject and change kind,
reconstructs its directional operands, and describes its strategy and recovery
limits. Missing handlers, invalid replies, errors, and cancellation return no
partial result. Callers must retain and report the limits with the plan.

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
inspection limits separately.

`engine.Provider.Relations` assigns dependency discovery by target, model kind,
and source representation. `runtime.CaptureRelations` validates a complete
`schemaext.RelationRequest` before dispatching one batch per owner. Each concrete
value has an ordered receipt. Missing handlers, malformed receipts, errors, and
cancellation return no snapshot. A receipt can explicitly report unresolved
references; source coverage remains unchanged.

`RelationSnapshot.CaptureRelated` captures the connected feature context around
structured object references. It retains each multi-table object once, with its
complete definition, and includes other values sharing its dependencies.
Exclusive table ownership comes from the object identity; facet ownership comes
from its location. Unknown namespaces or unresolved references return
`ErrIncompleteRelations`, even when some definitions are known. Runtime growth
does not enroll an older source in a new model. Returned references establish
neither the existence of their targets nor permission to expand user scope.

These are explicit embedding contracts. Built-in comparison and filtering do
not yet invoke relationship discovery automatically. Policy extraction and that
pipeline integration remain part of #4140. The external-provider fixture checks
the public path without built-in providers. Process adapters must map model
values and references into explicit records; implicit JSON encoding of a
relation value or snapshot is refused.

`Facets.WithTargetScope` binds a value to target names from its source.
`ForTarget` uses an explicit `TargetSelection`, including its registered aliases.
An excluded value retains its binding without its payload. `Kinds` and `Len`
describe concrete values; `DeclaredKinds` includes exclusions, and `IsZero`
remains false when an exclusion is present. Reproject the source declaration
when selecting a target that needs a previously excluded value.

Built-in schema rendering and direct AST rendering resolve facet scopes before
checking support. An excluded table facet contributes no SQL; its source
binding remains available in the captured model. Included unknown facets are
refused, as is rendering a captured exclusion on a target that needs its value.
Go annotation export refuses facet bindings it cannot preserve, including
bindings whose payload was excluded.

`EncodeFacets` and `DecodeFacets` carry `EncodedFacet` records with separate
host-owned target bindings and owner-defined payload envelopes. An excluded
record has no payload and needs no model codec. `SnapshotFacets` and common
schema conversion preserve bindings; ordinary value replacement does too.
Changing a value's target scope does not change its local semantic equality.

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

`schemaproperties.DecodeTables` attaches decoded property groups as desired
facets bound to the selected target. It consumes only claimed keys; other keys
and target groups remain in `Overrides`. `EncodeTables` writes table facets as
properties for that target. `DecodeIndexes` and `EncodeIndexes` apply these
rules to index owners. Index decoding consumes the common `Type` declaration
only when a selected definition absorbs `schemaext.IndexTypeAttribute`, and it
refuses an index property of the selected target that no owner claims, because
nothing else reads index properties. `Decode` applies the table and then the
index rules. Export writes owned settings to scoped properties.

These operations refuse duplicate alias keys and mixed typed and property
declarations, even when a property's value is empty. They copy the selected
owners, also when there is nothing to decode or export, and leave other schema
data shared and read-only. Errors and cancellation return no schema. They
establish no inspection coverage and do not resolve omitted settings.

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
holds. Go annotations and YAML enroll it, so a table without the properties
requests no TTL; HCL, SQL and hand-built schemas do not, and leave a live policy
unmanaged.

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
Planning changes to storage settings other than TTL remains part of
[stokaro/ptah#4140](https://github.com/stokaro/ptah/issues/4140).

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
cloneable owner payloads. The ALTER interface stays sealed. YDB changefeed
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
`RequestRotation` is the only way a plan writes `ALTER SECRET`. The common
schema, catalog, AST, and diff types contain no secret fields.

The secret services in `ydbcompare`, `ydbconvert`, `ydbplan`, `ydbreverse`, and
`ydbreport` consume this model, `ydbdiff.Secret` captures both change operands,
and `ydbast.Secret` is the statement payload `ydbrender.SecretHandler` writes.
Planning creates or drops a secret before the common statements, except a
creation at a path the plan frees, and before every external data source,
async replication, or transfer that names the secret by path. Reverse planning
reports the values a rollback cannot restore.

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
A comparison involving feature state also requires an explicit target. A supplied
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

MySQL-family readers populate the JSON-hidden
`catalog.Function.Definer` and `CurrentAccount` execution facts.
Database-aware `schemadiff.CompareWithDatabase` entry points use them to refuse
a modified `SQL SECURITY DEFINER` routine when recreating it would change the
executing account. Custom readers that supply a modified definer routine must
preserve both fields; missing facts fail closed with
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
- `IndexPartitioningChanged`, a change of a YDB global index's partitioning,
  as `IndexPartitioningChange` entries carrying the declared settings and the
  ones the database holds;
- `IndexCommentsChanged`, an index comment a plan writes apart from the
  index, as `IndexCommentChange` entries naming the table, the index, the
  name a renamed index had, and both comments. An entry is a comment that
  differs, a renamed index's comment, which moves to the new name, or a
  dropped index's comment, which its table keeps until a plan removes it.

An index renamed or repartitioned is in neither `IndexesAdded` nor
`IndexesRemoved`. The comparison fills these lists only on a target whose
capability set holds `index_rename`, `index_partitioning` or
`comment_attributes`, which only the YDB presets do. Every planner but YDB's
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
other than a ClickHouse refresh schedule is a drop and a create, and the create
renders from this field. The type now has two fields called `Desired`, at
different scales: this one is the view, and `RefreshChange.Desired` is one
schedule.

`migration/schemadiff/difftypes.TriggerRef` and `TriggerDiff` carry a `Desired`
field too, and the reference type is the one place where it means something on
one list and nothing on another: a `TriggersAdded` entry carries the declaration
CREATE TRIGGER renders from, while a `TriggersRemoved` entry carries none,
because a DROP is written from the trigger's name and its table. A rollback
therefore does more than exchange the two lists: it resolves each reversed
addition against the pre-change database, and strips the operand from each
reversed removal.

`migration/schemadiff/difftypes.FunctionDiff`, `SequenceDiff` and
`SynonymDiff` carry a `Desired` field for the same reason: a function
modification renders as CREATE OR REPLACE and needs the whole body and
attribute set, an ALTER SEQUENCE reads the option values off the declaration
while the change map only names which options moved, and a retargeted synonym
is a drop and a create because no dialect has an ALTER SYNONYM. An empty one
plans nothing for that entry. `FunctionDiff.Desired` is the declaration as
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

Two more modification entries carry the object they render rather than a
reference to it. `migration/schemadiff/difftypes.ContinuousAggregateDiff` and
`RoleDiff` each hold a `Desired` field, off the wire, holding the declaration
the planner writes the change from: an aggregate modification is a drop and a
create, and the create needs the schema, the name and the comment that the two
body strings do not carry, while a role's `password_update_required` entry
records only that a password has to be set and never the value. An embedder
that builds either entry by hand and leaves `Desired` empty gets no statement
for that entry. The field is also what makes a rollback correct: a reversal
resolves it against the pre-change database, so a rolled-back aggregate is
recreated from the definition that database held and a rolled-back password
change sets nothing, the database holding no password to restore.

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
