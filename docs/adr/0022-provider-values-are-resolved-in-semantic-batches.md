# ADR 0022: Provider values are resolved in semantic batches

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#4204](https://github.com/stokaro/ptah/issues/4204)

This record defines the process boundary for maintainers extracting complete
engine implementations. The normalization service is the first part of that
boundary. It does not establish complete YDB, DuckDB, or Spanner support.

## Context

The observations below use the tree at `d4d6bcc62fa0f5ecc20087022ecafb2e671fb3e8`.

| Existing contract | Extraction requirement |
| --- | --- |
| `provider-sdk/protocol/experimental/provider.proto` carries native envelopes but has no owner operation that validates or normalizes them. | Receiving bytes cannot establish their meaning or compatibility. |
| `core/schemaext/value.go` requires `Clone` and `Equal` to be pure local operations. | A remote call from either method would hide failure and cancellation behind an interface that cannot report them. |
| `core/schemaext/comparison.go` and `facet_comparison.go` compare whole desired and observed batches with coverage and owner context. | Transport must retain contextual comparison instead of comparing opaque payload bytes. |
| `core/schemaext/codec.go` distinguishes desired, observed, operation, and change representations. | Canonical equality must be scoped to a representation and a versioned owner contract. |
| `engine/builtin/runtime.go` and `ydb_ttl_runtime.go` compose YDB sources, lowering, objects, facets, comparison, planning, and rendering. | Extracting the connection alone would leave the engine inside the host. |
| `internal/providerhost/schema.go` requires common table identity and refuses unsupported feature semantics. | The host adapter must gain the required batch services before real engines can replace their embedded implementations. |

## Decision

### Resolve values before they enter local model interfaces

Add an offline `NormalizeValues` service. A request identifies the canonical
engine, explicit target features, and an ordered batch of value slots. Each slot
has a correlation ID, representation, and versioned owner envelope. The envelope
subject retains its structured database, catalog, schema, path, kind, name,
overload, and display components.

The selected provider validates every field of every payload or refuses the
whole batch. It returns normalized payloads, separate normalized subject
references, and a canonical projection with an owner-defined format identifier.
Original subject spelling remains in the envelope. The transport validates
ownership, required features, result cardinality and order, and unchanged
envelope metadata. An incomplete reply, cancellation, or error exposes no prefix.

This operation has no session handle or connection address. Source interpretation
and normalization cannot require database credentials. The provider receives a
private copy of each request so its implementation cannot alter the metadata
against which the transport checks the result.

The host may retain validated snapshots behind local value interfaces. Cloning
copies local state. Representation equality uses the owner's explicit canonical
contract, never raw envelope bytes or serialized Protobuf. A cached projection
must remain bound to provider semantic identity, engine, target features,
representation, kind, schema version, and canonical format. An identifier such
as `json` alone does not define canonical semantics.

Normalization does not establish that desired and observed values are equivalent.
Observed absence, authored omission, explicit null, and an exact numeric value
remain distinct unless the owning representation explicitly defines a
normalization. Unknown fields must be refused rather than discarded.

### Keep comparison and global planning separate

The next owner services compare whole object and facet batches. Their inputs must
include complete desired and observed values, relevant coverage, selected owners,
parent lifecycle changes, capabilities, and structured references. Their outputs
must distinguish modeled, normalized, unsupported, and uninspected facts.
Incomplete relevant coverage refuses comparison; it cannot become known absence.

Owners return native change contributions and their dependencies, effects,
preconditions, risk, and transaction constraints. Ptah keeps common comparison,
policy, the global dependency graph, ordering, and migration history decisions.
A provider cannot append private SQL after scheduling or supply an independent
global migration plan. Native operation payloads retain their owner codec and
version through rendering, saved plans, execution, and reversal.

Source decoding is another offline batch. The shared frontend keeps HCL evaluation
and passes evaluated values with source locations, field paths, exact numbers,
and absent/null distinctions. Owners interpret their registered declarations.
The host does not serialize its Go AST or ask providers to duplicate HCL execution.

These comparison, source, and planning services remain required work in #4204.
The normalization service alone does not enable them or relax the current native
adapter's refusal of unsupported feature values.

### Keep transactional state in the process that owns the connection

Live session and transaction handles belong to one provider process. Execution,
history writes, and transaction verification must use the same handle when the
engine supports an atomic contract. A process restart invalidates every handle.
The host never recreates a transaction from an opaque identifier.

The migration backend needs explicit lock acquisition and release, bounded
history reads and writes, begin/commit/rollback, and verification. A lock claim
must describe the actual distributed exclusion mechanism. A generic query hook
with arbitrary SQL and rows is not a replacement for these contracts.

YDB's separation of DDL and transactional data operations must remain visible.
The host must not infer atomic DDL from an available transaction handle. Spanner
DDL likewise needs ordered execution with a confirmed prefix and explicit
transaction refusals. Lost replies and cancellation after dispatch leave the
outcome unknown; they never authorize automatic replay.

These backend operations remain required work in #4204. Each needs tests against
the engine's actual behavior before its embedded implementation is removed.

## Alternatives considered

- Treat opaque envelopes as ordinary local values and compare their bytes.
  This would confuse encoding differences with schema changes and omit owner
  defaults, normalization, and target-dependent equivalence.
- Forward each existing Go value method through RPC. This would make pure methods
  depend on process lifetime and provide no error channel for failed comparisons.
- Give the provider the entire planner. This would duplicate global ordering,
  policy, and history decisions and prevent one graph from representing all
  dependencies.
- Define and stabilize every service before extracting an engine. The design
  could miss engine-specific requirements. Keep the protocol experimental while
  implementing and qualifying complete YDB, DuckDB, and Spanner providers.

## Captured source batches and model identity

The SDK transports evaluated extension fragments with exact scalar values,
structured bindings, and authored source positions. HCL evaluation, common
schema parsing, and file confinement stay in the host. A decoder receives the
whole captured document, including bindings needed for forward references. It
interprets every field or refuses the complete batch.

Each document enrolls exact desired-model identities before dispatch. These
identities include owner-declared definition digests and canonical projection
formats. A provider's later registry cannot enlarge an earlier document's
scope. The response preserves that scope, returns each fragment in order, and
binds every emitted object or facet to explicit subject coverage and an
original source trace. Duplicate normalized subjects are refused. Unknown
coverage cannot masquerade as absence; defaults belong only to desired state.

The client validates independently of the server. The server gives the owner a
clone so owner mutation cannot rewrite the original provenance used to check
its response. Neither boundary returns a usable prefix after failure. Shared
validation preserves typed source paths and exact numbers without interpreting
engine syntax or comparing raw payload bytes.

The independently compiled fixture demonstrates this contract for evaluated
YAML, HCL, and Go annotation fragments. It does not implement SQL source parsing
or a real engine's native semantics. Host frontend capture and schema adapters
remain required before the SDK can replace the in-process source owners.

## Host model resolution

Negotiation carries each complete canonical JSON model definition beside its
SHA-256 digest. The SDK and host normalize JSON without losing numeric text.
Duplicate keys and malformed Unicode are refused. Their implementations remain
independent because public host contracts cannot depend on an external SDK,
and the SDK cannot import the host. A shared-input test checks their outputs
and refusals at the application boundary, where both modules are available.

The host registers desired and observed model codecs in the common schema
registry. Its codec definition binds the owner definition, executable digest,
semantic identity, protocol revision, engine features, and identifier rules.
A changed binding invalidates earlier serialized envelopes and plan contracts.
Models sharing a kind and representation across engines use one codec with
explicit per-engine definitions.

Decoding is an explicit context-aware batch service. The registry validates all
envelopes first, then calls each owner once across kinds and representations.
The host validates every engine and feature group before normalization. It
returns no partial result on failure. Returned values contain normalized owner
data and a validated canonical projection, with no process client. Clone,
equality, encoding, and fingerprints remain local after provider shutdown.

Persistence stores owner input and its binding, not a claim of prior validation.
Loading it requires the selected owner to reconstruct the semantic projection.
Change models enter the shared comparison runtime through the contextual
service below. Operation models retain captured safety facts and use the
selected target renderer. Native planning uses the shared graph described below.
Engine history services still require application adapters. Transaction
qualification remains specific to each engine.

## Cancellation during source interpretation

Schema loading carries the caller context through file and directory reads,
external command output, and selected SQL and HCL owner calls. The external
command deadline covers output interpretation as well as process execution.
An owner result received after cancellation cannot become a desired schema.
Public YAML and Atlas HCL parsing APIs take the context explicitly; no
old-signature wrapper substitutes a background context. This is required before
source decoding can safely call an independent process.

## YAML document capture

The YAML frontend invokes a document owner after resolving all common table
bindings. Its claimed top-level and table keys travel together, in source order,
with original YAML nodes, source bytes, and the filename. This preserves exact
numbers, explicit nulls, aliases, and references to later declarations. Unknown
keys, duplicate mappings, and cyclic aliases are refused before dispatch.

Validation does not impose Go integer or floating-point ranges on explicitly
typed owner numbers. Their tags and original scalar text reach the owner
unchanged, including decimal exponents beyond machine ranges. Finite numbers
can be converted to exact JSON number syntax without expanding exponents or
rounding. Nonfinite YAML values remain distinguishable at capture; a transport
that accepts only finite numbers must refuse them explicitly.

Every captured node carries a range in the original source bytes, including
its exclusive end. Character columns from the parser are converted to byte
columns using the source encoding. UTF-8 and UTF-16 BOMs, supplementary Unicode,
and YAML line breaks keep their original offsets. An alias has its own range
and shares the captured declaration node; the declaration retains its range.

The maintained YAML decoder still owns schema decoding and structural checks.
A pinned AST-only parser supplies end marks because the decoder's node API
exposes only start positions. Before accepting a range, capture checks the
node's kind, start position, tag, value, anchor, and child count against the
primary parse. A disagreement fails before owner dispatch. This dependency
supplies source locations; it does not decode Go values or select schema
semantics. No owner keys means no range-index pass.

A document owner returns contributions and desired coverage in one result.
Missing fragments do not establish absence. The registry validates model
ownership and table bindings, isolates mutable input per owner, and discards the
result on cancellation or failure. The same owner cannot also register local
per-key decoders or static coverage. In-process owners can keep their local
callbacks; neither path serializes their values through the SDK.

## Negotiated YAML ownership

An engine declares each YAML key it owns, the document or table scope, the
selector its decoder understands, and the desired models that key can declare.
A YAML-capable model must have an explicit key claim. The host must check claims
against common frontend keys and the other selected owners before parsing.
Model kind names and owner JSON definitions do not imply key ownership.

A captured YAML fragment carries its frozen claim. Both SDK boundaries refuse a
changed claim, a selector mismatch, a model outside the captured document's
scope, or a missing table binding before invoking owner code. Document keys do
not carry an implicit table attachment. A returned value must belong to a model
named by its fragment's key, even when the document enrolls other models.
Other source formats cannot carry YAML claims. The host includes this grammar in contract and model-definition digests,
with key and model declaration order normalized as sets.

## Native YAML provider adapter

The host registers negotiated document and table keys with the same YAML owner
set as in-process extensions. Each engine receives its claimed fragments and
the complete common table binding set in one offline request per document.
A missing key still enrolls its models, requiring an explicit coverage result.
The host does not select the first engine or treat a provider ID as an engine.
YAML has no selected server profile, so the request declares no target features.

Conversion preserves exact finite numbers, explicit nulls, string tags,
sequence order, alias-use positions, and original encoded byte coordinates.
Mapping merges retain YAML override precedence and declaration positions for
inherited entries. Cyclic aliases, expansion limits, non-string mapping keys,
nonfinite numbers, and unsupported tags cause refusal. The provider cannot
recover missing data by reading source files.

Validated results enter the shared schema as target-scoped table facets or
native objects. Their coverage uses the registered host codec identity, and
their canonical values require no second normalization RPC. Copying, encoding,
and comparison of resolved values remain local after the process closes.
Any failure discards the complete desired schema. If another registered engine
cannot express the same model in YAML, kind-wide coverage stays uninspected;
explicit subject records retain their engine scope. Target-specific absence
claims need target-scoped kind coverage in the shared schema before this limit
can be removed.

The shared placement identity retains database components and ordered namespace
paths and overload tuples with separate source and normalized forms. Immutable tuple nodes keep
IDs comparable without exposing writable slices or delimiter-based keys.
Collections, coverage, dependency capture, and codecs retain these components.

A provider result that changes the path or overload component count during normalization
is refused: the shared model cannot pair its source and normalized components.
A common table facet with database, catalog, path, or overload qualification is refused;
matching it by table name and schema would drop part of its identity.

Facets of columns and indexes also require placement adapters. Conflicting
values for the same model and subject across engines are refused by the shared
collections. These gaps, HCL and Go annotation capture, and completion of native
planning remain part of #4204. The fixture's retention value proves transport and schema
placement; it does not implement SQLite retention or qualify YDB extraction.

## Observed capture

Inspection returns an explicit engine, target features, and observed coverage,
even when the database has no common tables or native objects. The SDK checks
model definitions, source identities, and complete subject coverage before
exposing a normalization batch. Column facets identify their parent table as a
separate path component. Filtered inspection binds each requested selector to
owner-defined namespaces in request order. Bindings interpret the selection;
they establish no knowledge. Tables and coverage subjects must stay inside them,
and each coverage region must fit wholly inside one bound namespace. A filtered
reply cannot claim global completeness. An empty binding means an empty selection.

The host matches the returned target and features to the opened session, then
normalizes all native objects and common table and column facets in one call.
Every normalized identity must agree with the corresponding coverage record.
The captured projection retains the session features and model definition.
No partial catalog survives a failed or canceled inspection or normalization.

Native objects retain their database, catalog, namespace, and overload tuples.
Common facets use the shared table and column identity builders. The adapter
checks the session database and identifier comparison rules before removing
the implicit database or moving a column's table into the common parent field.
Unexpected qualifiers and inconsistent normalized names cause refusal.

The SDK carries database, catalog, schema, and path-prefix regions with source
and normalized components. Each model declares outside knowledge separately.
Unknown regions override overlapping completeness, and explicit subjects remain
more specific. Empty qualifiers differ from wildcards. Duplicate normalized
regions, invalid components, and namespace-wide absence or defaults are refused.
Full-document source decoding refuses regions because it must account for the
complete enrolled document scope.

The host accepts native regions only when their database component preserves
the exact nonempty session database. It adds the selected engine to each region
and retains all remaining qualifiers and path components. Global completeness
still becomes uninspected. Native regions select only the model's native subject
kind. Common table and column regions omit the database and catalog, retain the
session default schema, and require an exact empty namespace path. A column's
wire path moves into its parent field. Subject-kind limits prevent either
coordinate space from claiming knowledge in the other. Unknown parent regions
survive this mapping. Unbound, foreign, or renamed database scopes and common
normalization mismatches cause refusal of the whole catalog.

Every inspected table supplies a normalized identity, including tables without
facets. The host checks it against the negotiated common identifier semantics.
Filtered dispositions remain refused because they lack a normalized identity.
The synthetic fixture proves filtered native CLI inspection and bounded common
and native capture. It does not implement SQLite retention or qualify an engine.

Desired and observed models may supply a static plural display name. The name
must agree across engines that share that model representation and participates
in frozen contract and model digests. The local reporting adapter validates
captured values and supplies omission metadata without payload metrics or an RPC.
Missing labels remain missing reporting support; the host invents no substitute.

Native JSON inspection uses the common schema format. It warns about omitted
native objects and facets with owner labels. The shared conversion and codecs
retain their payloads separately. JSON output does not qualify lossless export.

## Semantic representation conversion

`ConvertValues` is an optional offline owner service. It converts a complete
batch between negotiated desired and observed models. The host retains the
normalization metadata privately and clones it with the captured value. The
persistent wire format still contains source data only; decoding always resolves
it with the owner.

The conversion request carries a captured target profile. Mixed profiles are
refused before dispatch. The owner validates payload semantics again; cached
projections cannot authorize skipping that work. Destination formats and defaults
may differ. Ordered IDs, kinds, source identities, and normalized identity tuples
must survive, while every destination requirement stays within the profile.
Both SDK boundaries refuse malformed or partial results. Cancellation discards
the batch even when a service returns a result afterward.

The host registers paired model kinds under the selected engine's conversion
service. The shared schema pipeline retains object and facet placement and
converts coverage definitions without widening their knowledge. No conversion
response supplies new coverage. Filtered inspection supplies selector bindings
before conversion. The resulting common JSON export still reports payload loss.

## Consequences

Experimental revision 19 refuses earlier revisions. Pre-v1 compatibility does not justify
an alias, a second decoder, or an embedded fallback. The independent fixture
proves normalization without a session, exact integer preservation, explicit
refusal of null and unknown fields, and complete-batch failure. Its synthetic
retention model is not a claim of SQLite or official-engine capability.

The staged implementation keeps current refusals until their replacement is
complete. Qualification must cover each engine's objects, facets, source formats,
planning, rendering, history, transaction limits, and failure recovery before
its dependency graph can be removed from the host. Stable v1 requires all
extracted engines, not only the fixture, to pass that evidence.

## Namespace knowledge in the shared model

A kind claim carries an immutable domain with an outside claim and explicit
namespace regions. Regions retain structured database, catalog, schema, and path
components, plus target names, subject kinds, and parent components. A scope can
require its path to match exactly instead of accepting descendants. Nil qualifiers are unrestricted; present empty
qualifiers match only empty identity components. Matching uses normalized parts,
and capture preserves their source spelling.

Overlapping regions intersect knowledge. Independent sources also intersect,
retaining separate claims so one source cannot fill another source's unknown
area. Target projection filters regions before identity lookup. Explicit subject
and parent claims retain their priority. A comparison reply cannot introduce a
complete region behind an uninspected global summary. A merged subject claim
must be supported across every retained target name. Without a target resolver,
a possible alias cannot justify discarding a negative namespace claim.

Coverage wire format 2 stores these domains without a scalar compatibility
reader. This changes behavior; pre-v1, so compatibility with older Ptah is not
owed. Rebinding only explicit subjects could move a table outside an unknown
region and authorize a drop. Inspection maps owner-bound namespaces at capture.
Common identity binding then transforms typed relative bounds with their
subjects. Contextual provider comparison, planning, and faithful source export
remain work in #4204.

## Binding namespace limits for comparison

`KnowledgeDomain.MapScopes` preserves each source's outside knowledge and every
region's knowledge while rebinding its scope. A region may split by subject
kind; it cannot disappear. Each callback input and output is copied. Errors,
invalid scopes, and size-limit violations return no partial domain. The caller
owns the identity transformation; the shared model does not infer one.

The common binder uses one subject-kind classification for explicit claims and
namespace regions. Comparison binds tables, table-owned objects, indexes, and
routines. Reverse creation binds tables and their children. Connection defaults,
identifier folding, quoted names, and index namespaces follow the same builders
on both paths. Verbatim table binding retains whether a schema came from the
connection, so a later binding can apply a different connection default.
Independent source claims and overlapping unknown regions remain
independent after their names collide under the selected rules.

The binder resolves the target before mapping. Bound regions name the selected
canonical target, so an alias cannot grant the transformed scope to another
engine. A database-qualified or namespace-qualified native region keeps its
coordinates. Relative common regions require explicit subject kinds, an exact
empty database, and an exact namespace path. Common tables, indexes, and routines
also require an exact empty catalog. Ambiguous native parent bounds remain
refused. These refusals prevent a wildcard from mixing identities that move
with identities that stay qualified.

The synthetic provider proves that captured namespace limits can pass this
binding after its process closes. Public comparison tests retain an unknown
relative namespace in the source snapshot used for planning. Neither test
qualifies a provider's contextual comparison or change-planning service.


## Contextual comparison transport

An engine assigns native object models or attached facet models to named
`CompareModels` services. Each declaration includes its change models, facet
owner kinds, and supported native actions. The host freezes these declarations
in both provider and model identities. Changing placement or semantic ownership
invalidates an earlier captured contract.

The request preserves the complete shared comparison context. Identity parts
carry their source and normalized spelling, quoting, and default markers.
Parents, namespace paths, and overload signatures have distinct coordinates.
The capture-phase `Reference` remains an owner's source identity; the bound
comparison identity represents the later host phase without parsing a joined
name or exporting a host interface. Capability maps retain established false
answers separately from missing facts.

Coverage transports independent knowledge sources, each with an outside claim
and bounded regions. `KnowledgeDomain.Sources` returns independent snapshots;
the adapter restores each source and intersects them. Flattening their regions
would let one source grant authority another source lacks. Subject claims,
explicit empty namespace parts, common parents, exact paths, and target limits
survive the round trip.

Object batches carry common parent lifecycles, explicit actions, declared
relations, roles and columns, exclusions, required objects, and the database
path. Facet batches carry their owner lifecycles and desired and observed
constraint definitions. Deliberately excluded facets retain their model,
subject, and target binding even though their value is absent. The response
must preserve each exclusion and cannot return a change or diagnostic for it.

The owner revalidates payload semantics before comparing desired and observed
states. It returns the effective desired schema, typed changes, and undecided
reasons as one atomic reply. The shared engine still validates declaration
retention, coverage limits, related subjects, parent ownership, and migration
policy. The transport does not replace those rules. A missing comparison
service remains a refusal rather than a successful no-op.

A normalized change explicitly states whether it replaces a common owner or
removes state, and carries impact and access assessments with reasons. The
local change adapter implements the shared change and effect interfaces using
these captured facts. Required object identities use the same bound component
format. Canonical projections include the facts; the persistence format holds
only complete owner input and its frozen binding. Decoding invokes owner
normalization to reconstruct the facts. Cloning, encoding, and effect queries
remain local after the provider process closes.

The independent retention fixture exercises native objects and column facets,
changed and unchanged values, explicit actions, unknown coverage, exclusions,
and change capture and decoding. It establishes global and subject knowledge
only; bounded regions remain undecided in that synthetic owner. It does not
execute retention changes or qualify any official database engine. Rewrite and
execution qualification, history services, full source support,
and official-engine extraction remain part of #4204.


## Captured operation rendering

An operation is a separate model representation. Its codec returns a typed AST
payload with pure local cloning, encoding, impact, and access assessments. It
cannot be used as a desired value or comparison change. Persistence contains
owner input and its frozen model binding; decoding asks the owner to reconstruct
the operation facts. Canonical projections include those facts, so an approved
operation cannot retain its fingerprint after its assessment changes.

The existing coarse `Render` service accepts captured native operations beside
common changes. Each native input names its grammatical role. An `ALTER TABLE`
operation carries the actual parent as a structured reference; a standalone
operation carries none. The owner validates the role and parent and emits the
whole executable unit. The host never constructs SQL from a native payload.
Captured operations must match the selected engine and target feature profile.
Rendering capabilities remain explicit, including established false values.

Both transport boundaries validate ownership, model definitions, captured
facts, and input provenance. The server gives owner code a copy, so mutating an
input cannot change the identity against which its reply is checked. A late
reply after cancellation, a malformed unit, or an owner refusal discards the
whole batch. A successful earlier unit is not exposed as a usable prefix.

The independent fixture normalizes and executes table creation, renaming, and
removal through an operation model. Inspection proves the database changes;
round trips prove that operation assessments are recomputed. This exercises
operation transport and rendering. The contextual planning adapter described
below binds receipts, dependencies, and effects for the selected runtime. The
common migration planner dispatches the service through the shared graph.
History services and YDB transaction qualification remain required before extraction.


### Contextual planning transport

`PlanFeatures` carries captured declarations and observations, normalized native
changes, common graph steps, and explicit parent assignments. Each service owns
its declared change and operation models. Parent models are assigned separately
so an unchanged facet still receives assessment when its table is changed or
dropped.

A completed reply contains ordered receipts for every input change and parent
assignment. Every contributed operation has a receipt. Graph identities retain
owner and name separately; effects retain bound object identities. Explicit
rewrite claims name common steps with known footprints. The host must check the
replacement effects and schedule all contributions together.

The SDK validates the complete exchange at both boundaries. It isolates owner
inputs, refuses successful output alongside diagnostics, and discards a reply
after cancellation. Transaction requirements are data, not evidence that an
executor honored them. The application adapter must preserve them through the
final execution boundary before native planning is qualified.

The host adapter registers the planning service and includes its assignments in
provider and model identities. It maps declarations and observations separately,
preserves excluded facets and independent coverage sources, and refuses unmapped
common properties. Primary-key facets use the shared capture authority; a value
on another primary-key slot is refused even when the authoritative slot is empty.

The common migration planner sends native changes, parent actions, unchanged
captured tables, and exact common column operands through the selected runtime.
Its shared graph places prerequisites and types before common table creations,
ordinary feature operations before common column and table removals, and drops
of prerequisites and types after those removals. Conflicts and unsupported
placements discard the complete plan. Unsupported inline definitions still fail
before rendering; planning never strips settings to fit the common wire model.
Default-phase replacements inherit their common source boundaries. An explicit
phase adds window constraints, so an incompatible phase refuses the complete
graph instead of moving the source operation.

Owner transaction requirements survive lowering to AST envelopes. Migration
classification recognizes a forbidden transaction without a built-in dialect
profile. An unsafe split of common and native work remains a refusal. The
external planner refuses required transactions unless the selected engine
negotiates transaction sessions and establishes transactional DDL. Prepared schema apply, saved JSON plans, and dev rehearsal
retain requirements in statement order and refuse a conflicting mode before
executing any statement. An edit to assessed SQL needs a fresh owner assessment.
Atlas HCL output refuses requirements it cannot preserve. Raw SQL assessment
remains unfinished.

An independent SDK-only fixture exercises this production planner, shared graph,
rendering, explicit unwrapped execution, and inspection. It checks unchanged
captured parents, parent receipts without changed native values, graph conflicts,
completed refusals, required-transaction refusal, and cancellation. A common
column replacement uses the exact accepted operand, emits once, and retains its
transaction requirement. The fixture refuses unsupported operand properties.
Transaction enforcement through every final executor remains part of #4204
before this service can support YDB extraction.

### Session capability facts

An opened session carries the capabilities established for that server. Presence
is preserved: false is a measured answer, and absence remains unknown. The SDK
validates capability names without importing the host registry. The host checks
its vocabulary and dependencies before exposing server facts to planning.
Session capability claims do not establish transaction-session support.

Both transport boundaries validate newly opened sessions. Invalid replies return
no usable handle and trigger an attempt to close any returned handle. Returned
maps are independent snapshots, so later owner mutation cannot change captured
server facts.

### Transaction ownership and uncertain completion

`sessions.transactions` negotiates session-bound begin and finish RPCs. The
caller chooses each transaction ID before dispatch so a failed begin reply can
trigger cleanup of the exact handle. A provider reserves the ID before opening
the database transaction. A rollback of an unused ID fences a delayed begin.
Begin, execution, finish, and session close serialize within that ownership.

Execution carries the transaction ID. Its applied count confirms statements
executed inside the transaction, not a commit. Finish reports committed, rolled
back, not started, or unknown. Confirmed terminal results survive until session
close; duplicate finish calls cannot repeat the database operation. Transaction
lifetime belongs to the session rather than the begin RPC context. Session close
rolls back unfinished handles and prevents later begin or execution.

The host schema writer requires both the selected engine's service advertisement
and established transactional DDL. An available data transaction does not prove
atomic schema changes. Failed execution prevents commit; bounded rollback remains
available after cancellation. The host never retries an uncertain commit.

An independent SQLite-backed provider exercises actual schema commit, rollback,
failed-prefix rollback, lost commit replies, concurrent begin cleanup, session
isolation, and native schema apply. This establishes the adapter contract, not
YDB, DuckDB, or Spanner qualification. History persistence still needs a typed
service that uses these same session and transaction handles.

## Typed revision storage in the session owner

The experimental history service transports stored revision facts rather than
SQL queries or migration commands. The host keeps its existing policy for
ordering, checksums, partial progress, retry, and repair. Native and Atlas
representation are explicit; an exact empty Atlas identity is preserved.

A history scope names the live session, optional transaction, schema, table,
and representation. The provider validates storage ownership and layout before
use. Preparation creates missing storage outside transactions and refuses
incompatible layouts. No earlier history decoder or upgrade path is added.

Reads produce a consistent complete snapshot within a caller-selected bound.
Oversized histories fail explicitly. A mutation compares the complete expected
row and performs one atomic insert, replacement, or deletion. A conflict makes
no change. This prevents stale record replacement; it does not replace a
migration lock or serialize whole migration plans.

Execution and history mutations use the same transaction handle where the
engine supports it. A mutation acknowledgment within that transaction proves
execution only. Lost acknowledgments stop the workflow and require
reconciliation; request IDs never authorize replay. YDB data transactions do
not establish transactional DDL, and Spanner DDL remains unwrapped.

The SDK and independent fixture establish this boundary. Migrator integration,
provider lock primitives, and qualification of the official engines remain
required before #4204 is complete.

## Bind host history access to the active executor

`DatabaseConnection.HistoryStore` exposes neutral host records without importing
SDK transport types into the migrator. A native session supplies its bound store.
An active transaction must supply its own store; absence cannot fall back to
unwrapped writes on the parent session. The subprocess adapter serializes history
with execution and finish, and a dispatched history failure prevents commit.

The common migrator reads provider history through the same ordering, progress,
identity, and dirty-state rules as SQL-backed history. Dry-run initialization
reads storage without creating it. Providers validate layout and ownership;
the host does not send SQL catalog queries or upgrade statements to native
sessions. Ordinary revision writes use the same typed store. Logging,
lock ownership, and default versioned CLI integration remain unfinished under #4204.

## Preserve uncertain outcomes across the host boundary

The host translates an SDK unknown-outcome error to
`ptaherr.ErrOutcomeUnknown` while retaining its cause. Shared migration recovery
and embedders can recognize uncertainty without importing the provider SDK.
This applies to execution, transaction completion, and revision mutations.
Confirmed refusals and local validation failures do not acquire this marker.

The common statement-outcome classifier checks the explicit marker before
transport heuristics. A lost acknowledgment must keep an in-flight revision
marked uncertain; it must not become an ordinary failure that permits replay.
The marker does not replace durable-state inspection, and an unmarked error is
not evidence that no write occurred.

Rollback failures retain their error causes. An explicit uncertain execution or
rollback receipt overrides the SQL cancellation inference; cancellation alone
must not turn that receipt into proof that the transaction rolled back.


## Bind cancellation evidence to the transaction owner

The common migrator accepts cancellation-based rollback evidence only through
`catalog.CancellationRollback`. SQL-backed writers retain their transaction's
begin context and record its cancellation when rollback is attempted. A context
canceled after commit does not provide this evidence. Provider transactions do
not implement the interface: their lifetime belongs to the remote session,
and an RPC's cancellation does not end that transaction.

The rollback result remains authoritative when it succeeds. When it fails,
the absence of owner evidence leaves the outcome unconfirmed, even if the
execution error contains `context.Canceled`. An explicit unknown-outcome marker
also prevents cancellation inference. Dialect rules still account for schema
statements that implicitly committed before rollback.


## Record revision transitions through typed storage

The common migrator writes pending, in-flight, checkpoint, failed, and applied
records through the active history store. Rollback removes the exact revision
through that store. Each mutation reads complete history, validates identities
and progress, and compares the entire prior record before replacement. Conflicts
and uncertain write acknowledgments are not retried.

A pending provider revision starts with an unknown-outcome marker. Successful
execution and history completion clear it. An unconfirmed transaction outcome
leaves durable state untouched: commit may already have stored an applied
revision or deleted a rolled-back revision. Failed commits attempt rollback to
release a provider transaction that may still be active. This cleanup retains
the commit error and does not itself establish the transaction outcome.
Confirmed atomic rollback retains
only progress committed before this attempt, using the provider's established
transactional-DDL capability rather than a built-in dialect name.

Independent process checks exercise native revision storage with explicit
caller options that disable migration logging and locks. These options isolate
unfinished service integration; they do not change the defaults or qualify a
complete versioned workflow. The fixture also implements Atlas storage using
ordinary revision columns. Logging, tags, locking, and the default versioned CLI
workflow remain required before this history slice can merge.

## Keep history transactions independent of transactional DDL

YDB needs atomic revision-data writes without a claim that schema changes can
roll back. `HistoryTransactor` opens the existing session transaction service
through a restricted `HistoryTransaction` handle. The handle offers revision
storage and finish operations; its dynamic type has no schema executor. Schema
transactions still require established transactional DDL. Neither interface
claims a distributed migration lock. History callers explicitly select default
or serializable isolation. The SDK carries that level in both begin and reply;
missing, unknown, or substituted levels are refused. Providers must establish
the requested guarantee or return an error. Serializable isolation covers range
reads, including absent revisions. Committed reads and writes must be equivalent
to a serial execution. Default isolation makes no such promise.

The independent SQLite process test makes two history transactions read the
same empty range, then insert different identities. After one commits, the
other cannot commit its write based on that snapshot. A fresh transaction sees
the committed row and can proceed. The fixture also refuses serializable begins
when its connection enables uncommitted reads, without leaving a handle active.

Baseline validates stored revisions and writes the selected batch in this data
transaction. SQL and native stores share the dirty-state, nonempty-store, and
above-baseline policy. Preparation occurs before beginning. Failure rolls back
the batch; a failed commit attempts cleanup and retains its uncertain outcome.
No migration body runs, and no SQL history statements cross the provider boundary.

An independent provider fixture advertises history and transactions without
transactional DDL. Process tests exercise commit, rollback, baseline, forced
baseline restrictions, and lost mutation or commit replies. A lost reply on the
last revision rolls back the earlier revisions too. This proves the host path;
YDB storage, lock ownership, and official-engine qualification remain required
under #4204.

## Keep repair and checksum policy in the common migrator

Native repair replaces the exact stored record after the common migrator has
checked direction, committed progress, source hashes, and recovery options.
Resuming an unknown statement outcome is refused even with force. An operator
can complete manual recovery without replaying SQL, or resume a confirmed
prefix and let the common executor record the remaining progress. A completed
rollback removes its revision. Lost write acknowledgments remain unknown;
repair does not repeat the write or mark an uncertain result successful.

Checksum reconciliation uses the same projection planner as SQL-backed history.
The provider path reads and validates a complete snapshot inside a data
transaction, plans the replacement hashes, and compares each whole prior record.
A failure rolls back the batch. A commit error retains uncertainty while cleanup
releases a transaction that may still be active. Migration bodies already
completed before reconciliation are not undone or replayed by this cleanup.

Independent process checks cover resumed up and down repair, manual recovery,
force restrictions, lost repair receipts, a late checksum refusal, and lost
checksum commit requests and replies. They use native storage with Atlas-format
source directories for running-hash qualification. They do not qualify Atlas
storage itself. Logs, tags, locks, default CLI integration, and official
engine qualification remain part of #4204.

A prior applied-time cutoff is evidence for the stored hashes, not a unique
history identifier. Several cutoffs can prove the same complete stored cohort
when later revisions retain their application-time hashes. The shared verifier
accepts any complete proof and still checks every row. Partial reconciliation
and edited source hashes remain refusals. Deterministic SQL-backed and native
process tests cover recovery after an atomic reconciliation rollback.


## Set revision history through serializable storage

Provider history set reads the complete history inside an explicitly serializable
transaction. It uses the common identity and retirement planner, then compares
whole records for every deletion and replacement. A failed write rolls back the
batch. An uncertain commit returns an error without a success summary or an
automatic retry. Preparation occurs before the transaction, and no migration
body runs.

Native storage keeps numeric keys even when its source uses exact Atlas
identities. A repeated set preserves clean rows and reports no change. Dirty
native rows become applied with complete progress and the current source hash;
their description, timestamps, duration, and error diagnostics remain intact.
Setting zero removes every native revision, including stored zero and negative
keys. SQL and provider storage use these same decisions.

Process qualification covers native storage, late mutation acknowledgment loss,
lost commit requests and replies, refusal of weaker isolation, dry runs, and
fresh-session recovery. Atlas storage has separate process checks against its
ordinary revision columns, including tables created through the SQL path.

## Derive Atlas revision state from stored facts

Atlas records contain exact identities, progress, hashes, timestamps, type bits,
diagnostics, and operator identity. They do not store native state or direction.
The SDK refuses those derived fields on Atlas records. The common migrator uses
the same derivation as its SQL reader, including the pending rollback marker.
Providers implement storage without deciding migration state.

The independent SQLite fixture reads and writes ordinary Atlas revision columns.
It verifies column types, key identity, binary collation, and the absence of
triggers and foreign keys. A comparison covers the complete canonical record;
SQL null and empty diagnostics have the same representation. An unwrapped
comparison and write uses one serializable data transaction. A caller-owned
transaction retains its commit boundary.

Process tests exchange the same database between SQL and provider connections.
They cover metadata preservation, empty exact identities, baseline, apply and
rollback, dirty-row set, external-write conflicts, and lost acknowledgments.
Schema effects are inspected through the independent SQL connection: the fixture's
schema reader does not accept the full Atlas table grammar. Official-engine
qualification remains required under #4204.

Atlas repair tests start with partial up and down histories written by the SQL
migrator. The provider resumes the uncommitted suffix and refuses a changed
committed prefix, including when force is selected. A partially completed
rollback stays dirty until its remaining statements finish and its row is deleted.
Manual recovery can replace an uncertain record only after the operator resolves
the database state.

Checksum tests insert an out-of-order migration into an Atlas source directory.
A late refusal rolls back all checksum replacements while preserving completed
migration bodies. Lost commit requests and replies retain unknown outcomes;
a fresh session reads the stored hashes before recovery. The SQL reader verifies
the final cohort, and changed source bytes remain an error.

A process-death test exits the provider after a committed DDL statement and before
its acknowledgment. A fresh process sees the pending Atlas record and refuses
forced statement resumption. After the operator applies the missing statement,
explicit metadata repair completes without replaying the committed prefix.


## Bind lock ownership to the provider session

The optional `sessions.locks` service owns acquisition, ownership checks, and
release. Each engine declares support. The host supplies a single-use handle and
exact lock name within a live session, plus a fresh request ID for each call.
Receipts preserve those identities. A confirmed busy result permits another
attempt with a new handle under the host wait policy. An uncertain result does
not permit acquisition retries. Release of an unused handle fences a late
acquisition, and closing a session releases all its locks.

Both transport boundaries attempt bounded cleanup of the requested handle after
an uncertain acquisition. Cleanup survives caller cancellation, retains the
original error, and does not establish that guarded work was safe. Ownership
checks must establish the engine's current guarantee, including lease freshness.
Providers check guarding locks before execution, history writes, and commits.
Lost or uncertain ownership permanently fences further writes in that session;
rollback, release, and close remain available.

Independent SQLite fixture processes hold named locks through private sidecar
write transactions. The fixture checks an uncommitted ownership row rather than
a cached acquired flag. This proves process exclusion for one canonical database
pathname without hard-link aliases. Tests cover name and session binding, fenced
late attempts, cleanup after lost acquisition receipts, release on close and
process death, and write refusal after ownership loss. A process death after a
committed schema statement releases the lock without undoing that statement.
A lost lock before commit leaves the transaction available for explicit rollback.

The host exposes the service through `dbschema.SessionLocker` on the same
execution session. Shared lock acquisition selects it before built-in dialect
services. Missing native support refuses the operation rather than selecting a
no-op lock. Only confirmed busy results permit a fresh attempt; the host owns
waiting, no-wait behavior, timeout, and cancellation. A watcher checks ownership
and cancels guarded work after loss. Release verifies current ownership again,
cleans up even after loss, and retains uncertainty from execution or cleanup.
A session that lost or cannot establish ownership refuses further writes.

Independent process tests run the common migrator with locking enabled across
its transaction modes. A competing process times out without executing schema
changes. Loss cancels the guard, commit failure retains an unknown outcome, and
loss discovered during release does not undo already committed schema or history.
Lock acquisition on an active transaction is refused. Preview lock acquisition
also refuses: this SDK revision does not establish a read-only acquisition.

Default versioned CLI use, logging, tags, read-only lock previews, and
official-engine checks remain required under #4204. This fixture does not establish YDB coordination lease behavior.
