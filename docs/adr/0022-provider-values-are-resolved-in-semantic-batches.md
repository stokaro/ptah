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
Operation and change models remain refused until their native adapters exist.
Native object comparison, planning, and engine transaction and history
services still require application adapters.

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
collections. These gaps, HCL and Go annotation capture, and native planning
remain part of #4204. The fixture's retention value proves transport and schema
placement; it does not implement SQLite retention or qualify YDB extraction.

## Observed capture

Inspection returns an explicit engine, target features, and observed coverage,
even when the database has no common tables or native objects. The SDK checks
model definitions, source identities, and complete subject coverage before
exposing a normalization batch. Column facets identify their parent table as a
separate path component. Schema-filtered native coverage is refused until the
contract can preserve its namespace limits. The native `schema inspect` path
passes explicit schema names, so it refuses these model-bearing snapshots.
An end-to-end test pins the empty output and error until scoped capture is
implemented; catalog-reader support is not CLI inspection qualification.

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

The SDK coverage transport has no database or engine namespace boundaries.
Therefore a complete session inspection leaves global kind knowledge
uninspected, with an explicit reason. Concrete complete and absent subject records retain their target scope.
This prevents one database's read from asserting absence on another database.
Connecting scoped kind coverage to the SDK and owner identity binding remains
required for native comparison and planning in #4204. The synthetic observed-model fixture proves transport, capture, codecs,
and local encoding after process shutdown; it does not implement SQLite
retention or qualify an official engine.

## Consequences

Experimental revision 7 refuses earlier revisions. Pre-v1 compatibility does not justify
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
components, plus target names. Nil qualifiers are unrestricted; present empty
qualifiers match only empty identity components. Matching uses normalized parts,
and capture preserves their source spelling.

Overlapping regions intersect knowledge. Independent sources also intersect,
retaining separate claims so one source cannot fill another source's unknown
area. Target projection filters regions before identity lookup. Explicit subject
and parent claims retain their priority. A comparison reply cannot introduce a
complete region behind an uninspected global summary.

Coverage wire format 2 stores these domains without a scalar compatibility
reader. This changes behavior; pre-v1, so compatibility with older Ptah is not
owed. Common table binding refuses bounded domains until owner binding can
transform their namespace limits with the subjects. Rebinding only explicit
subjects could move a table outside an unknown region and authorize a drop.
The SDK scope adapter and native CLI export remain required work in #4204.
