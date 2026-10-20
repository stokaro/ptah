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
This boundary does not yet connect frontend source capture, native object
comparison, planning, or engine transaction and history services.

## Cancellation during source interpretation

Schema loading carries the caller context through file and directory reads,
external command output, and selected SQL and HCL owner calls. The external
command deadline covers output interpretation as well as process execution.
An owner result received after cancellation cannot become a desired schema.
Public YAML and Atlas HCL parsing APIs take the context explicitly; no
old-signature wrapper substitutes a background context. This is required before
source decoding can safely call an independent process.

## Consequences

Experimental revision 5 refuses earlier revisions. Pre-v1 compatibility does not justify
an alias, a second decoder, or an embedded fallback. The independent fixture
proves normalization without a session, exact integer preservation, explicit
refusal of null and unknown fields, and complete-batch failure. Its synthetic
retention model is not a claim of SQLite or official-engine capability.

The staged implementation keeps current refusals until their replacement is
complete. Qualification must cover each engine's objects, facets, source formats,
planning, rendering, history, transaction limits, and failure recovery before
its dependency graph can be removed from the host. Stable v1 requires all
extracted engines, not only the fixture, to pass that evidence.
