# ADR 0041: A provider plans authored declarations without inventing observed absence

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0023](0023-engine-knowledge-moves-with-the-provider.md),
[ADR 0033](0033-common-objects-reach-a-provider-through-the-common-wire.md)

Contract: 25.5.

## Context

`ptah schema render` renders an authored declaration without reading a server.
The host's `core/featureplan.DeclarationService` lowers standalone owned objects
and table children to creation operations. The host schedules those operations
with common creation steps before it renders any statement.

A subprocess provider has comparison and change-planning services, but no
service for declarations. The common-table schema renderer cannot create an
owned coordination node or a table whose provider owns its creation. Comparing
the declaration with an empty observed schema would assert that its objects do
not exist. A source file provides no such evidence.

## Decision

### An engine declares its authored-object services

The two-way feature `schema.declarations` adds `Engine.declarations`. The
provider lists the feature in its description and each supporting engine; the
host lists it when negotiating. Each assignment has an ID, desired model kinds,
and the operation model kinds it may return. The negotiated description omits
assignments when the host does not list the feature.

A kind has one declaration service per engine. Every input kind has a DESIRED
model on that engine. Every output kind has an OPERATION model. A service can
handle several kinds together when their creation order depends on one another.

### The request carries declarations and their context

`PlanDeclarations` runs offline and requires an explicitly selected engine and
service. Its request carries:

- The target features, identifier semantics and capability facts supplied by
  the caller.
- Ordered objects as `ComparisonRecord` values containing normalized DESIRED
  models and their structured identities.
- Declared tables with structured subjects, using `ValidationTable` and
  `DeclaredTable`; these supply a child's parent and declared dependencies.
- Declared type identities and references as `PlanningType` values.
- The host's common creation steps as `CommonPlanningStep` values.

There is no current schema, observed coverage, session handle or implied
absence. A declaration without an explicit engine is invalid. Normalized models
retain the input provenance and are checked against that engine's registered
model definitions.

Each subject's source and normalized components match the corresponding value
owner. Database, catalog, schema, name, path and overload components remain
separate. A child appends its parent as the last path component. Explicitly
defaulted schemas follow the request's identifier semantics. A receipt cannot
account for an object whose value belongs to another subject.

### The reply accounts for every object and operation

A completed successful reply carries `PlanningContribution` values and one
`DeclarationReceipt` per input object, in input order. A receipt holds its
unchanged subject, a nonempty strategy and the IDs of its creation steps. An
empty step list states that the common creation steps implement the declaration.
Every contributed step is named by a receipt.

Contributions carry the existing operation, effect, transaction and dependency
records. Operations belong to the selected service's declared vocabulary.
Dependencies can name contributed steps or supplied common steps. No reply can
replace a common step, introduce an executable prefix or schedule its graph
independently. The host validates and schedules the complete graph before
rendering in text form.

A completed refusal carries only declaration diagnostics. Each diagnostic holds
a `SchemaDiagnostic` and an optional zero-based object index. An indexed
problem names that object's model kind. A failed or canceled call returns no
successful prefix, receipts or diagnostics.

### The host owns whole-schema orchestration

The process adapter registers `engine.Declarations` from the negotiated engine
assignments. Its declaration service maps the captured local request to the
wire, validates the reply, and binds its operations to the selected runtime.

The host's schema-rendering path combines table creation, common objects and
owned declaration contributions through the same dependency graph. It does not
borrow the embedded engine's renderer or infer server facts. The caller's
capabilities and database path remain available to the appropriate existing
creation and rendering boundaries.

### Validation

Both transport boundaries reject unnegotiated assignments, duplicate service
IDs or kind assignments, missing desired or operation models, and undeclared
services. They reject mismatched engine facts, duplicate input subjects, values
with the wrong representation, and tables whose captures disagree with their
subjects.

Reply validation rejects incomplete results, mixed refusals and output,
changed or missing receipt subjects, empty strategies, repeated or unknown step
IDs, unaccounted operations, invalid effects and operations, foreign owners,
and dependencies outside the supplied and contributed graph. Diagnostic indexes
must be in range. Validation uses the complete batch; a valid prefix does not
make an invalid suffix acceptable.

### Versioning

This addition belongs to contract 25.5. It does not raise a provider's minimum
contract. An older host receives no declaration assignments and cannot call the
service. The provider's existing services remain independently negotiated.

## Alternatives considered

**Compare with an empty observed schema.** This manufactures absence and can
select replacement or destructive behavior that an authored declaration never
requested. Rejected.

**Return SQL from a whole-schema RPC.** This makes the provider schedule common
objects and other owners' objects without the complete host graph. Rejected.

**Register the common-table renderer for every provider.** A provider that owns
table creation does not promise the common-table creation semantics, and
standalone model values cannot be flattened into common AST nodes. Rejected.

## Acceptance criteria

- SDK negotiation and mutation-tested validators cover each boundary above.
- A subprocess fixture renders a standalone owned object and a named table
  child with their common creation dependencies, without opening a session.
- Refusal, cancellation, an invalid receipt and a missing dependency expose no
  SQL prefix through the public schema-rendering API.
- YDB renders authored tables, owned standalone objects and common objects from
  Go, YAML, HCL and SQL with the same ordering guarantees as its migration
  planner. Live apply and readback establish the rendered definition on each
  supported YDB line.
