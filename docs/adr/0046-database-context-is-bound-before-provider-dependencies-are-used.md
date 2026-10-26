# ADR 0046: Database context is bound before provider dependencies are used

Status: Accepted

Contract: 25.7

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md),
[ADR 0039](0039-a-provider-declares-how-a-scope-names-its-objects-and-which-grants-name-them.md)

## Context

A provider computes `NormalizedValue.required_objects` while it normalizes a
captured value. A source declaration can contain an absolute reference whose
local identity depends on the database selected by the operator. For example,
a YDB external data source can name a secret as `/local/auth/password`, while
the source declares that secret under the relative identity `auth/password`.

The source decoders have no established database context. Comparison receives
`DatabasePath`, but the host collects required objects before it dispatches
any comparison service. Scope selection runs earlier still. An unchanged data
source must retain its secret in both stages. Computing the dependency inside
one comparer cannot protect a secret handled by another comparer, and service
iteration order cannot establish that protection.

The model codec persists source envelopes and recomputes normalization when it
decodes them. Context kept only in a response disappears during that round
trip. Putting context in `Envelope.owner.database` changes the declaration's
identity: declaration receipts bind their subject to both owner spellings, and
source selectors name relative objects. Context and identity need separate
representations.

The host also accepts source files with a database URL without opening that
database. Its built-in root reader supplies SQL parsing with an authored root.
A provider alias cannot use that reader as its own address authority. A dev
realm can have an execution root beneath the database root against which
source references were authored. A generic bridge must retain that distinction
without parsing provider addresses or paths in the host.

## Proposed decision

### Resolve an address without opening it

The negotiated feature `database.context` adds an offline service:

```proto
message DatabaseContextRequest {
  string engine;
  string address;
}
message DatabaseContext {
  string database;
  string source_database;
}
rpc ResolveDatabaseContext(DatabaseContextRequest) returns (DatabaseContext);
```

`database` is the exact namespace selected for execution and inspection.
`source_database` is the root against which the caller's absolute authored
references are interpreted. They are equal for an ordinary database address.
An owner can return distinct values for an explicitly selected isolated realm.
The host neither joins them nor strips one from the other. Their semantics
belong to the provider.

The request address can contain credentials. The service parses it without
network access, credential lookup, environment reads or provisioning. Neither
its diagnostics nor its receipt contains the address or credentials. Both
receipt fields are required, nonempty, single-line strings and are compared
exactly. A provider that cannot establish them refuses the request.

An engine supporting this feature also returns the same context in its
`Session` receipt. `context.database` must equal `Session.database`. The host
checks an opened session against any context it resolved for the same address.
An explicit caller-supplied established context can be used without resolving
an address. An absent context means unknown; it never means a default root.

### Preserve context separately from source identity

The two-way feature `values.context` adds these fields:

```proto
message Envelope {
  // Existing fields retain their meanings.
  DatabaseContext context;
}
message NormalizedValue {
  // Existing fields retain their meanings.
  bool requires_database_context;
}
```

The feature requires `values.normalize`. A present context is immutable
metadata during normalization, conversion and ordinary model transport. It
does not change either owner identity. Model cloning, durable encoding,
canonical identity and target-profile validation include it. The codec
recomputes every dependency from the source payload and captured context; it
does not trust persisted dependency lists.

A provider sets `requires_database_context` only when the absent context
prevents a complete dependency answer. In that state `required_objects` is
empty, so a prefix of known dependencies cannot be mistaken for a complete
answer. The flag is not a claim that the value needs no dependencies. A
context-bearing result cannot retain the flag: missing or inconsistent facts
refuse the entire batch. An ordinary relative declaration can be complete
without context and keeps its offline behavior.

Decode, structural validation and lossless source conversion may retain an
unbound declaration. A consumer of dependency facts must refuse it. Comparison,
feature planning, declaration planning and execution do not accept unresolved
context. Source export may retain the original declaration only when its
format preserves every authored reference and does not make a dependency or
scope decision from the incomplete facts.

### Bind a complete snapshot before selection

The host adds one explicit, contextful binding stage over a schema's named
objects and facets. It clones every affected envelope, supplies the established
context and calls the existing offline `NormalizeValues` service. There is no
second RPC for interpreting the same payload.

The stage preserves the exact ordered inventory, source and normalized owner
identities, target scopes, coverage, model definitions and required features.
Only the context and the owner's recomputed canonical and dependency facts can
change. A refusal or cancellation publishes no prefix and mutates no input.
Repeating the stage with the same context is idempotent. A captured value bound
to another context refuses; rebinding requires decoding the authored source
for the explicitly selected new target.

Binding precedes scope selection, collection of `ObjectComparisonRequest.Required`,
declaration graph construction and plan capture. A local source load can bind
from an explicitly supplied address. Connected apply binds from its opened
session before selecting desired and current states. Source-to-source diff
binds each side from its declared context before projection and selection.
Offline operations with no context retain context-independent declarations
and refuse an unresolved dependency before using it.

The shared `schemaext.Requirer` contract returns an error alongside its
identities. This lets every dependency consumer refuse an unresolved value,
including a caller that bypasses the binding stage. No consumer interprets an
error as an empty dependency list. Existing in-process implementations return
their existing complete lists and a nil error. This is a reviewed pre-v1 API
change; there is no second fallback interface.

The provider maps a fully bound absolute dependency to the same local identity
as the declared object. It rejects paths outside the established source root
or selected realm according to its own rules. The host does not infer path
semantics, rewrite payload text or borrow a built-in provider.

### Retain the binding through plans

Schema and operation codecs retain context independently of owner identity.
Saved plans include it in the provider target digest and in each native model
whose meaning uses it. Loading a plan recomputes normalization with its
captured context. Dispatch compares that context with the opened session before
approval-bound work can run. A different database or realm requires a new plan.
Addresses and secret values never enter the digest or artifact.

The host uses explicit request fields and immutable snapshots throughout.
Neither a global current database nor context hidden in an RPC client is
allowed. Independent sessions for the same engine can bind different roots in
one process without affecting each other.

## Acceptance evidence

- Absolute and relative references to the same secret produce the same local
  dependency after binding. The original payload and owner remain unchanged.
- Selecting an external data source retains its declared secret. An unchanged
  source also prevents removal of its inspected secret when the desired source
  omits the secret. Reversing comparison-service registration order preserves
  the result.
- Unknown context, a foreign root, a foreign realm and a changed context refuse
  before selection or planning; none become an empty dependency list.
- A relative declaration with no context still parses, validates, exports and
  renders. An absolute declaration can parse and export without falsely
  claiming resolved dependencies.
- Address resolution works through a provider alias and performs no connection,
  credential lookup or environment read. Malformed-address diagnostics are
  redacted. A resolved address and its opened session must agree.
- Ordinary database and isolated-realm cases exercise distinct authored and
  execution roots. No host fixture contains provider-specific path parsing.
- Clone and codec round trips preserve context and recompute dependency facts.
  Tampering with a persisted dependency list has no authority.
- Saved plans refuse another database or realm before dispatch. Per-session
  isolation, cancellation, missing receipts and reordered receipts are covered.
- Old peers cannot negotiate or silently consume either feature or its fields.

## Sequence

Review this decision with the common-view facet transport before changing the
protocol. Publish one coherent SDK revision after its complete contract,
runtime, fixture and documentation gates. Pin that published SDK in the host
and fixture module, then implement the generic binding stage and provider
adapters. Qualify the combined source, scope, comparison and saved-plan paths
against live targets. A plugin release is a separate action.
