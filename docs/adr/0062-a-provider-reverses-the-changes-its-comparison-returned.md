# ADR 0062: A provider reverses the changes its comparison returned

Status: Proposed

Contract: 25.19

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md),
[ADR 0054](0054-a-filtered-read-carries-the-planning-context-it-does-not-compare.md)

## Context

`ptah migrations generate` writes a forward and a reverse migration from one
comparison. The reverse plan is planned from the forward diff, turned around:
a created table becomes a removal, a changed column the prior definition. A
change a feature owner's comparison returns is the owner's payload, and the
host does not read it. The host inverts it through the owner's reversal
service (`schemaext.ReversalService`), which returns the inverse change, what
the forward change leaves at the subject (its forward state), how the reverse
restores the prior definition and what it cannot recover. Every built-in
engine registers one for its change kinds. The built-in YDB engine, for
example, drops a created secret and recreates a dropped one, and restores a
coordination node's settings in place.

An executable provider has no such service. Its comparison returns changes
through `CompareModels`, and nothing on the contract undoes one, so the host
refuses the reverse plan and writes no migration at all. Measured through the
YDB provider on YDB 26.2.1.14:

```text illustration
error: reverse feature changes: unsupported feature: no reversal for "ydb"/"github.com/stokaro/ptah-ydb-plugin/secret-change"
```

The same refusal names `coordination-node-change` for a created or altered
coordination node, and it holds for every change kind the provider compares:
topics, external sources and tables, resource pools and their classifiers,
streaming queries, async replications, transfers, and the native facets of a
table. The built-in engine is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)), and with
it every reverse migration of such an object, unless the provider can undo
its changes.

## Decision

### The owner reverses its own changes

Contract 25.19 adds the RPC `ReverseChanges` and a declaration of the
services that answer it:

```proto
rpc ReverseChanges(ReverseChangesRequest) returns (ReversedChanges);

message Engine {
  // ...
  repeated ModelReversal reversals = 21;
}
message ModelReversal {
  string id = 1;
  repeated string change_kinds = 2;
}
message ReverseChangesRequest {
  string engine = 1;
  string reversal_id = 2;
  repeated string target_features = 3;
  ComparisonIdentifiers identifiers = 4;
  map<string, bool> capabilities = 5;
  repeated PlanningChange changes = 6;
}
message ReversedChange {
  ComparisonIdentity subject = 1;
  NormalizedValue value = 2;
  repeated ForwardState forward_state = 3;
  string strategy = 4;
  repeated string limitations = 5;
}
message ForwardState {
  enum Placement { PLACEMENT_UNSPECIFIED = 0; OBJECT = 1; FACET = 2; }
  Placement placement = 1;
  string kind = 2;
  NormalizedValue value = 3;
}
```

The call carries the shape of the host's reversal service across the
boundary. The host sends the accepted changes of one service in plan order,
each as the comparison returned it, and the provider answers one result per
change, in order. The result's value is a CHANGE value of the same kind whose
operands describe the reverse direction, or nothing, with a limitation, when
the reverse has nothing to run. Its forward state predicts what the forward
change leaves: the subject itself, of the subject's kind, or a facet attached
to it, each a complete OBSERVED value or the absence of one. The host
registers each declared service as the owner's reversal service, so the
engine runtime dispatches and checks a provider's reversal as it checks a
built-in one: the reply keeps its subject and change kind, and the provider
owns the codecs of every value it predicts.

The call is offline: no session, no database and no source document. A
change carries what its owner needs to undo it, as the change model already
promises; a comparison that returns an incomplete operand cannot be
reversed, and the provider refuses rather than guesses.

### What the SDK enforces

`models.reverse` is a two-way feature. It comes with an addition to `Engine`,
so a host lists it to receive the declarations, and both the provider and each
reversing engine list it beside `models.plan` and `models.compare`. A change
kind belongs to at most one reversal service and must be a CHANGE model of the
engine.

Both transport boundaries refuse a request without the negotiated feature, for
a service the engine does not declare, without identifier rules or changes,
with a change of another service's kind, or with one subject and kind twice;
each change is validated as a CHANGE value. They refuse an incomplete reply or
one of another length, a result that changes its subject or the change kind, a
strategy or a limitation that is not one line, a result without a value or a
limitation, and a forward state without a placement, of a kind with no
OBSERVED model, of an object of another kind than its subject, or named twice
for one subject. Each present value is validated in its representation.

## Alternatives considered

**The host compares again with the sides swapped.** The reverse plan would be a
second `CompareModels` whose desired side is the database before the change,
converted to its declaration, and whose current side is the state the forward
plan leaves. The comparison's current side is an observation with inspection
coverage. The state the forward plan leaves has never been read, so its
observation would have to be predicted from the declaration, and only the
owner can predict it: a secret's value is never returned, a coordination
node's omitted setting keeps the value the server held before an `ALTER`. The
host would then also have to claim inspection knowledge for a prediction,
which conversion must never establish ([ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md)).
A reversal service returns exactly that prediction, labeled as one, beside
the inverse change. Rejected.

**The host inverts the change.** The host would swap the operands of each
provider change. A payload is the owner's, and the host cannot say which part
of it is an operand, which representation each holds, or which settings an
`ALTER` leaves as they were. Rejected.

**A reverse plan the provider writes whole.** The provider would plan the
reverse migration itself, outside the host's graph. Its steps would then not
order against the host's own reverse steps, such as a table the forward plan
created beside a secret, and the host could not report the owner's
limitations beside the rest. The reversed change instead goes through the
host's planning as any other change does. Rejected.

## Consequences

- Contract 25.19 adds an RPC, four messages, an `Engine` field and a two-way
  feature.
- The SDK release comes first. The host lists the feature and registers a
  provider's reversal services in its own change, and a provider declares
  them in its own.
- Without the feature on either side nothing changes: the reverse plan of a
  provider change is refused as before, and the forward plan is not written.
- The reverse migration of a provider change carries the owner's strategy and
  limitations as comments, as the built-in engines' reverse migrations do.

## Acceptance evidence

- SDK tests accept a negotiated reversal, a result with nothing to run that
  states a limitation, and a forward state that establishes an absence. They
  refuse a description, a request and a reply in each malformed shape above,
  and a host or a provider that does not list the feature, at both transport
  boundaries.
