# ADR 0056: A comparison service counts the objects it compares

Status: Proposed

Contract: 25.13

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4900](https://github.com/stokaro/ptah/issues/4900)

Relates to: [ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md),
[ADR 0035](0035-the-operators-planning-policy-reaches-the-provider.md)

## Context

A drift report's inventory says how many objects of each kind each side holds.
For a kind a feature owner compares, the host cannot count them itself: the
owner pairs its own objects and returns only the ones that differ. A built-in
owner states its counts in `schemaext.ObjectCount`, one per change kind, and
the host checks each count against the owner's changes. A provider's comparison
service has no field for the same counts, so a kind a provider owns has no
inventory entry.

The host cannot derive the counts from a reply. A change record carries a
normalized `CHANGE` value whose payload only its owner reads, and nothing in the
record says whether the change creates an object, drops one, or alters one both
sides hold. Counting the captured records does not work either. They include
objects the service excludes, keeps without comparing or leaves undecided, and
which desired record and which observed record are one object is the owner's
decision.

## Decision

### The owner states its counts

`ComparedModels.counts` carries one `ComparisonCount` per change kind a service
counts, with `matched`, `added` and `removed`. They mean what
`schemaext.ObjectCount` means. `matched` counts the objects both sides hold that
the service takes to be one object, whether or not they differ, and an object
the plan drops and creates again is matched. `added` counts the changes that
create an object the observed schema lacks, and `removed` the changes that drop
an object the desired schema does not declare. The desired schema holds
`matched + added` objects of the kind, and the observed schema
`matched + removed`. An object the service leaves out counts nowhere: one the
run excludes, one it keeps without comparing, one it reports as undecided, and
one below a parent only one side holds.

A facet service counts the objects its settings stand for, such as the tables
with row-level security enabled, by the same rules.

An entry claims every object of its kind in the batch. A service may leave a
kind out, and a reader then knows nothing about the kind rather than that it
holds none.

### The SDK refuses what a reply contradicts

Both transport boundaries refuse a count of a kind that is not one of the
service's change kinds, and a kind counted twice. Every change of a kind adds an
object, drops one, or alters one both sides hold, and no change does two of
these. So the SDK refuses `added + removed` above the reply's changes of the
kind, and more changes of the kind than `matched + added + removed`. Checking
`added` and `removed` each against the changes would accept one change counted
as both. A later minor could not add the stricter check, because a validator
that refuses what an earlier peer sends is an incompatible change, so the field
carries it from the start.

`matched` is not checked against the captured records: which records pair up
is what the owner decides.

### The field is negotiated

The two-way feature `models.compare-counts` switches the field on. The provider
lists it beside `models.compare`, an engine lists it only when its provider
does, and a reply carries counts only when the provider, the engine and the
request's target features list it. The contract uses an addition only when
both sides advertise its feature. A service also learns from the request that
the host reads counts, so it counts only when the result is used. And the SDK
server can refuse counts the host did not ask for, so every count a host
receives passed the checks the host negotiated.

### Older peers

A host built before this contract does not list the feature. The provider sees
no target feature and sends no counts, and the SDK server refuses a reply that
sends them anyway. A host whose SDK does not know the field ignores it.

A host that lists the feature and a provider that does not get no counts. The
provider's kinds then stay out of the inventory: the host leaves a kind out
when an owner that ran did not count it, and never reports it as zero.

## Alternatives considered

**The host counts the captured records.** The records hold objects the service
leaves out, and pairing them is the owner's decision. Rejected.

**A direction on each change.** A change record could say whether it adds or
drops an object. It cannot say how many objects match without differing,
because those have no change record. Rejected.

**An unnegotiated field.** A service could not tell whether its counts are
read, and the SDK server could not refuse counts the host never agreed to
check. Rejected.

**Counts from object services only.** A facet owner, such as the one for
row-level security enablement, compares settings that stand for objects the
inventory counts. Rejected.

## Consequences

- Contract 25.13 adds a field, a message and a two-way feature.
- Counting stays optional for each kind.
- The host must list the feature in a request's target features even when the
  comparison captured no value. Otherwise a service cannot count an empty batch,
  and its kinds drop out of the inventory.
- The SDK release comes first. The host adopts it in its own change, against
  the published tag.

## Acceptance evidence

- SDK tests accept consistent counts from object and facet services, and a
  reply without counts. They refuse counts the provider, the engine, the
  request or the host did not negotiate, while the same reply without counts
  passes, and each count the reply's changes contradict.
- An SDK-only proof provider, started as a process, counts its matched objects
  for a host that lists the feature. For a host that does not, the same
  comparison carries no counts, and that host cannot ask for them.
