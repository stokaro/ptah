# ADR 0049: Native object context is assessed without inventing schema differences

Status: Accepted

Contract: 25.7

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0044](0044-lifecycle-operations-use-the-complete-common-sequence.md),
[ADR 0046](0046-database-context-is-bound-before-provider-dependencies-are-used.md).

## Context

A running object can require lifecycle work while its declaration stays equal
to its observation. A streaming query must stop before an input is replaced
and restart afterward. An input that the desired schema omits must remain
protected when an unchanged query still requires it.

Comparison correctly returns no query change when its stored settings already
match. Planning receives only assigned changes, common table captures, and
common operations. Its runtime invokes a service only for changes or assigned
table parent actions. It also refuses every contributed operation that no
change or parent receipt accounts for. An unchanged standalone object therefore
has neither a planning input nor a receipt that can account for its lifecycle.

The host already retains named desired and current objects in
`SchemaDiff.Features`. These values must reach planning as captured context,
without modifying the declaration or manufacturing a schema difference.

## Decision

The two-way feature `planning.object-context` adds native object assessment to
feature planning. It requires owned desired and observed model contracts.

### Preserve complete owner-local context

`PlanFeaturesRequest` carries `desired_objects` and `current_objects` as
`ComparisonState`, including their coverage, and a sorted `changed_subjects`
list. The host sends the complete captured state belonging to the provider for
the selected engine. It does not narrow these states to the service's change
kinds or context kinds. An object of another family may be the input whose
replacement requires a stop.

The host snapshots values through their selected codecs and checks target
profiles and bound database context before dispatch. Unknown coverage remains
unknown. Missing records never establish absence. Pending dependency context,
foreign model ownership, duplicate identities, changed representations, or
inconsistent bindings refuse the batch.

`changed_subjects` is the unique union of subjects of accepted feature changes
that name a captured native object. The selected runtime derives it from the
complete accepted change batch before assigning changes to individual services.
A caller cannot supply a different list. The provider receives no additional
change ownership through this list. Common table actions and common operation
metadata continue to describe common mutations.

This summary lets an owner recognize a referenced native mutation without
repeating another comparison service's normalization and default rules. A
desired and observed envelope can have different representations even when
their settings compare equal. Service iteration order does not establish
whether that object changes.

### Assign assessment independently of changes

`ModelPlanning.context_kinds` assigns native object assessment to a planning
service. The provider must own both model representations, and a model kind has
at most one context planner per target. Registration and negotiation preserve
this assignment. The request carries the assigned `context_kinds`; the runtime
sets them from registration and ignores caller attempts to narrow them.

The runtime invokes each selected target service with assigned context kinds
once, even if it has no changes, no table parent action, or no named records.
Kind-level unknown coverage can hide dependents and must reach the owner for
assessment before mutations. Empty collections do not bypass that assessment.
Other services retain their existing invocation rules and carry none of the
object-context request fields. Engine-level negotiation does not assign
assessment to an ordinary service. Context without a
registered assignment grants no permission to emit operations for it.

The public host contract mirrors these facts with
`featureplan.ObjectContext`, containing desired and current
`schemaext.ObjectState` values and `ChangedSubjects`,
`featureplan.Request.Objects` and `ContextKinds`, and
`engine.Planning.ContextKinds`. The generic provider adapter captures
`SchemaDiff.Features`; it contains no engine-specific dependency parsing.

### Account for every assigned object

`PlannedFeatures.objects` contains an exact ordered receipt for every
`(model kind, subject)` in the union of the desired and current records and
subject coverage for the assigned context kinds. Kind-level unknown coverage
also reaches the provider and may require a diagnostic; it creates no invented
subject. Receipt order is model kind in bytewise order, then structured subject
order. Structured order is `schemaext.CompareRefs` in the host and
`provider.CompareIdentities` in the SDK: identity kind, normalized database,
catalog, schema, path components, parent, name, and signature components.
Equivalent normalized identities pair both sides; the desired spelling is
authoritative when present, otherwise the current spelling is retained.

Each receipt preserves its kind and exact selected subject, states a nonempty
strategy, and lists existing contributed step IDs once. Empty steps explicitly
mean no operation is required. A changed object may share steps with its change
receipt. No receipt can name a foreign or missing object, and no object can be
omitted, repeated, or reordered. Every contributed step must be accounted for
by a change, table parent, or native object receipt.

The public host result carries equivalent `Objects []ObjectPlan` receipts.
Diagnostics can identify an object by its exact union index. Refusal returns no
successful contributions or receipts. Existing change and parent receipt rules
keep their meanings.

Context assessment permits an unchanged query's stop and restart to carry
their real settings transitions, independently checked preconditions, and
ordinary resource effects. Its object receipt accounts for both operations.
The complete host graph orders them around input mutations. A query whose
input will disappear without a valid replacement is preserved by dependency
selection or refused; restarting it against a missing input is not a solution.

## Alternatives considered

**Emit a fabricated comparison change.** This changes the meaning of equality
and persists a declaration edit the operator did not request. Rejected.

**Attach the query to a table parent.** A standalone object can depend on topics,
external data sources, pools, and several tables. Its identity and coverage are
not those of a table facet. Rejected.

**Invoke every planner without receipts.** This weakens ownership and allows
unaccounted operations. Rejected.

**Let planners call earlier comparison or planning services.** Ordering between
services cannot establish shared state, and replaying another family's default
rules is a second comparison implementation. Rejected.

## Acceptance evidence

- An unchanged running object is assessed when only its input changes. Its
  operations receive exact object receipts and enter the complete host graph.
- A native input replacement, a common table rebuild, and a removed input are
  exercised separately. The resulting apply reaches a fixed point.
- Required inputs survive scope selection and source omission. Explicit
  exclusion or unknown dependency coverage refuses instead of inventing absence.
- A query with no relevant input mutation emits no lifecycle operations, even
  when an unrelated object changes. An unchanged schema remains a no-op.
- Reversing service registration order preserves results. Inputs and output
  receipts remain isolated through codec snapshots.
- Missing, reordered, duplicate, foreign, and unbound-context inputs or
  receipts refuse. A changed-subject list cannot widen write ownership.
- An unaccounted operation still fails. Existing change and table parent
  controls pass without object context or its negotiated feature.
- Live public CLI controls include unchanged query text with input replacement
  and source omission; a probe that changes the query text is insufficient.

## Localized observation identity

An observed native model retains its captured database in both envelope owner
spellings. The host's selected object identity can omit that database after
verifying it against the opened session. A request with an empty subject database
therefore requires both owner database spellings to equal its nonempty
`database_path` before localization. Every other coordinate remains exact.
A foreign database or a missing binding is refused.
