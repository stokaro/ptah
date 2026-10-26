# ADR 0048: Common column observations have one complete wire representation

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0045](0045-a-provider-states-common-column-comparison-and-readback-semantics.md)

Contract: 25.7.

## Context

Inspection's `Column` carries a name, type, nullable and primary-key flags, an
optional default expression, and native facets. Common column comparison and
readback use `ObservedColumn`, which also carries identity settings, serial
metadata, sequence restart, generated expressions and the other common column
facts. A setting kept only in an opaque facet cannot reach the common default
reader, whose request contains the common column without native facets.

The YDB adapter exposes this boundary with serial columns. Complete native
observations retain their sequence settings, but the common catalog cannot
represent those settings through the inspection wire. Reconstructing a second
writable default or sequence inside a desired native facet would give the same
setting independent common and native owners.

## Decision

The negotiated feature `schema.observed-columns` adds `Column.observed` of the
existing `ObservedColumn` type. The provider and supporting engine both list
the feature. It requires inspection and value normalization. It adds no RPC.

When negotiated, every inspected column carries the complete common observation
in this field. `Column.name` is its attachment identity and must equal
`observed.name` exactly. `observed.ordinal_position` is its one-based position
in the captured table, its data type is nonempty, and its nullability is `YES`
or `NO`. An absent default remains distinct from an explicit empty value.

The legacy `type`, `nullable`, `primary_key` and `default_expression` fields are
empty in this mode. Their values cannot override or supplement the observation.
Without negotiation, `observed` is refused. A peer never silently drops it or
chooses between conflicting representations.

Native facets remain attached to the wrapper column and retain their full
observed evidence. Existing model coverage and normalization accounting still
include them. The host converts `ObservedColumn` with the same complete mapping
used for creation predictions and captured comparison operands, with checked
integer conversions and independent copies.

Observed-to-desired conversion has one writable owner for each setting. A
provider omits native desired default and sequence fields represented by the
common source field. It retains independently represented native settings or
refuses a setting it cannot transport. It never removes an entire facet merely
because part of its observation is also represented by common metadata.

## Acceptance

SDK controls reject missing observations, conflicting legacy fields, changed
names, missing types, unknown nullability, and reordered or missing ordinals.
Unnegotiated fields refuse. Native facet inventory and coverage remain exact.

Host and provider public-path tests inspect literal defaults, explicit empty
defaults, serial types and identity settings. Export and readback retain their
meaning, comparison reaches a fixed point, and common and native desired
settings do not conflict. Observed sequence restart remains captured evidence
for its existing operation owner, never an authored source default.

The SDK revision is reviewed, released and pinned before host and provider
adapters use the field. This contract extends the inspection input needed by
the column semantics service; it does not claim provider parity without the
combined tests.
