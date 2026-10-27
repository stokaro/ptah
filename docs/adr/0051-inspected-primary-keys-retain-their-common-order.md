# ADR 0051: Inspected primary keys retain their common order

Status: Proposed

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0048](0048-common-column-observations-have-one-complete-wire-representation.md)

Contract: 25.9.

## Context

An inspected column states whether it belongs to a primary key. Membership
does not establish key order: table columns `(a, b)` can have primary key
`(b, a)`. The common catalog records that order in a constraint, and source
conversion reads it there. Inspection's common table message cannot carry
that constraint. The YDB row-table facet retains its observed order, but the
common source exporter cannot read provider-owned payloads.

Exporting the column flags alone changes a composite key's order. Keeping a
second writable key in a desired native facet gives one declaration two owners.

## Decision

The negotiated feature `schema.observed-primary-keys` adds `Table.primary_key`
using the existing `ObservedConstraint` message. Both the provider and selected
engine declare the feature. It requires `schema.observed-columns`, inspection
and value normalization. An unnegotiated record is refused.

Each table carries either one complete primary-key record or no record when
the table has no primary key. The record has type `PRIMARY KEY`, binds the
exact table name and schema, and lists its columns in key order. Every member
is a known column, appears once, and agrees with the column's observed
membership flag. A missing record with any primary-key membership is refused.

The record retains its name, included columns, method, comment, deferrability,
initial mode, enforcement and hash-shard settings. Included columns are known,
unique and separate from key members. Foreign-key, check, unique-null and
dependency fields are refused because they do not describe this primary key.
The singular `column_name` alias stays empty: `column_names` owns key order.

The host maps the record to the common catalog constraint without sorting it
or inventing a name. Source conversion then gives the common table declaration
the ordered primary key. Native observed facets can retain read-only evidence;
native desired facets cannot independently author this common key. A provider
removes such a duplicate only after its complete source round trip proves
that the common declaration preserves the same order.

## Alternatives

Derive order from table-column order. Rejected: the orders are independent,
and a successful export would silently change the declaration.

Keep order only in a provider-specific source directive. Rejected: the common
model already owns primary keys and common planning consumes that model.
Duplicating ownership also makes ordinary source formats disagree.

Transport the entire observed table in a second field. Rejected: it would
duplicate the existing column and facet inventory when only a missing common
constraint needs transport.

## Acceptance and consequences

SDK controls reject missing records, unknown or repeated members, inconsistent
membership, foreign table bindings, invalid attributes and missing negotiation.
An independent protobuf round trip retains a composite order that differs
from table-column order and preserves optional attributes without aliasing.

Host tests export the inspected constraint through the common source model and
pin the composite order. A real provider must inspect, export, reload and plan
the same key without a change before dropping its native desired duplicate.
The SDK is released and pinned before those adapters use the new field.
