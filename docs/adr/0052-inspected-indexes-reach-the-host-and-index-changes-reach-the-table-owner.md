# ADR 0052: Inspected indexes reach the host, and index changes reach the table owner

Status: Proposed

Contract: 25.10

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0045](0045-a-provider-states-common-column-comparison-and-readback-semantics.md),
[ADR 0048](0048-common-column-observations-have-one-complete-wire-representation.md),
[ADR 0051](0051-inspected-primary-keys-retain-their-common-order.md).

## Context

A schema source declares common indexes on a table, and the host compares them
declaratively: an index the source adds is created, and one it leaves out is
dropped. For a table a provider owns, the host has no current side to compare
with. Inspection's `Table` carries columns, native facets and, since contract
25.9, the primary key, but no common index. The YDB provider keeps its indexes
in a native facet only.

Two failures follow, both measured with the YDB provider against YDB 26.2. A
table created from a schema with an index fails to plan, because the host's
provider planner refuses the diff's index fields. A schema that adds an index to
an existing table, or removes one, plans nothing at all: the change is lost
without a word.

## Decision

### The provider reports the held indexes

The two-way feature `schema.observed-indexes` adds `Table.indexes`, a list of
`ObservedIndex`. It requires `schema.observed-columns`. When negotiated, the
list is the table's whole set of common secondary indexes, so an index it does
not name is absent. Without negotiation the field refuses.

Each index names its table exactly, has a name unique within the table, names a
key, and names only known columns, each once. The primary key is never an index
in this list. An index the common model cannot describe is left out of the list
and kept in the provider's native facets: approximating it would let the host
plan a change the provider cannot make, and leaving it out lets the host
neither create nor drop it.

### The host compares, and the table owner plans

The host maps the list into the common catalog and compares declared indexes
with it as it does for any engine. For a table the provider owns, a new table's
declaration carries its indexes, and an index change on a table the plan
otherwise leaves alone makes that table an altered parent, so its owner
receives both captured sides. Each index addition and removal also becomes a
common planning step whose effects read the table and create or drop the index
subject. The owner claims the step by rewriting it to its own operation. An
unclaimed step refuses the plan, as an unclaimed column step does, so a
provider that does not plan indexes is refused rather than dropping the change.

The planning step carries no new operand. The owner reads the declared index
from the altered parent's desired table, and the held one from its current
table.

## Consequences

The host refuses an index change for a provider-owned table until the provider
negotiates `schema.observed-indexes` and claims index steps. That refusal is
the fail-closed state, and it replaces planning nothing.

The SDK release comes first. The host and provider adapters adopt it in their
own changes, against the published tag.
