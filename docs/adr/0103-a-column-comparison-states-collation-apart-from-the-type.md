# ADR 0103: A column comparison states collation apart from the type

Status: Proposed

Contract: 25.32

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204);
the open contract gaps of [ADR 0102](0102-the-provider-contract-freezes-with-ptah-1-0-and-keeps-major-25.md)

Relates to: [ADR 0045](0045-a-provider-states-common-column-comparison-and-readback-semantics.md)

## Context

`schema.columns` (ADR 0045) has a provider compare the common columns of a
table the host pairs. The host names the aspects it asks for, and the set is
closed: `COLUMN_TYPE`, `COLUMN_DEFAULT`, `COLUMN_NULLABILITY` and
`COLUMN_SEQUENCE`. Both captures already carry a column's collation: the
desired `DeclaredColumn.collation` and the observed `ObservedColumn.collation`.

No aspect asks for the collation, so an engine with collations has two
choices: ignore them, or fold them into the type. The DuckDB provider folds
them (`internal/backend/columns.go`): it writes each side as
`<type> COLLATE <collation>` and compares that text as the type. A column
whose collation alone changes then reaches the host as a changed type, and the
host's column diff records `type: VARCHAR COLLATE NOCASE -> VARCHAR`, the
change a type change plans and reports (DuckDB contract gap 6).

## Decision

### A collation aspect under a two-way feature

`schema.column-collations` adds `ColumnAspect.COLUMN_COLLATION`. The provider
lists the two-way feature, and so does each engine whose column comparison
states collation, beside `schema.columns`. Under it:

- the host asks the collation of each column pair it sends;
- the provider answers a `ColumnDifference` whose spellings are the declared
  and the held collation, empty where a side leaves the engine's default, and
  whose relation says whether the engine holds them as one collation;
- its `COLUMN_TYPE` answer leaves collation out.

A column whose collation alone changes has a collation change and no type
change. The host records it apart from the type, so a report says what
changed, and a planner that owns the table can tell a collation change from a
type change. A host that does not list the feature asks no collation, and an
engine compares collation inside the type, as it does for a host of contract
25.31.

### What the SDK enforces

`ValidateColumnsRequest` refuses the aspect when the engine or the request's
target features do not list the feature. A collation receipt needs `SAME` or
`CHANGED`, each spelling on one line without surrounding white space, and two
different spellings for `CHANGED`; equal spellings under `SAME` are allowed,
since the engine may fold case. `ValidateDescription` refuses the engine's
feature without the provider's or without the column comparison.

## Alternatives considered

**Keep collation inside the type.** It needs nothing new and works for
DuckDB. The host cannot tell a collation change from a type change, so a
report names the wrong change and a planner that treats a type change as a
rebuild treats a collation change the same way. The freeze would then keep a
`COLUMN_TYPE` whose meaning depends on the engine.

**The host compares collation itself.** The host has both spellings. It does
not know which spellings an engine holds as one collation: DuckDB folds
`NOCASE` and `nocase`, and an engine's default collation may be written out
on one side and absent on the other. The provider owns that knowledge, as it
owns type equivalence.

**A collation field on the type receipt.** A second difference inside the
`COLUMN_TYPE` receipt would keep the aspect list short. It couples two
answers the host may want separately: a host asks no type for a column whose
type a domain owns, and would then lose the collation too.

## Consequences

- DuckDB can report a collation change as one, and keep its type comparison
  free of collation, once it adopts the feature.
- A host of contract 25.32 asks one more aspect for each pair of an engine
  that lists the feature, so its comparison reply grows by one receipt per
  column.
- The freeze decides whether the aspect becomes the only form for an engine
  with collations (ADR 0102, open contract gaps).

## Acceptance evidence

- SDK unit tests: `TestColumnCollations_HappyPath`,
  `TestColumnCollationReceipt_FailurePath`,
  `TestColumnCollationRequest_FailurePath` and
  `TestColumnCollationsDescription_FailurePath` in `provider-sdk/provider`.
- The conformance suite's narrowed host profiles include the feature for every
  provider that offers it.
- The host adoption asks the aspect of an engine that lists the feature and
  records a collation change apart from the type.
