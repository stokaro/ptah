# ADR 0096: An engine states a partial index's predicate as its server holds it

Status: Proposed

Contract: 25.29

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0057](0057-a-provider-spells-a-declared-index-kind-as-it-reports-a-held-one.md),
[ADR 0090](0090-a-provider-decides-whether-two-checks-are-one.md),
[ADR 0091](0091-an-engine-states-the-nulls-order-of-every-index-key.md)

## Context

A partial index carries a predicate: `CREATE INDEX ... WHERE score > 0`. A
declaration states it in `DeclaredIndex.condition` and a read in
`ObservedIndex.condition`. A server keeps a predicate in a spelling of its
own, as it does a CHECK, and some add a predicate the declaration never
wrote.

The host compares the two predicates by its shared rules for CHECK
expressions, without case, spacing and redundant parentheses, and a built-in
target may give it a rule of its own for the spelling. A provider gives it
none. Spanner holds `score > 0` as `(score > '0'::bigint)`, which those rules
read as another predicate, and a unique index carries an implicit filter
`<key> IS NOT NULL` in `information_schema.indexes.filter`, in the same
column a user's filter of a non-unique index uses. So a partial index never
converges on the provider path. The Spanner provider refuses a declared
predicate and a read of a filtered index rather than plan a rebuild on every
run, and Ptah's built-in Spanner engine at d59a3b63d drops a user's filter on
read, so an export replays an unfiltered index.

## Decision

### Preparation states the declared predicate as the server holds it

Under the two-way feature `schema.index-predicates`, table preparation
states the predicate of each declared index in the new field
`PreparedIndex.condition`: the declaration's predicate as the engine would
hold it, in the spelling its read states `ObservedIndex.condition` in,
without the `WHERE` keyword, empty exactly when the declaration states none.
A read under the feature states a held index's predicate in that spelling and
leaves out what the engine adds by itself, such as a unique index's implicit
filter. The host compares the two exactly, so a predicate the server rewrote
converges and a changed one plans a rebuild of the index.

The provider and the engine list the feature, and the engine needs
`schema.observed-indexes` and `schema.prepare-tables`. `PreparedTable.indexes`
holds one entry for each declared index under any of `schema.index-methods`,
`schema.index-nulls-order` and this feature; an entry states a predicate
only under this one.

The prepared predicate serves the comparison alone. The declaration keeps
the predicate its source wrote, so a plan creates the index as it was
declared.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's,
without observed indexes or without table preparation. Preparation validation
refuses a predicate without negotiation and, with it, a predicate for an
index the declaration states none for, none for one it states one for, and a
predicate with surrounding white space. Inspection refuses, under the feature,
a held predicate with surrounding white space.

## Alternatives considered

**An equivalence call, as for CHECKs (ADR 0090).** `CompareChecks` answers
whether two texts are one constraint because a probe of a CHECK is costly and
a CHECK is compared in place. The predicate of an index is compared to choose
between keeping and rebuilding it, and the preparation already states the
declared index's kind and key order in the server's spelling; one more field
there costs no call.

**A host rule for each engine's spelling.** The host would collect one
exception per engine, which the provider extraction removes.

**Leave the predicate out of the comparison.** A changed predicate would then
plan nothing, which hides a change the user made.

## Consequences

- A partial index converges on a provider that states predicates, and a
  changed predicate plans a rebuild of the index.
- A provider that lists the feature owns the spelling of both sides: a wrong
  spelling plans a rebuild on every run or hides a change.
- A host of an earlier contract compares predicates as before.

## Acceptance evidence

- SDK unit tests: `TestIndexPredicates_HappyPath`,
  `TestPreparedIndexPredicates_FailurePath`,
  `TestInspectedIndexPredicates_FailurePath` and
  `TestIndexPredicatesDescription_FailurePath` in `provider-sdk/provider`.
- The host adoption compares the prepared predicate of each declared index
  with the predicate a read reports.
