# ADR 0088: A read reports the foreign keys and checks a table holds

Status: Proposed

Contract: 25.28

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0051](0051-inspected-primary-keys-retain-their-common-order.md),
[ADR 0052](0052-inspected-indexes-reach-the-host-and-index-changes-reach-the-table-owner.md)

## Context

A provider's read describes a table through `protocol.Table`: its columns,
its facets, its primary key under `schema.observed-primary-keys` and its
secondary indexes under `schema.observed-indexes`. Nothing carries a FOREIGN
KEY or a CHECK constraint.

Spanner is the next engine to leave the host. Ptah's built-in Spanner engine at
d59a3b63d reads both, measured on the PGAdapter emulator v0.56.2 against a
schema of two tables with a foreign key and two CHECKs. `ptah db read` writes:

```sql
CONSTRAINT "posts_score_check" CHECK ((score >= '0'::bigint))
ALTER TABLE "posts" ADD CONSTRAINT "fk_posts_author" FOREIGN KEY ("author_id") REFERENCES "authors"("id");
```

The Spanner provider (stokaro/ptah-spanner-plugin#3) can read neither through
the contract. Reporting the table without them would make the read look
complete while it leaves out the constraints, and a plan would add each
declared constraint again. So the provider refuses, with a reason, a read of a
database that holds a foreign key or a CHECK.

## Decision

### A read lists the table's foreign keys and checks

`Table.constraints` carries the FOREIGN KEY and CHECK constraints a table
holds, as `ObservedConstraint`, the message a planning request already uses
for a held table's constraints. The list is the whole set: a constraint of
those types it does not name is absent. The primary key stays in
`Table.primary_key`. An index the engine builds to enforce a constraint is in
neither list: it is the engine's, and no declaration names it.

### The feature is negotiated

`schema.observed-constraints` is a two-way feature. The provider and the
engine list it, and the host lists it in its host features. The engine needs
`schema.observed-columns`, against whose columns the SDK checks each
constraint. A read whose session did not negotiate it carries no constraint.

### One spelling for the parts the host compares

The host compares a foreign key's columns, referenced table and columns, rules,
MATCH type and deferral with the declaration, with the same rules for every
engine. A foreign key therefore states its rules in the spelling of
`information_schema` (`NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`,
`SET DEFAULT`), its MATCH type as `FULL`, `PARTIAL` or empty for SIMPLE, and
its deferral as a primary key does. A CHECK states its expression as the
server keeps it; whether that text and a declaration are one constraint is the
engine's to say ([ADR 0090](0090-a-provider-decides-whether-two-checks-are-one.md)).

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's or
without observed columns. Inspection validation refuses constraints without
negotiation and, with it, a constraint without a name or with a name the table
uses twice, one bound to another table, a type other than FOREIGN KEY or
CHECK, an unknown or repeated column, the singular `column_name`, index
attributes, a foreign key without its referenced table or with a different
number of referenced columns, a rule or MATCH type in another spelling, a
timing without deferral, a CHECK without its expression and a CHECK with a
foreign key's attributes. A creation operand never carries the list.

## Alternatives considered

**Carry the constraints as provider facets.** The provider could report each
constraint as a facet of its own model and compare it itself. The host would
then hold foreign keys it cannot order a plan by, cannot export to HCL, Go or
YAML as foreign keys, and cannot compare with the declarations every source
writes in the common model. The common catalog already models both.

**Carry UNIQUE constraints too.** A UNIQUE constraint is an index on most
engines and is already reported as one under `schema.observed-indexes`; an
engine that keeps it apart can ask for it in a later minor. Admitting it now
would give a host two places to look for one object.

**Let the host normalize the rules.** The host could accept `cascade` or
`NO_ACTION`. Each engine spells its rules its own way, and only the engine
knows its spellings; one spelling on the wire keeps the comparison the host's
and the reading the provider's.

## Consequences

- A provider can report a table with foreign keys and checks as a complete
  read, and the host compares them as it compares a built-in engine's.
- A provider must leave out an index its engine builds for a foreign key, or
  a host would plan to drop it.
- A host that does not list the feature reads no constraint, and a provider
  that cannot describe one keeps refusing that read.

## Acceptance evidence

- SDK unit tests: `TestObservedConstraints_HappyPath`,
  `TestObservedConstraints_FailurePath`,
  `TestObservedConstraintsDescription_FailurePath` and
  `TestObservedConstraintIsNotACreationOperand` in `provider-sdk/provider`.
- The host adoption reads each constraint into the catalog and compares it.
