# ADR 0099: An engine states how it builds an enum column

Status: Proposed

Contract: 25.31

Deciders: Ptah maintainers

Issue: [stokaro/ptah#5658](https://github.com/stokaro/ptah/issues/5658), for
[stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0088](0088-a-read-reports-the-foreign-keys-and-checks-a-table-holds.md)

## Context

A source declares an enum column two ways: a column typed `ENUM` with its
values, such as a Go field with `enum="a,b,c"`, or a column typed with the
name of an enum the source declares. The host builds such a column as the
target's descriptor says (`descriptor.Preparation.Enums`): a type of its own
that columns name, as on PostgreSQL; a column type that lists the values, as
on MySQL; or a text column with a CHECK, as on SQLite. The comparison and the
renderer read the same answer, so a render builds the column a read reports.

An engine a provider serves has no descriptor, so the host keeps the enum a
type of its own for it. The deleted built-in Spanner engine had a descriptor
that stated a `TEXT` column with a CHECK. The Spanner provider's test
`TestHostCreatesChecksE2E` creates the column as

```sql
"kind" TEXT NOT NULL DEFAULT 'a' CONSTRAINT "authors_kind_check" CHECK (kind IN ('a', 'b', 'c'))
```

and passes while that descriptor is registered. On a host without it the
comparison plans `SchemaDiff.EnumsAdded`, and the provider path refuses the
plan (Spanner register S84).

The three provider engines build enums three ways:

- Spanner's PostgreSQL interface has no enum type; the built-in used `TEXT`
  with a CHECK.
- DuckDB 1.4.1 accepts the column type `enum('a','b','c')`, reads it back as
  `ENUM('a', 'b', 'c')`, keeps no named type for it in `duckdb_types()`, and
  refuses `'d'` with `Conversion Error: Could not convert string 'd' to
  UINT8`.
- YDB has no enum. The built-in YDB engine at 74de0b65b refused an enum by the
  capability `enum_custom_type`.

## Decision

### The engine states one form

`Engine.enum_columns` states how the engine builds an enum column, under the
two-way feature `schema.enum-columns`. The provider and the engine list it,
and an engine states the field exactly when it lists the feature. The form is
one of:

- `VALUES_TYPE`: the column's type lists the values, `enum('a','b','c')`, the
  values in declaration order. This is the form the host already writes for
  MySQL's comparison. The engine compares it with the type a read reports, in
  its own spelling.
- `CHECK`: the column is of `check_type` and holds the values under its CHECK,
  `kind IN ('a', 'b', 'c')`, with the column named bare. A column with a CHECK
  of its own holds `(<own>) AND kind IN (...)`. This is the form SQLite's
  descriptor states.
- `REFUSED`: the engine has no enum.

Spanner states `CHECK` with `TEXT`, DuckDB `VALUES_TYPE`, and YDB `REFUSED`.

### The host builds the column before anything reads it

The host builds every enum column of a declaration in the engine's form
before it validates, compares, plans or renders it, and keeps no enum type for
the engine. The provider receives an ordinary column, a comparison sees the
column a read reports, and a render and a plan create the same column. Under
`REFUSED` the host refuses an enum column by its table and column, and a
declared enum by its name, before it calls the provider.

A host that does not list the feature keeps the enum a type of its own, as it
does for an engine of contract 25.30. The built-in engines keep their
descriptors.

### What the SDK enforces

`ValidateDescription` refuses the field without the feature and the feature
without the field, the engine's feature without the provider's, a form left
unspecified, a `CHECK` whose column type is empty, spans lines or has white
space around it, and a column type beside another form. A host that does not
list the feature receives neither.

## Alternatives considered

**Carry the enum as a named type on the common-object wire.** DuckDB also has
`CREATE TYPE ... AS ENUM`. A new common kind would let the host read, compare
and create named enum types through a provider. It needs a read of types, a
change for each value added or removed, and an order between the type and its
columns, and neither Spanner nor YDB can use it. A later minor can add it for
an engine that wants named types; `VALUES_TYPE` covers DuckDB's columns now.

**Derive the form from capabilities.** The capabilities `enum_inline_column`
and `enum_custom_type` exist, and a provider states offline capabilities
under `engine.default-capabilities`. They do not say which column type holds
a CHECK, they describe a release line rather than the engine's syntax, and a
host has none before a session for a provider that states no defaults.

**Send the enum and let the provider build the column.** The provider could
receive the enum and decide in `PrepareTables` or in its planner. The host
compares before either runs, and would still find an enum type to create, so
the plan would keep `EnumsAdded`.

## Consequences

- A Spanner, DuckDB or YDB declaration with an enum column compares, plans and
  renders as the built-in engines' do, or is refused by name before any call.
- The `CHECK` form names the column bare, as SQLite's and the built-in
  Spanner's did. A column whose name needs quoting in the engine fails there;
  an engine that needs a quoted name can ask for it in a later minor.
- A provider that states `VALUES_TYPE` compares `enum('a','b','c')` with its
  own spelling of the type, and the host's text does not change.

## Acceptance evidence

- SDK unit tests: `TestEnumColumns_HappyPath` and `TestEnumColumns_FailurePath`
  in `provider-sdk/provider`.
- The conformance suite's memory provider states the `CHECK` form, and
  `TestCheck_WireFailurePath` reports one that describes it to a host that did
  not list the feature.
- The host adoption builds the column in each form through the test provider,
  and refuses it by table and column under `REFUSED`.
