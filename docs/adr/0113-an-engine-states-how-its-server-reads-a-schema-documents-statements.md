# ADR 0113: An engine states how its server reads a schema document's statements

Status: Proposed

Contract: 25.33

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204);
[stokaro/ptah#5701](https://github.com/stokaro/ptah/issues/5701) (Spanner
register S88)

Relates to: [ADR 0094](0094-an-engine-claims-a-table-clause-of-the-shared-sql-grammar.md)

## Context

The host reads the SQL schema documents of an engine in the FRAGMENTS mode with
the shared grammar, and runs them as the server would: a statement the server
refuses is refused, and one the server skips changes nothing. Where servers
differ, the host reads the answer from the engine's descriptor. One such
answer is what `DROP VIEW IF EXISTS t` does when `t` is a table. MySQL 26.10
and MariaDB 12.3 do nothing; PostgreSQL, SQL Server and the other built-in
engines refuse the statement as they refuse one without `IF EXISTS`. The
descriptors of MySQL and MariaDB state it
(`Statements.DropViewIfExistsSkipsOtherKinds`).

An engine a provider serves has no descriptor, so the host refuses the
statement for every such engine:
`DROP VIEW IF EXISTS t: the schema declares t as another kind of object`. The
servers disagree. The Spanner emulator behind PGAdapter 0.56.2 takes the
statement and changes nothing, and Ptah's built-in Spanner engine at
49e1d61ec read the document so. DuckDB 2.0.0 refuses it
(`Existing object "t" is of type Table`). The Spanner provider's test
`TestHostReadsADroppedViewAsTheServerEndsE2E` skips on the host's refusal.

## Decision

### A FRAGMENTS engine states the fact under a two-way feature

`source.sql-statement-facts` adds
`SQLSchemaDeclaration.drop_view_if_exists_skips_other_kinds`. True states that
the engine's server does nothing for `DROP VIEW IF EXISTS` of a name another
kind of object holds, and the host's reading leaves the object as it is.
False, the zero value, states that the server refuses the statement, and the
reading refuses it too. The provider lists the two-way feature, and so does
each engine that states facts. A host that does not list it reads every fact
as false, which is the answer that accepts nothing a server could refuse.

The fact describes the shared grammar's reading, so only an engine in the
FRAGMENTS mode states it. An engine in the DOCUMENT mode reads every statement
with its own grammar.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's or
in the DOCUMENT mode, and a fact stated by an engine that does not list the
feature. `NegotiatedDescription` clears the facts for a host that does not
list it.

## Alternatives considered

**Keep the refusal for every provider engine.** It is never looser than a
server, and it is what the host does today. It refuses a Spanner document the
emulator applies and the built-in engine read, so a schema that ran on the
server cannot be read back.

**Accept the statement for every provider engine.** It reads the Spanner
document, and it accepts a DuckDB document DuckDB refuses. A reading that takes
what the server refuses lets a mistake through to apply time, which Ptah's
compatibility policy rules out.

**Carry the descriptor's whole `Statements` record.** The descriptor states
other reading facts: whether a second CREATE ROLE is refused, whether views
block a DROP TABLE, whether relations share one namespace, the language of a
DO block. No provider has asked for them yet, and each is a promise the SDK
would have to validate. Each joins the declaration as a field of its own when
an engine needs it.

**An opt-out for views that block a drop.** For an engine without a
descriptor the reading refuses `DROP VIEW a` while view `c` reads `a`,
matching the Spanner emulator (S89). DuckDB 2.0.0 takes that drop but then
refuses to create `c`, so the refusal loses nothing DuckDB could apply, and
no engine needs the opposite answer. A fact such as `views_do_not_block_drops`
waits for one that does.

## Consequences

- A Spanner document that drops a view under a table's name with `IF EXISTS`
  reads as the emulator runs it, once the Spanner provider states the fact.
- DuckDB and any engine that states nothing keep the refusal.
- The host reads the fact from the provider's description; a built-in engine
  keeps reading its descriptor.

## Acceptance evidence

- SDK unit tests: `TestSQLStatementFacts_HappyPath` and
  `TestSQLStatementFacts_FailurePath` in `provider-sdk/provider`.
- The host adoption reads the document through a fixture engine that states the
  fact, and refuses it through one that does not.
