# ADR 0094: An engine claims a table clause of the shared SQL grammar

Status: Proposed

Contract: 25.29

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0025](0025-a-provider-declares-the-source-syntax-it-owns.md),
[ADR 0027](0027-a-provider-reads-the-sql-schema-documents-of-its-own-dialect.md),
[ADR 0074](0074-a-provider-claims-the-index-kinds-it-models.md)

## Context

An engine reads its SQL schema documents in one of two modes. In the DOCUMENT
mode the provider reads every document whole (ADR 0027). In the FRAGMENTS
mode, the default, the host reads them with the shared grammar, the one the
PostgreSQL-family readers use, and the engine has no way to read a part of a
statement itself: no code sends a SQL fragment to `DecodeSource`.

Spanner's PostgreSQL interface writes a row deletion policy as a clause of
CREATE TABLE after the column list: `TTL INTERVAL '30 days' ON created_at`.
Ptah's built-in Spanner engine at d59a3b63d reads it through a SQL extension
targeted at Spanner documents, which claims the clause before the shared
grammar reads `TTL` as ClickHouse's raw table option. Through the provider,
which reads the policy in Go annotations, YAML and HCL, the shared grammar
reads the clause into `Table.Overrides`, and the host refuses the declaration
(`the experimental provider contract does not support Table.Overrides`). The
export of a database that holds a policy, `db read` or `schema inspect
--format sql`, writes the clause, so such an export does not replay through
the provider.

Reading the whole document in the DOCUMENT mode would answer it, at the price
of a second SQL grammar in the provider for one clause.

## Decision

### A FRAGMENTS engine claims a clause by its opening words

`SQLSchemaDeclaration.table_clauses` lists the clauses of CREATE TABLE an
engine reads itself. Each `SQLTableClause` names the words the clause opens
with, a selector, and the DESIRED models the clause declares, each a facet of
the table and each a model SQL documents state. The two-way feature
`source.sql-table-clauses` enables it; the provider and the engine list it
beside `source.decode`, and only an engine in the FRAGMENTS mode claims a
clause.

In a SQL document of the engine, the host reads a clause that opens with a
claim's words among the options after the column list, up to the next
top-level comma or the end of the statement. It sends the clause to
`DecodeSource` as a SQL fragment that carries the claim, whose content is the
clause's text with its opening words, whose binding is the table and whose
subject is the table. The engine returns the facets the clause declares.

A document of the engine states each claimed model in full for every table it
declares: a table without the clause has none. The host claims that coverage
for every SQL document of the engine, so removing the clause from a document
plans the facet's removal.

### Other engines read the grammar as before

A claim applies to the documents of its engine only. ClickHouse keeps reading
`TTL <expression>` as its raw table option, and a document of no engine reads
it as before. A built-in engine's SQL extension keeps the place it has: an
owner claims its option before the shared grammar reads it.

### What the SDK enforces

`ValidateDescription` refuses a claim without the features or in the DOCUMENT
mode, a claim without words, selector or models, a word that is not one ASCII
upper-case word, two claims that open with one word, and a model that is not
a desired model of the engine with SQL among its source formats. A
`DecodeSource` request refuses a table-clause fragment outside a SQL document
or beside another claim, one whose claim or selector changed, one whose
binding is not a table or whose subject is not that table, one whose content
does not open with the claim's words, and one whose models are outside the
document's scope. A reply refuses a value of such a fragment that is not a
facet of its table or not of the claim's models. `SQLTableClauseClaim`
matches a clause's text to a claim as the host does.

## Alternatives considered

**The DOCUMENT mode.** The provider would parse CREATE TABLE, CREATE INDEX,
views, sequences and comments in the PostgreSQL dialect again, to read one
clause. Every later difference between its grammar and the shared one would
be a defect in one of them.

**A host rule for Spanner's clause.** The host would know a provider's
syntax, which is what the provider extraction removes.

**Claim any table option.** A claim by a leading word is enough for the
clauses measured, and it keeps the claim data: the host never runs the
engine's grammar, and the engine never sees the rest of the statement. A
clause the words cannot single out stays the DOCUMENT mode's.

## Consequences

- A Spanner SQL document with a row deletion policy plans through the
  provider, and its export replays.
- A provider that claims a clause decodes every claimed clause of every
  document of its engine; a clause it cannot read is refused with its
  position.
- A host of an earlier contract reads the clause with the shared grammar and
  refuses it as before.

## Acceptance evidence

- SDK unit tests: `TestSQLTableClauses_HappyPath`,
  `TestSQLTableClauseDeclarations_FailurePath`,
  `TestSQLTableClauseFragments_FailurePath` and
  `TestSQLTableClauseValues_FailurePath` in `provider-sdk/provider`.
- The host adoption captures a claimed clause of a provider's SQL document and
  plans the facet it decodes, and a ClickHouse document keeps its TTL.
