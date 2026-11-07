# ADR 0118: A view query comparison receives the declared columns and may leave a pair undecided

Status: Proposed

Contract: 25.37

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(Spanner register S91, contract gaps item 11)

Amends: [ADR 0108](0108-a-provider-decides-whether-two-view-queries-are-one.md)

## Context

Under `schema.view-query-equivalence` (ADR 0108) the owner of an engine
answers every view query pair, and the host compares no view query of that
engine by its text. That suits an engine whose stored query differs from the
declaration in ways the owner can read from the two texts. It does not suit an
engine whose server rewrites the query with facts the request does not carry.

Spanner is one. Measured on the Spanner emulator with 34 view shapes over two
tables (Spanner register, section "View bodies"), the server stores `SELECT *`
with every column named and qualifies the columns of a `UNION`
(`SELECT t.id FROM t UNION ALL SELECT u.id FROM u`), beside literal casts,
parentheses, `IN` written as `= ANY (ARRAY[...])` and more.

Through the host, 26 shapes replan `CREATE OR REPLACE VIEW` on every plan. The other 8 converge
because the host's text fold expands `*` with the declared columns of the
relation and strips the qualifiers a server adds. A provider comparer that
reads only the two texts, measured by listing the feature with a comparer
answering `ViewQuery(declared) == held`, replans the `SELECT *` and `UNION`
views too: the provider cannot expand `*` without the columns, and it has no
answer that hands a pair back to the fold. So the provider could not adopt the
feature without losing convergence it has today.

## Decision

`schema.view-query-context` adds two fields to the call of ADR 0108.

`CompareViewQueriesRequest.declared_columns` carries the columns of every
table the declaration declares, each named by its table's identity, with its
declared type, in the order the declaration lists them. They are the columns
the host's fold expands a `*` with, and they say which relation a column a
server qualifies belongs to.

`ComparedViewQuery.undecided` lets the owner leave a pair to the host. The
host then compares the two queries by its own reading of their texts, as it
does for an engine that does not compare view queries: the same fold, the same
`*` expansion, the same spelling probe where the target has one. An undecided
answer is not equivalent.

A two-way feature: the host lists it, and so do the provider and each engine
that compares view queries under it, beside `schema.view-query-equivalence`.
A host that does not list it sends no columns, and a provider must answer
every pair. The SDK refuses declared columns or an undecided answer without
the feature on both sides, a column of anything but a table, a column without
a name or a type, a table's column named twice, an undecided answer that is
also equivalent, and the engine's feature without the provider's or without
view query equivalence.

## Alternatives considered

**Send the declared query after the host's `*` expansion.** The host already
expands `*` for a single-relation select. The expanded text would serve a
`SELECT *` view and nothing else: the host recognizes one shape, and a provider
that also strips casts or reads `UNION` qualifiers needs the relations' columns
anyway. The columns are the general form of the same fact.

**Add only the undecided answer.** A provider could answer the shapes it
decides and leave `SELECT *` and `UNION` views to the fold. A view that
combines a `*` with a shape only the provider reads, such as
`SELECT * FROM t WHERE id IN (1, 2)`, would then be decided by neither.

**Let the owner create the declared view on a session and read it back.** ADR
0108 rejected deciding on a live session: some engines create a view outside
any transaction, and an offline call also serves a file-to-file comparison.

## Consequences

- A provider whose server rewrites view queries can adopt view query
  equivalence and keep the convergence the host's fold gives it.
- A request carries the columns of every declared table, once per comparison,
  whatever the views read. A schema with many tables and few views sends more
  than the views need; the alternative is a host that parses each query to
  find its relations, which is the provider's job here.
- The host's reading of a text stays the fallback for the pairs an owner
  leaves undecided, so its fold remains maintained for provider engines too.

## Acceptance evidence

- SDK: `TestViewQueryContext_HappyPath`, `TestViewQueryContext_FailurePath`
  and `TestViewQueryContextDescription_FailurePath` in `provider-sdk/provider`.
- Host adoption: the host lists the feature, sends the declared columns, and
  compares an undecided pair by its fold, through a provider process.
- Spanner adoption: the view shapes of the Spanner register measured through
  the host before and after.
