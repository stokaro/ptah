# ADR 0090: A provider decides whether two checks are one

Status: Proposed

Contract: 25.28

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0088](0088-a-read-reports-the-foreign-keys-and-checks-a-table-holds.md)

## Context

A server rarely keeps a CHECK as it was written. It parses the expression and
prints it back, with literals cast, lists rewritten and parentheses added. A
comparison of the declared text with the held text therefore finds a
difference where there is none, and plans a drop and an add of a constraint
nobody changed, on every run.

The host resolves this for PostgreSQL and SQL Server by asking the connected
server to rewrite each declaration the way it rewrote the held one, with a
temporary probe table. For every other engine it folds the two texts by
PostgreSQL's rules: it ignores case and spaces, drops literal casts and reads
`= ANY (ARRAY[...])` as `IN`.

Ptah's built-in Spanner engine at d59a3b63d uses those rules, and they do not
fit Spanner. Measured on the PGAdapter emulator v0.56.2, a field declared with
`check="score >= 0"` is held as `(score >= '0'::bigint)`, and the second
`schema apply` of an unchanged schema plans

```sql
ALTER TABLE "posts" DROP CONSTRAINT "posts_score_check";
ALTER TABLE "posts" ADD CONSTRAINT "posts_score_check" CHECK (score >= 0);
```

again and again: the schema never converges. A probe table is no answer on
Spanner, where every schema change is a long-running operation outside any
transaction.

## Decision

### The engine's owner answers

`CompareChecks` asks the owner of an engine whether each declared CHECK
expression and the CHECK a held table keeps under the same name are one
constraint. The request carries, for each held table, its subject, the table
as held, with its columns, which the expressions read, and its constraints,
among which each named CHECK is, and the pairs: the name of a held CHECK and
the declared expression, without the `CHECK` keyword and its enclosing
parentheses. The reply answers each pair with `equivalent`, in the request's
order. The owner decides from the request alone; it opens no session and runs
nothing, so it may parse, normalize or compare as its engine requires.

A host that asks keeps a held CHECK the owner calls equivalent and replaces
one it does not, and compares no CHECK of that engine by its text.

### The feature is the provider's alone

`schema.check-equivalence` enables the call. The provider and each engine that
compares checks list it. The call is new and adds nothing to an existing
message, so a host that does not know it never makes it, and the feature is
not two-way.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's.
`ValidateCompareChecksRequest` refuses a request for an engine that does not
compare checks, invalid identifier facts, a table without its held state or
bound to another subject, a held table that carries captured features, a table
without pairs, a constraint named twice, a pair that names no CHECK the table
holds with an expression, a blank declared expression and two tables of one
subject. `ValidateComparedChecks` refuses an incomplete reply and one that
leaves out, reorders or renames a table or a pair.

## Alternatives considered

**Ask the provider to spell the declaration as its server would keep it.**
This is the host's PostgreSQL probe moved to the provider: the provider
returns a text, and the host compares it with the held text. PostgreSQL showed
that one rewrite is not always enough, since a server does not always print a
CHECK in a form that reads back to itself, and the host needed a second pass.
A spelling also leaves the comparison of two texts to the host's rules. An
answer of one bit leaves nothing for the host to fold.

**Fold the texts by more host rules.** A rule that drops the quotes around a
number fixes `'0'::bigint` and breaks a comparison with a text column. Each
engine's rewrites are its own, and the host would collect one exception per
engine.

**Decide on a live session.** The owner could probe its server, as the host
does for PostgreSQL. Some engines cannot run a probe cheaply, and an offline
call serves a comparison that has no session at hand. An owner that needs its
server can still keep what it learns from its reads in the held table it
reports.

## Consequences

- A CHECK on a provider's engine converges when the engine's owner says the
  held form is the declared one, however the server rewrote it.
- A provider that lists the feature owns every CHECK comparison of its
  engine, and a wrong answer plans a replacement or hides a change.
- A host of an earlier contract compares the texts as before.

## Acceptance evidence

- SDK unit tests: `TestCheckEquivalence_HappyPath`,
  `TestCompareChecksRequest_FailurePath`, `TestComparedChecks_FailurePath` and
  `TestCheckEquivalenceDescription_FailurePath` in `provider-sdk/provider`,
  and the CompareChecks conformance tests in `provider-sdk/runtime`.
- The host adoption asks a provider that compares checks about every CHECK it
  compares on the provider's engine.
