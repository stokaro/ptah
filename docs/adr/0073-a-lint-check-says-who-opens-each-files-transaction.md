# ADR 0073: A lint check says who opens each file's transaction

Status: Proposed

Contract: 25.23

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4956](https://github.com/stokaro/ptah/issues/4956)

Relates to: [ADR 0056](0056-a-comparison-service-counts-the-objects-it-compares.md)

## Context

A rule about transaction control has to know who opens the transaction a
migration file runs in. A `BEGIN` nests inside the transaction of a migrator
that wraps each file. Under a tool that opens none, such as golang-migrate, the
same `BEGIN` is the file's only transaction. `lintext.CheckRequest.TransactionOwner`
states this, and `TX201` reads it: where the file opens its own transaction,
the rule reads a PostgreSQL up file with the reading the migrator applies it
with, and judges every other file as one the migrator wraps.

The provider wire does not carry the field. A provider-served engine therefore
judges every file as one its migrator wraps. No provider publishes a rule that
reads the owner, so nothing reports differently yet. A provider rule that does,
such as a rule that reports an explicit transaction block in YDB, would report
a defect in a file that is correct for the tool that applies it.

## Decision

### The request carries the owner

`CheckLintRequest.transaction_owner` carries a `LintTransactionOwner`.
`LINT_TRANSACTION_OWNER_MIGRATOR` is the zero value: a migrator that opens a
transaction around each file, unless the file opts out. It is what a host that
does not know the tool sends and what a request from an earlier minor means, so
a provider reads one meaning from an absent field and a zero one.
`LINT_TRANSACTION_OWNER_FILE` is a tool that opens no transaction around a file.

The field applies to the whole check. A run lints one directory, and one tool
applies it. `LintFile.no_transaction` stays what it is: one file's opt-out from
a migrator that wraps the others.

### The field is negotiated

The two-way feature `lint.transaction-owner` switches the field on. The provider
lists it beside `lint.migrations`, and so does each engine whose rules read the
field. A request names `LINT_TRANSACTION_OWNER_FILE` only when the engine and
the host list the feature. Both transport boundaries refuse the value otherwise,
and refuse an owner the contract does not name.

A provider whose rules do not read the field has nothing to honor, so it does
not list the feature. It keeps judging every file as one its migrator wraps,
which is what it does for a host of an earlier minor.

### Older peers

A host built before this contract does not list the feature and sends no owner,
and the provider sees the zero value. A provider built before this contract does
not list the feature, so the host sends no owner and has nothing to refuse.

## Alternatives considered

**An unnegotiated field.** A provider that does not read the field would ignore
it, and the host could not tell. The contract uses an addition a provider would
have to honor only when both sides list its feature. Rejected.

**A string, as `CheckLintRequest.compatibility` is.** The values name a closed
set the SDK can check, and an owner the SDK refuses is a defect that reaches no
rule. Rejected.

**An owner per file.** `lintext.CheckRequest` states one owner per run, because
one tool applies a directory. A per-file value could only repeat it. Rejected.

## Consequences

- Contract 25.23 adds a field, an enum and a two-way feature.
- The SDK release comes first. The host adopts it in its own change, against
  the published tag: it lists the feature and sets the field in the adapter
  that builds the wire request.
- A rule in a provider can then judge a golang-migrate directory by the tool
  that applies it.

## Acceptance evidence

- SDK tests accept the migrator with or without the feature and the file with
  it. They refuse the file without it and an owner the contract does not name,
  and an engine that lists the feature without its lint service or the
  provider's feature.
- Both transport boundaries refuse an owner a host did not negotiate, and the
  owner's service is not called.
- An SDK-only proof provider, started as a process, judges a file's `BEGIN` by
  the owner the check names. For a host that does not list the feature it judges
  the file as one its migrator wraps, and that host cannot name another owner.
