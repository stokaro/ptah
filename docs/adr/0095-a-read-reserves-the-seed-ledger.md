# ADR 0095: A read reserves the seed ledger

Status: Proposed

Contract: 25.29

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0028](0028-a-provider-owns-how-rows-are-read-written-and-seeded.md)

## Context

`ptah seed` records each seed file it applies in a ledger, `schema_seeds`
for the built-in engines and a ledger of the provider's own layout for an
engine that offers `data.seeds`. A provider reports the storage it keeps for
itself as reserved objects of a read, each with a purpose, and the host
leaves them out of every comparison and refuses a declaration that takes
their names. The purposes are the migration history, the lock, the log and
the tags. None is the seed ledger.

So after a seed run, `schema apply` of a declaration without the ledger plans
`DROP TABLE "schema_seeds"`. That holds for a provider, measured with the
Spanner provider on the PGAdapter emulator v0.56.2, and for the built-in
engines, whose readers leave out the migration history, its log and the tags
table by name and read `schema_seeds` as a table no declaration has: measured
on SQLite and PostgreSQL 18, and on the built-in Spanner engine at d59a3b63d.
Applying the plan loses the record of every seed run, and the next run
applies every file again.

## Decision

### SEEDS is a purpose of reserved storage

`ReservedObject.Purpose` gains `SEEDS`, under the two-way feature
`schema.reserved-seeds`. A read reports the engine's seed ledger with it. The
provider and each engine whose seed service keeps a ledger in the database
list the feature, beside `data.seeds`. The host leaves the ledger out of
every comparison, as it does the migration history, and refuses a
declaration of a table of its name.

### The built-in engines leave it out as they leave out the history

`schema_seeds` joins the tables Ptah's own bookkeeping writes under their
default names, beside `schema_migrations`, its log and
`ptah_migration_tags`. Every built-in reader leaves it out, and a cleanup that
reports the bookkeeping tables it removes names it. A plan of a built-in
engine and a plan through a provider then agree: neither drops the ledger.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's or
without the engine's seed service. Inspection refuses a reservation of purpose
`SEEDS` in a read that did not negotiate the feature.

## Alternatives considered

**Leave the ledger out by its name on the host.** The host could drop a table
named `schema_seeds` from every provider read. A provider owns its ledger's
layout and may name it otherwise, and a provider's table of that name could
be the user's.

**Leave it as it is and document a `--exclude`.** Every user who seeds would
have to add the exclusion, and the default would still lose the ledger.

## Consequences

- A plan never drops the seed ledger, on a built-in engine or through a
  provider that reports it.
- A declaration of a table named `schema_seeds` is no longer compared with the
  table on a built-in engine: the reader leaves it out, as it leaves out
  `schema_migrations`.
- A provider of an earlier contract keeps its ledger in the comparison.

## Acceptance evidence

- SDK unit tests: `TestReservedSeeds_HappyPath` and
  `TestReservedSeeds_FailurePath` in `provider-sdk/provider`.
- The conformance suite's memory provider reserves its ledger, and
  `TestCheck_FailurePath` reports `live.inspect` for one that reserves it for
  a host that did not list the feature.
- The host adoption leaves out a provider's reserved ledger, and an
  end-to-end test seeds and then plans on built-in engines without a drop of
  the ledger.
