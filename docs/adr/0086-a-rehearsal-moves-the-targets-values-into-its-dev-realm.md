# ADR 0086: A rehearsal moves the target's values into its dev realm

Status: Proposed

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0046](0046-database-context-is-bound-before-provider-dependencies-are-used.md),
[ADR 0065](0065-a-filtered-read-reports-the-objects-of-its-database.md)

## Context

Before `schema apply --dev-url` runs a provider's plan on the dev database,
it recreates the target's current schema there. The baseline is the target's
read, converted to desired values, compared with a read of the dev database,
and planned. When the dev URL names the target, or a database the target's
server holds, the YDB provider gives the rehearsal a dev realm: a directory
of the target's database with a context of its own, `database` the realm and
`source_database` the target's database.

A provider binds each value it reads to the session's database (ADR 0046),
and the YDB provider names each object it reads by that database. So the
baseline's values are bound to the target, and the dev realm's comparison
receives values of two databases. The provider refuses it: `YDB comparison has
conflicting database contexts`, which the host shows as `provider compare
models failed (InvalidArgument)`. ADR 0046 allows no repair: a value bound to
another context refuses, and rebinding requires decoding the authored source
for the new target. A baseline has no authored source. It is a read.

Every rehearsal in a realm of a database that holds a provider's object
fails this way. On a YDB server with `EnableResourcePools` every database
holds the resource pool `default`, so every rehearsal there fails, whatever the
plan holds. Measured with Ptah master cda568c0d and the YDB provider at
0f9522b1 on YDB 25.1.4.7 and 26.2.1.14: native `schema apply --dev-url` and
`ptah-compat schema apply --dev-url`, from Go, YAML and SQL sources, fail at
the baseline; on a server without resource pools, so does a change to a
directory that holds a topic. The built-in YDB engine at 74de0b65b rehearsed
and applied all of them.

A second limit follows once the values move. A realm reads no object of a
database kind (ADR 0065): the pools it would see are its database's. The
baseline asks the realm to hold the pool `default`, the realm cannot say
whether it does, and the comparison refuses as incomplete.

## Decision

### A desired value moves between two contexts of one root

`schemaext.ValueContextRequest.From` names the context a desired value is
bound to when the batch moves it to `Context`. The host's binding of a
provider's values clones each value bound to `From`, binds it to `Context`,
and normalizes it through the provider as any binding does, so the provider
computes its dependencies again. An owner that names `From`'s database names
its object relative to the database instead, as a session names an object of
its own database. The payload is not changed.

A move is refused unless both contexts name the same `source_database`. A
desired value reads its authored references against that root, so they keep
their meaning, and a relative owner names the same object in the database the
value moves to. An observation is never moved: it describes the database it
was read from. A value bound to any third context is refused as before.

### The baseline moves the target's values, and leaves out what the realm does not read

The rehearsal resolves the target's address and the dev database's, and when
the two contexts differ and share a root, it moves the converted target's
values to the dev database. It then leaves out every object the dev read
makes no claim about, with the coverage records of its subject: the realm
can neither hold nor check it. A plan that changes such an object is planned
again for the dev database, which cannot plan the change either, so the two
plans differ and the rehearsal refuses the apply. Nothing reaches the target
unrehearsed.

Where the contexts are equal, or there is none, or the roots differ, the
baseline keeps the target's values bound as the read bound them.

## Alternatives considered

**Export the target's read as a source and decode it for the dev database.**
That is what ADR 0046 asks for. An export can refuse what it cannot write back
unchanged, so a rehearsal would fail on a read the plan never touches, and the
source formats differ in what they carry. Rejected.

**Let the provider accept values of the realm's database.** The provider
would have to treat objects of one database as the realm's, and the SDK
refuses a planning record bound to another database. Rejected.

**Skip the baseline's provider values.** The realm would lack the target's
topics, and the dev database's plan would create them where the target's
does not. Rejected.

## Consequences

- `schema apply --dev-url` and `ptah-compat schema apply --dev-url` rehearse
  in a YDB dev realm again, with resource pools on or off.
- A dev database of another root, a database of another name, gets the
  target's values as they are, and a provider refuses the comparison where
  the values matter.
- The change is the host's. The contract and the providers are unchanged.

## Acceptance evidence

- The engine refuses a move of an observation, a move between two roots and
  an incomplete `From` before any service runs.
- Through the value-context fixture, a moved value is bound to the new
  context and its dependency names the new database. A move between two roots,
  and of a value bound to neither context, refuses and leaves the value as it
  was.
- Through the YDB provider on 25.1.4.7 and 26.2.1.14, a rehearsal in a dev
  realm applies with `EnableResourcePools` on, and a change to a directory
  that holds a topic and a coordination node applies.

## Amendments

- `schema diff`, native and compatible, compares two reads with the same
  move. When the `--to` side was read from a database and the `--from` side
  from another context of its root, a dev realm of it or the database a
  realm belongs to, the `--to` side's values move to the `--from` side's
  context before the comparison. A `--from` file created in a dev realm of
  the `--to` database is read back in that realm, so the same holds for it.
  When either side was read in a dev realm, both sides leave out the
  permissions on the database root: the realm's root does not stand in for
  them, as the baseline leaves them out too.
  The built-in YDB engine at 74de0b65b planned these diffs; the provider
  refused them as `conflicting database contexts` without the move.
