# ADR 0125: A provider keeps the record of repeatable migrations

Status: Proposed

Contract: 25.44

Deciders: Ptah maintainers

Issue: [stokaro/ptah#6153](https://github.com/stokaro/ptah/issues/6153)

## Context

A repeatable migration, `R__<identity>.sql`, runs again whenever its content
changes. The native migrator keeps one row per identity in
`<revision table>_repeatable`: the digest and the bytes of the content that
last applied, and the state, digest, order, transaction and run of the last
attempt. On the built-in engines it reads and writes that table with SQL. The
migrator uses the table in a fixed set of ways:

- it reads every row, without bodies, to plan a run, to report a status and to
  refuse a rollback;
- it creates the table before the run's first statement, and refuses one
  another role owns;
- it claims an identity before the body runs: an `UPDATE` that matches only
  while the row names the run it read, or an `INSERT` the key refuses when
  another run inserted first, settled by reading the row's run back;
- it records the content as applied in the body's own transaction when there
  is one, and a failure on the session;
- it reads one body when a drift check replays a file that changed or is gone;
- `ptah migrations repair --repeatable` deletes one row under the migration
  lock;
- the migration log names the identity of each attempt.

An engine reached through an executable provider exposes no SQL connection the
host can run those statements on. The YDB provider keeps the revision table
with `sessions.history`, but the protocol has no operation for this record, so
the host refuses a directory holding a repeatable migration on every
provider-backed engine.

## Decision

`sessions.repeatable-record` adds four calls, each as narrow as one of the
migrator's uses:

- `ReadRepeatables` returns every row without its body, as one complete
  snapshot bounded by `MaxRepeatableRows`, and creates nothing.
- `PrepareRepeatables` creates a missing record outside any transaction,
  checks the ownership and layout of an existing one, and never upgrades
  another layout.
- `ReadRepeatableBody` returns the bytes kept for one identity, with whether
  the row exists and whether it keeps a body. An empty body is kept.
- `MutateRepeatable` writes one row atomically: a claim, a completion, a
  failure or a removal.

A claim states what the host read: `insert` when it read no row, and
`expected_run_id` otherwise. The provider applies it only when the row still
matches, so the run ID decides between two runs, as it does on the built-in
engines. A completion and a failure carry their run and apply only to the row
it claimed. Any other row is a `CONFLICT` that names the run holding it. Only a
completion may carry a transaction, the one its body ran in, so the record and
the work commit together. An `UNKNOWN` outcome permits no replay; the host
settles it by reading the row back.

Concurrent runs are settled by the same rules the built-in engines follow:

- the provider checks a claim's precondition and writes the row in one atomic
  step, so of N concurrent claims against one precondition exactly one is
  `APPLIED`, and each of the others is a `CONFLICT` that names the winner;
- a claim never expires, and the provider never changes a `RUNNING` row on its
  own;
- a `RUNNING` row a crashed run left behind is taken over by a claim whose
  `expected_run_id` names that run;
- the host's migration lock, not the provider, keeps a live run from being
  taken over, since the provider cannot tell a live run from a crashed one.

A completion carries the claimed digest: its `checksum` equals the row's
`attempt_checksum`, and the provider checks it. A completion of another digest
is `REFUSED`, not a `CONFLICT`, because no other run took the row: the host
sent content it did not claim. `CONFLICT` is a row race and nothing else.
`REFUSED` covers every other confirmed refusal: a foreign owner or layout, a
completion of another digest, or a write the store cannot hold. Neither
changes anything.

A removal is a message, `RepeatableRemoval`, not a bare identity, so a guarded
removal can add the run it expects without a new arm in the `oneof`.

Every field of a row is bounded, so a complete snapshot of
`MaxRepeatableRows` rows stays under the 64 MiB a host receives in one message:
an identity at `MaxRepeatableIdentityBytes`, a digest and a run ID at
`MaxRepeatableTokenBytes`, and an error at `MaxRepeatableErrorBytes`, 4096
bytes. The host truncates a longer error before it sends a failure. Without the
bound, one long error makes the snapshot unreadable, and the record can then be
neither run nor repaired.

The record keeps the host's meaning out of the provider. The provider stores
states and digests; the host decides which file runs, whether a dirty record
blocks the run, what a retry is, and when a removal is safe.

The feature also adds `MigrationLogRecord.repeatable`, the identity a log entry
about a repeatable migration carries, with version `0`, which makes it a
two-way feature. The provider and each engine that keeps the record list it
beside `sessions.history`, and the host lists it. The SDK refuses every call
and the log field without the feature, on the client and on the server, and a
provider or an engine that lists the feature without its history. The SDK
checks the feature against the negotiated description; a call carries only
its session, so the provider checks on every call that the session's engine
lists the feature.

## Consequences

- A provider-backed engine that lists the feature runs repeatable migrations
  with the semantics of the built-in engines: the claim, the record in the
  body's transaction, retries with `--allow-dirty`, repair, the log and the
  drift check.
- A provider or an engine that does not list the feature keeps the host's
  refusal of a directory holding a repeatable migration, before anything
  changes, and the refusal names the provider and the feature.
- A body is at most `MaxRepeatableBodyBytes`, so a completion and a body read
  fit one message. The host refuses a longer body before it runs anything.
- A host built on an SDK older than contract 25.44 gets an error where it reads
  a log page that holds an entry about a repeatable migration, because the
  SDK refuses the field without the feature. Pre-v1, so no compatibility is
  owed.
- Which statements of a body run in a transaction stays the host's decision,
  read from the engine's capabilities: on an engine that does not roll schema
  changes back as a unit, every repeatable migration runs outside one.

## Alternatives considered

- **Reuse `MutateHistory` with a second store.** A history row has a numeric
  version, progress counters and no body, and its compare-and-swap sends the
  whole expected row. The record keys on an identity, carries a body of up to
  megabytes, and claims by run ID; fitting it into a revision row would change
  what the history validators accept for every provider.
- **A generic compare-and-swap of the whole row.** The host would send the
  body it read as the expected value of every claim, and the provider could
  not tell a claim from a completion, so it could not keep the body and the
  applied digest unchanged across an attempt.
- **Run the record's SQL through `Execute`.** The host would need the
  engine's dialect for the table, the key and the binary column, which is what
  the provider exists to own, and the YDB record would need statements the
  provider confines in a dev realm.
- **Keep the body outside the record, addressed by digest.** It saves bytes
  only when two identities hold the same content, and needs a second store
  with its own ownership check.
