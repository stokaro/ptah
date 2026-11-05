# ADR 0063: A read names the tables it leaves out

Status: Proposed

Contract: 25.20

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0054](0054-a-filtered-read-carries-the-planning-context-it-does-not-compare.md),
[ADR 0059](0059-a-provider-says-why-a-call-failed-in-text-it-wrote.md)

## Context

A YDB async replication dropped without `CASCADE` leaves its replica tables
behind, and unless it was failed over first YDB keeps them read-only for good
(`path is an async replica table`). YDB refuses to alter such a table, and no
table can be created at its path. Only an operator's `DROP TABLE` removes it.

The built-in YDB engine reads such a table and does not describe it. Its
reader records the table as not described, under a coverage kind that never
reaches a document, and its planner refuses a schema that declares a table at
that path. The YDB provider also leaves the table out of the tables it
returns, but the contract gives it no way to say so. The host therefore takes
the table for one the database does not have.

Measured with Ptah master 8674fbe1a and `stokaro/ptah-ydb-plugin` master
115cac7 on local YDB 25.1.4.7 and 26.2.1.14, with the same results on both
lines. The fixture creates a table `src`, replicates it to `rep`, waits for the
replica, and drops the replication without `CASCADE`.

| Case | The provider path | The built-in YDB engine |
| --- | --- | --- |
| a schema that declares `rep` | plans `row-table-create-operation`, then fails: `provider execution failed (0 of 1 units confirmed): provider execution: NOT_STARTED (0 units confirmed applied)` | refuses at planning: ``rep is a replica table no replication of this database writes any more, which YDB keeps read-only for good (`path is an async replica table`); drop it by hand with DROP TABLE, which YDB accepts, or remove it from the schema`` |
| a schema without `rep` | `Schema is synced` | `Schema is synced` |
| `db read`, `schema inspect` | names nothing about `rep` | names nothing about `rep` |

The built-in engine is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)), so the
provider becomes the only reader of a YDB replica table.

None of the channels a read has today fits:

- `Schema.dispositions` exists, and every host refuses it ("inspection
  dispositions"). A provider cannot start sending one without breaking every
  host released so far. A disposition is also about one field of an object,
  and two of its states, `MODELED` and `NORMALIZED`, say nothing about a table
  that is left out whole.
- `ModelCoverage` subjects belong to native models. The host refuses a subject
  of kind `table` as one of an unenrolled kind.
- A reserved object is storage Ptah itself keeps, under one of four purposes:
  history, lock, log and tag. A table declared at a reserved name is refused
  as "reserved for lock storage", which says the wrong thing about a replica.

## Decision

### A read names each table it found and does not describe

`schema.unread-tables` is a two-way feature: the provider and each engine that
reports such tables list it, and the host lists it in its host features.
Contract 25.20 adds one field and one message:

```proto
message Schema {
  // ...
  repeated UnreadTable unread_tables = 12;
}
// A table the read found and does not describe. Contract 25.20, under
// schema.unread-tables.
message UnreadTable {
  Reference table = 1;
  ProviderDiagnostic diagnostic = 2;
}
```

An entry says that the table exists, that the read does not describe it, and
why. Its absence from `tables` then says nothing about the table. The provider
writes the reason under the rule of
[ADR 0059](0059-a-provider-says-why-a-call-failed-in-text-it-wrote.md): one
line that names what and why, and quotes no value, address or credential. The
reason is required, and the host shows it whether or not the session
negotiated `errors.reasons`, because the entry has no meaning without it.

Which tables a read leaves out is the provider's decision. The host does not
guess it from a table's attributes.

### What the SDK enforces

Both transport boundaries refuse a snapshot with `unread_tables` unless the
feature is negotiated: the engine lists it in the negotiated description, and
the host lists it. They also refuse an entry whose reference is not a table of
the session's engine and database, whose schema lies outside the schemas the
request named, that names one table twice, that names a table `tables` or
`reserved_objects` also names, or whose reason `ValidateDiagnostic` refuses.

### What the host does with an unread table

The host keeps the entries with the read, beside its reserved names. No
document carries them: an export of the read names nothing about the table,
as an export through the built-in engine names nothing.

Before preparation, the comparison refuses a schema that declares a table at
the identity of an unread table, or a foreign key that references one, with
the provider's reason: `table dir.rep: <reason>`. It checks the same places it
checks for a reserved name. A table the read does not describe is not one the
plan drops or alters, so nothing else changes.

The commands that print the note about roles a read does not describe print
one more note when the read leaves tables out:
`note: 1 table this read does not describe: dir.rep: <reason>`. The built-in
engine prints nothing here. The note tells an operator that the read found a
table and left it out, which the output does not show otherwise.

### What the YDB provider sends

The provider lists the feature for its engine. It sends an entry for each
replica table that no replication of the database writes, with the reason
the built-in engine gives. A replica that a live replication writes is not an
entry: the replication's items name it, and the provider's planning service
already refuses a table declared there.

## Alternatives considered

**Accept `Schema.dispositions` for tables.** No new field would be needed. But
a provider that sent one would fail every read on each host released so far,
and the message would mean one thing for a table and another for a field.
Rejected.

**A reserved object with a new purpose.** The host already refuses a declared
table at a reserved name and drops nothing there. But a reservation is Ptah's
own storage, its refusal names a storage purpose, and a dev cleanup decides by
purpose what to keep. Rejected.

**A native kind for the orphaned replica.** The provider could report it as an
observed object of a kind of its own and refuse in its own planning. Every
export of a filtered read would then write a not-described line for a kind no
author declares, and a comparison would need a kind whose desired side never
exists. Rejected.

**The provider refuses at planning.** Its planning service receives the tables
of the plan, and the table the read left out is not one of them. The host
would have to send the list in any case. Rejected.

**Send the unread tables to planning services as context too.** The built-in
engine also refuses a replication declared again over an orphaned replica.
Through the provider, YDB accepts it on both lines: the replication reads as
running, and a row written to `src` after it was created reaches `rep` within
ten seconds. No measured case needs the list in planning, so the contract does
not carry it there. A case that does can add it, with its own measurement.

## Consequences

- Contract 25.20 adds a field, a message and a two-way feature. It is numbered
  after contract 25.19, which ADR 0062 proposes for reversing a provider's
  changes.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- Without the feature nothing changes: the provider leaves the table out and
  says nothing, and a declared table there fails when it runs.
- With it, a declared table at an orphaned replica is refused before anything
  runs, in the provider's words, as the built-in engine refuses it.
- The refusal lives in the host's comparison, not in code of one engine, so
  any provider that leaves a table out uses it.

## Acceptance evidence

- SDK tests accept a negotiated entry and refuse an entry from an engine that
  does not list the feature, to a host that does not list it, and each
  malformed entry above.
- Through the SDK fixture provider, the host refuses a declared table and a
  foreign key at an unread identity before any step, with the provider's
  reason; plans nothing for a schema without the table; prints the note on
  `db read`; and writes nothing about the table in an export. A provider
  without the feature behaves as today.
- Through the YDB provider on 25.1.4.7 and 26.2.1.14, a schema that declares a
  table at an orphaned replica is refused at planning with the reason above,
  a schema without it is synced, and `db read` prints the note.
