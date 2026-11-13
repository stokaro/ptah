---
title: The schema apply log
description: "What `ptah schema apply` records in the target database, how a caller names a run in it, and what the record cannot be used for."
type: reference
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "Which plan changed this database, when, on whose word, and how did it end?"
goal: "Read the record a schema apply leaves in the target and know its limits."
sourceOfTruth:
  - "migration/migrator"
  - "internal/cli/schema"
generated: false
overlaps: []
disposition: keep
---

A schema apply leaves the changed tables behind and nothing that says how they
got that way. `ptah schema apply` therefore keeps `ptah_schema_apply_log` in
the target database, as `ptah migrations up` keeps
[the migration log](../../versioned/migration-log/): when an apply started,
which plan it ran, who the caller said ran it, how it ended, and the statement
it stopped at.

No command reads it. It is a table, and SQL reads it:

```sql
SELECT logged_at, run_id, state, actor, actor_origin, run_reference,
       plan_name, statement_count, error_position
FROM ptah_schema_apply_log
ORDER BY logged_at, run_id, seq;
```

## What a row means

Each run writes two entries under one `run_id`: `started` before the first
statement leaves, and the outcome once the apply has returned.

| Column | Meaning |
| --- | --- |
| `run_id`, `seq` | The run Ptah named, and the entry's place in it |
| `run_reference` | The caller's identifier for the run, from `--run-reference` |
| `state` | `started`, then `applied`, `failed` or `unknown` |
| `actor`, `actor_source`, `actor_origin` | The name the run was given, where Ptah got it, and the caller's word for where the caller got it |
| `plan_name`, `plan_digest` | A saved plan's name, and the `sha256:` digest of the file the run read. Both are empty for a plan computed in the run |
| `statement_count` | How many statements the plan holds |
| `error_position`, `error_stmt` | The 1-based position and the whole text of the statement the run stopped at |
| `error` | The error the run reported, as `error.message` of the `--json` document says it |
| `logged_at` | When the entry was written, in UTC |

The outcome is the one [the apply document](../apply/#the-apply-document)
reports. `applied` and `failed` mean what they mean there. Every other ending
of a run that sent a statement is `unknown`: a failure after a statement the
engine could not take back, such as a schema statement on MySQL or one run with
`--tx-mode none`, or a run whose end could not be confirmed. `failed` says
the engine took the run's statements back; `unknown` says that nothing after
the first statement is confirmed.

A run that sent nothing writes nothing, and does not create the table: a dry
run, a declined prompt, a refused stale plan, a plan with no changes, or a
failure before the first statement. The record is of statements that reached
the database.

The started entry is written over the session the plan runs on. The outcome is
written after the apply has returned and released its lock, over the connection
pool rather than that session, so a run whose session was lost still records
how it ended. Neither
entry is part of the plan's transaction: a plan that rolled back still leaves
the record that it ran.

## Naming the run

`--actor` names the run, as it does for `ptah migrations up`. It is unverified:
`actor_source` is `provided` for a name the caller gave and `process-user` for
the user the process runs as when nobody gave one.

`--actor-origin` is the caller's word for where the name came from, such as
`admission` for a name an admission controller stamped. It is one lowercase
word of up to 64 characters, letters, digits, `.`, `_`, `:` and `-`, and it
needs `--actor`: beside the process user it would describe a name the caller
did not give. Ptah checks its shape and nothing else.

`--run-reference` is the caller's identifier for the run, stored on both
entries. Ptah Operator passes its operation ID, `sha256:` and 64 hex digits, so
a row can be joined to the operator's record of the change. Any string of up to
255 visible ASCII characters is accepted. A reference the column would shorten,
or a character a server's charset could replace, would name another run, so
both are refused before the command connects.

`ptah migrations up` and `ptah migrations down` take the same three flags, and
[the migration log](../../versioned/migration-log/#the-run-reference) stores
them the same way.

## When an entry cannot be written

A failed write never fails the apply, and it changes neither the outcome nor
the exit status. `--json` reports what the writes came to under `apply_log`:

| `apply_log` | Meaning |
| --- | --- |
| absent | The run wrote no entry and tried to write none: it sent no statement, the project turned the log off, or the target is served by an executable provider |
| `{ "written": 2, "run_reference": "sha256:…" }` | Both entries reached the table, carrying that reference |
| `{ "written": 1, "lost": [ … ] }` | The server accepted one entry, and `lost` names the other |

Each lost entry names its `state`, the server's `sqlstate` or `error_number`
where it gave one, and a `message` for a person. `run_reference` is the
reference the written entries carry, and it is absent when the run was given
none or nothing was written.

## Where Ptah keeps no such table

- `schema_apply.log: false` in `ptah.yaml` turns it off for the project.
- The Atlas-compatible `schema apply` keeps none: an Atlas user reads a
  database only Atlas writes, for the reason the Atlas revision format keeps no
  migration log.
- A target an executable provider serves keeps none. The provider contract
  has no store for it yet.

## What this is not

**It is not an audit trail.** The account that applies a plan can rewrite every
row of the record that it did. Tamper evidence is the database's own audit.

**It is not part of the schema.** Every reader leaves the table out, so
`ptah schema inspect` does not show it and the next plan does not drop it.
`ptah db drop-all` names it beside the revision tables and drops it with them.

**It keeps everything.** Nothing prunes it. A deployment that needs a retention
rule applies its own `DELETE`.
