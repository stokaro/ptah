---
title: CockroachDB, YugabyteDB, and Spanner
description: The PostgreSQL-compatible distributed engines in Ptah - what each capability preset excludes, the coverage behind each one, and CockroachDB row-level TTL.
---

CockroachDB, YugabyteDB, and the Spanner PostgreSQL interface accept
PostgreSQL-like syntax while missing PostgreSQL capabilities, so Ptah routes
each one as a distinct dialect through the PostgreSQL implementation family
with its own capability preset instead of treating the server as a drop-in
PostgreSQL server. A live connection reads the server banner and selects the
matching preset automatically.

- **CockroachDB**: the preset excludes concurrent index creation and drops,
  `XML` columns, and advisory locks. Live CockroachDB v26.2.5 accepts role
  management, row-level security, standalone sequences, and `SERIAL` columns.
  It is also the one target that ADDS to PostgreSQL's surface rather than
  subtracting from it: see [CockroachDB row-level TTL](#cockroachdb-row-level-ttl).
- **YugabyteDB**: the preset includes concurrent index creation, role
  management, row-level security, standalone sequences, `XML` columns, and
  advisory locks on the measured 2026.1 line. `DROP INDEX CONCURRENTLY`
  remains excluded because that server line rejects it. A generated concurrent
  create therefore rolls back with ordinary `DROP INDEX`; only the forward
  migration requires no-transaction execution.
- **Spanner**: foreign keys are included, including composite and circular
  relationships rendered in two phases. Spanner manages the referenced-key
  backing index, so Ptah does not require an input unique/index declaration.
  Participating columns must have compatible key-capable types; JSON and array
  columns fail before rendering.
  The preset excludes enums, standalone sequences, row-level security, `XML`
  columns, advisory locks, and concurrent indexes. Foreign key actions are
  limited to `ON DELETE NO ACTION` or `CASCADE`; `ON UPDATE` fails before
  rendering.

## Coverage in continuous integration

CockroachDB and YugabyteDB run in integration coverage against live
open-source containers. Their reader coverage seeds a table, index, view,
materialized view, sequence, and row-level security policy, then verifies both
`ptah db read` and `ptah-compat schema inspect`.

Spanner runs both now: its capability rows are measured on every pull request,
and an integration target exercises render, apply, read and compare against the
Cloud Spanner emulator behind PGAdapter, which the `spanner` compose profile
starts.

It stays best-effort for a reason that no amount of coverage changes: an
emulator is evidence about the PostgreSQL interface, not about the managed
service. Review generated SQL before relying on it.

PostgreSQL and YugabyteDB reject unsupported database-scoped publications,
subscriptions, logical replication slots, event triggers, and non-extension
foreign-data objects before dev-database cleanup. PostgreSQL additionally
removes database large objects inside the cleanup transaction; YugabyteDB does
not support that catalog write path.

## CockroachDB row-level TTL

CockroachDB expires rows on a schedule the server runs, declared as table
storage parameters. Ptah manages that policy through the render, plan, apply,
introspect, and diff cycle: a declared TTL is applied, read back from
`pg_class.reloptions`, and compared to zero difference on the next run.

Declare it as attributes on the table, named exactly for the storage parameters
they become:

```go
//ptah:schema:table name="sessions" ttl_expiration_expression="expires_at" ttl_job_cron="@daily"
type Sessions struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="expires_at" type="TIMESTAMPTZ"
	ExpiresAt time.Time
}
```

`ptah schema render --dialect cockroachdb` emits that as:

```sql
CREATE TABLE "sessions" (
  "id" BIGINT PRIMARY KEY NOT NULL,
  "expires_at" TIMESTAMPTZ
) WITH (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily');
```

Changing the policy emits `ALTER TABLE ... SET (...)`, and removing it emits
`ALTER TABLE ... RESET (ttl)`, which drops the whole configuration in one
statement and leaves the table alone.

### What Ptah manages

Ten parameters. One of the two enablers is required; the rest are refused
without one. Nine read back from the catalog exactly as written on both declared
lines, and `ttl_expire_after` is compared by the interval it denotes rather than
by its text, because the server rewrites the value it stores:

| Attribute | What it sets |
| --- | --- |
| `ttl_expiration_expression` | The SQL expression whose value is when a row expires. |
| `ttl_expire_after` | The interval after a row is written at which it expires, such as `3 days`. |
| `ttl_job_cron` | The schedule the deletion job runs on. |
| `ttl_select_batch_size` | Rows selected per batch; at least 1. |
| `ttl_delete_batch_size` | Rows deleted per batch; at least 1. |
| `ttl_select_rate_limit` | Rows selected per second; at least 1. |
| `ttl_delete_rate_limit` | Rows deleted per second; at least 1. |
| `ttl_pause` | Pauses the deletion job without removing the policy. |
| `ttl_label_metrics` | Labels the job's metrics with the table name. |
| `ttl_disable_changefeed_replication` | Omits the job's deletes from changefeeds. |

### What Ptah refuses, and why

- **`ttl_row_stats_poll_interval` is not supported**: the
  server canonicalizes the duration (`'600s'` becomes `'10m0s'`) and stores
  nothing at all for a value below one second.
- **An interval Ptah cannot read is refused.** `ttl_expire_after` accepts a
  sequence of quantity-and-unit pairs (`3 days`, `2 years 3 months`,
  `1 day 2 hours`), an optional trailing `HH:MM:SS`, and the ISO-8601 form
  (`P1Y2M3D`, `PT1H30M`). A spelling outside that surface is refused rather than
  sent, because the server would normalize it into a form Ptah could not predict
  and the plan would re-issue the change forever. Ambiguous abbreviations such as
  a bare `m` are refused for the same reason: minutes and months are two
  different retention policies.
- **`ttl` cannot be declared.** It is derived from the other parameters, and
  the server refuses it when it arrives alone.
- **A knob without `ttl_expiration_expression` is refused**, because the server
  refuses it too: every other `ttl_` parameter needs an expiry configured.
- **Zero and negative knob values are refused.** The server rejects a negative
  value and accepts zero while storing the parameter nowhere at all, so neither
  can ever read back as declared. Omit the attribute to keep the engine default.
- **A `false` boolean normalizes to "not declared"**, because on the server
  those are the same state: `ttl_pause = false` is stored nowhere, and setting
  it erases an existing `true` exactly as a reset does.

### Two things the server does that Ptah works around

**The interval is rewritten on the way in.** Measured on both declared lines,
`ttl_expire_after = '72 hours'` is stored as `'72:00:00'`, `'5 minutes'` as
`'00:05:00'`, `'1 week'` as `'7 days'`, and `'P1Y2M3D'` as
`'1 year 2 mons 3 days'`. Ptah sends what you wrote and compares what the
interval *denotes*, so a declaration converges whichever spelling it uses. The
three fields of a PostgreSQL interval stay apart in that comparison: a month is
not thirty days and a day is not twenty-four hours, and the server keeps them
apart too.

**Hidden columns are left out of the description.** `ttl_expire_after` adds a
`crdb_internal_expiration` column that CockroachDB marks hidden, and a table
declaring no primary key gets a hidden `rowid` the same way. Neither is a column
anybody declared, and describing them made a read unreplayable — applying it
back asked for a column the engine owns. Both are now excluded from a
CockroachDB read. PostgreSQL and YugabyteDB have no such notion and their reads
are unchanged.

### On other engines

Row-level TTL is refused on every target without the capability — PostgreSQL,
YugabyteDB, Spanner, MySQL, MariaDB, SQLite, SQL Server and ClickHouse — before
anything is applied. PostgreSQL answers `unrecognized parameter
"ttl_expiration_expression"` on its own, but YugabyteDB first answers `WARNING:
storage parameter ttl_expiration_expression is unsupported, ignoring`. An engine
that ignores a retention policy is worse than one that refuses it, so Ptah does
not leave that decision to the server.

A CockroachDB dev database is required for dev-database workflows on a
CockroachDB target; a mismatched `--dev-url` is refused with
`--dev-url dialect "postgres" does not match --url dialect "cockroachdb"`.

## Next steps

- Which release lines are declared and at what support level: [Database support matrix](../support-matrix/).
- What PostgreSQL itself manages: [PostgreSQL](../postgresql/).
- Capability keys per dialect: [Capabilities](../../reference/capabilities/).
