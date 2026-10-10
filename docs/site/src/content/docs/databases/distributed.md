---
title: CockroachDB, YugabyteDB, and Spanner
description: The PostgreSQL-compatible distributed engines in Ptah - what each capability preset excludes, the coverage behind each one, and CockroachDB row-level TTL.
type: reference
audience:
  - "database-engineer"
readerQuestion: "How do capability and DDL support differ across CockroachDB, YugabyteDB, and Spanner?"
goal: "Compare capability and DDL support across CockroachDB, YugabyteDB, and Spanner."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - "internal/dbschema"
generated: false
overlaps: []
disposition: keep
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
  A column declared `INT` or `INTEGER` without a width is compared at the
  width CockroachDB builds for it, which the session's `default_int_size`
  sets: `INT8` by default, `INT4` when the session sets 4. A declared width
  such as `INT4` or `BIGINT` is compared as written. A column declared in
  CockroachDB's own names, `STRING`, `BYTES` or `STRING[]`, is compared as the
  `text`, `bytea` or `text[]` the catalog reports. A sized `STRING(10)` is a
  `text` column with a width rather than a `VARCHAR(10)`, and it is read and
  inspected as `STRING(10)`, so a width change is still a change.
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

Spanner refuses a schema statement inside an explicit transaction, so the
migrator applies a migration body unwrapped and every statement commits as it
runs. A body that fails partway keeps the statements that already ran, and the
revision row records that prefix for the retry to resume from; `--tx-mode all`
is refused. CockroachDB and YugabyteDB take the transactional path, where a
failed body leaves nothing behind. See
[Apply migrations](../../versioned/apply/) for what each target records.

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

PostgreSQL and YugabyteDB keep the database-scoped publications,
subscriptions, logical replication slots, event triggers, and non-extension
foreign-data objects a dev database held when the command started, and reject
one the run created before dev-database cleanup. PostgreSQL additionally
removes database large objects inside the cleanup transaction; YugabyteDB does
not support that catalog write path.

## CockroachDB row-level TTL

CockroachDB expires rows on a schedule the server runs, declared as table
storage parameters. Ptah manages that policy through the render, plan, apply,
introspect, and diff cycle: a declared TTL is applied, read back from
`pg_class.reloptions`, and compared to zero difference on the next run.

Declare it as `platform.cockroachdb` properties on the table, named exactly for
the storage parameters they become. A property whose name begins with `ttl` but
is not one of them, misspelled or in upper case, is refused by name rather than
ignored:

```go
//ptah:schema:table name="sessions" platform.cockroachdb.ttl_expiration_expression="expires_at" platform.cockroachdb.ttl_job_cron="@daily"
type Sessions struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="expires_at" type="TIMESTAMPTZ"
	ExpiresAt time.Time
}
```

A YAML schema puts the same names in the table's `cockroachdb` platform group:

```yaml
tables:
  sessions:
    platform:
      cockroachdb:
        ttl_expiration_expression: expires_at
        ttl_job_cron: "@daily"
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
statement and leaves the table alone. A table in a Go or YAML schema that names
no TTL property declares no TTL, so a policy on the live table is removed.
`ptah introspect` writes a read policy back as the same properties.

### What Ptah manages

Eleven parameters. One of the two enablers is required; the rest are refused
without one. Nine read back from the catalog exactly as written on both declared
lines. `ttl_expire_after` and `ttl_row_stats_poll_interval` are compared by the
interval and the duration they denote rather than by their text, because the
server rewrites the value it stores:

| Property | What it sets |
| --- | --- |
| `ttl_expiration_expression` | The SQL expression whose value is when a row expires. |
| `ttl_expire_after` | The interval after a row is written at which it expires, such as `3 days`. |
| `ttl_row_stats_poll_interval` | How often the job refreshes its row-count estimate, such as `10m`. |
| `ttl_job_cron` | The schedule the deletion job runs on. |
| `ttl_select_batch_size` | Rows selected per batch; at least 1. |
| `ttl_delete_batch_size` | Rows deleted per batch; at least 1. |
| `ttl_select_rate_limit` | Rows selected per second; at least 1. |
| `ttl_delete_rate_limit` | Rows deleted per second; at least 1. |
| `ttl_pause` | Pauses the deletion job without removing the policy. |
| `ttl_label_metrics` | Labels the job's metrics with the table name. |
| `ttl_disable_changefeed_replication` | Omits the job's deletes from changefeeds. |

### What Ptah refuses, and why

- **An interval Ptah cannot read is refused.** `ttl_expire_after` accepts a
  sequence of quantity-and-unit pairs (`3 days`, `2 years 3 months`,
  `1 day 2 hours`), an optional trailing `HH:MM:SS`, and the ISO-8601 form
  (`P1Y2M3D`, `PT1H30M`). A spelling outside that surface is refused rather than
  sent, because the server would normalize it into a form Ptah could not predict
  and the plan would re-issue the change forever. Ambiguous abbreviations such as
  a bare `m` are refused for the same reason: minutes and months are two
  different retention policies.
- **A poll interval the server would not keep is refused.** The server
  truncates `ttl_row_stats_poll_interval` to whole seconds and stores nothing
  at all for a value below one second.
- **`ttl` cannot be declared.** It is derived from the other parameters, and
  the server refuses it when it arrives alone.
- **A knob without `ttl_expiration_expression` or `ttl_expire_after` is
  refused**, because the server refuses it too: every other `ttl_` parameter
  needs an expiry configured.
- **Zero and negative knob values are refused.** The server rejects a negative
  value and accepts zero while storing the parameter nowhere at all, so neither
  can ever read back as declared. Omit the property to keep the engine default.
- **A `false` boolean normalizes to "not declared"**, because on the server
  those are the same state: `ttl_pause = false` is stored nowhere, and setting
  it erases an existing `true` exactly as a reset does.

### Two server behaviors Ptah works around

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

The `platform.cockroachdb` properties apply to CockroachDB only, as every
platform property applies to its own target, so the same schema renders for
PostgreSQL without the policy. A policy attached to a table in Go code without
that binding is refused on every other target before anything is applied.
PostgreSQL answers `unrecognized parameter "ttl_expiration_expression"` on its
own, but YugabyteDB first answers `WARNING: storage parameter
ttl_expiration_expression is unsupported, ignoring`. An engine that ignores a
retention policy is worse than one that refuses it, so Ptah does not leave that
decision to the server.

A schema format that cannot declare the policy, such as HCL or SQL, leaves a
live policy alone instead of removing it, and HCL export reports the policy it
cannot write.

A CockroachDB dev database is required for dev-database workflows on a
CockroachDB target; a mismatched `--dev-url` is refused with
`--dev-url dialect "postgres" does not match --url dialect "cockroachdb"`.

## Spanner row deletion policy

Spanner deletes a row once an interval has passed since the time a timestamp
column holds. Ptah manages that policy through the render, plan, apply,
introspect, and diff cycle. Declare it as `platform.spanner` properties on the
table:

```go
//ptah:schema:table name="sessions" platform.spanner.row_deletion_column="created_at" platform.spanner.row_deletion_interval="30 days"
type Sessions struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="created_at" type="TIMESTAMPTZ"
	CreatedAt time.Time
}
```

A YAML schema puts `row_deletion_column` and `row_deletion_interval` in the
table's `spanner` platform group. `ptah schema render --dialect spanner` emits:

```sql
CREATE TABLE "sessions" (
  "id" BIGINT PRIMARY KEY NOT NULL,
  "created_at" TIMESTAMPTZ
) TTL INTERVAL '30 days' ON "created_at";
```

A policy needs both properties. The interval is a whole number of days, which
is all Spanner accepts: `36 hours` is refused before anything runs. Spanner
stores the interval in its own spelling, `30 days` as `4 WEEKS 2 DAYS`, so Ptah
compares the days the two spellings denote, not their text. A stored interval
Ptah cannot read is compared as written.

On a table that exists, a plan emits `ALTER TABLE ... ADD TTL` for a new policy,
`ALTER TABLE ... ALTER TTL` for a changed one, and `ALTER TABLE ... DROP TTL`
when the declaration names none. Spanner refuses `ADD` and `ALTER` in each
other's place, so the plan chooses by what the table holds. A policy that moves
to a new column is changed after the column is added. A table in a Go or YAML
schema that names no policy declares none, so a policy on the live table is
removed. `ptah introspect` writes a read policy back as the same properties,
with the interval in the spelling Spanner stores.

The `platform.spanner` properties apply to Spanner only, so the same schema
renders for PostgreSQL without the policy. A YDB TTL is a different policy with
its own properties, and nothing turns one into the other; see
[YDB TTL](../ydb/#ttl). `schema inspect` writes the policy into HCL as a
`platform "spanner"` block of the same properties, and a document read back
declares it. HCL that names no policy keeps the table's policy rather than
removing it.

## Invisible indexes

CockroachDB hides an index from the optimizer while it keeps the index up to
date. The clause comes last in `CREATE INDEX`, after the key parts and after
`WHERE`, and `NOT VISIBLE WHERE ...` is a syntax error:

```sql
CREATE INDEX k_a ON ic (a) WHERE a > 0 NOT VISIBLE;
```

Ptah reads the clause from `pg_get_indexdef`, which prints it the same way,
and from a schema file, where MySQL's `INVISIBLE` is taken as a synonym. It
writes `NOT VISIBLE` and changes a visibility in place with
`ALTER INDEX ic@k_a VISIBLE`. The server refuses to hide a primary key.

A partially visible index, `VISIBILITY 0.5`, is refused by name, in a schema
file and in a database read. The model holds a visible or a hidden index, and
either reading would move the index on the next apply. `VISIBILITY 0.0` and
`VISIBILITY 1.0` are read as hidden and visible. Measured on CockroachDB
26.3.2; the capability probe measures the 25.4 and 26.2 lines. PostgreSQL,
YugabyteDB and Spanner have no such index, and Ptah refuses an invisible index
there rather than build it visible.

## Trigger conditions

A trigger's `WHEN` condition is read through `pg_get_triggerdef`, which
CockroachDB 25.4 does not have. On that line Ptah reads each trigger without
its condition and does not compare one, so a declared condition is neither
reported as missing nor planned again. CockroachDB 26.2 and 26.3 have the
function and print the condition without the outer parentheses and with each
constant annotated, `(new).a > 0:::INT8`; the comparison drops the annotation.
Every YugabyteDB line prints the condition as PostgreSQL does.

CockroachDB takes a condition on a row value written as `(NEW).a`, and refuses
`NEW.a` with `no data source matches prefix: new`. It also refuses `UPDATE OF`
column lists, statement-level triggers and so `TRUNCATE` triggers. YugabyteDB
refuses `REFERENCING` transition tables. Measured on CockroachDB 25.4.16,
26.2.7 and 26.3.1 and on YugabyteDB 2024.2, 2025.2 and 2026.1.

## YugabyteDB's own extensions

Every YugabyteDB database starts with extensions the server installs into
`pg_catalog`: `pg_stat_statements` on 2024.2, 2025.2 and 2026.1, and
`postgres_fdw` as well on 2026.1. On 2026.1 the server's global views depend
on both, and YugabyteDB refuses `DROP EXTENSION` for either one.

A comparison never plans the removal of these extensions, whatever the ignore
list holds, so a declaration that does not name them applies. Only the removal
is withheld: a declaration that names `postgres_fdw` creates it on a line that
does not install it.

## Next steps

- Which release lines are declared and at what support level: [Database support matrix](../support-matrix/).
- What PostgreSQL itself manages: [PostgreSQL](../postgresql/).
- Capability keys per dialect: [Capabilities](../../reference/capabilities/).
