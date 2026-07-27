---
title: Reference data
description: Declare reference/seed rows and generate reversible data migrations from the drift against a live database.
---

`ptah seed` applies SQL seed files imperatively — "run these INSERTs once." Ptah
also supports a **declarative** model for reference/lookup tables: declare the
rows a table should contain, and let Ptah diff them against the live table and
generate a reversible data migration (`INSERT`/`UPDATE`/`DELETE`) that
reconciles the two.

Atlas keeps declarative data and data-migration generation in its proprietary
Pro build (an Atlas account and the closed-source binary). Ptah provides it as
an MIT, local, no-account, embeddable capability.

## Declaring managed data

Attach a `//migrator:schema:data` annotation to a Go entity. It names the target
table, the key column(s) that identify a row, and a YAML file holding the desired
rows:

```go
//migrator:schema:data table="countries" key="code" file="countries.yaml"
type Country struct {
    //migrator:schema:field name="code" type="VARCHAR(2)" primary="true"
    Code string
    //migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
    Name string
}
```

The row-data file, resolved relative to the Go source directory, is a top-level
YAML list of column maps:

```yaml
- code: US
  name: United States
- code: CZ
  name: Czechia
```

Use a comma-separated `key` for composite keys (`key="tenant_id,code"`).

Add an optional `schema="..."` to target a table in a non-default schema; both
the live-row read and the generated DML are then schema-qualified (for
PostgreSQL, `"reference"."countries"`). Omit it to use the connection's default
schema.

## Generating a data migration

```bash
ptah migrations data \
  --root-dir ./models \
  --db-url postgres://user:pass@localhost/db \
  --migrations-dir ./migrations
```

For each managed table, Ptah loads the desired rows, reads the live rows for the
managed columns (the key columns plus every column named in the desired rows),
and diffs them by key:

- a desired row with no live match → `INSERT`;
- a key present in both whose managed columns differ → `UPDATE` of the changed
  columns;
- a live row with no desired match → `DELETE`.

It writes an ordinary migration pair (`NNNNNNNNNN_data.up.sql` / `.down.sql`) and
refreshes `ptah.sum`, so the data migration applies and rolls back like any other.
`--dry-run` prints the SQL instead of writing files; a run with no drift writes
nothing.

## Safety gates

A data migration is applied through the ordinary migration path, where neither
the lint nor the safety gate classifies row `INSERT`/`UPDATE`/`DELETE` as
destructive. So `ptah migrations data` gates destructive changes when it
generates them:

- Unless `--allow-destructive` is set, a migration that would `UPDATE` or
  `DELETE` existing rows is refused with a per-table summary of the volume.
  Insert-only migrations are additive and are always allowed.
- Naming a table with `--protected-table` refuses any change to it — insert,
  update, or delete — unless `--allow-prod` is also set, mirroring the
  protected-target posture of `ptah seed`.

Both gates run before any SQL is emitted, so they apply to `--dry-run` too:
combine `--allow-destructive --dry-run` to preview a destructive change without
writing files.

## Reversibility

The generated `down` is the exact inverse of `up`: an inserted row's down is a
keyed `DELETE`, an update's down restores the prior values, and a deleted row's
down re-inserts it. Applying up then down restores the original table contents.

Values are rendered as dialect-correct, safely-escaped SQL literals, so a value
containing quotes, backslashes, or semicolons cannot break out of its literal.

Known limits (tracked follow-ups): only the managed columns are reconciled and
restored, so emptying a populated table's desired set is refused rather than
generating a rollback that could not restore the non-key columns; and tables are
ordered alphabetically rather than by foreign-key dependency, so FK-related
tables may need manual reordering (a violation aborts the transactional migration
cleanly).

## Declarative data versus `ptah seed`

`ptah seed` remains the imperative path — it runs environment-scoped SQL seed
files once and tracks them in `schema_seeds`. Declarative reference data instead
describes a *desired row state* and computes the migration to reach it, so drift
(a changed lookup value, a removed row) is reconciled rather than reapplied. Use
`seed` for one-off setup data, managed data for tables whose exact contents Ptah
should own.

## Embedding

The pieces are exported for embedding: `migration/datadiff` computes and renders
the diff, `dbschema.ReadTableRows` reads the live rows, and `core/goschema`
carries the managed-data model — so the whole pipeline can be driven from Go with
no CLI, account, or cloud.
