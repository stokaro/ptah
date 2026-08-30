---
title: Count schema objects
description: Run ptah schema stats to emit one OpenMetrics gauge per object kind in a live database and feed it to a metrics collector.
type: how-to
audience:
  - "schema-author"
readerQuestion: "How do I run ptah schema stats to emit one OpenMetrics gauge per object kind in a live database and feed it to a metrics collector?"
goal: "Publish schema object counts as OpenMetrics gauges."
sourceOfTruth:
  - "cmd/schema"
  - "internal/schemastats"
generated: false
overlaps: []
disposition: keep
owns:
  - cli-ptah-schema-stats
---

`ptah schema stats` reads a live database and writes one gauge per object kind
in the OpenMetrics text format. Use it to record how much a schema holds over
time — tables, columns, indexes, views, policies, grants — on a dashboard beside
the rest of your infrastructure metrics.

Prerequisites: an installed `ptah` binary ([Install Ptah](../../start/install/))
and the URL of the database to read. The command connects and reads nothing
else: it takes no `--root-dir` and no `--schema-file`, so it describes a
database rather than a desired schema. See
[Database URLs and dev databases](../../concepts/database-urls-and-dev-databases/)
for the URL forms `--db-url` accepts.

## Starting state

The examples run against one SQLite database, so they need no server. Save this
as `schema.sql`:

```sql
CREATE TABLE customers (
  id INTEGER NOT NULL PRIMARY KEY,
  email TEXT NOT NULL,
  country TEXT NOT NULL
);

CREATE TABLE orders (
  id INTEGER NOT NULL PRIMARY KEY,
  customer_id INTEGER NOT NULL REFERENCES customers (id),
  total_cents INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_customers_country ON customers (country);

CREATE VIEW order_totals AS
SELECT customer_id AS buyer, total_cents AS cents FROM orders;

CREATE VIEW customer_orders AS
SELECT c.email, o.total_cents FROM customers c JOIN orders o ON o.customer_id = c.id;
```

Create the database from it:

```bash
ptah schema apply --schema-file schema.sql --db-url sqlite://shop.db --auto-approve
```

Expected output includes:

```text
Auto-approval enabled; applying schema changes.
Schema apply completed successfully.
```

Substitute your own database URL throughout.

## Read the counts

```bash
ptah schema stats --db-url sqlite://shop.db
```

Expected output includes:

```text
# HELP ptah_schema_schemas Schemas declared or read.
# TYPE ptah_schema_schemas gauge
ptah_schema_schemas{dialect="sqlite"} 0
# HELP ptah_schema_tables Tables.
# TYPE ptah_schema_tables gauge
ptah_schema_tables{dialect="sqlite"} 2
# HELP ptah_schema_columns Columns across all tables.
# TYPE ptah_schema_columns gauge
ptah_schema_columns{dialect="sqlite"} 6
# HELP ptah_schema_indexes Indexes.
# TYPE ptah_schema_indexes gauge
ptah_schema_indexes{dialect="sqlite"} 1
# HELP ptah_schema_constraints Table-level constraints.
# TYPE ptah_schema_constraints gauge
ptah_schema_constraints{dialect="sqlite"} 0
# HELP ptah_schema_enums Enum types.
# TYPE ptah_schema_enums gauge
ptah_schema_enums{dialect="sqlite"} 0
# HELP ptah_schema_extensions Extensions.
# TYPE ptah_schema_extensions gauge
ptah_schema_extensions{dialect="sqlite"} 0
# HELP ptah_schema_functions Functions and procedures.
# TYPE ptah_schema_functions gauge
ptah_schema_functions{dialect="sqlite"} 0
# HELP ptah_schema_sequences Standalone sequences.
# TYPE ptah_schema_sequences gauge
ptah_schema_sequences{dialect="sqlite"} 0
# HELP ptah_schema_domains Domain types.
# TYPE ptah_schema_domains gauge
ptah_schema_domains{dialect="sqlite"} 0
# HELP ptah_schema_composite_types Composite types.
# TYPE ptah_schema_composite_types gauge
ptah_schema_composite_types{dialect="sqlite"} 0
# HELP ptah_schema_range_types Range types.
# TYPE ptah_schema_range_types gauge
ptah_schema_range_types{dialect="sqlite"} 0
# HELP ptah_schema_views Views.
# TYPE ptah_schema_views gauge
ptah_schema_views{dialect="sqlite"} 2
# HELP ptah_schema_materialized_views Materialized views.
# TYPE ptah_schema_materialized_views gauge
ptah_schema_materialized_views{dialect="sqlite"} 0
# HELP ptah_schema_triggers Triggers.
# TYPE ptah_schema_triggers gauge
ptah_schema_triggers{dialect="sqlite"} 0
# HELP ptah_schema_rls_policies Row-level security policies.
# TYPE ptah_schema_rls_policies gauge
ptah_schema_rls_policies{dialect="sqlite"} 0
# HELP ptah_schema_roles Roles.
# TYPE ptah_schema_roles gauge
ptah_schema_roles{dialect="sqlite"} 0
# HELP ptah_schema_grants Privilege grants.
# TYPE ptah_schema_grants gauge
ptah_schema_grants{dialect="sqlite"} 0
# EOF
```

That block is the whole answer, and the `HELP` lines are the list of families:
no other page enumerates them. Three properties of the block are worth stating.
The family list is fixed rather than derived from the target, so a run against
PostgreSQL or MySQL emits the same names in the same order and a dashboard built
against one engine keeps working against another. Every family appears on every
run, at zero where the reader found none. The body ends with a literal `# EOF`
line, which is how a collector separates a complete scrape from a truncated one.

The counts describe the schema, not the contents. Row counts, table sizes, and
index bloat are properties of the data, and the database's own statistics views
report those.

## Label the numbers by schema

`--schemas` takes a comma-separated list. It selects which schemas to count on
the PostgreSQL family, and adds a `schemas` label to every family whatever the
engine:

```bash
ptah schema stats --db-url sqlite://shop.db --schemas main
```

Expected output includes:

```text
ptah_schema_tables{dialect="sqlite",schemas="main"} 2
```

The label carries the flag's value as one string. `--schemas app,audit`
produces `schemas="app,audit"` on every family rather than one series per
schema, so a pipeline that wants a series per schema runs the command once per
schema.

## Feed a metrics collector

The command writes one scrape and exits. There is no listener and no `/metrics`
path, so the usual shape is a scheduled run that writes a file a collector
already watches, such as the node_exporter textfile directory:

```bash
ptah schema stats --db-url sqlite://shop.db > schema-shape.prom
grep '^ptah_schema_tables' schema-shape.prom
```

Expected output includes:

```text
ptah_schema_tables{dialect="sqlite"} 2
```

## Failure modes

A successful read exits `0`. A usage error or a connection failure exits `2`
with one line on stderr, for example `error: --db-url is required`. See
[Exit codes](../../reference/exit-codes/) for the contract.

## Limitations

- A zero counts what Ptah's reader returned, not what the server holds. Where
  Ptah reads no triggers for a dialect, `ptah_schema_triggers` is `0` and
  nothing separates that from a database with no triggers.
- `ptah_schema_schemas` reports `0` on SQLite, which models no schema catalog.
- `--db-url` or `PTAH_DB_URL` is the only source of the target. A `ptah.yaml`
  carrying a `url:` does not satisfy it, and the run exits `2` with
  `error: --db-url is required`.

## Exact reference

Run `ptah schema stats --help` for the flag set with its environment variables.
[Schema object counts](../../reference/native-commands/#schema-object-counts)
places the verb in the native tree, and
[Exit codes](../../reference/exit-codes/) carries its row.

## Next steps

- Want the shape as a picture rather than a count?
  [Visualize the schema](../visualize/).
- Want to know what changed rather than what is there?
  [Compare and drift](../../direct/compare-and-drift/).
- Reviewing privileges and owners on the same database?
  [Report schema security findings](../security/).
