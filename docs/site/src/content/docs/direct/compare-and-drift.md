---
title: Compare and drift
description: See how a live database differs from the desired schema, and turn that difference into a check that gates pipelines.
type: how-to
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "How do I detect schema drift and turn it into a CI gate?"
goal: "Detect schema drift and turn it into a CI gate."
sourceOfTruth:
  - "internal/cli/schema"
  - "migration/schemadiff"
  - "migration/planner"
generated: false
quickstart: true
searchAliases:
  - "schema drift"
overlaps: []
disposition: keep
sourceMode: source-neutral
owns:
  - cli-ptah-schema-compare
  - cli-ptah-schema-diff
  - cli-ptah-schema-drift
---

You have a desired schema and a live database, and you want to know how the two
differ — as SQL you can read, or as a check that fails a pipeline when they
diverge. `ptah schema compare` answers the first question; `ptah schema drift`
answers the second. A third form, the plan-only run, shows the exact SQL that
would reconcile the difference without executing anything.

Prerequisites:

- A `ptah` binary on your machine ([Install Ptah](../../start/install/)).
- A desired schema, in any source Ptah reads. The examples use
  `--schema-file schema.sql`, so they require no Go toolchain.
- The URL of the database to check.

The examples start from a synced state: `schema.sql` describes exactly the one
`users` table in a local SQLite database, `sqlite://app.db`. Substitute your own
database URL throughout.

If you do not have that pair, build it here. Start in an empty directory:

```console
mkdir ptah-drift
cd ptah-drift
```

Save this as `schema.sql`:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY
);
```

Apply it, so the database exists and matches the file:

```console
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --auto-approve
```

Expected output on standard output:

```text
Schema apply completed successfully.
```

## Compare: the difference as SQL

`ptah schema compare` diffs the desired schema against the live database. It
reports the difference twice: once as the categories of change the comparison
found, and once as the SQL that reconciles them. In the synced starting state
there is neither:

```console
ptah schema compare --schema-file schema.sql --db-url "sqlite://app.db"
```

Expected output on standard output:

```text
=== SCHEMA COMPARISON ===

No schema differences detected.
```

Now add a column to the table declaration in `schema.sql`:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP
);
```

The comparison shows the change the database is missing:

```text
=== SCHEMA COMPARISON ===

Differences detected (1 category):
  tables_modified (1): users

Reconciling SQL:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
```

The category names are the field names of the schema diff, so a category
carries the same spelling here that it does in machine-readable output. The
list comes from the comparison result itself rather than from the SQL, which
is what makes it complete: a difference your database dialect has no statement
for is still named. When that happens the SQL section reads
`Reconciling SQL: none.` and standard error names the categories the dialect's
planner could not turn into statements, so an empty statement list is never
reported as agreement.

The command exits `0` whether or not differences exist; add `--exit-code` to
exit `1` on a non-empty diff when a script needs the answer as a status code.
Every category counts toward that check, including one whose SQL the dialect
cannot produce.

## Drift: the difference as a check

`ptah schema drift` evaluates the same difference as a pass/fail check with
severity-classified findings. With the extra model column still in place:

```console exits=1
ptah schema drift --schema-file schema.sql --db-url "sqlite://app.db"
```

The command exits `1` because drift was found, which is the check working
rather than a fault in it. Expected output on standard output:

```text
Schema drift detected (highest severity: warning).
Failure threshold: all. Failing: true.

Findings:
- columns_added: 1 (warning)
```

The command exits `1` because drift was found. Findings describe what applying
the desired schema would change on the database: `columns_added` means the
desired schema has a column the database lacks, and `columns_removed` is
classified destructive because applying would drop data.

`--severity destructive` lowers the threshold to data-risking drift only. The
same findings print, and the check passes:

```console
ptah schema drift --schema-file schema.sql --db-url "sqlite://app.db" --severity destructive
```

Expected output on standard output:

```text
Schema drift detected (highest severity: warning).
Failure threshold: destructive. Failing: false.
Database: sqlite://app.db

Findings:
- columns_added: 1 (warning)
```

Once the database gains the column — through a migration or a direct apply —
the check passes at either threshold. The next section reconciles it and runs
the check again.

Three more flags shape the check:

- `--ignore` excludes scopes from the check, for example
  `--ignore tables=audit_log`. An excluded table's declared reference rows are
  excluded with it.
- `--ignore-extension` names a database extension the comparison leaves alone,
  and is repeatable. See [An extension the schema does not
  describe](#an-extension-the-schema-does-not-describe).
- `--format` selects `text`, `json` (the findings plus the full structured
  diff, for tooling), or `github-actions` (workflow annotations).

### An extension the schema does not describe

An extension a bootstrap step installs, and the desired schema deliberately
does not declare, reads as removed: the comparison sees an object the database
has and the declaration does not, and plans `DROP EXTENSION`. That statement is
destructive, so `--check-destructive` fails on it and a drift gate reports it on
every run.

Declaring the extension is not the way out. Declaring it hands Ptah the object,
and that includes removing it; dropping one cascades into everything built on
it, and `CREATE EXTENSION IF NOT EXISTS` has no safe reverse.

Name it instead, and the comparison neither creates nor drops it:

```bash illustration
ptah schema drift --root-dir ./models --db-url "$DATABASE_URL" \
  --ignore-extension pg_trgm
```

The same flag is on `ptah schema compare`, `ptah migrations plan` and
`ptah migrations generate`, and the list can live in `ptah.yaml` instead, which
is where it belongs when every invocation would repeat it:

```yaml
ignore_extensions:
  - pg_trgm
```

Both spellings add to Ptah's own list rather than replacing it, so `plpgsql`
stays ignored. A flag on the command line wins over the file.

#### Or say it in the schema itself

A serialized desired schema — HCL, or SQL — can carry the statement instead, as
a directive in its leading comment header:

```hcl illustration
// ptah:not-described extension "pg_trgm"

schema "public" {}
```

The two are different statements, and which one fits depends on what is true.
The directive says the **description** does not describe that extension, and it
travels with the document: push the schema to a registry and whoever pulls it
reads the same limit. The flag says this **run** must leave the extension alone,
whatever the description claims, and it is the only way for a schema built from
Go annotations, which carry no directive of this kind and no comment header to
put one in.

Dropping the name makes the directive cover every extension:
`// ptah:not-described extension`. [What a document says it does not
describe](../../atlas/schema-commands/#the-document-says-what-it-does-not-describe)
is the wider subject; coverage governs schemas, roles and several other object
kinds the same way.

### Reference rows are part of the check

A desired schema that declares reference rows — a `//ptah:schema:data`
annotation, or a `data` block in an HCL schema — is checked against the rows the
database holds as well as against its structure. A database whose tables match
and whose lookup values somebody edited by hand is drift, and the check exits
`1` for it.

The report names the tables whose rows differ and how many rows moved. It
carries no key, no column name and no value, so a pipeline may publish it
where the row data itself must not go:

```json
{
  "drift": true,
  "highest_severity": "destructive",
  "findings": [
    { "category": "data_rows_updated", "count": 1, "severity": "destructive" }
  ],
  "managed_data": {
    "tables": [
      { "table": "countries", "inserts": 0, "updates": 1, "deletes": 0 }
    ]
  }
}
```

The text report prints the same counts under a `Managed data:` heading, and
`--format github-actions` writes one annotation per table.

Three findings carry the volume, and they set the severity the threshold reads:

| Finding | Severity | What it means |
| --- | --- | --- |
| `data_rows_inserted` | safe | The declaration holds a row the database does not. Writing it takes nothing away. |
| `data_rows_updated` | destructive | A live row holds a value the declaration does not. Applying overwrites it, and the value it held is not recoverable from the plan. |
| `data_rows_deleted` | destructive | The database holds a row the declaration no longer does. Applying removes it. |

So `--severity destructive` passes a database that is only missing reference
rows and fails one whose rows were edited or added to by hand.

A database that is behind on structure gets a defined answer rather than an
error, because a check has to report rather than fall over, and the structural
half of the report is what names what is missing:

- a declared table the database has not created yet is compared against no
  rows, so every declared row counts as an insert beside the `tables_added`
  finding;
- a column the declaration names and the table does not carry yet is left out
  of the row comparison, and the `columns_added` finding reports it. No live
  row holds a value for that column, so applying takes nothing away, and a run
  whose rows are otherwise in place stays clean at `--severity destructive`.

The comparison reads each declared table, so the role the check connects with
needs `SELECT` on those tables as well as the catalog access the structural
half needs. `--ignore tables=...` is what excludes a table from it, and it
excludes the table from the structural half too.

[Reference data](../../versioned/reference-data/) declares the rows and
reconciles them.

In the JSON document, a PostgreSQL row-level-security policy is reported by the
table that owns it together with its name, because a policy name is scoped to
its table and two tables may each carry one called `tenant_isolation`. Both
`diff.rls_policies_added` and `diff.rls_policies_removed` hold objects:

```json
{
  "diff": {
    "rls_policies_added": [
      { "policy_name": "tenant_isolation", "table_name": "zeta_orders" }
    ]
  }
}
```

`rls_policies_added` held bare policy-name strings in Ptah v0.2.0 and earlier —
the v0.2.0 tag itself still declares `RLSPoliciesAdded []string`, so the object
form has not appeared in a release yet. A consumer reading that field reads
`.policy_name` now; nothing was removed.

A constraint carries an `identity` beside the name and table it is written with.
The two are not the same answer: a description leaves a table unqualified where
a catalog reports it with its schema, so one modified constraint arrives as
`widget` on one side and `public.widget` on the other. The written spelling is
what a statement and a diagnostic use; the identity is what says the two records
are one object.

```json
{
  "diff": {
    "constraints_removed": [
      {
        "name": "uq_widget_scope",
        "table_name": "public.widget",
        "type": "UNIQUE",
        "identity": { "schema": "public", "table": "widget", "name": "uq_widget_scope" }
      }
    ]
  }
}
```

The parts are kept separate rather than joined so a consumer never has to parse
one back out, and they are already folded by the target's rules — which is why
`identity.name` can differ in case from `name` on MySQL and MariaDB, where the
server resolves a constraint name case-insensitively.

The same pair identifies a policy everywhere else it is named: the plan resolves
`rls_policies_added`, `rls_policies_removed` and `rls_policies_modified` by the
owning table together with the policy name, and the table is matched under the
target's identifier rules, so `orders` and `public.orders` are one table. A
reference the target schema cannot resolve is rejected rather than skipped.

## Plan-only runs

When the drift check fails, the next question is what SQL would fix it.
`ptah migrations plan` prints the reconciling migration SQL with a safety
classification, without writing files or touching the database:

```console
ptah migrations plan --schema-file schema.sql --db-url "sqlite://app.db"
```

Expected output on standard output:

```text
Safety classification:
  #  severity      subject                  reason
  1  safe         users                    does not remove data or tighten constraints
=== MIGRATION SQL ===

ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;

Generated 1 migration statements.
```

Applying that plan is what closes the loop. A direct apply executes the same
statement:

```console
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --auto-approve
```

Expected output on standard output:

```text
Schema apply completed successfully.
```

The check that failed at the start of this page now passes, and exits `0`:

```console
ptah schema drift --schema-file schema.sql --db-url "sqlite://app.db"
```

Expected output on standard output:

```text
No schema drift detected.
```

From here the workflows diverge: [Generate migrations](../../versioned/generate/)
turns this plan into committed migration files, while
[Apply directly](../apply/) executes an equivalent plan straight against the
database (`ptah schema apply --dry-run` is its plan-only form).

## Schema sources

All three commands resolve the desired schema the same way: `--schema-file`
adds SQL, YAML, HCL, DBML, or OCI sources; `--root-dir` scans Go annotations;
and `--schema-cmd` runs an explicit external loader. A configured
`external_schema` needs `--config ... --allow-external-schema`. Repeated
sources merge into one composite desired schema. See
[Composite desired schema](../../schema/composite/) for the merge rules.

## Diff two arbitrary schema states

`schema compare` and `schema drift` always pair desired sources with the live
`--db-url` database. `ptah schema diff` generalizes the pair: each side is a
schema file (repeatable), a database URL, or an Atlas-format migration
directory, so CI can answer "do these two schema files differ?" or "does this
migration directory converge to `schema.hcl`?" without a production database:

Save the two sides as `old-schema.sql`:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY
);
```

and `new-schema.sql`:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT
);
```

```console
ptah schema diff --from old-schema.sql --to new-schema.sql --dev-url "sqlite://diff-dev.db"
```

Expected output on standard output:

```text
ALTER TABLE "users" ADD COLUMN "email" TEXT;
```

The SQL dialect is pinned by `--dev-url` first, then by `--from`/`--to`
database URLs; schema files alone still require `--dev-url` (a disposable
database that is reset destructively). `--format json` emits a stable
document, described below, and `--schemas`, `--include`, and
`--exclude` scope both sides. Synced states print
`Schemas are synced, no changes to be made.`

### The JSON document

`--format json` answers one comparison two ways:

```json
{
  "format_version": 1,
  "statements": [
    "CREATE TABLE \"orders\" (\n  \"id\" INTEGER PRIMARY KEY\n)",
    "ALTER TABLE \"users\" ADD COLUMN \"email\" TEXT"
  ],
  "changes": {
    "tables_added": ["orders"],
    "tables_modified": [
      {"table_name": "users", "columns_added": ["email"]}
    ]
  }
}
```

`statements` is the DDL a migration would run — what will happen. `changes` is
the structural comparison those statements were planned from — what differs,
with tables, columns, enums, indexes, constraints, views, triggers, roles and
grants each under their own key. A check like "does this diff drop a column"
reads a field; deciding it from `statements` means parsing DDL, and a DDL parser
is wrong for every dialect it was not tested against. What that dropped column
would break in a view is a separate question, answered by
[Trace view column lineage](../../schema/lineage/).

The two halves always agree. `changes` reports the comparison after
[diff policy](../../reference/configuration/) has been applied, so a diff run
with `skip.drop_table` shows neither a `DROP TABLE` statement nor the removed
table.

Both keys are always present. An empty `statements` is a comparison that found
nothing to do; `changes` is present and empty when the schemas are synced, so a
CI check reads the same fields either way. Within `changes`, a collection with
no members encodes as `null` or `[]` — read both as empty. `format_version`
rises when a field's meaning changes, not when one is added.

An explicit `--include` selection that matches neither side is invalid. The
command prints no diff and exits 2 instead of reporting a synced schema. This
is outcome-based: a matching top-level identifier that contains a dot remains
selectable with a dotted spelling.

When the desired side is a live PostgreSQL database, a selected extension keeps
its installation schema. Creating a non-default placement emits `CREATE SCHEMA`
and `CREATE EXTENSION ... WITH SCHEMA ...`; an identical placement is synced,
and extension drops remain supported. A placement change exits 2 before SQL
output because Ptah does not yet plan `ALTER EXTENSION ... SET SCHEMA`.

## Failure modes

- `ptah schema drift` exits `2` (not `1`) when the check itself cannot run —
  an unreachable database, a bad URL, or an unparsable schema source — so
  pipelines can tell "drift found" from "check broken".
- `ptah schema compare` without `--exit-code` never signals differences through
  its status code; scripts that forget the flag silently pass.

For symptoms beyond these, see
[Troubleshooting](../../operate/troubleshooting/).

## Next steps

- Apply the difference straight to the database:
  [Apply directly](../apply/).
- Turn the difference into reviewed migration files:
  [Generate migrations](../../versioned/generate/).
- Run the drift check on every pull request: [CI](../../testing/ci/).
