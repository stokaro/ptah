---
title: Compare and drift
description: See how a live database differs from the desired schema, and turn that difference into a check that gates pipelines.
---

You have a desired schema and a live database, and you want to know how the two
differ — as SQL you can read, or as a check that fails a pipeline when they
diverge. `ptah schema compare` answers the first question; `ptah schema drift`
answers the second. A third form, the plan-only run, shows the exact SQL that
would reconcile the difference without executing anything.

Prerequisites:

- A `ptah` binary on your machine ([Install Ptah](../../start/install/)).
- A desired schema — the examples use annotated Go models under `./models`.
- The URL of the database to check.

The examples start from a synced state: the models in `./models` describe
exactly the one `users` table in a local SQLite database,
`sqlite://$PWD/app.db`. Substitute your own database URL throughout.

## Compare: the difference as SQL

`ptah schema compare` diffs the desired schema against the live database. It
reports the difference twice: once as the categories of change the comparison
found, and once as the SQL that reconciles them. In the synced starting state
there is neither:

```bash
ptah schema compare --root-dir ./models --db-url "sqlite://$PWD/app.db"
```

Expected output includes:

```text
=== SCHEMA COMPARISON ===

No schema differences detected.
```

Now add a column to the model:

```go
//ptah:schema:field name="created_at" type="TIMESTAMP"
CreatedAt *string
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

```bash
ptah schema drift --root-dir ./models --db-url "sqlite://$PWD/app.db"
```

Expected output includes:

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

Once the database gains the column — through a migration or a direct apply —
the check passes with exit `0`:

```text
No schema drift detected.
```

Three flags shape the check:

- `--severity` sets the failure threshold: `all` (the default) fails on any
  drift, `destructive` fails only on data-risking drift. With the added column
  and `--severity destructive`, the same findings print but the check passes:

  ```text
  Schema drift detected (highest severity: warning).
  Failure threshold: destructive. Failing: false.

  Findings:
  - columns_added: 1 (warning)
  ```

- `--ignore` excludes scopes from the check, for example
  `--ignore tables=audit_log`.
- `--format` selects `text`, `json` (the findings plus the full structured
  diff, for tooling), or `github-actions` (workflow annotations).

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

The same pair identifies a policy everywhere else it is named: the plan resolves
`rls_policies_added`, `rls_policies_removed` and `rls_policies_modified` by the
owning table together with the policy name, and the table is matched under the
target's identifier rules, so `orders` and `public.orders` are one table. A
reference the target schema cannot resolve is rejected rather than skipped.

## Plan-only runs

When the drift check fails, the next question is what SQL would fix it.
`ptah migrations plan` prints the reconciling migration SQL with a safety
classification, without writing files or touching the database:

```bash
ptah migrations plan --root-dir ./models --db-url "sqlite://$PWD/app.db"
```

Expected output includes:

```text
Safety classification:
  #  severity      subject                  reason
  1  safe         users                    does not remove data or tighten constraints
=== MIGRATION SQL ===

ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;

Generated 1 migration statements.
```

From here the workflows diverge: [Generate migrations](../../versioned/generate/)
turns this plan into committed migration files, while
[Apply directly](../apply/) executes an equivalent plan straight against the
database (`ptah schema apply --dry-run` is its plan-only form).

## Schema sources

All three commands resolve the desired schema the same way: `--root-dir` scans
Go annotations, `--schema-file` adds YAML, HCL, or SQL files, `--schema-cmd`
runs an external loader, and repeated sources merge into one composite desired
schema. See [Composite desired schema](../../schema/composite/) for the merge
rules.

## Diff two arbitrary schema states

`schema compare` and `schema drift` always pair desired sources with the live
`--db-url` database. `ptah schema diff` generalizes the pair: each side is a
schema file (repeatable), a database URL, or an Atlas-format migration
directory, so CI can answer "do these two schema files differ?" or "does this
migration directory converge to `schema.hcl`?" without a production database:

```bash
ptah schema diff \
  --from old-schema.sql \
  --to new-schema.sql \
  --dev-url "sqlite://$PWD/diff-dev.db"
```

The SQL dialect is pinned by `--dev-url` first, then by `--from`/`--to`
database URLs; schema files alone still require `--dev-url` (a disposable
database that is reset destructively). `--format json` emits a stable
`{"statements": [...]}` document, and `--schemas`, `--include`, and
`--exclude` scope both sides. Synced states print
`Schemas are synced, no changes to be made.`

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
