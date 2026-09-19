---
title: Apply a desired schema
description: Apply desired-schema changes to a database with ptah schema apply, saved plan files, and hybrid patterns.
type: how-to
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "How do I apply a desired schema directly and verify the result?"
goal: "Apply a desired schema directly and verify the result."
sourceOfTruth:
  - "internal/cli/schema"
  - "migration/schemadiff"
  - "migration/planner"
generated: false
quickstart: true
searchAliases:
  - "apply desired schema"
overlaps: []
disposition: keep
sourceMode: source-neutral
owns:
  - cli-ptah-schema-apply
---

Direct application ships natively as `ptah schema apply`. The separate
`ptah-compat` drop-in binary exposes the same engine to scripts that expect an
Atlas-style executable (`ptah-compat schema apply`); the plan, the approval prompt,
the plan files, and the fingerprint checks behave identically. This page
covers the workflow, the saved plan files that separate review from execution,
and the hybrid patterns that combine a native drift gate with a direct apply.

Prerequisites:

- A `ptah` binary on your machine ([Install Ptah](../../start/install/)).
- A desired schema as local files — the examples use a single `schema.sql`.
- The URL of the database to change.

The examples use a local SQLite database, `sqlite://app.db`, whose one
`users` table matches `schema.sql` except for a `created_at` column added to
the file. Substitute your own database URL throughout.

If you do not have that database, build it here. Start in an empty directory:

```console
mkdir ptah-direct-apply
cd ptah-direct-apply
```

Save this as `schema.sql`:

```sql
CREATE TABLE users (
    id    INTEGER PRIMARY KEY,
    email TEXT NOT NULL
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

Now add the column the rest of this page applies, by replacing `schema.sql`:

```sql
CREATE TABLE users (
    id         INTEGER PRIMARY KEY,
    email      TEXT NOT NULL,
    created_at TIMESTAMP
);
```

:::caution
A direct apply leaves no migration file to review and no revision history to
replay. Keep direct applies on databases you alone own. For shared and
production databases, use [versioned migrations](../../versioned/overview/), or
require a reviewer's signature on the plan with
[Plan and approve changes](../plan-and-approve/).
:::

## Native spellings

The native verbs use Ptah's own flag spellings — `--db-url` for the target
database, `--schema-file` (SQL, YAML, HCL, DBML, or OCI sources; repeatable),
and `--root-dir` (Go annotations; repeatable) for the desired schema. These
selectors match `schema compare` and `migrations generate`:

```bash illustration
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --dry-run
ptah schema plan  --db-url "sqlite://app.db" --schema-file schema.sql --output change.plan.json
ptah schema apply --db-url "sqlite://app.db" --plan change.plan.json
```

On the Atlas-compatible surface, `--to` additionally accepts a database URL
whose live schema becomes the desired schema, or an Atlas-format migration
directory replayed on the required `--dev-url` dev database. When `--dev-url`
is set, the ordered plan is rehearsed on the dev database before the target is
touched, and a failed rehearsal refuses the apply. The rehearsal runs entirely
inside the dev database, which is handed back empty afterwards — see
[Atlas schema commands](../../atlas/schema-commands/). `--lock-timeout`
bounds the session advisory lock that serializes concurrent applies,
`--tx-mode` selects the transaction mode, `--edit` opens the planned SQL in
`$VISUAL`/`$EDITOR`, and `--schemas`, `--include`, and `--exclude` scope both
comparison sides.

## Locking and `--lock-timeout`

A session advisory lock stops two applies from planning against one database at
the same time, and `--lock-timeout` bounds how long the apply waits for it.
PostgreSQL, YugabyteDB, MySQL, MariaDB and SQL Server give Ptah such a lock.
SQLite, ClickHouse, CockroachDB and Spanner do not, so an apply against one of
them runs unlocked.

Asking a target that cannot lock for a lock timeout is refused:

```console exits=2
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --lock-timeout 5s --auto-approve
```

Expected output on standard error:

```text
error: --lock-timeout requested a schema apply lock, and dialect "sqlite" has none:
```

The refusal comes before the connection, so nothing is planned and nothing is
applied. Remove the flag to apply unlocked, which is what such a target does
anyway. `PTAH_LOCK_TIMEOUT` sets the same flag and is refused the same way,
naming itself in the message so the setting can be found.

`ptah-compat schema apply` keeps the Atlas behavior instead: it accepts the
flag, writes a note to standard error, and applies unlocked.

## Preview the plan

`--dry-run` prints the planned SQL and stops:

```console
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --dry-run
```

Expected output on standard output:

```text
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
```

## Apply with approval

Without `--dry-run`, the command shows the same plan and asks for confirmation
before executing; anything other than `YES` cancels:

```bash illustration
ptah schema apply \
  --db-url "sqlite://app.db" \
  --schema-file schema.sql
```

Expected output on standard output:

```text illustration
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Apply these schema changes? Type 'YES' to confirm: Schema apply canceled.
```

`--auto-approve` skips the prompt for scripted runs:

```text illustration
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Auto-approval enabled; applying schema changes.
Schema apply completed successfully.
```

`--edit` opens the planned SQL in `$VISUAL`/`$EDITOR` before approval, and the
edited SQL is what gets applied.

### Statements that cannot run inside a transaction

The default `--tx-mode file`, and `--tx-mode all`, run the whole plan inside one
transaction. PostgreSQL refuses some statements there: `CREATE INDEX
CONCURRENTLY` and `DROP INDEX CONCURRENTLY`, and a statement that uses an enum
value an earlier statement added to an existing type. The apply checks the plan
for both before it rehearses, asks for approval, or sends anything, and refuses
it by name. A plan that adds `archived` to an existing enum and a column
defaulting to it stops with exit code `2`:

```text illustration
error: the planned changes cannot run inside a transaction: statement 2 of 2: it uses 'archived', a value an earlier statement in the same transaction adds to the pre-existing enum type "probe_status", and a new enum value is not usable until the transaction that added it commits; rerun with --tx-mode none, which commits each statement as it runs
SQL: ALTER TABLE "messages" ADD COLUMN "archive_state" probe_status NOT NULL DEFAULT 'archived'
```

`--tx-mode none` runs each statement in its own transaction, where both are
allowed. If a later statement then fails, the earlier ones stay applied. A value
added to an enum type that the same plan creates is usable at once, so a saved
or edited plan that creates the type, adds the value and uses it is not refused.
The check covers `--plan` and `--edit` as well as a computed plan.

## Separate review from execution with a plan file

Approving whatever the tool plans at execution time is the workflow's weakest
point. `ptah schema plan` computes the same plan and saves it as a local JSON
file instead, so the SQL can be reviewed — or code-reviewed — before anything
runs:

```console
ptah schema plan --db-url "sqlite://app.db" --schema-file schema.sql --output add-created-at.plan.json
```

Expected output on standard output:

```text
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Plan saved to file://add-created-at.plan.json
```

The file records the ordered statements with per-statement safety severity and
SHA-256 fingerprints of the starting and desired schema states:

```json
{
  "format_version": 1,
  "name": "plan_31a90d35a7bc",
  "dialect": "sqlite",
  "from_fingerprint": "sha256:f768e541305e03ee...",
  "to_fingerprint": "sha256:5fa9d95a6e87c76d...",
  "destructive": false,
  "statements": [
    {
      "sql": "ALTER TABLE \"users\" ADD COLUMN \"created_at\" TIMESTAMP",
      "severity": "safe",
      "reason": "does not remove data or tighten constraints"
    }
  ]
}
```

`ptah schema apply --plan` executes exactly the reviewed statements, after
verifying that the database still matches the plan's starting fingerprint:

```console
ptah schema apply --db-url "sqlite://app.db" --plan add-created-at.plan.json --auto-approve
```

Expected output on standard output:

```text
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Auto-approval enabled; applying schema changes.
Schema apply completed successfully.
```

A plan file can carry an `-- atlas:txmode none` or `-- atlas:txmode file`
header in its first statement; `ptah-compat schema plan --directive` writes
one. `ptah schema apply --plan` executes the plan in the mode the header
selects, so a plan holding `CREATE INDEX CONCURRENTLY` on PostgreSQL applies
without `--tx-mode none` on the command line. The header and `--tx-mode`
combine under the rule a versioned migration's directive answers to, and
`ptah-compat schema apply --plan` resolves the same file the same way: the
header wins, except under `--tx-mode all`, where a plan carrying a header is
refused before the database is contacted.

A plan file can also carry a reviewer's signature. `ptah schema approve` signs
one with an SSH key and `ptah schema apply --plan --require-approval` refuses a
plan that carries no signature from a list of approvers you commit:
[Plan and approve changes](../plan-and-approve/).

## Verification

After an apply, rerunning the dry run confirms nothing is left to change:

```console
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --dry-run
```

Expected output on standard output:

```text
Schema is synced, no changes to be made.
```

When the desired schema also exists as SQL, YAML, HCL, or DBML files, Go
annotations, or an OCI artifact, `ptah schema drift` gives the same
confirmation with
`No schema drift detected.` and exits `0`.

## Hybrid patterns

- **Gate natively, apply on approval.** `ptah schema drift --severity
  destructive` in a pipeline blocks data-risking divergence, while routine
  changes go through `ptah schema apply` with a saved plan file as the
  review artifact. [Compare and drift](../compare-and-drift/) covers the gate.
- **Iterate directly, ship versioned.** Prototype against a disposable local
  database with `ptah schema apply`, then run `ptah migrations generate`
  against a database at the released state, so the reviewed migration file —
  not the ad-hoc changes — is what reaches shared environments.
  [Generate migrations](../../versioned/generate/) covers that step.

## Failure modes

- Running a plan file against a database that changed since the plan was
  computed refuses with exit code `2` instead of executing reviewed SQL
  against unreviewed state:

  ```text
  error: pre-planned migration is stale: the target database schema does not
  match the plan's source fingerprint (plan sha256:f768e541..., database
  sha256:05a1209c...); the database changed since the plan was computed, so
  re-run `schema plan` against the current database and review the fresh plan
  ```

- Declining the confirmation prompt cancels with `Schema apply canceled.` and
  no changes.

## Limitations

- `--schema-file` accepts local SQL, YAML, HCL, and DBML files plus OCI schema
  artifacts. `--to` accepts a live database or a migration directory; a
  migration directory requires a disposable `--dev-url`. Direct apply does
  not register `--schema-cmd` or configured `external_schema` execution.
- Registry `atlas://` plan URLs are rejected; saved plan files are local.
- The Atlas-compatible flag surface, `--env` project-config support, and
  transaction modes are documented in
  [Atlas schema commands](../../atlas/schema-commands/#apply-a-desired-schema).

## Next steps

- Not sure direct changes fit your project:
  [Choose a workflow](../../start/choose-a-workflow/).
- Ship the change to shared environments as a reviewed file:
  [Generate migrations](../../versioned/generate/).
- See the whole Atlas-compatible surface:
  [Atlas compatibility overview](../../atlas/overview/).
