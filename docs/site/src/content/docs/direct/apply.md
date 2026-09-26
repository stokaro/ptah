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
the same time, and `--lock-timeout` bounds how long the apply waits for it. Not
every engine gives Ptah such a lock, and an apply against one that does not runs
unlocked.

Asking a target that cannot lock for a lock timeout is refused. The refusal
names the engines that do lock, so this page carries no second copy of the list:

```console exits=2
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --lock-timeout 5s --auto-approve
```

Expected output on standard error:

```text
error: --lock-timeout requested a schema apply lock, and dialect "sqlite" has none: only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. Remove --lock-timeout to apply without a lock
```

Nothing is planned and nothing is applied. A URL that names its dialect is
refused before the connection opens, which is why the `sqlite://app.db` above
leaves no file behind. A PostgreSQL-wire URL names no product: `postgres://` can
reach a server that is not PostgreSQL, and that target is decided once the
server has named itself — after the connection, still before the plan. Remove
the flag to apply unlocked, which is what such a target does anyway.
`--dry-run` is refused on the same targets: a dry run against an engine that
does lock acquires the lock and waits out the timeout, so the flag means the
same thing there.

`PTAH_LOCK_TIMEOUT` fills the same flag, and a value that arrives that way
writes a note to standard error and applies unlocked instead of refusing. The
variable is shared: `ptah migrations up` and `ptah migrations down` read it as
their own `--lock-timeout`, which is the per-migration statement lock timeout,
so exporting it for a versioned workflow configures nothing about this apply.
Type the flag to get the refusal.

The compatibility surface takes the other side: `ptah-compat schema apply`
accepts the flag, writes a note to standard error, and applies unlocked. That
carries no claim about the Atlas CLI — what it does with `--lock-timeout` on a
dialect that cannot lock is not measured here.

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
CONCURRENTLY` and `DROP INDEX CONCURRENTLY`, a statement that uses an enum
value an earlier statement added to an existing type, and, on a server with
TimescaleDB, a per-chunk index build (`WITH
(timescaledb.transaction_per_chunk)`). The apply checks the plan for these
before it rehearses, asks for approval, or sends anything, and refuses it by
name. A plan that adds `archived` to an existing enum and a column
defaulting to it stops with exit code `2`:

```text illustration
error: the planned changes cannot run inside a transaction: statement 2 of 2: it uses 'archived', a value an earlier statement in the same transaction adds to the pre-existing enum type "probe_status", and a new enum value is not usable until the transaction that added it commits; rerun with --tx-mode none, which commits each statement as it runs
SQL: ALTER TABLE "messages" ADD COLUMN "archive_state" probe_status NOT NULL DEFAULT 'archived'
```

`--tx-mode none` runs each statement in its own transaction, where each is
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

## Read the result in a script

`--json` on `ptah schema plan` and `ptah schema apply` prints one JSON document
on standard output, on success and on failure. The text a person reads goes to
standard error instead: the planned statements, the confirmation prompt, and
the `error:` line. The exit codes stay the same. A caller parses standard
output and does not match any sentence on either stream.

Against the database this page has converged, a plan reports that nothing is
left to change:

```console
ptah schema plan --db-url "sqlite://app.db" --schema-file schema.sql --dry-run --json
```

Expected output on standard output:

```text
{
  "contract_version": 1,
  "outcome": "no-changes"
}
```

The plan file saved earlier was computed against the database before it
changed, so applying it again is refused, and the document names the reason:

```console exits=2
ptah schema apply --db-url "sqlite://app.db" --plan add-created-at.plan.json --auto-approve --json
```

Expected output includes, on standard output:

```text
  "outcome": "refused",
  "refusal": {
    "code": "stale-plan",
    "changed": "schema",
```

`contract_version` is the version of the document, not of Ptah. A consumer that
does not know the version it reads should refuse the document. The version rises
when a field changes meaning or leaves, and when an outcome gains a value. A new
field does not raise it, and neither does a new refusal code, because every
refusal means that nothing reached the database.

### The plan document

| Field | Meaning |
| --- | --- |
| `outcome` | `changes`, `no-changes`, `refused` or `failed` |
| `plan_digest` | SHA-256 of the plan file, as `sha256:<hex>`: the bytes `--save` and `--output` write and `--dry-run` prints without `--json` |
| `plan_path` | Where `--save` or `--output` wrote the file |
| `plan` | The plan file itself, with each statement's `sql`, `severity` and `reason`, the plan-level `destructive` flag, and the `name` and fingerprints that identify it |
| `refusal` | Why planning refused; see below |
| `error` | The message printed on standard error, for `refused` and `failed` |

`plan`, `plan_digest` and `plan_path` appear only with `changes`. `no-changes`
means the database already matches the desired schema, and no file is written.
`failed` means the plan could not be computed or saved, for a reason that has
no refusal code. The document does not depend on when it was written, so two
plans against an unchanged database print the same bytes.

### The apply document

| Field | Meaning |
| --- | --- |
| `outcome` | How the run ended; see the next table |
| `plan_name`, `plan_digest` | The `name` the `--plan` file records, and the SHA-256 of the bytes that were read |
| `statements` | The statements the run listed as its planned changes, in order |
| `refusal` | Why the apply refused; see below |
| `error` | The message printed on standard error, for `refused`, `failed` and `unknown` |

| Outcome | Meaning |
| --- | --- |
| `applied` | Every statement ran |
| `no-changes` | The database already matches the desired schema |
| `dry-run` | Nothing ran. With `--plan`, the fingerprint was verified first |
| `canceled` | The confirmation prompt was declined |
| `refused` | Nothing reached the database, for the reason `refusal` names |
| `failed` | Nothing reached the database, for a reason with no refusal code |
| `unknown` | The statements were sent and the run returned an error |

`unknown` is the outcome a caller must not retry from. The statements were
sent, and the document cannot say how far they got: a transaction may have
rolled them all back, a run without one may have left some, and a lost
connection hides even that. Read the database before deciding. A lock session
that fails after every statement committed is reported this way too, rather
than as `applied`.

### Refusal codes

| Code | Raised by | Details |
| --- | --- | --- |
| `stale-plan` | `apply --plan` | `changed` is `schema` or `rows`; `plan_fingerprint` and `database_fingerprint` are the two values compared |
| `protected-table` | `plan`, and `apply` without `--plan` | `tables` lists the fenced tables the plan would change |
| `lock-timeout` | `apply` | Another session held the schema apply lock longer than `--lock-timeout` |
| `transaction-preflight` | `apply` | A statement cannot run inside the transaction `--tx-mode` opens |
| `simulation-failed` | `apply` with `--dev-url` | The plan failed its rehearsal on the dev database |

A consumer that does not know a code still knows what `refused` means, and
reads `error` for the rest.

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

  Under `--json` the document reports it as the refusal code `stale-plan`,
  with both fingerprints; see
  [Read the result in a script](#read-the-result-in-a-script).

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
