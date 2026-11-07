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
touched, and a failed rehearsal refuses the apply. A dev database that is the
target, by its URL or by what the two servers answer about the database each
session selected, is refused before it is reset. The rehearsal runs entirely
inside the dev database, which is handed back empty afterwards — see
[Atlas schema commands](../../atlas/schema-commands/). `--lock-timeout`
bounds the session advisory lock that serializes concurrent applies,
`--tx-mode` selects the transaction mode, `--edit` opens the planned SQL in
`$VISUAL`/`$EDITOR`, and `--schemas` (or `schemas` in `ptah.yaml`),
`--include`, and `--exclude` scope both comparison sides.

A plan statement whose effect reaches past the dev database, such as a role or
a routine body, is refused before anything runs unless the dev server is the
run's own; see
[the baseline a rehearsal writes](../../concepts/database-urls-and-dev-databases/#the-baseline-a-rehearsal-writes).

A plan that holds a provider's native operations is bound to the target, so
the rehearsal plans the same desired schema again for the dev database and runs
that plan through the provider. A plan whose operations differ from the
target's refuses the apply; see
[executable providers](../../extend/executable-providers/).

Saved-plan rehearsal and post-apply verification require complete comparison
evidence. An empty diff with an unknown feature state does not establish that
the plan reached its desired schema. If verification after apply lacks that
evidence, the error states that the plan was applied and verification remains
incomplete.

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
error: --lock-timeout requested a schema apply lock, and dialect "sqlite" has none: only postgres, mysql, mariadb, sqlserver, yugabytedb take a session advisory lock. Remove --lock-timeout to apply without a lock
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

On YDB the provider takes the lock as a semaphore on the coordination node
`ptah_locks`, which the first apply that locks creates. A dry run creates no
lock node. A coordination session of its own holds the semaphore, so the server
can take it away while the apply goes on; the apply then cancels the statement
it is running, starts no other, and fails with the loss. [Locking](../../versioned/apply/#locking-and---migration-lock-timeout)
says when the server takes it.

`ptah-compat schema apply` keeps its lock on a database session of its own on
every engine that locks, and plans and applies over other connections. When the server
ends that session, as `pg_terminate_backend` or `KILL` does, the apply stops the
same way and fails with the loss, also when the session ended while the apply
waited for its confirmation.

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

The plan is written for the database the command read a moment ago. On a
PostgreSQL-family database its statements carry no `IF EXISTS` or `IF NOT
EXISTS` unless an earlier statement of the plan may already have removed the
object, and a drop carries `CASCADE` only where the plan relies on it to remove
a dependent it does not drop itself. An object the schema does not declare that
depends on a dropped one, such as a view in a schema the run leaves out, stops
the apply at the drop with exit code `2`, and the plan's transaction leaves the
database as it was:

```text illustration
error: apply schema changes: failed to execute SQL statement: SQL execution failed: ERROR: cannot drop column b of table t because other objects depend on it (SQLSTATE 2BP01)
SQL: ALTER TABLE "t" DROP COLUMN "b"
```

Change or drop the dependent, or declare it, and run the command again. No
setting selects another spelling. The MySQL-family, SQLite, SQL Server, Oracle
and ClickHouse planners keep the guards they write.

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

A plan with a statement rated above safe shows the rating beside the prompt,
by the rule [`schema plan`](../plan-and-approve/#how-a-column-change-is-rated)
records. A safe plan shows the prompt above:

```text illustration
Plan rating: destructive
Statement 1 of 1, destructive: DROP COLUMN removes existing column data
  ALTER TABLE "users" DROP COLUMN "created_at"
Apply these schema changes? Type 'YES' to confirm:
```

`--auto-approve` skips the prompt for scripted runs, and prints no rating:

```text illustration
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Auto-approval enabled; applying schema changes.
Schema apply completed successfully.
```

`--edit` opens the planned SQL in `$VISUAL`/`$EDITOR` before approval, and the
edited SQL is what gets applied. It is refused together with `--json`, whose
standard output belongs to a program rather than to an editor.

Prepared plans and saved JSON plans also retain owner transaction requirements.
Apply checks them before rehearsal, approval, or execution. A required transaction
refuses `--tx-mode none`; a forbidden transaction refuses `file` and `all`.
Editing SQL with these requirements needs a fresh owner assessment.

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

The schema selection belongs to the plan too. `ptah schema plan` records the
schemas `--schemas` or `schemas` in `ptah.yaml` names, applying the plan
verifies those schemas alone, and `ptah schema apply --plan` refuses a
`--schemas` of its own; see
[Plan and approve changes](../plan-and-approve/).

`--protected-table` belongs to the plan, not to the apply of one. `ptah schema
plan --protected-table` refuses to save a plan that would change a fenced row
set. `ptah schema apply --plan` refuses the fence, typed or set as
`PTAH_PROTECTED_TABLE`, before it reads the plan: the file records its
statements and not which declared row sets they change, so the apply has
nothing to check the fence against.
[Fencing a table off](../../versioned/reference-data/#fencing-a-table-off) has
the rule.

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

## Read the result in a script

`--json` on `ptah schema plan` and `ptah schema apply` prints one JSON document
on standard output for every run that starts, on success and on failure. The
text a person reads goes to standard error instead: the planned operations, the
confirmation prompt, and the `error:` line. The exit codes stay the same. A
caller parses standard output and does not match any sentence on either stream.
An internal error writes a document too, `failed` if nothing was sent and
`unknown` once operations were, before the command exits `2` with `error:
internal error: ...`.

A command line refused before the run starts prints no document, because no
run exists to report. An unknown flag, a flag value that does not parse
(`--json=maybe`, or a `PTAH_*` variable such as `PTAH_DRY_RUN=maybe`), flags
that cannot be combined (`--save` with `--dry-run`, `--to` with `--root-dir`)
and a positional argument each leave standard output empty, print the
`error:` line on standard error, and exit `2`. Nothing ran.

`--json` has to be typed on the command line: it reads no environment variable.
`PTAH_JSON` is what the versioned commands read, and a value exported for them
must not change what standard output means for a schema verb that never asked
for a document. `--edit` is refused under `--json` and reported as `failed`,
because the editor waits for a person and writes to the stream the document is
on.

Against the database this page has converged, a plan reports that nothing is
left to change:

```console
ptah schema plan --db-url "sqlite://app.db" --schema-file schema.sql --dry-run --json
```

Expected output on standard output:

```text
{
  "contract_version": 3,
  "command": "schema plan",
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
field does not raise it. Neither does a new refusal code, because every refusal
means that nothing reached the database, or a new error kind, which a consumer
reads as no kind.

`command` names the command that wrote the document, `schema plan` or `schema
apply`, so a consumer can refuse a document it did not ask for.

A run that fails carries `error`, an object with two fields. `message` is the
sentence printed on standard error. It can quote the database, so a consumer
that must not publish what the database said reads `kind` and `failure`. `kind`
says why the target database could not be opened or read, from a closed list:
`database_unreachable`, `authentication_failed`, `database_not_found`,
`permission_denied` or `tls_failed`. It is absent for every other failure.
[Why a command failed](../../reference/exit-codes/#why-a-command-failed)
says what each kind means and where to look.

Against a database whose password the URL gets wrong, the plan reports:

```json
{
  "contract_version": 3,
  "command": "schema plan",
  "outcome": "failed",
  "error": {
    "kind": "authentication_failed",
    "message": "connect to --db-url: failed to ping database: failed to connect to `user=app database=app`: 10.0.0.5:5432 (db.internal): failed SASL auth: FATAL: password authentication failed for user \"app\" (SQLSTATE 28P01)"
  }
}
```

### The plan document

| Field | Meaning |
| --- | --- |
| `outcome` | `changes`, `no-changes`, `refused` or `failed` |
| `plan_digest` | SHA-256 of the plan file, as `sha256:<hex>`: the bytes `--save` and `--output` write and `--dry-run` prints without `--json` |
| `plan_path` | Where `--save` or `--output` wrote the file |
| `plan` | The plan file itself, with each statement's `sql`, `severity` and `reason`, the plan-level `destructive` flag, and the `name` and fingerprints that identify it |
| `undecided` | The declared objects the comparison could not check; see [Undecided objects](#undecided-objects) |
| `command` | `schema plan` |
| `refusal` | Why planning refused; see below |
| `error` | The failure, for `refused` and `failed`: `message`, printed on standard error, and `kind` when the target database could not be opened or the account cannot see what the command reads there |
| `schema_diagnostics` | Every diagnostic a provider reported when it refused the SQL schema documents it read: `code`, `severity`, `path`, `start` and `end` (byte `offset`, `line`, `column`) and `message` |

`plan` also appears with `no-changes` when it contains notes. A saved or
previewed document includes `plan_digest`, and a saved file includes `plan_path`.
`no-changes` means no statement was planned, and a plan with notes alone can
still be saved. The database matches the desired schema unless `undecided` is
present.
`failed` means the plan could not be computed or saved, for a reason that has
no refusal code. Two plans against an unchanged database print the same bytes.

`plan` is the plan's content, not its bytes. `plan_digest` covers the bytes of
the plan file, and those are not in the document: encoding `plan` again does not
reproduce them. A caller that needs the bytes, to
compare them with `plan_digest`, to sign them with `ptah schema approve` or to
pass them to `ptah schema apply --plan`, runs `ptah schema plan --output <path>
--json` and reads the file.

### The apply document

Apply reports use contract version `4`, and plan reports contract version `3`.
Native summaries describe captured operations without exposing provider
payloads; they cannot be passed back as an executable plan. SQL and native
entries share one ordered `units` list. Plan reports keep the saved SQL plan
format.

| Field | Meaning |
| --- | --- |
| `outcome` | How the run ended; see the next table |
| `plan_name`, `plan_digest` | The `name` the `--plan` file records, and the SHA-256 of the bytes that were read. The file is read once, so with `--require-approval` these are also the bytes the signature was checked against |
| `units` | The planned operations in order. Each entry has `sql` or a `native` summary with its kind, subject, severity, reason, and access assessment |
| `annotations` | Non-executable notes with `text` and a zero-based `before` position in `units`. A position equal to the unit count is a trailing note |
| `undecided` | The declared objects the comparison could not check; see [Undecided objects](#undecided-objects). Absent with `--plan` |
| `command` | `schema apply` |
| `refusal` | Why the apply refused; see below |
| `error` | The failure, for `refused`, `failed` and `unknown`: `message`, printed on standard error, and `kind` when the target database could not be opened or the account cannot see what the command reads there |
| `failure` | [Where a run stopped](../../reference/exit-codes/#where-a-run-stopped), for `unknown`: a position in `units` and the server's codes, and no text |
| `schema_diagnostics` | As in the plan document |

| Outcome | Meaning |
| --- | --- |
| `applied` | Every operation ran |
| `no-changes` | No operation was planned. The database matches the desired schema unless `undecided` is present |
| `dry-run` | Nothing ran. With `--plan`, the fingerprint was verified first |
| `canceled` | The confirmation prompt was declined |
| `refused` | Nothing reached the database, for the reason `refusal` names |
| `failed` | Nothing reached the database, for a reason with no refusal code |
| `unknown` | The operations were dispatched and the run returned an error |

`unknown` is the outcome a caller must not retry from. The operations were
sent, and `failure` says where the run stopped, not what the database holds: a
transaction may have rolled them all back, a run without one may have left some,
and a lost connection hides even that. Read the database before deciding. A lock
session that fails after every operation committed is reported this way too,
rather than as `applied`.

Stopping before the first statement, even during `BEGIN`, is `refused` or
`failed`.

### Undecided objects

A comparison can only plan what the read of the database describes. When the
account may not read a catalog, the read leaves that kind of object out, so
nothing checks whether a declared object of that kind already exists. A MySQL
account without access to the role tables is the usual case: a role the schema
declares is withheld. No statement is planned for it, because creating it could
fail or diverge against a role that is already there.

Both documents record common-object limits in `undecided.common`, with
`kind`, `name`, `reason`, and `provenance`. Feature limits appear in
`undecided.features`, with the model `kind`, a structured `subject`, and a
`reason`. A feature limit may describe a whole table-owned namespace. Subject
components preserve catalog, schema, parent, and name separately, so a dot in a
name cannot become a qualification boundary.

The report omits `undecided` when neither collection has a limit. Standard
error names every limit and why, before the plan: one limit is its own line,
and several are one warning that names each. The field is independent of the outcome: next to
`changes` or `applied` it names state the comparison could not establish, and
next to `no-changes` it means the database is not shown to match.
A caller that decides "in sync" from the outcome reads `undecided` too. A plan
file does not record the field, so `apply --plan` does not report it; the
`schema plan` run that computed the file did.

### Refusal codes

| Code | Raised by | Details |
| --- | --- | --- |
| `stale-plan` | `apply --plan` | `changed` is `schema` or `rows`; `plan_fingerprint` and `database_fingerprint` are the two values compared |
| `protected-table` | `plan`, and `apply` without `--plan` | `tables` lists the fenced tables the plan would change |
| `lock-timeout` | `apply` | Another session held the schema apply lock longer than `--lock-timeout` |
| `transaction-preflight` | `apply` | A statement conflicts with the selected `--tx-mode` |
| `transaction-unavailable` | `apply` | The target cannot open a transaction; `--tx-mode none` runs without one |
| `simulation-failed` | `apply` with `--dev-url` | The plan failed its rehearsal on the dev database and was not sent to the target |
| `view-security-option` | `plan`, and `apply` without `--plan` | The plan would take `security_invoker` or `security_barrier` from a view whose schema does not state it. `view_options` lists each `view` and `option`, and the [PostgreSQL page](../../databases/postgresql/#a-security-option-the-declaration-leaves-out) gives the remedies |
| `dependent-object` | `plan`, and `apply` without `--plan` | The plan drops an object something else depends on. The error message names both |
| `declaration-refused` | `plan`, and `apply` without `--plan` | The target cannot host or plan what the schema states. The error message names it |

A consumer that does not know a code still knows what `refused` means, and
reads the error message for the rest.

`apply --plan` never raises `protected-table`. A fence passed to it is a usage
error, reported as `failed` with no code, because the plan was never checked
against it. A plan that `--require-approval` turns away, with no
approval or with one that does not verify, is `failed` too, and the document
names no plan, because the gate runs before the file is decoded.

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

## A whole MySQL or MariaDB server

A `--db-url` that names no MySQL or MariaDB database is the whole server. The
plan creates each database the desired schema declares and the server lacks,
changes a declared character set or collation, and drops every other user
database. When the server holds databases the file does not describe, name
the ones it does describe with `--schemas`, and the others are left alone.
`schema plan` reads the same server, and `schema apply --plan` applies its plan
there.
[Atlas schema commands](../../atlas/schema-commands/#a-whole-mysql-or-mariadb-server)
gives the rules, which both surfaces share.

`schema compare` and `schema drift` read the same server and report the changes
the plan makes, a database to create under `schemas_added`, as they do for a
declared schema on PostgreSQL, CockroachDB, YugabyteDB, Spanner and SQL Server. The versioned verbs
(`migrations plan`, `generate`, `up` and `status`) read one database and
refuse a URL that names none.

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
