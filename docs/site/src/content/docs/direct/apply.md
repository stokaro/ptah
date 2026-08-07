---
title: Apply directly
description: Apply desired-schema changes straight to a database with ptah schema apply, saved plan files, and hybrid patterns.
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

The examples use a local SQLite database, `sqlite://$PWD/app.db`, whose one
`users` table matches `schema.sql` except for a `created_at` column added to
the file. Substitute your own database URL throughout.

:::caution
The apply-time approval is this workflow's only gate: there is no migration
file to review and no revision history to replay. Keep direct applies on
databases you alone own — for shared and production databases, use
[versioned migrations](../../versioned/overview/).
:::

## Native spellings

The native verbs use Ptah's own flag spellings — `--db-url` for the target
database, and `--root-dir` (Go annotations) or `--schema-file` (HCL, YAML, or
SQL files; repeatable) for the desired state, matching `schema compare` and
`migrations generate`:

```bash
ptah schema apply --db-url "sqlite://$PWD/app.db" --schema-file schema.sql --dry-run
ptah schema plan  --db-url "sqlite://$PWD/app.db" --schema-file schema.sql --output change.plan.json
ptah schema apply --db-url "sqlite://$PWD/app.db" --plan change.plan.json
```

On the Atlas-compatible surface, `--to` additionally accepts a database URL
whose live schema becomes the desired state, or an Atlas-format migration
directory replayed on the required `--dev-url` dev database. When `--dev-url`
is set, the ordered plan is rehearsed on the dev database before the target is
touched, and a failed rehearsal refuses the apply. The rehearsal runs entirely
inside the dev database, which is handed back empty afterwards — see
[Atlas schema commands](../../atlas/schema-commands/). `--lock-timeout`
bounds the session advisory lock that serializes concurrent applies,
`--tx-mode` selects the transaction mode, `--edit` opens the planned SQL in
`$VISUAL`/`$EDITOR`, and `--schemas`, `--include`, and `--exclude` scope both
comparison sides.

## Preview the plan

`--dry-run` prints the planned SQL and stops:

```bash
ptah schema apply \
  --db-url "sqlite://$PWD/app.db" \
  --schema-file schema.sql \
  --dry-run
```

Expected output includes:

```text
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
```

## Apply with approval

Without `--dry-run`, the command shows the same plan and asks for confirmation
before executing; anything other than `YES` cancels:

```bash
ptah schema apply \
  --db-url "sqlite://$PWD/app.db" \
  --schema-file schema.sql
```

Expected output includes:

```text
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Apply these schema changes? Type 'YES' to confirm: Schema apply canceled.
```

`--auto-approve` skips the prompt for scripted runs:

```text
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Auto-approval enabled; applying schema changes.
Schema apply completed successfully.
```

`--edit` opens the planned SQL in `$VISUAL`/`$EDITOR` before approval, and the
edited SQL is what gets applied.

## Separate review from execution with a plan file

Approving whatever the tool plans at execution time is the workflow's weakest
point. `ptah schema plan` computes the same plan and saves it as a local JSON
file instead, so the SQL can be reviewed — or code-reviewed — before anything
runs:

```bash
ptah schema plan \
  --db-url "sqlite://$PWD/app.db" \
  --schema-file schema.sql \
  --output add-created-at.plan.json
```

Expected output includes:

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

```bash
ptah schema apply \
  --db-url "sqlite://$PWD/app.db" \
  --plan add-created-at.plan.json \
  --auto-approve
```

Expected output includes:

```text
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Auto-approval enabled; applying schema changes.
Schema apply completed successfully.
```

## Verification

After an apply, rerunning the dry run confirms nothing is left to change:

```bash
ptah schema apply \
  --db-url "sqlite://$PWD/app.db" \
  --schema-file schema.sql \
  --dry-run
```

Expected output:

```text
Schema is synced, no changes to be made.
```

When the desired schema also exists as native schema sources — Go models,
YAML, or HCL — `ptah schema drift` gives the same confirmation with
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
  computed refuses with exit code `1` instead of executing reviewed SQL
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

- `--schema-file` accepts local schema files (HCL, YAML, or SQL); database
  URLs and migration directories as the desired schema are explicit gaps of
  the native verb, and registry `atlas://` plan URLs are rejected.
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
