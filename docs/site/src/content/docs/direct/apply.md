---
title: Apply directly
description: Apply desired-schema changes straight to a database with the Atlas-compatible schema apply, saved plan files, and hybrid patterns.
---

Native Ptah has no direct apply verb: the `ptah <verb>` tree deliberately stops
at inspecting, comparing, drift-checking, and planning, and changes databases
only through versioned migration files. Direct application ships on the
Atlas-compatible surface — `ptah atlas schema apply`, in the same binary (the
`ptah-compat` drop-in exposes it to scripts that expect an Atlas-style
executable). This page covers that command, the saved plan files that separate
review from execution, and the hybrid patterns that combine a native drift
gate with a direct apply.

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

## Preview the plan

`--dry-run` prints the planned SQL and stops:

```bash
ptah atlas schema apply \
  --url "sqlite://$PWD/app.db" \
  --to file://schema.sql \
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
ptah atlas schema apply \
  --url "sqlite://$PWD/app.db" \
  --to file://schema.sql
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
point. `ptah atlas schema plan` computes the same plan and saves it as a local
JSON file instead, so the SQL can be reviewed — or code-reviewed — before
anything runs:

```bash
ptah atlas schema plan \
  --from "sqlite://$PWD/app.db" \
  --to file://schema.sql \
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

`ptah atlas schema apply --plan` executes exactly the reviewed statements,
after verifying that the database still matches the plan's starting
fingerprint:

```bash
ptah atlas schema apply \
  --url "sqlite://$PWD/app.db" \
  --plan file://add-created-at.plan.json \
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
ptah atlas schema apply \
  --url "sqlite://$PWD/app.db" \
  --to file://schema.sql \
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
  changes go through `ptah atlas schema apply` with a saved plan file as the
  review artifact. [Compare and drift](../compare-and-drift/) covers the gate.
- **Iterate directly, ship versioned.** Prototype against a disposable local
  database with `ptah atlas schema apply`, then run `ptah migrations generate`
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

- `--to` accepts local `file://` schema files (HCL, YAML, or SQL); database
  URLs and migration directories as the desired schema are explicit gaps, and
  registry `atlas://` plan URLs are rejected.
- The full flag surface, `--env` project-config support, and transaction modes
  are documented in
  [Atlas schema commands](../../atlas/schema-commands/#apply-a-desired-schema).

## Next steps

- Not sure direct changes fit your project:
  [Choose a workflow](../../start/choose-a-workflow/).
- Ship the change to shared environments as a reviewed file:
  [Generate migrations](../../versioned/generate/).
- See the whole Atlas-compatible surface:
  [Atlas compatibility overview](../../atlas/overview/).
