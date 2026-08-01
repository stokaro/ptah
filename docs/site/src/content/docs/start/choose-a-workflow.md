---
title: Choose a workflow
description: Decide between versioned migrations and direct schema changes before wiring Ptah into a project.
---

Ptah changes databases in two operating models, and this page helps you pick one before you wire Ptah into a project. In the **versioned workflow**, every change is a SQL migration file that is reviewed, committed, applied in order, and recorded in a revision table. In the **direct workflow**, Ptah compares a live database with the desired schema and applies the computed difference, with approval at apply time instead of a file in your repository.

Both workflows read the same desired schema from the same schema sources: annotated Go structs, YAML, HCL, or SQL files, or an external loader. The choice decides how changes reach databases, not how you model the schema — you can switch or combine workflows without remodeling.

## How Ptah models the two workflows

### Versioned migrations

The native `ptah migrations` namespace owns this workflow. `generate` diffs the desired schema against a database and writes paired `*.up.sql` and `*.down.sql` files into the migration directory; `hash` seals the directory in the `ptah.sum` integrity file; `up` applies pending files in order and records each one in the revision table; `down` replays the committed rollback files.

```bash
ptah migrations generate \
  --root-dir ./models \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --name add_orders

ptah migrations hash --dir ./migrations

ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --verify-sum
```

The migration directory lives in your repository, so every change is code-reviewed as SQL, and every environment replays the same files in the same order. The [Quick start](../quick-start/) runs this exact loop end to end.

### Direct schema changes

Native commands cover the whole loop: `ptah db read` prints a live schema, `ptah schema compare` shows how it differs from the desired schema, `ptah schema drift` turns that difference into a check that exits non-zero when the database has diverged, and `ptah schema apply` applies the planned change directly.

```bash
ptah schema drift --root-dir ./models --db-url "$DATABASE_URL"

ptah schema apply \
  --db-url "$DATABASE_URL" \
  --schema-file schema.sql \
  --dry-run
```

With `--dry-run`, `ptah schema apply` prints the planned SQL under a `Planned schema changes:` heading and stops. Without it, the command shows the same plan and asks for approval before executing (`--auto-approve` skips the prompt). To separate review from execution, `ptah schema plan` writes a fingerprinted local plan file, and `ptah schema apply --plan <path>` runs it only while the database still matches the plan's recorded starting state.

## Consequences and tradeoffs

| Question | Versioned migrations | Direct schema changes |
| --- | --- | --- |
| What gets reviewed | Migration SQL committed to the repository | Planned SQL at apply time, or a saved plan file |
| What records history | Migration directory, `ptah.sum`, revision table | Nothing; the live database is the only state |
| How rollback works | `ptah migrations down` replays committed down files | A new diff toward the schema you want back |
| Where it fits | Shared and production databases, teams, CI gates | Prototypes, local development, single-owner databases |
| Main commands | `ptah migrations ...` | `ptah schema drift`, `ptah schema apply` |

The versioned workflow costs you a migration directory to maintain and the discipline of hashing and reviewing it. In exchange, changes are auditable, rollback is a committed file rather than an improvisation, and hashed directories verify integrity before anything touches a database, and `--verify-sum` additionally requires the sum file to exist. The direct workflow removes the file overhead and iterates fastest, but the apply-time approval is its only gate, and there is no history to replay or audit.

A common hybrid uses both: iterate with `ptah schema apply` against a disposable local database, then run `ptah migrations generate` against a database at the released state so the reviewed migration file — not the ad-hoc changes — is what reaches shared environments. In either model, `ptah schema drift` works as a pipeline guard that fails when a database no longer matches the desired schema.

## Where each workflow appears

- The versioned lifecycle — plan, generate, apply, roll back, verify — lives in [Versioned migrations](../../versioned/overview/).
- The direct workflow — inspect, compare and drift, apply — lives in [Direct schema changes](../../direct/inspect/), with direct application on [Apply directly](../../direct/apply/).
- Bringing a database that already exists into either workflow is covered in [Adopt an existing database](../adopt-an-existing-database/).
