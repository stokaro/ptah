---
title: Choose a workflow
description: Decide between versioned migrations and direct schema changes for schema work. Persistent inference state uses a separate lifecycle.
---

For schema changes, wiring Ptah into a project answers two questions, and they
are independent.

Persistent inference state follows a separate generation lifecycle. Start with
[Inference migrations](../../inference/overview/).

**Where does the change come from?** Either you write the migration yourself —
`ptah migrations create`, then the SQL — or you describe the schema you want and
Ptah works out the difference. The first needs no schema source at all; the
second reads one from annotated Go structs, YAML, HCL, or SQL files, or an
external loader.

**How does it reach the database?** Either as versioned migration files that are
reviewed, committed, applied in order and recorded in a revision table, or as a
direct apply that computes the difference and executes it after approval.

|  | Versioned files | Direct apply |
| --- | --- | --- |
| **You write the migration** | `migrations create`, write the SQL, `hash`, `up` — [Quick start: versioned migrations](../quick-start-migrations/) | — |
| **Ptah derives it from a desired schema** | `migrations generate`, then the same `hash` and `up` | `schema apply` |

The empty cell is not an omission: a direct apply computes a difference, and
with no description of the schema you want there is nothing to compute one
against. Everything else combines, and neither row is a reduced form of the
other — a project can run for years on migrations it writes by hand and use
every verb below except `generate` and `plan`.

## How Ptah models the two workflows

### Versioned migrations

The native `ptah migrations` namespace owns this workflow, and only two of its
verbs read a desired schema. `create` scaffolds an empty `*.up.sql`/`*.down.sql`
pair for you to fill in; `generate` writes the same pair from the difference
between a desired schema and a database. From there the lifecycle is identical:
`hash` seals the directory in the `ptah.sum` integrity file, `up` applies
pending files in order and records each one in the revision table, `down`
replays the committed rollback files, and `status`, `validate`, `lint`,
`import`, `checkpoint` and the history verbs never ask what the schema should
look like.

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

The migration directory lives in your repository, so every change is
code-reviewed as SQL, and every environment replays the same files in the same
order. [Quick start: versioned migrations](../quick-start-migrations/) runs the
hand-written half of this loop end to end.

### Direct schema changes

If you arrived looking for a "declarative" workflow, this is it. Ptah does not use that word as the name, because `ptah migrations generate` reads the same desired schema and is not in this group: "declarative" names where a change came from, not how it lands. [Direct schema changes](../../direct/overview/) sets out the distinction.

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
| Whether a desired schema is required | No, except for `generate` and `plan` | Yes |
| What gets reviewed | Migration SQL committed to the repository | Planned SQL at apply time, or a saved plan file |
| What records history | Migration directory, `ptah.sum`, revision table | Nothing; the live database is the only state |
| How rollback works | `ptah migrations down` replays committed down files | A new diff toward the schema you want back |
| Where it fits | Shared and production databases, teams, CI gates | Prototypes, local development, single-owner databases |
| Main commands | `ptah migrations ...` | `ptah schema drift`, `ptah schema apply` |

The versioned workflow costs you a migration directory to maintain and the discipline of hashing and reviewing it. In exchange, changes are auditable, rollback is a committed file rather than an improvisation, and hashed directories verify integrity before anything touches a database, and `--verify-sum` additionally requires the sum file to exist. The direct workflow removes the file overhead and iterates fastest, but the apply-time approval is its only gate, and there is no history to replay or audit.

A common hybrid uses both: iterate with `ptah schema apply` against a disposable local database, then run `ptah migrations generate` against a database at the released state so the reviewed migration file — not the ad-hoc changes — is what reaches shared environments. In either model, `ptah schema drift` works as a pipeline guard that fails when a database no longer matches the desired schema.

## Where each workflow appears

- The versioned lifecycle — plan, generate, apply, roll back, verify — lives in [Versioned migrations](../../versioned/overview/).
- The direct workflow — inspect, compare and drift, apply — lives in [Direct schema changes](../../direct/overview/), with direct application on [Apply directly](../../direct/apply/).
- Bringing a database that already exists into either workflow is covered in [Adopt an existing database](../adopt-an-existing-database/).
