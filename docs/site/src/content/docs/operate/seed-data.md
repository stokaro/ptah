---
title: Seed data
description: Apply one-off, environment-scoped SQL seed files with ptah seed and track them in schema_seeds.
---

Use `ptah seed` to load one-off setup rows — development fixtures, demo
accounts, initial admin users — into an environment without putting them in
migration history. You need a built `ptah` binary, a reachable database, and a
directory of seed files.

`ptah seed` is the imperative data path: it runs each matching SQL file once
and records it in the `schema_seeds` table. For lookup tables whose exact
contents Ptah should converge on every migration, use
[declarative reference data](../../versioned/reference-data/) instead.

## Name the seed files

Seed files follow the `NNN_description.env.sql` convention inside a seeds
directory (default `./seeds`):

- `NNN` is a numeric version; files apply in version order.
- `env` selects the environment: `002_demo_users.dev.sql` applies only with
  `--env dev`, and `001_countries.all.sql` applies in every environment.
- A `.sql` file in the directory that does not match the convention fails the
  run before anything is applied.

Starting state for the steps below:

```text
seeds/
  001_countries.all.sql
  002_demo_users.dev.sql
app.db
```

```sql
-- seeds/001_countries.all.sql
INSERT INTO countries (code, name) VALUES ('US', 'United States');
INSERT INTO countries (code, name) VALUES ('DE', 'Germany');
```

```sql
-- seeds/002_demo_users.dev.sql
INSERT INTO users (email, display_name) VALUES ('dev@example.com', 'Dev User');
```

## Apply seeds to an environment

```bash
ptah seed --db-url "sqlite://app.db" --env dev
```

Expected output includes:

```text
=== SEED ===
Database: sqlite://app.db
Dialect: sqlite
Seeds directory: seeds
Environment: dev

Matching seeds: 2
Applied seeds: 2
Skipped seeds: 0
Seeds completed successfully.
```

Re-running the same command is a no-op, because both files are recorded in
`schema_seeds`:

```text
Matching seeds: 2
Applied seeds: 0
Skipped seeds: 2
Database seed data is already up to date.
```

Add `--verbose` to list which files were applied or skipped, and
`--seeds-dir <path>` when the directory is not `./seeds`.

## Verify

Query the tracker table:

```bash
sqlite3 app.db "SELECT seed_path, env FROM schema_seeds ORDER BY seed_path;"
```

Expected output includes:

```text
001_countries.all.sql|dev
002_demo_users.dev.sql|dev
```

## Protect production-like environments

`--env prod` and `--env production` are refused unless `--allow-prod` is set:

```text
error: refusing to seed protected environment "prod" without --allow-prod
```

The command exits with code 2 (see [Exit codes](../../reference/exit-codes/)).
Adjust the protected set with repeatable `--protected-env` flags, and add
repeatable `--protected-table` flags to require `--allow-prod` whenever a seed
file targets a named existing table.

## Edit an applied seed

`schema_seeds` records a SHA-256 checksum of each file's bytes alongside its
path, and the next run reads it. A seed file that changed after it was applied
is refused rather than reported as skipped:

```text
error: error applying seeds: seed 001_countries.all.sql changed after it was
applied: recorded checksum 85d0..., current 5bbc...; add a new seed file with
the change, or pass --force to re-apply this one
```

The command prints that on one line, and the two checksums are full SHA-256 hex
digests; both are wrapped and elided here.

Adding a new seed file is the normal answer, for the same reason it is with
migrations: the rows the old file wrote are already in the database, and the new
file says what changes about them. `--force` re-applies the edited file and
records its new checksum.

## Re-run seeds

- `--force` re-runs seeds that are already recorded in `schema_seeds`, and is
  what gets past the checksum refusal above. Plain `INSERT` statements then hit
  duplicate-key errors on tables with primary or unique keys.
- `--idempotent` treats a duplicate-key conflict as already-applied data,
  using a per-file savepoint, so `--force --idempotent` re-runs cleanly over
  existing rows.

## Limitations

- Seed files are plain SQL applied once per environment; there are no down
  files and no rollback command.
- Seeds are outside migration history: `ptah migrations hash`, `validate`, and
  the revision table do not cover them.

## Next steps

- Ptah should own a table's exact contents:
  [Reference data](../../versioned/reference-data/).
- Wiring seeds into a scripted environment setup:
  [Native commands](../../reference/native-commands/).
- A seed run failed: [Troubleshooting](../troubleshooting/).
