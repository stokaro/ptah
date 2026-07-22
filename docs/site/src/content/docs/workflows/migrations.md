---
title: Migrations
description: Plan, generate, apply, roll back, hash, validate, and inspect Ptah migrations.
---

Ptah migrations are the operational boundary between desired schema and live
database state. Treat migration files as code: review them, hash them, lint
them, and apply them through a repeatable command.

## Recommended loop

```bash
ptah migrations plan \
  --root-dir ./models \
  --db-url "$DATABASE_URL"

ptah migrations generate \
  --root-dir ./models \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations

ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --verify-sum
```

## Manual migration files

Create an empty pair when you want to write SQL by hand:

```bash
ptah migrations create add_accounts --migrations-dir ./migrations
```

Then edit the generated `*.up.sql` and `*.down.sql` files. Keep the rollback
real even if the first consumer only applies migrations forward; `down` support
is part of Ptah's migration contract.

## Rollback

Rollback requires an explicit target and confirmation:

```bash
ptah migrations down \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --target 5 \
  --confirm
```

Use `--dry-run` before changing shared environments:

```bash
ptah migrations down \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --target 5 \
  --dry-run
```

## Integrity

Commit `ptah.sum` with the migration files:

```bash
ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
```

Use `--verify-sum` on `migrations up` to block out-of-band migration edits.

## Safety gates

Use dry-run and lint before applying to shared environments:

```bash
ptah migrations lint --dir ./migrations --dialect postgres
ptah migrations lint --dir ./migrations --latest 1
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --verify-sum \
  --dry-run
```

Destructive statements require explicit policy. Use `--allow-destructive` only
after the plan has been reviewed and the rollback path is understood.

## Status

```bash
ptah migrations status \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --json
```

Set `--exit-code` in CI when pending migrations should fail the job.

## Atlas-style directories

Ptah can read Ptah split files and supported Atlas-style migration directories.
Use `--dir-format atlas` when auto-detection should not guess:

```bash
ptah migrations validate --dir ./migrations --dir-format atlas
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir ./migrations --dir-format atlas
```

Add `--dev-url` to `ptah migrations validate` when CI should also replay the
migration directory on a disposable database and catch SQL execution failures:

```bash
ptah migrations validate \
  --dir ./migrations \
  --dir-format atlas \
  --dev-url "sqlite://dev.db"
```

Atlas-compatible command paths are available under `ptah atlas migrate ...`:

```bash
ptah atlas migrate hash --dir ./migrations
ptah atlas migrate apply --url "$DATABASE_URL" --dir ./migrations
```

## Operational hooks

`ptah.yaml` can configure pre-migration hooks, backup destinations, timeouts,
revision table settings, and webhooks. Use those for production-like runs
instead of relying on ad hoc wrapper scripts.

Reference: [Configuration](../../reference/configuration/).
