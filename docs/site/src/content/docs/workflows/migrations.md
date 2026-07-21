---
title: Migrations
description: Plan, generate, apply, roll back, hash, validate, and inspect Ptah migrations.
---

Ptah migrations are the operational boundary between desired schema and live database state.

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

## Status

```bash
ptah migrations status \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --json
```

Set `--exit-code` in CI when pending migrations should fail the job.

## Atlas-style directories

Ptah can read Ptah split files and supported Atlas-style migration directories. Use `--dir-format atlas` when auto-detection should not guess:

```bash
ptah migrations validate --dir ./migrations --dir-format atlas
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir ./migrations --dir-format atlas
```
