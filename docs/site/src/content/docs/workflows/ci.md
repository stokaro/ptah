---
title: CI
description: Run Ptah checks in pull requests and release pipelines.
---

Run Ptah in CI to catch migration drift, destructive changes, hash mismatches, and unsupported capabilities before merge.

## GitHub Action

Ptah ships a GitHub Action:

```yaml
name: Ptah

on:
  pull_request:

permissions:
  checks: write
  contents: read
  issues: write
  pull-requests: write

jobs:
  ptah:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7

      - uses: stokaro/ptah-action@v1
        with:
          dir: ./internal/models
          db-url: ${{ secrets.PTAH_DATABASE_URL }}
          dialect: postgres
          migration-dir: ./migrations
          lint: "true"
          comment: "true"
```

## Minimal shell checks

```bash
ptah migrations validate --dir ./migrations
ptah migrations lint --dir ./migrations --dialect postgres
ptah schema render --root-dir ./models --dialect postgres >/tmp/ptah-schema.sql
```

Use a disposable database for `migrations plan`, `migrations generate`, and `migrations up` in pull requests.

## Recommended pull-request contour

| Check | Why it exists |
| --- | --- |
| `migrations validate` | Fails when committed migration files and `ptah.sum` disagree. |
| `migrations lint` | Catches risky SQL before it reaches a database. |
| `schema render` | Proves the desired schema source still parses. |
| `migrations plan` against a disposable DB | Shows the SQL Ptah would apply. |
| `migrations up --verify-sum --dry-run` | Exercises the apply path without changing the shared target. |

For live checks, prefer throwaway databases or service containers. Do not point a
pull-request job at a production database.

## Exit behavior

See [Exit codes](../../reference/exit-codes/) before using Ptah as a gate. `0`
means success, `1` is reserved for command-specific negative check results such
as drift, lint findings, pending migrations with `--exit-code`, or migration
hash drift. Usage errors, parse failures, connection failures, unsupported
dialects, and other command errors use `2`.

## Keep CI deterministic

- Pin the Ptah version used by CI.
- Commit migration files and `ptah.sum` together.
- Keep database URLs in secrets.
- Run Atlas-compatible scripts through `ptah atlas ...`, or use `ptah-compat`
  renamed or symlinked as `atlas` when preserving existing Atlas scripts.
- Link CI failures to [Troubleshooting](../../operate/troubleshooting/) so users
  have recovery steps.
