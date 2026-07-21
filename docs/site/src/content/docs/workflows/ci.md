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

Reference: [GitHub Action](https://github.com/stokaro/ptah/blob/master/docs/github_action.md).

## Minimal shell checks

```bash
ptah migrations validate --dir ./migrations
ptah migrations lint --dir ./migrations --dialect postgres
ptah schema render --root-dir ./models --dialect postgres >/tmp/ptah-schema.sql
```

Use a disposable database for `migrations plan`, `migrations generate`, and `migrations up` in pull requests.

## Exit behavior

See [Exit codes](../reference/exit-codes/) before using Ptah as a gate. `migrations status --exit-code` returns `1` when pending migrations exist; most other usage, parse, connection, and safety failures return `1`.
