---
title: Test migrations and schemas
description: Assert migration and schema behavior with declarative test cases against a throwaway database.
---

Ptah can run declarative test cases that apply your migrations or desired schema
to a throwaway database, load fixtures, run SQL, and assert on the result — so
migration and schema behavior is exercised in CI without a bespoke harness.

Two commands share one test-case format:

- `ptah migrations test` applies a migration directory (to a version, or all the
  way up) and asserts against the migrated database.
- `ptah schema test` applies a desired schema — rendered from Go annotations —
  once, then asserts against it.

Atlas keeps `migrate test` and `schema test` in its proprietary Pro build (an
Atlas account and the closed-source binary). Ptah provides both as MIT, local,
no-account, embeddable capabilities. Scripts written against the Atlas Pro CLI
can run them through the Atlas-compatible surface too: `ptah atlas migrate
test` and `ptah atlas schema test` (also via the `ptah-compat` binary) forward
to the native runners with Atlas-shaped flags (`--dir` / `-u --url`,
`--dev-url`, `--run`, `--config/--env/--var`) and the native exit codes, with
Ptah-native YAML test files as the executable payload — see
[Atlas compatibility overview](../../atlas/overview/).

## Test-case format

A test file is a YAML document with a top-level `cases:` list. Each case is a
named, ordered list of steps, and each step performs exactly one action:

- `migrate_to` — migrate the database to a target version: a non-negative
  integer, `latest` (migrate up to the newest migration), or `0` (roll
  everything back). Valid in migration tests only.
- `apply_schema` — apply the Go-annotation desired schema under `--root-dir`.
  Write `apply_schema: true`. Schema tests already converge this schema before a
  case; the step rechecks live state and repairs supported drift.
- `exec` — run raw SQL against the database.
- `seed` — apply environment-scoped SQL seed files from a directory (the seeder's
  `NNN_description.env.sql` convention: files matching `env` plus `.all.sql`).
  Set `dir` on the step, or provide a shared default with `--seed-dir`.
- `assert` — run a query and check exactly one condition: `row_count`, `scalar`
  (the first column of the first row, compared as text), or `error_contains`
  (the query is expected to fail with a message containing the substring).

```yaml
cases:
  - name: users table accepts rows
    steps:
      - migrate_to: latest
      - apply_schema: true
      - seed: { dir: ./seeds, env: test }
      - exec: INSERT INTO users (name) VALUES ('ada')
      - assert: { query: SELECT id FROM users, row_count: 2 }
      - assert: { query: SELECT name FROM users ORDER BY id LIMIT 1, scalar: seeded }
      - assert: { query: SELECT * FROM does_not_exist, error_contains: does_not_exist }
```

## Running tests

```bash
# Migration tests: apply the migrations directory, then assert.
ptah migrations test --dir ./tests --migrations-dir ./migrations --root-dir ./models --seed-dir ./seeds

# Schema tests: apply the desired schema from Go annotations, then assert.
ptah schema test --dir ./tests --root-dir ./models --seed-dir ./seeds
```

Both commands load every `*.yaml`/`*.yml` file under `--dir`, run the cases, print
a report, and exit non-zero if any case fails — so they slot straight into a CI
gate. `--run` accepts a Go regular expression and selects matching case names.
`--seed-dir` is the default directory for seed steps that omit their own `dir`.
`--report` selects the output format: `text` (default), `json` (for CI tooling),
or `html`. A `migrate_to` step is rejected in a schema test (there are no
migrations), and reported as a failed step rather than silently skipped.

## Database isolation

By default — no `--db-url` — each case runs against its **own fresh ephemeral
SQLite database**, so state created by one case is never visible to another. For
schema tests the desired schema is applied to each of those fresh databases.

Pass `--db-url` to run against a specific throwaway database (for example to
exercise a real PostgreSQL or MySQL dialect). All cases then share that one
database — it is provisioned once, cases accumulate state, and keeping them
independent is the caller's responsibility. Never point `--db-url` at a real
database: tests mutate schema and data, and seed steps bypass the seeder's
protected-environment guards.

Desired-schema application is additive at object boundaries: unrelated objects
created by migrations remain in place, while declared objects are planned with
the live target's capabilities and identifier semantics. Roles and grants are
not accepted because their security effects can be cluster-scoped.

See [Database test commands](../../reference/test-cases/) for the exact flags,
step scopes, assertions, reports, and exit contract.

## Embedding

The runner is exported as `github.com/stokaro/ptah/migration/dbtest`
(`RunMigrationTest` / `RunSchemaTest`, with the `Case`/`Step`/`Assertion` model),
so migration and schema tests can be driven directly from Go — no CLI, no account,
no cloud.
