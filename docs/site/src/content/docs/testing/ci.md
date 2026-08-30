---
title: CI
description: Gate pull requests with the Ptah GitHub Action or shell checks, and read exit codes correctly.
type: how-to
audience:
  - "ci-operator"
readerQuestion: "How do I gate pull requests with the Ptah GitHub Action or shell checks, and read exit codes correctly?"
goal: "Gate a pull request on stable Ptah exit codes."
sourceOfTruth:
  - "cmd/migrationstest"
  - "cmd/schema"
  - "integration"
generated: false
searchAliases:
  - "use Ptah Action with schema file"
overlaps: []
disposition: keep
sourceMode: source-neutral
---

Run Ptah in CI to catch migration drift, destructive changes, hash mismatches,
and unsupported capabilities before merge. The source-neutral path invokes the
CLI directly and names the desired-schema source explicitly:

```bash
ptah schema validate --schema-file schema.sql --dialect postgres
ptah migrations plan \
  --schema-file schema.sql \
  --db-url "$PTAH_DATABASE_URL" \
  --report json --check-destructive
ptah schema drift \
  --schema-file schema.sql \
  --db-url "$PTAH_DATABASE_URL"
```

Replace `--schema-file schema.sql` with the exact selector for another source.
Use a disposable database for the plan and a controlled long-lived environment
for the drift gate.

## GitHub Action

Ptah ships a Marketplace GitHub Action as `stokaro/ptah-action@v1`. On a pull
request it generates a migration plan, evaluates the safety verdict, optionally
lints the migration directory, updates one sticky pull request comment, and
writes a `Ptah destructive-change verdict` check run from the machine-readable
safety report.

The current action is Go-specific: its `dir` input is passed to
`ptah migrations plan --root-dir`, and it has no schema-file or external-loader
input. Use the direct CLI form above for SQL, YAML, HCL, DBML, OCI, or external
sources. [ptah-action issue #1](https://github.com/stokaro/ptah-action/issues/1)
tracks generic desired-schema source inputs.

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

Behind the scenes the action runs:

```bash
ptah migrations plan --report text
ptah migrations plan --report json --check-destructive
ptah migrations lint --format json
```

The text plan keeps the SQL in the pull request comment for reviewers; the
separate JSON safety run drives the destructive-change gate. With
`allow-destructive: "false"` (the default), a destructive plan fails the job
after the comment is posted; lint failures also fail the job after the comment
is posted.

### Inputs

| Input | Default | Description |
| --- | --- | --- |
| `version` | `latest` | Ptah release tag to download. |
| `binary-path` | empty | Existing Ptah binary path. Skips release download. |
| `setup-go` | `true` | Set up the Go toolchain before running Ptah. |
| `go-version` | empty | Go version passed to `actions/setup-go`. Empty reads the toolchain from `go-version-file` instead. |
| `go-version-file` | `go.mod` | Go module file the toolchain is read from, relative to the calling repository. |
| `dir` | `.` | Root directory scanned for Go schema entities. |
| `db-url` | required | Target database URL used to read the current schema. |
| `dialect` | empty | Dialect passed to `ptah migrations lint`. |
| `migration-dir` | `migrations` | Migration directory passed to lint. |
| `schemas` | empty | Comma-separated database schemas to inspect. |
| `comment` | `true` | Whether to write a sticky PR comment. |
| `lint` | `true` | Whether to run `ptah migrations lint`. |
| `lint-fail-on` | `error` | Lint failure threshold: `error`, `any`, or `none`. |
| `allow-destructive` | `false` | Allows destructive plans after review. |
| `output-dir` | temporary | Directory for generated reports. |

The outputs `plan-path`, `safety-path`, `lint-path`, their captured stderr
paths, and the `destructive` verdict (`true`, `false`, or `unknown`) let later
workflow steps consume the generated reports.

### Database, permissions, and pinning

- The action requires a database URL; use a disposable database in pull
  request workflows. For SQLite smoke tests,
  `sqlite:///${{ runner.temp }}/ptah.db` is enough; for server databases,
  start a service container or provide a secret URL.
- `checks: write` is needed for the destructive-change check run;
  `issues: write` and `pull-requests: write` for the sticky comment. On forked
  pull requests without write-scoped tokens, the action skips those writes and
  still completes the local validation path.
- Pin `version` to a release tag instead of `latest`, or point `binary-path`
  at a Ptah built from source earlier in the workflow.

## Minimal shell checks

The same gate on any CI system:

```bash
ptah migrations validate --dir ./migrations
ptah migrations lint --dir ./migrations --dialect postgres
ptah schema render --schema-file schema.sql --dialect postgres >/tmp/ptah-schema.sql
```

`schema render` writes executable SQL to stdout and diagnostics to stderr. A CI
job can apply `/tmp/ptah-schema.sql` unchanged to a disposable database to test
the public command path, including cyclic foreign key ordering.

Use a disposable database for `migrations plan`, `migrations generate`, and
`migrations up` in pull requests.

## Upload lint findings to code scanning

`ptah migrations lint --format sarif` emits a SARIF 2.1.0 document that
GitHub code scanning ingests, turning findings into pull-request annotations
with stable rule identifiers:

```yaml
      - name: Lint migrations
        run: >
          ptah migrations lint --dir ./migrations --dialect postgres
          --fail-on none --format sarif > ptah-lint.sarif
      - uses: github/codeql-action/upload-sarif@v3
        with:
          sarif_file: ptah-lint.sarif
          category: ptah-lint
```

The upload step needs the `security-events: write` permission. Use
`--fail-on none` when code scanning owns the failure policy — above the
threshold the report goes to stderr and the command exits `1`, so a plain
stdout redirect would capture an empty file. Prefer
`--format github-actions` when inline annotations are wanted without the
code-scanning permission model.

## Recommended pull-request contour

| Check | Why it exists |
| --- | --- |
| `migrations validate` | Fails when committed migration files and `ptah.sum` disagree. |
| `migrations lint` | Catches risky SQL before it reaches a database. |
| `schema render` | Proves the desired schema source parses and produces executable, capability-valid SQL. |
| `migrations plan` against a disposable DB | Shows the SQL Ptah would apply. |
| `migrations up --verify-sum --dry-run` | Exercises the apply path without changing the shared target. |
| `schema drift` | Fails when a long-lived environment diverged from the desired schema. |
| `schema validate --dialect <each target>` | Fails on a structural problem in the desired schema, with no database at all. |
| `schema fmt --check` | Fails when an HCL schema file is not canonically formatted. |
| `schema security --fail-on any` | Fails on a privilege, owner or role finding. Without `--fail-on any` the check reports and never fails, because no rule is error-severity. |

[Validate and format schema files](../../schema/validate-and-format/) and
[Report schema security findings](../../schema/security/) cover the last three.

For live checks, prefer throwaway databases or service containers. Do not point
a pull-request job at a production database.

## Exit behavior

See [Exit codes](../../reference/exit-codes/) before using Ptah as a gate. For
native Ptah commands, `0` means success, `1` is reserved for command-specific
negative check results such as drift, lint findings, pending migrations with
`--exit-code`, or migration hash drift, and `2` means a usage, parse,
connection, unsupported-dialect, or other command failure. Atlas-compatible
surfaces use `1` for both negative results and command failures to match Atlas
CE. A recovered internal panic remains exit `2` on either surface, so scripts
should interpret the code according to the selected CLI surface.

## Keep CI deterministic

- Pin the Ptah version used by CI.
- Commit migration files and `ptah.sum` together.
- Keep database URLs in secrets.
- Run Atlas-compatible scripts through `ptah-compat`, renamed or symlinked as
  `atlas` when preserving existing Atlas scripts.
- Link CI failures to [Troubleshooting](../../operate/troubleshooting/) so users
  have recovery steps.

## Next steps

- Assert behavior rather than safety:
  [Test migrations and schemas](../migrations-and-schema/).
- Understand the gates the contour relies on:
  [Integrity and safety](../../versioned/integrity-and-safety/) and
  [Lint and gate unsafe SQL](../../versioned/lint/).
- Add a drift gate for long-lived environments:
  [Compare and drift](../../direct/compare-and-drift/).
