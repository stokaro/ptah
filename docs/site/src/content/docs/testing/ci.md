---
title: CI
description: Gate pull requests with the Ptah GitHub Action or shell checks, and read exit codes correctly.
type: how-to
audience:
  - "ci-operator"
readerQuestion: "How do I gate pull requests with the Ptah GitHub Action or shell checks, and read exit codes correctly?"
goal: "Gate a pull request on stable Ptah exit codes."
sourceOfTruth:
  - "internal/cli/migrationstest"
  - "internal/cli/schema"
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

The action takes the same desired-schema sources the CLI does. `dir` selects
Go annotation roots, `schema-file` selects SQL, YAML, HCL, DBML or `oci://`
sources, and `schema-cmd` with `schema-format` selects an external loader. Each
one forwards to the flag of the same meaning, and `dir` and `schema-file` take
one value per line and compose.

A run that selects no Go source installs no Go toolchain, even with the default
`setup-go: true`.

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

### What the Action posts

The comment is assembled from the three commands above. On a pull request that
adds one table it reads:

````markdown
## Ptah migration plan

Safety: safe.
Plan command: exit 0.
Safety command: exit 0.
Lint command: exit 0.

<details><summary>Migration SQL and safety text</summary>

```sql
Generating migration from ./models to database sqlite://app.db
=== GENERATE MIGRATION SQL ===

Safety classification:
  #  severity      subject                  reason
  1  safe         *ast.CreateTableNode     does not remove data or tighten constraints
=== MIGRATION SQL ===

-- Migration generated from schema differences
-- Generated on: now
-- Source: ./models
-- Target: sqlite://app.db

CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL
);

Generated 1 migration statements.
⚠️  Review the SQL carefully before executing!

```

</details>

<details><summary>Safety JSON</summary>

```json
{
  "highest": "safe",
  "destructive": false,
  "assessments": [
    {
      "index": 1,
      "node_type": "*ast.CreateTableNode",
      "statement": "CREATE TABLE \"users\" (\n  \"id\" INTEGER PRIMARY KEY,\n  \"email\" TEXT NOT NULL\n)",
      "severity": "safe",
      "reason": "does not remove data or tighten constraints"
    }
  ]
}

```

</details>

<details><summary>Lint JSON (0 finding(s))</summary>

```json
{
  "failed": false,
  "failure_threshold": "error",
  "dialect": "sqlite",
  "dir": "./migrations",
  "findings": []
}

```

</details>
````

The summary lines are the review surface; the collapsed sections carry the
evidence. A reviewer who only reads the four lines still learns the safety
verdict and whether any command failed.

The check run beside it, `Ptah destructive-change verdict`, carries the same
facts in the form a branch protection rule can require:

```text
Ptah migration safety: safe

Safety verdict: safe.
Destructive changes: no.
Plan command exit code: 0.
Safety command exit code: 0.
Lint command exit code: 0.
Lint findings: 0.
```

Its conclusion is `failure` when any of the three commands exited non-zero, and
when the plan is destructive and `allow-destructive` is not set.

To reproduce the comment body without a pull request, run the three commands
into files and call the Action's own assembler, which is exported for this:

```bash
node -e '
  const { buildComment } = require("./.github/actions/ptah/comment.js");
  console.log(buildComment());
'
```

with `PTAH_PLAN_PATH`, `PTAH_SAFETY_PATH`, `PTAH_LINT_PATH`, their `_ERROR_`
counterparts, and the three `_EXIT_CODE` variables pointing at that run.

### Inputs

| Input | Default | Description |
| --- | --- | --- |
| `version` | `latest` | Ptah release tag to download. |
| `binary-path` | empty | Existing Ptah binary path. Skips release download. |
| `setup-go` | `true` | Set up the Go toolchain before running Ptah. |
| `go-version` | empty | Go version passed to `actions/setup-go`. Empty reads the toolchain from `go-version-file` instead. |
| `go-version-file` | `go.mod` | Go module file the toolchain is read from, relative to the calling repository. |
| `dir` | empty | Root directory scanned for Go schema entities, repeatable one per line. Empty selects no Go source unless nothing else is selected, in which case it falls back to the current directory. |
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

## Any CI platform

The Action is GitHub-shaped, and the parts of it that are not are the two
machine-readable outputs it renders. Both are available to any platform:

| Output | What it answers |
| --- | --- |
| `ptah migrations plan --report json --check-destructive` | Whether the plan is destructive, as a document rather than an exit code |
| `ptah migrations lint --format json` | Every finding, with rule, severity, file and line |

`--format sarif`, `--format github-actions` and `--format gitlab` are the same
findings in a form a particular platform ingests. A platform with no format of
its own reads the JSON and renders it however it reports.

Ptah ships a container image, so a platform with no marketplace entry needs no
installation step. Its entry point is `ptah`, so the image takes the arguments
directly:

```bash
docker run --rm -v "$PWD:/src" -w /src \
  ghcr.io/stokaro/ptah:v0.4.0 \
  migrations lint --dir ./migrations --dialect postgres --format json
```

Pin the image by digest in a pipeline that must be reproducible, the same way
the [deployment path](../../operate/deliver/) pins a migration artifact.

### GitLab CI

`--format gitlab` is a Code Quality artifact GitLab renders on the merge
request. The job is [below](#report-findings-on-a-gitlab-merge-request).

### Azure DevOps

Azure has no findings format of its own here. Publish the SARIF as a build
artifact and let the exit code fail the step:

```yaml
- script: |
    ptah migrations lint --dir ./migrations --dialect postgres --format sarif > ptah-lint.sarif
  displayName: Lint migrations
- task: PublishBuildArtifacts@1
  inputs:
    pathToPublish: ptah-lint.sarif
    artifactName: ptah-lint
```

### CircleCI

```yaml
- run:
    name: Lint migrations
    command: ptah migrations lint --dir ./migrations --dialect postgres --format json > ptah-lint.json
- store_artifacts:
    path: ptah-lint.json
```

### Bitbucket Pipelines

```yaml
- step:
    name: Lint migrations
    script:
      - ptah migrations lint --dir ./migrations --dialect postgres --format json | tee ptah-lint.json
    artifacts:
      - ptah-lint.json
```

What none of these get is the sticky comment and the check run. Those are
rendered by `comment.js` and `check-run.js` in the Action and have no
equivalent elsewhere yet.

## Minimal shell checks

The same gate on any CI system, without a container:

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

## Report findings on a GitLab merge request

`ptah migrations lint --format gitlab` emits a GitLab Code Quality report.
GitLab reads it as a `codequality` artifact and annotates the changed lines of
a merge request:

```yaml
ptah-lint:
  image: alpine:3
  script:
    - ptah migrations lint --dir ./migrations --dialect postgres
        --fail-on none --format gitlab > gl-code-quality-report.json
  artifacts:
    reports:
      codequality: gl-code-quality-report.json
```

Ptah's three severities map onto GitLab's scale as `error` to `major`,
`warning` to `minor`, and `info` to `info`. A file-level finding carries no
line, and GitLab requires one, so those anchor to the first line of the file
they name.

Use `--fail-on none` when the report owns the outcome, for the same reason as
the SARIF step above: above the threshold the findings go to stderr and the
command exits `1`, so a plain stdout redirect would capture an empty file.

A run that fails before it can lint is reported as one `blocker` entry rather
than as an empty report, because an empty Code Quality artifact is
indistinguishable from a clean one on the merge request.

## Recommended pull-request checks

| Check | Why it exists |
| --- | --- |
| `migrations validate` | Fails when committed migration files and `ptah.sum` disagree. |
| `migrations lint` | Catches risky SQL before it reaches a database. |
| `schema render` | Proves the desired schema source parses and produces executable, capability-valid SQL. |
| `migrations plan` against a disposable DB | Shows the SQL Ptah would apply. |
| `migrations up --verify-sum --dry-run` | Exercises the apply path without changing the shared target. |
| `schema drift` | Fails when a long-lived environment diverged from the desired schema. |
| `schema validate --dialect <each target>` | Fails on a structural problem in the desired schema, with no database at all. |
| `schema validate --no-skipped` | Also fails when rendering for a target would drop a declared object or property. |
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
