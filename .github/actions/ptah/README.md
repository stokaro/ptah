<!--
This file is published to stokaro/ptah-action from stokaro/ptah. Edit it at
.github/actions/ptah/README.md in that repository; a change made in the
published copy is overwritten by the next publish.
-->

# Ptah Action

Run Ptah migration planning, safety checks, lint, sticky pull request comments,
and a destructive-change Check Run from GitHub Actions.

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
```

## Inputs

| Input | Default | Description |
| --- | --- | --- |
| `version` | `latest` | Ptah release tag to download. |
| `binary-path` | empty | Existing Ptah binary path. Skips release download. |
| `setup-go` | `true` | Set up the Go toolchain before running Ptah. |
| `go-version` | empty | Go version passed to `actions/setup-go`. Empty reads the toolchain from `go-version-file` instead; set it only to override. |
| `go-version-file` | `go.mod` | Go module file the toolchain is read from, relative to the calling repository. Used when `go-version` is empty. |
| `dir` | empty | Root directories scanned for Go schema entities, one per line. |
| `schema-file` | empty | SQL, YAML, HCL, DBML, or `oci://` sources, one per line. |
| `schema-cmd` | empty | External program whose standard output is the desired schema. |
| `schema-format` | empty | Format of the `schema-cmd` output: `sql`, `hcl`, or `yaml`. |
| `db-url` | required | Target database URL used to read the current schema. |
| `dialect` | empty | Dialect passed to `ptah migrations lint`. |
| `migration-dir` | `migrations` | Migration directory passed to lint. |
| `schemas` | empty | Comma-separated database schemas to inspect. |
| `comment` | `true` | Whether to write a sticky PR comment. |
| `lint` | `true` | Whether to run `ptah migrations lint`. |
| `lint-fail-on` | `error` | Lint failure threshold: `error`, `any`, or `none`. |
| `allow-destructive` | `false` | Allows destructive plans after review. |
| `output-dir` | temporary | Directory for generated reports. |
| `generate` | `false` | Write the migration files the plan describes into `migration-dir`. |
| `generate-name` | empty | Name passed to `ptah migrations generate`. Empty lets Ptah name it. |

## Outputs

| Output | Description |
| --- | --- |
| `plan-path` | Text migration plan report. |
| `safety-path` | JSON safety report. |
| `safety-error-path` | Text stderr captured from the safety report command. |
| `lint-path` | JSON lint report. |
| `generate-exit-code` | Exit code of `ptah migrations generate`, or `0` when `generate` is off. |
| `generated-list-path` | File listing the migration files generation wrote, one per line. |
| `generated-count` | How many migration files generation wrote. |
| `lint-error-path` | Text stderr captured from the lint command. |
| `destructive` | `true`, `false`, or `unknown`. |

## Behavior

The action downloads the requested Ptah release binary by default. If a release
asset is not available yet, it falls back to:

```bash
GOPROXY=direct go install github.com/stokaro/ptah/cmd/ptah@master
```

when `version` is `latest`. Direct module fetch avoids stale Go module proxy
results for moving refs. Use `binary-path` when a workflow builds Ptah from
source before invoking the action.

The action runs:

```bash
ptah migrations plan --report text
ptah migrations plan --report json --check-destructive
ptah migrations lint --format json
```

The text plan is generated without `--check-destructive` so the pull request
comment still contains SQL for review. The separate JSON safety command drives
the destructive-change failure gate and Check Run.

## Provenance

This repository packages the composite action source maintained in
[`stokaro/ptah`](https://github.com/stokaro/ptah) under
`.github/actions/ptah`.

## Generating the migration

With `generate: true` the Action runs `ptah migrations generate` into
`migration-dir` and lists the files in the sticky comment, with their SQL, so a
reviewer reads what would be committed rather than a plan of it. It is off by
default: a job that only reviews should not write files.

The files are written into the workspace and **not committed**. What happens to
them is the workflow's decision, because the two answers have different
requirements:

```yaml
      - uses: stokaro/ptah-action@v1
        id: ptah
        with:
          dir: ./internal/models
          db-url: ${{ secrets.PTAH_DATABASE_URL }}
          generate: "true"

      # Commit them to the contributor's branch. Needs `contents: write`, and
      # does nothing useful on a pull request from a fork, where the token has
      # no write access to the head repository.
      - if: steps.ptah.outputs.generated-count != '0'
        run: |
          git add $(cat "${{ steps.ptah.outputs.generated-list-path }}")
          git -c user.name=ptah -c user.email=ptah@users.noreply.github.com \
            commit -m "chore: generate migration"
          git push
```

The alternative is to leave them uncommitted and let the author take the SQL
from the comment. That needs no write scope and works on forks, at the cost of
a manual step.

### The line generation must stay behind

`ptah migrations rebase` refuses a migration the target database has already
applied. That check reads one database, and **unapplied is not unpublished**: a
migration already pushed to an OCI registry lives in an immutable artifact, so
renumbering the local files produces a directory whose migration identities
disagree with an artifact someone may already be deploying. Re-hashing and
`--verify-sum` do not catch it, because the renumbered directory does agree
with its own integrity file.

Nothing refuses a rebase because a version was published. So generation belongs
before the publication step, and an automation that renumbers has to know where
that line is -- in practice, that a version is published once its branch merged
and the publish job ran.

## Choosing the desired schema

The Action forwards the source to Ptah rather than reinterpreting it. Each
input maps to the flag Ptah already has, so anything Ptah accepts works here:

```yaml
      # Go annotations, as before
      - uses: stokaro/ptah-action@v1
        with:
          dir: ./internal/models
          db-url: ${{ secrets.PTAH_DATABASE_URL }}

      # A SQL, YAML, HCL, DBML, or OCI source. No Go toolchain is installed.
      - uses: stokaro/ptah-action@v1
        with:
          schema-file: ./schema.sql
          db-url: ${{ secrets.PTAH_DATABASE_URL }}

      # An external program that prints the schema
      - uses: stokaro/ptah-action@v1
        with:
          schema-cmd: go run ./loader
          schema-format: sql
          db-url: ${{ secrets.PTAH_DATABASE_URL }}
```

`dir` and `schema-file` take one value per line and compose, the way repeating
`--root-dir` and `--schema-file` composes on the command line.

### Two behaviours worth knowing

**`dir` no longer defaults to `.`** A run that selected a SQL file used to scan
the working directory for Go entities as well, and silently composed a schema
nobody asked for. Selecting nothing at all still falls back to `--root-dir .`,
so an existing workflow is unaffected.

**Go is installed only when a Go source is selected.** `setup-go` still
decides, but a run whose schema is a SQL file installs no toolchain even with
the default `setup-go: true`.

**A `schema-format` with no `schema-cmd` fails with exit 2** before Ptah runs.
The format describes the output of a command that is not there, and running
anyway would silently ignore the input.
