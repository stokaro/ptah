# Ptah GitHub Action

Ptah ships a composite GitHub Action at `.github/actions/ptah`. The action
generates a migration plan for a pull request, evaluates the safety verdict,
optionally lints a migration directory, and updates one sticky pull request
comment. It also writes a dedicated `Ptah destructive-change verdict` check run
from the machine-readable safety report.

Marketplace publication as `stokaro/ptah-action@v1` is tracked separately in
[#463](https://github.com/stokaro/ptah/issues/463). Until that package is
published, use the in-repository composite action path shown below.

For local repository use:

```yaml
name: Ptah

on:
  pull_request:

permissions:
  checks: write
  contents: read
  issues: write
  pull-requests: read

jobs:
  ptah:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7

      - uses: stokaro/ptah/.github/actions/ptah@master
        with:
          dir: ./internal/models
          db-url: ${{ secrets.PTAH_DATABASE_URL }}
          dialect: postgres
          migration-dir: ./migrations
          lint: "true"
          comment: "true"
```

The action downloads the requested Ptah release binary by default. Use
`version` to pin a release tag, or `binary-path` when a workflow builds Ptah
from source before invoking the action. If a release asset is not available yet,
the installer falls back to `go install github.com/stokaro/ptah/cmd/ptah@...`
using `master` for `version: latest`.

The `checks: write` permission is needed for the destructive-change check run.
The `issues: write` permission is needed for sticky pull request comments. On
forked pull requests without write-scoped tokens, the action skips the comment
or check-run write and still completes the local Ptah validation path.

## Inputs

| Input | Default | Description |
| --- | --- | --- |
| `version` | `latest` | Ptah release tag to download. |
| `binary-path` | empty | Existing Ptah binary path. Skips release download. |
| `setup-go` | `true` | Set up the Go toolchain before running Ptah. |
| `go-version` | `1.26.5` | Go version passed to `actions/setup-go`. |
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

## Outputs

| Output | Description |
| --- | --- |
| `plan-path` | Text migration plan report. |
| `safety-path` | JSON safety report. |
| `lint-path` | JSON lint report. |
| `destructive` | `true`, `false`, or `unknown`. |

## Behavior

The action sets up Go by default, downloads the selected Ptah release unless
`binary-path` is provided, and then runs:

```bash
ptah migrations plan --report text
ptah migrations plan --report json --check-destructive
ptah migrations lint --format json
```

The text plan is always generated without `--check-destructive` so the pull
request comment still contains the SQL reviewers need to inspect. The separate
JSON safety command controls the destructive-change failure gate. When
`allow-destructive` is `false`, destructive plans fail the job after the comment
is posted. Lint failures also fail the job after the comment is posted.

The action requires a database URL. Use a disposable database for pull request
workflows. For SQLite smoke tests, `sqlite:///${{ runner.temp }}/ptah.db` is
enough; for PostgreSQL, MySQL, MariaDB, SQL Server, CockroachDB, or YugabyteDB,
start the database as a service container or provide a secret URL.
